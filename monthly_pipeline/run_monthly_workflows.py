"""
run_monthly_workflows.py
-----------------------------------------------------------------------
Implements Workflow 0-2 from the existing production pipeline (main.py),
scoped to a single month's staged files instead of "whatever is latest
in the shared staging folder":

    Workflow 0 — TMS Data Prep        -> preprocess_tms()
    Workflow 1 — ZSDR SKU-Level       -> process_data()
    Workflow 2 — ZSDR Shipment-Level  -> create_shipment_report()

The transformation logic in each of those functions is copied verbatim
from the production main.py so behavior matches exactly — only the
input source (a specific month's staged files, found by month-scoped
Drive query) and the output destination (per-month tabs, auto-created,
fully replaced on every run) differ.

Output tabs in the monthly Sheet (auto-created if missing, fully
cleared + rewritten every run — "if updated, the data will replace the
old one"):
    <Mon>_TMS                  e.g. Jan_TMS             (Workflow 0)
    <Mon>_ZSDR002               e.g. Jan_ZSDR002          (Workflow 1)
    <MON>_ZSDR002_ShipLevel     e.g. JAN_ZSDR002_ShipLevel (Workflow 2)

Master data (Master_Truck_Input, Master_Days_Input, Master_Sales_LGS)
is read cross-sheet from the EXISTING PRODUCTION Sheet, not duplicated
into the monthly Sheet — masters stay maintained in one place.

Required environment variables:
    GOOGLE_CREDENTIALS         Service account JSON (shared repo secret,
                                same one the production pipeline uses).
    MONTHLY_STAGING_FOLDER_ID  Drive folder this monthly interface stages
                                files into.
    MONTHLY_TARGET_SHEET_ID    Spreadsheet ID of the NEW monthly Sheet —
                                where the <Mon>_TMS / <Mon>_ZSDR002 /
                                <MON>_ZSDR002_ShipLevel tabs live.
    TARGET_SHEET_ID             Spreadsheet ID of the EXISTING PRODUCTION
                                Sheet — where Master_Truck_Input /
                                Master_Days_Input / Master_Sales_LGS are
                                read from. This reuses the SAME secret
                                name the original pipeline.yml already
                                has configured in this repo — no new
                                secret needed.
    UPLOAD_MONTH                Month key, e.g. "Aug".

ASSUMPTION: the ZSDR002 export has the same column layout as the ZSDR
export the production pipeline already processes (Delivery, SD
Document, Material, Block, DO Means of Pack, Material Group, District
Name, Sales District, Sales PIC Name, etc.). If ZSDR002's actual columns
differ, process_data() below will need adjusting to match.
"""
import os
import io
import sys
import json
import logging
from datetime import time

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
pd.set_option('future.no_silent_downcasting', True)

MONTH_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Every TMS date column, shared between Workflow 0 (parsing them) and
# Workflow 1 (merging all of them — not just a handful — into the
# ZSDR002 SKU-level output via the Delivery -> Delivery No join). Keeping
# this as one list means both workflows always agree on the full set.
TMS_DATE_COLUMNS = [
    'Shipment Date', 'Request Date', 'Delivery Date', 'Tender Date', 'Accept Date',
    'Assign Date', 'Confirm Date', 'Register Date', 'Inbound Date', 'Loading Date',
    'GI Date', 'Intransit Date', 'Arrive Date', 'Unloading Date', 'Delivered Date',
    'Gr Date', 'CancelDate', 'CreateDate'
]


def get_env(key):
    value = os.environ.get(key)
    if not value:
        logger.error(f"Missing required environment variable: {key}")
        sys.exit(1)
    return value


def authenticate_services():
    creds_json = json.loads(get_env('GOOGLE_CREDENTIALS'))
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    drive_svc = build('drive', 'v3', credentials=creds)
    return gc, drive_svc


# ==========================================
# Master data — read from the PRODUCTION sheet, not the monthly one.
# Identical to load_master_data() in the production main.py.
# ==========================================
def load_master_data(gc, production_sheet_id):
    logger.info("Loading master data from the production Sheet...")
    df_masters = {}
    master_names = ['Master_Truck_Input', 'Master_Days_Input', 'Master_Sales_LGS']

    sheet = gc.open_by_key(production_sheet_id)
    for name in master_names:
        try:
            ws = sheet.worksheet(name)
            df_masters[name] = pd.DataFrame(ws.get_all_records())
            logger.info(f"Master [{name}] loaded ({len(df_masters[name])} rows).")
        except gspread.exceptions.WorksheetNotFound:
            logger.warning(f"Master tab [{name}] not found in production Sheet. Skipped.")
            df_masters[name] = pd.DataFrame()
    return df_masters


# ==========================================
# Download this month's staged file for a given type/prefix.
# ==========================================
def download_staged_file(drive_svc, folder_id, month_abbr, file_type, skip_rows=0):
    prefix = f"{file_type}_{month_abbr}_"
    logger.info(f"Looking for staged file with prefix '{prefix}'...")
    query = f"'{folder_id}' in parents and name contains '{prefix}' and trashed = false"
    results = drive_svc.files().list(
        q=query, orderBy="createdTime desc", fields="files(id, name)", pageSize=1
    ).execute()

    files = results.get('files', [])
    if not files:
        raise FileNotFoundError(f"No staged file found for prefix '{prefix}'. Upload it via the Apps Script interface first.")

    file_id = files[0]['id']
    file_name = files[0]['name']
    logger.info(f"Found '{file_name}'. Downloading...")

    bio = io.BytesIO()
    request = drive_svc.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(bio, request)
    while downloader.next_chunk()[1] is False:
        pass

    bio.seek(0)
    df = pd.read_excel(bio, engine='openpyxl', skiprows=skip_rows)
    logger.info(f"Read '{file_name}': {len(df)} rows.")
    return df, file_name


# ==========================================
# Workflow 0 — TMS Data Prep (verbatim from production main.py)
# ==========================================
def preprocess_tms(df_tms):
    logger.info("Workflow 0: standardizing TMS date columns...")
    existing_cols = [c for c in TMS_DATE_COLUMNS if c in df_tms.columns]
    df_tms[existing_cols] = df_tms[existing_cols].apply(pd.to_datetime, dayfirst=True, errors='coerce')
    return df_tms


# ==========================================
# Workflow 1 — ZSDR SKU-Level (verbatim from production main.py)
# ==========================================
def process_data(df_zsdr, df_tms, df_masters):
    logger.info("Workflow 1: processing ZSDR002 SKU-level data...")

    # Capture the original ZSDR002 column order BEFORE anything is added,
    # so it can be restored unchanged at the end — every computed helper
    # column (Type of Truck, SM, Sales LGS, the merged TMS dates, Is
    # SA2/SA4/SB, Are blocked, Is Scrap, Vendor, Truck Vendor Type,
    # TRAIN/TRUCK, Last Status) goes to the LEFT of this original block,
    # never interleaved into it.
    original_cols = list(df_zsdr.columns)

    d_cols = [c for c in ['Release Date', 'SO Create Date', 'DO Create Date', 'Schedule Date', 'Act. Gds Mvmnt Date'] if c in df_zsdr.columns]
    t_cols = [c for c in ['Release Time', 'SO Create Time', 'DO Create Time'] if c in df_zsdr.columns]

    df_zsdr[d_cols] = df_zsdr[d_cols].apply(pd.to_datetime, dayfirst=True, errors='coerce')
    for c in t_cols:
        df_zsdr[c] = pd.to_datetime(df_zsdr[c], format='%H:%M:%S', errors='coerce').dt.time

    df_tms['Delivery No'] = df_tms['Delivery No'].astype(str).str.strip()
    df_zsdr['Delivery'] = df_zsdr['Delivery'].astype(str).str.strip()

    tms_map = df_tms.drop_duplicates('Delivery No').set_index('Delivery No')
    truck_cat_map = df_masters.get('Master_Truck_Input', pd.DataFrame()).drop_duplicates('Vendor').set_index('Vendor')['Cat'] if not df_masters.get('Master_Truck_Input', pd.DataFrame()).empty else {}

    df_sales_master = df_masters.get('Master_Sales_LGS', pd.DataFrame())
    if not df_sales_master.empty and 'Sales' in df_sales_master.columns and 'LGS' in df_sales_master.columns:
        df_sales_master['Sales_Clean'] = df_sales_master['Sales'].astype(str).str.strip().str.upper()
        sales_lgs_map = df_sales_master.drop_duplicates('Sales_Clean').set_index('Sales_Clean')['LGS']
    else:
        sales_lgs_map = {}

    df_zsdr['Type of Truck'] = df_zsdr['Delivery'].map(tms_map['Trucktype Name'] if 'Trucktype Name' in tms_map else {}).fillna('')
    df_zsdr['SM'] = df_zsdr['Delivery'].map(tms_map['Shipment No'] if 'Shipment No' in tms_map else {}).fillna('')

    pic_clean = df_zsdr.get('Sales PIC Name', pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    pic_clean = pic_clean.replace('NAN', '')
    df_zsdr['Sales LGS'] = pic_clean.map(sales_lgs_map).fillna('Belum ada PIC')

    # Merge in EVERY TMS date column (not just Tender/Accept/Register/
    # Loading) so nothing has to be added piecemeal later. If a ZSDR002
    # column already has that exact name (unlikely, but possible), the
    # TMS version is written under "<col> (TMS)" instead of silently
    # overwriting the ZSDR002 source column. Track the actual column
    # names used so they can be moved to the very front of the sheet at
    # the end, ahead of even the original ZSDR002 columns.
    tms_merged_cols = []
    for col in TMS_DATE_COLUMNS:
        if col not in tms_map.columns:
            continue
        target_col = col if col not in df_zsdr.columns else f'{col} (TMS)'
        if target_col != col:
            logger.warning(f"ZSDR002 already has a '{col}' column — TMS version written to '{target_col}' instead of overwriting it.")
        df_zsdr[target_col] = df_zsdr['Delivery'].map(tms_map[col])
        tms_merged_cols.append(target_col)

    df_zsdr['Is SA2,SA4,SB'] = np.where(df_zsdr['SD Document'].astype(str).str.startswith(('SA2', 'SA4', 'SB')), 'Yes', 'No')

    mat_ok = df_zsdr['Material'].notna() & (df_zsdr['Material'] != '')
    df_zsdr['Are blocked'] = np.where(~mat_ok, '', np.where(df_zsdr['Block'].notna(), 'Blocked', 'Not Blocked'))

    do_pack = df_zsdr['DO Means of Pack'].fillna('').astype(str).str.lower()
    mat_group = df_zsdr['Material Group'].fillna('').astype(str).str.lower()
    is_scrap = do_pack.str.contains('scr') | mat_group.str.contains('bfr|mtbe')
    df_zsdr['Is Scrap'] = np.where((do_pack == '') & (mat_group == ''), '', np.where(is_scrap, 'SCRAP/BFR/MTBE', 'NOT SCRAP'))

    is_export = df_zsdr['District Name'].fillna('').astype(str).str.lower() == 'export'
    not_scrap = df_zsdr['Is Scrap'] == 'NOT SCRAP'
    in_tms = df_zsdr['Delivery'].isin(tms_map.index)

    mapped_carrier = df_zsdr['Delivery'].map(tms_map['Carrier Name'] if 'Carrier Name' in tms_map else {}).fillna('Not assigned yet').replace('', 'Not assigned yet')

    df_zsdr['Vendor'] = ''
    df_zsdr.loc[is_export, 'Vendor'] = 'Export'
    df_zsdr.loc[not_scrap & ~is_export & in_tms, 'Vendor'] = mapped_carrier[not_scrap & ~is_export & in_tms]

    def get_v_type(v): return '' if pd.isna(v) or v in ('', 'Not assigned yet') else truck_cat_map.get(v, 'Vendor Truck')
    df_zsdr['Truck Vendor Type'] = df_zsdr['Vendor'].map(get_v_type)

    truck_type = df_zsdr['Type of Truck'].str.lower()
    sales_dist = df_zsdr['Sales District'].fillna('').astype(str)

    df_zsdr['TRAIN/TRUCK'] = np.where(truck_type == '', '',
                                np.where(truck_type.str.contains('train'), 'TRAIN',
                                    np.where((sales_dist == 'OTHERS') & truck_type.str.contains('container'), 'VESSEL', 'TRUCK')))

    df_zsdr['Last Status'] = np.select(
        [df_zsdr['Loading Date'].notna(), df_zsdr['Register Date'].notna(), df_zsdr['Accept Date'].notna(), df_zsdr['Tender Date'].notna()],
        ['Loading', 'Register', 'Accept', 'Tender'], default=''
    )

    # Original ZSDR002 columns first, in their exact original order,
    # untouched — every computed helper column (Type of Truck, SM, Sales
    # LGS, the merged TMS dates, Is SA2/SA4/SB, Are blocked, Is Scrap,
    # Vendor, Truck Vendor Type, TRAIN/TRUCK, Last Status, in the order
    # they were computed above) goes to the RIGHT of that block.
    helper_cols = [c for c in df_zsdr.columns if c not in original_cols]
    df_zsdr = df_zsdr[original_cols + helper_cols]

    return df_zsdr


# ==========================================
# Workflow 2 — ZSDR Shipment-Level (verbatim from production main.py)
# ==========================================
def create_shipment_report(df_zsdr):
    logger.info("Workflow 2: rolling up to shipment-level...")
    df_report = df_zsdr.copy()
    cols_ref = ['SM', 'Delivery', 'SD Document', 'Contract Number']

    for col in cols_ref:
        if col in df_report.columns:
            df_report[col] = df_report[col].astype(str).str.strip().replace({'': np.nan, 'nan': np.nan, 'None': np.nan})

    bfill_cols = [c for c in cols_ref if c in df_report.columns]
    df_report['Shipment_ID'] = df_report[bfill_cols].bfill(axis=1).iloc[:, 0].fillna('UNASSIGNED')

    if 'Quantity' in df_report.columns:
        df_report['Quantity'] = pd.to_numeric(df_report['Quantity'], errors='coerce').fillna(0)
    if 'Quantity in MT' in df_report.columns:
        df_report['Quantity in MT'] = pd.to_numeric(df_report['Quantity in MT'], errors='coerce').fillna(0)

    group_cols = ['Shipment_ID'] + [c for c in cols_ref if c in df_report.columns]

    agg_dict = {}
    for col in df_report.columns:
        if col in group_cols: continue
        elif col in ['Quantity', 'Quantity in MT']: agg_dict[col] = 'sum'
        else: agg_dict[col] = 'first'

    df_grouped = df_report.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
    return df_grouped


# ==========================================
# Sheet export — auto-create tab if missing, FULLY replace its contents
# every run (clear then rewrite headers + data), since each of these
# tabs belongs entirely to this pipeline (no shared static header row
# to preserve, unlike the production pipeline's Shipped/Backlog tabs).
# ==========================================
def get_or_create_worksheet(sheet, tab_name, rows=3000, cols=60):
    try:
        return sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Tab '{tab_name}' not found — creating it.")
        return sheet.add_worksheet(title=tab_name, rows=rows, cols=cols)


def export_dataframe(worksheet, df, tab_name):
    logger.info(f"Writing {len(df)} row(s) to '{tab_name}' (full replace)...")
    worksheet.clear()

    if df.empty:
        worksheet.update(values=[['(no data for this run)']], range_name='A1')
        logger.warning(f"'{tab_name}': no data to write.")
        return

    df_export = df.copy()
    for col in df_export.columns:
        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
            df_export[col] = df_export[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(' 00:00:00', '')
        elif df_export[col].apply(lambda x: isinstance(x, time)).any():
            df_export[col] = df_export[col].astype(str)

    df_export = df_export.fillna("")
    headers = [str(c) for c in df_export.columns]
    values = [headers] + df_export.values.tolist()

    worksheet.update(values=values, range_name='A1', value_input_option='USER_ENTERED')
    logger.info(f"'{tab_name}': wrote {len(df_export)} row(s), {len(headers)} column(s).")


def main():
    month_abbr = get_env('UPLOAD_MONTH')
    if month_abbr not in MONTH_ABBR:
        logger.error(f"Unrecognized month key '{month_abbr}' — expected one of {MONTH_ABBR}.")
        sys.exit(1)

    staging_folder_id = get_env('MONTHLY_STAGING_FOLDER_ID')
    monthly_sheet_id = get_env('MONTHLY_TARGET_SHEET_ID')
    production_sheet_id = get_env('TARGET_SHEET_ID')

    logger.info(f"================ Running Workflows 0-2 for {month_abbr} ================")

    gc, drive_svc = authenticate_services()

    dict_masters = load_master_data(gc, production_sheet_id)

    df_tms_raw, tms_name = download_staged_file(drive_svc, staging_folder_id, month_abbr, 'TMS', skip_rows=1)
    df_zsdr_raw, zsdr_name = download_staged_file(drive_svc, staging_folder_id, month_abbr, 'ZSDR002', skip_rows=0)

    df_tms_clean = preprocess_tms(df_tms_raw)
    df_zsdr_sku = process_data(df_zsdr_raw, df_tms_clean, dict_masters)
    df_zsdr_shipment = create_shipment_report(df_zsdr_sku)

    monthly_sheet = gc.open_by_key(monthly_sheet_id)

    tms_tab_name = f"{month_abbr}_TMS"
    sku_tab_name = f"{month_abbr}_ZSDR002"
    ship_tab_name = f"{month_abbr.upper()}_ZSDR002_ShipLevel"

    ws_tms = get_or_create_worksheet(monthly_sheet, tms_tab_name)
    export_dataframe(ws_tms, df_tms_clean, tms_tab_name)

    ws_sku = get_or_create_worksheet(monthly_sheet, sku_tab_name)
    export_dataframe(ws_sku, df_zsdr_sku, sku_tab_name)

    ws_ship = get_or_create_worksheet(monthly_sheet, ship_tab_name)
    export_dataframe(ws_ship, df_zsdr_shipment, ship_tab_name)

    logger.info(f"================ FINISHED Workflows 0-2 for {month_abbr} ================")


if __name__ == "__main__":
    main()
