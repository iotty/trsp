"""
workflows_common.py
-----------------------------------------------------------------------
Shared logic between the two monthly pipeline scripts:

    run_monthly_workflows.py     — runs Workflow 0-2 against a freshly
                                    uploaded month's staged files.
    refresh_helper_columns.py    — re-derives helper columns for months
                                    that already have data, using the
                                    CURRENT master data, without needing
                                    any new file upload.

Both scripts must compute helper columns (Type of Truck, SM, Sales LGS,
TMS dates, Is SA2/SA4/SB, Are blocked, Is Scrap, Vendor, Truck Vendor
Type, TRAIN/TRUCK, Last Status) identically — this module is the single
source of truth for that logic so the two scripts can't drift apart as
it keeps changing.
"""
import os
import sys
import json
import logging
from datetime import time

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
# ZSDR002 SKU-level output via the Delivery -> Delivery No join).
TMS_DATE_COLUMNS = [
    'Shipment Date', 'Request Date', 'Delivery Date', 'Tender Date', 'Accept Date',
    'Assign Date', 'Confirm Date', 'Register Date', 'Inbound Date', 'Loading Date',
    'GI Date', 'Intransit Date', 'Arrive Date', 'Unloading Date', 'Delivered Date',
    'Gr Date', 'CancelDate', 'CreateDate'
]

# Every other helper column process_data() computes, besides the merged
# TMS dates above.
BASE_HELPER_COLUMNS = [
    'Type of Truck', 'SM', 'Sales LGS', 'Is SA2,SA4,SB', 'Are blocked',
    'Is Scrap', 'Vendor', 'Truck Vendor Type', 'TRAIN/TRUCK', 'Last Status'
]

# The full set of column names process_data() can ever add — used by
# refresh_helper_columns.py to strip stale helper columns out of a
# previously-written ZSDR002 tab before recomputing, so it starts from
# just the original ZSDR002 data again. Includes the "(TMS)" collision
# variant for every TMS date column, in case that path was ever taken.
HELPER_COLUMN_NAMES = set(BASE_HELPER_COLUMNS) | set(TMS_DATE_COLUMNS) | {f'{c} (TMS)' for c in TMS_DATE_COLUMNS}


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
# Workflow 0 — TMS Data Prep (verbatim from production main.py)
# ==========================================
def preprocess_tms(df_tms):
    logger.info("Workflow 0: standardizing TMS date columns...")
    existing_cols = [c for c in TMS_DATE_COLUMNS if c in df_tms.columns]
    df_tms[existing_cols] = df_tms[existing_cols].apply(pd.to_datetime, dayfirst=True, errors='coerce')
    return df_tms


# ==========================================
# Workflow 1 — ZSDR SKU-Level (verbatim from production main.py, plus
# the full TMS-date-merge and rightmost-first Last Status logic added
# this session)
# ==========================================
def process_data(df_zsdr, df_tms, df_masters):
    logger.info("Workflow 1: processing ZSDR002 SKU-level data...")

    # Capture the original ZSDR002 column order BEFORE anything is added,
    # so it can be restored unchanged at the end — every computed helper
    # column goes to the RIGHT of this original block, never interleaved
    # into it, and the original block itself is never reordered.
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
    # Loading). If a ZSDR002 column already has that exact name
    # (unlikely, but possible), the TMS version is written under
    # "<col> (TMS)" instead of silently overwriting the ZSDR002 source
    # column.
    tms_merged_cols = []
    tms_merged_map = {}  # original TMS column name -> actual column name written into df_zsdr
    for col in TMS_DATE_COLUMNS:
        if col not in tms_map.columns:
            continue
        target_col = col if col not in df_zsdr.columns else f'{col} (TMS)'
        if target_col != col:
            logger.warning(f"ZSDR002 already has a '{col}' column — TMS version written to '{target_col}' instead of overwriting it.")
        df_zsdr[target_col] = df_zsdr['Delivery'].map(tms_map[col])
        tms_merged_cols.append(target_col)
        tms_merged_map[col] = target_col

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

    # Last Status: check every stage from the RIGHTMOST (latest, Gr Date)
    # back to the LEFTMOST (earliest, Tender Date) and use whichever is
    # the first one filled — i.e. the furthest-along stage this delivery
    # has actually reached.
    LAST_STATUS_SEQUENCE = [
        'Gr Date', 'Delivered Date', 'Unloading Date', 'Arrive Date', 'Intransit Date',
        'GI Date', 'Loading Date', 'Inbound Date', 'Register Date', 'Confirm Date',
        'Assign Date', 'Accept Date', 'Tender Date'
    ]
    conditions = []
    choices = []
    for col in LAST_STATUS_SEQUENCE:
        actual_col = tms_merged_map.get(col)
        if actual_col is None or actual_col not in df_zsdr.columns:
            continue
        conditions.append(df_zsdr[actual_col].notna())
        choices.append(col.replace(' Date', ''))

    df_zsdr['Last Status'] = np.select(conditions, choices, default='') if conditions else ''

    # Original ZSDR002 columns first, in their exact original order,
    # untouched — every computed helper column goes to the RIGHT of that
    # block.
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

    # Group ONLY by Shipment_ID — the actual shipment identifier.
    #
    # BUG FIX: the original script's group_cols was
    # ['Shipment_ID'] + cols_ref (i.e. Shipment_ID PLUS the raw SM,
    # Delivery, SD Document, and Contract Number columns all together).
    # Since Shipment_ID is itself backfilled FROM those same four
    # columns, grouping by all of them at once meant two rows only
    # collapsed into one shipment row if SM, Delivery, SD Document, AND
    # Contract Number all matched exactly — not just the shipment
    # identifier. A shipment (one SM) covering multiple deliveries
    # never actually collapsed, because Delivery differed row to row,
    # producing multiple output rows that all show the same SM instead
    # of one rolled-up row. Grouping by Shipment_ID alone, and letting
    # SM/Delivery/SD Document/Contract Number aggregate with 'first'
    # like any other non-summed column, is what the "one row per
    # shipment" rollup was actually meant to do.
    group_cols = ['Shipment_ID']

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
            # Blank dates (NaT) become '' rather than the literal string
            # 'NaT' that .dt.strftime() would otherwise produce.
            formatted = df_export[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(' 00:00:00', '')
            df_export[col] = formatted.where(df_export[col].notna(), '')
            # Prefix with an apostrophe so Sheets stores this as literal
            # text instead of re-interpreting the date/time string as an
            # actual Sheets date value (which would round-trip back
            # through the API in an ambiguous, locale-dependent format).
            # This is what lets refresh_helper_columns.py read these
            # columns back reliably with plain pd.to_datetime().
            df_export[col] = df_export[col].apply(lambda v: f"'{v}" if v else v)
        elif df_export[col].apply(lambda x: isinstance(x, time)).any():
            df_export[col] = df_export[col].astype(str).replace('NaT', '')
            # Same apostrophe trick as above — stops Sheets from
            # reinterpreting "14:23:00"-style strings as an actual Time
            # value on entry, which would come back ambiguous on re-read.
            df_export[col] = df_export[col].apply(lambda v: f"'{v}" if v and v != 'None' else '')

    df_export = df_export.fillna("")
    headers = [str(c) for c in df_export.columns]
    values = [headers] + df_export.values.tolist()

    worksheet.update(values=values, range_name='A1', value_input_option='USER_ENTERED')
    logger.info(f"'{tab_name}': wrote {len(df_export)} row(s), {len(headers)} column(s).")


def read_tab_as_df(sheet, tab_name):
    """
    Reads an existing tab back into a DataFrame, or None if it doesn't
    exist. Uses UNFORMATTED_VALUE so numbers come back as plain
    ints/floats (not comma-formatted display strings like "16,000.00"
    that would fail to re-parse) — apostrophe-prefixed text cells (see
    export_dataframe's date/time handling) come back as their plain
    string value either way, since the apostrophe is just an entry hint
    and never part of the stored value.
    """
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        return None
    records = ws.get_all_records(value_render_option='UNFORMATTED_VALUE')
    return pd.DataFrame(records)
