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
import io
import json
import logging
from datetime import time

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ==========================================
# Robust date parsing.
#
# ROOT CAUSE of the "every date shows as 1970-01-01" bug: when the
# source Excel export stores a date cell as a plain number (Excel's
# internal date serial, e.g. 45500) instead of a cell that openpyxl
# recognizes as a formatted date, pandas reads that column as int/float
# — not as a datetime64 column. Calling plain pd.to_datetime(...) on a
# numeric value with no `unit=` treats the number as nanoseconds since
# 1970-01-01, not as an Excel day-serial. 45500 nanoseconds after epoch
# is still, for all practical purposes, 1970-01-01 00:00:00 — which
# after formatting/truncating the time portion is exactly the "every
# date is 1970-01-01" symptom.
#
# This helper checks, per column (and per cell, since a column can be a
# mix of real numbers and text depending on how each row's cell was
# saved), whether the value is numeric — if so it's decoded as an Excel
# day-serial (origin 1899-12-30, pandas' standard convention which
# already accounts for Excel's leap-year bug) — otherwise it's parsed
# as a normal date string. Already-parsed datetime64 columns pass
# through untouched.
# ==========================================
_EXCEL_EPOCH = '1899-12-30'

# Excel itself only represents dates up to 9999-12-31 (serial 2958465).
# BUG FIX: pd.to_datetime(..., unit='D') converts a numeric value to a
# date by multiplying it by nanoseconds-per-day internally. If a numeric
# cell that was never actually a date sneaks into a "date" column (a
# stray ID, a misaligned column, a garbage/huge value from a corrupted
# export), that multiply can overflow and crash the whole run with
# "FloatingPointError: overflow encountered in multiply" instead of the
# errors='coerce' -> NaT that was clearly intended. Values outside
# Excel's own valid date-serial range are replaced with NaN BEFORE the
# multiply ever happens, so they cleanly become NaT (same end result
# errors='coerce' already promises) instead of crashing the pipeline.
_EXCEL_SERIAL_MIN = -100000  # generous buffer; still well before 1899
_EXCEL_SERIAL_MAX = 2958465  # Excel's own maximum date, 9999-12-31


def _to_datetime_smart(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return series

    if pd.api.types.is_numeric_dtype(series):
        safe = series.where(series.between(_EXCEL_SERIAL_MIN, _EXCEL_SERIAL_MAX))
        return pd.to_datetime(safe, unit='D', origin=_EXCEL_EPOCH, errors='coerce')

    def parse_one(v):
        if pd.isna(v):
            return pd.NaT
        if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool):
            if not (_EXCEL_SERIAL_MIN <= v <= _EXCEL_SERIAL_MAX):
                return pd.NaT
            return pd.to_datetime(v, unit='D', origin=_EXCEL_EPOCH, errors='coerce')
        return pd.to_datetime(v, dayfirst=True, errors='coerce')

    return series.apply(parse_one)

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
# Drive download — verbatim from production main.py's
# download_latest_file(), used by cur_month_pipeline.py to read the
# 'ZSDR_*'/'TMS_*' files UploadApi.gs uploads straight to the
# production staging folder (the SAME folder/naming convention your
# original pipeline.yml already reads from). Picks the most recently
# CREATED file matching the given prefix.
# ==========================================
def download_latest_file(drive_svc, folder_id, prefix, skip_rows=0):
    logger.info(f"Looking for a file starting with '{prefix}' in the staging folder...")
    query = f"'{folder_id}' in parents and name contains '{prefix}' and trashed = false"
    results = drive_svc.files().list(
        q=query, orderBy="createdTime desc", fields="files(id, name, modifiedTime)", pageSize=1
    ).execute()

    files = results.get('files', [])
    if not files:
        raise FileNotFoundError(f"No file starting with '{prefix}' found in the staging folder.")

    file_id = files[0]['id']
    file_name = files[0]['name']
    modified_time = files[0].get('modifiedTime')

    logger.info(f"Found latest file: {file_name}. Downloading...")

    bio = io.BytesIO()
    request = drive_svc.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(bio, request)

    while downloader.next_chunk()[1] is False:
        pass

    bio.seek(0)
    df = pd.read_excel(bio, engine='openpyxl', skiprows=skip_rows)
    logger.info(f"Read '{prefix}': {len(df)} row(s).")

    return df, modified_time


# ==========================================
# Workflow 0 — TMS Data Prep (verbatim from production main.py)
# ==========================================
def preprocess_tms(df_tms):
    logger.info("Workflow 0: standardizing TMS date columns...")
    existing_cols = [c for c in TMS_DATE_COLUMNS if c in df_tms.columns]
    df_tms[existing_cols] = df_tms[existing_cols].apply(_to_datetime_smart)
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

    df_zsdr[d_cols] = df_zsdr[d_cols].apply(_to_datetime_smart)
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

    # BUG FIX: `.notna()` alone only catches an actual NaN — it does NOT
    # catch an empty string ''. A freshly-uploaded file reads genuinely
    # blank cells as NaN (pandas/openpyxl), so this worked fine there.
    # But refresh_helper_columns.py reads the ZSDR002 data back out of
    # the Sheet via read_tab_as_df() (gspread get_all_records()), where
    # a blank cell comes back as '' rather than NaN — so `.notna()`
    # treated every blank 'Block' cell as "filled," collapsing this
    # column to 'Blocked' for every row after any refresh. Checking for
    # both NaN and a blank/whitespace-only string is what actually means
    # "does this cell have something in it."
    block_filled = df_zsdr['Block'].notna() & (df_zsdr['Block'].astype(str).str.strip() != '')
    df_zsdr['Are blocked'] = np.where(~mat_ok, '', np.where(block_filled, 'Blocked', 'Not Blocked'))

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
_SHIPMENT_ID_COLS = ['SM', 'Delivery', 'SD Document', 'Contract Number']


def _assign_shipment_id(df):
    """
    Derives the Shipment_ID column the same way create_shipment_report()
    always has: backfill across SM, Delivery, SD Document, Contract
    Number (in that priority order), so a shipment is identified by
    whichever of those columns is actually populated on a given row.
    Factored out so consolidate_shipment_reports() can compute the same
    Shipment_ID at the SKU level, before any shipment-level rollup
    happens, for its (Shipment_ID, Material) upsert key.
    """
    df = df.copy()
    for col in _SHIPMENT_ID_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({'': np.nan, 'nan': np.nan, 'None': np.nan})
    bfill_cols = [c for c in _SHIPMENT_ID_COLS if c in df.columns]
    if bfill_cols:
        df['Shipment_ID'] = df[bfill_cols].bfill(axis=1).iloc[:, 0].fillna('UNASSIGNED')
    else:
        df['Shipment_ID'] = 'UNASSIGNED'
    return df


def create_shipment_report(df_zsdr):
    logger.info("Workflow 2: rolling up to shipment-level...")
    df_report = _assign_shipment_id(df_zsdr)

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


# ==========================================
# Header-driven export — mirrors the original production main.py's
# export_to_sheets() (the branch used for "ZSDR_Cur_Month" etc., where
# static_headers isn't passed): reads whatever header row ALREADY
# exists in the destination tab and writes ONLY the columns that match
# that header, in that header's exact order. Any column in `df` not in
# the header is silently dropped; any header column not in `df` is
# written blank.
#
# This is the actual explanation for why the original production
# 'ZSDR_Cur_Month' tab never shows 'Material': it's not that the
# original code strips it out — it's that the tab's own header row
# (set up once, by hand, by whoever built that sheet) simply never
# included it. The header row IS the report's column selection, not
# the code.
#
# If the destination tab has no header row yet, the export is skipped
# with a warning — same as the original — so a human sets the header
# row up once by hand (exactly the columns/order wanted) and every run
# after that preserves it untouched.
# ==========================================
def export_dataframe_header_driven(worksheet, df, tab_name, header_row=1, start_col='A'):
    existing_header = worksheet.row_values(header_row)
    if not existing_header:
        logger.warning(
            f"'{tab_name}': header row {header_row} is empty — export skipped. "
            f"Set up the header row by hand (exactly the columns/order you want), then re-run."
        )
        return

    export_dict = {}
    missing_from_df = []
    for col in existing_header:
        if col in df.columns:
            export_dict[col] = df[col]
        else:
            export_dict[col] = ""
            missing_from_df.append(col)
    if missing_from_df:
        logger.warning(f"'{tab_name}': header column(s) {missing_from_df} not found in the computed data — written blank.")

    dropped_cols = [c for c in df.columns if c not in existing_header]
    if dropped_cols:
        logger.info(f"'{tab_name}': {len(dropped_cols)} computed column(s) not in the header row are not exported: {dropped_cols}")

    df_final = pd.DataFrame(export_dict)

    for col in df_final.columns:
        if pd.api.types.is_datetime64_any_dtype(df_final[col]):
            formatted = df_final[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(' 00:00:00', '')
            df_final[col] = formatted.where(df_final[col].notna(), '')
            df_final[col] = df_final[col].apply(lambda v: f"'{v}" if v else v)
        elif df_final[col].apply(lambda x: isinstance(x, time)).any():
            df_final[col] = df_final[col].astype(str).replace('NaT', '')
            df_final[col] = df_final[col].apply(lambda v: f"'{v}" if v and v != 'None' else '')

    df_final = df_final.fillna("")
    values_to_write = df_final.values.tolist()

    data_start_row = header_row + 1
    clear_str = f'{start_col}{data_start_row}:ZZ'
    worksheet.batch_clear([clear_str])

    if not values_to_write:
        logger.warning(f"'{tab_name}': no data to write.")
        return

    worksheet.update(values=values_to_write, range_name=f'{start_col}{data_start_row}', value_input_option='USER_ENTERED')
    logger.info(f"'{tab_name}': wrote {len(values_to_write)} row(s) across {len(existing_header)} header column(s) (header row preserved, not overwritten).")


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


# ==========================================
# Consolidation — final step of a refresh run. Builds a single
# shipment-level 'ZSDR002_Consolidate' tab from SKU-level data across
# EVERY month, deduped at the LINE-ITEM level.
#
# UPSERT KEY: (Shipment_ID, Material) — not (Month, Shipment_ID).
#
# BUG FIX: keying the old version by (Month, Shipment_ID) meant the same
# shipment showing up in two different months' ZSDR002 uploads (e.g. the
# same line item accidentally re-uploaded into the wrong month, or
# staged twice) produced TWO rows in the consolidated tab — one per
# month — each with the full shipment quantity, which then double-
# counted anywhere those rows got summed. A real example that surfaced
# this: SM2601300532 / MA10000842 / DA10051755 / SA20012313, quantity
# 16000, showing up once under Jan and again, identically, under Feb.
#
# The fix works at the SKU level, before any shipment rollup happens:
#   1. Read every month's SKU-level '<Mon>_ZSDR002' tab (a shipment's
#      line items CAN legitimately span more than one month, so every
#      month has to be considered, not just the one(s) just refreshed).
#   2. Compute Shipment_ID per row (same SM/Delivery/SD Document/
#      Contract Number backfill create_shipment_report() already uses).
#   3. Upsert by (Shipment_ID, Material): if the same shipment+material
#      line item appears more than once across months, keep exactly
#      ONE row. A row from a month refreshed THIS run always wins over
#      one that wasn't (it's the freshest data); if neither side was
#      refreshed this run, the later calendar month wins, so a
#      pre-existing stale duplicate still resolves deterministically
#      the next time a full (REFRESH_MONTH unset) refresh runs.
#   4. THEN group the deduped SKU-level rows by Shipment_ID and sum
#      Quantity / Quantity in MT — the same aggregation
#      create_shipment_report() does per month, just run once across
#      the whole deduped, cross-month set, so a shipment whose line
#      items are genuinely split across two different materials (or two
#      different months) still sums correctly, without a duplicate
#      upload inflating the total.
# ==========================================
CONSOLIDATE_TAB_NAME = 'ZSDR002_Consolidate'


def consolidate_shipment_reports(sheet, months_to_process):
    """
    Returns the final consolidated, shipment-level DataFrame (after the
    (Shipment_ID, Material) upsert + groupby-sum) so the caller can also
    mirror it elsewhere — see mirror_consolidate_to_production() below.
    Returns None if there was nothing to consolidate.
    """
    if not months_to_process:
        logger.warning("consolidate_shipment_reports() called with no months — skipping.")
        return None

    existing_titles = {ws.title for ws in sheet.worksheets()}
    all_months_with_data = [m for m in MONTH_ABBR if f'{m}_ZSDR002' in existing_titles]
    if not all_months_with_data:
        logger.warning("No '<Mon>_ZSDR002' tabs found — nothing to consolidate.")
        return None

    logger.info(
        f"Consolidating SKU-level data into '{CONSOLIDATE_TAB_NAME}' — "
        f"reading {', '.join(all_months_with_data)}, with {', '.join(months_to_process)} treated as freshest..."
    )

    # 1) Every month's SKU-level data, tagged with which month it came
    #    from and whether that month was just refreshed this run.
    frames = []
    for month_abbr in all_months_with_data:
        df_sku = read_tab_as_df(sheet, f'{month_abbr}_ZSDR002')
        if df_sku is None or df_sku.empty:
            continue
        df_sku = df_sku.copy()
        df_sku['Month'] = month_abbr
        df_sku['_refreshed_this_run'] = month_abbr in months_to_process
        frames.append(df_sku)

    if not frames:
        logger.warning("Every '<Mon>_ZSDR002' tab was empty — nothing to consolidate.")
        return None

    df_all_sku = pd.concat(frames, ignore_index=True, sort=False)
    df_all_sku = _assign_shipment_id(df_all_sku)

    if 'Material' not in df_all_sku.columns:
        logger.warning("'Material' column not found in ZSDR002 data — upsert key falls back to Shipment_ID alone.")
        df_all_sku['Material'] = ''
    df_all_sku['Material'] = df_all_sku['Material'].fillna('').astype(str).str.strip()

    # 2) Upsert by (Shipment_ID, Material): sort so the row that
    #    survives drop_duplicates(keep='last') is a refreshed-this-run
    #    row over a stale one, and the later calendar month on a further
    #    tie — see the header comment above for why.
    month_order = {m: i for i, m in enumerate(MONTH_ABBR)}
    df_all_sku['_sort_key'] = (
        df_all_sku['_refreshed_this_run'].astype(int) * 100
        + df_all_sku['Month'].map(month_order).fillna(-1)
    )
    df_all_sku = df_all_sku.sort_values('_sort_key', kind='stable')

    before = len(df_all_sku)
    df_deduped = df_all_sku.drop_duplicates(subset=['Shipment_ID', 'Material'], keep='last')
    dropped = before - len(df_deduped)
    if dropped:
        logger.warning(
            f"'{CONSOLIDATE_TAB_NAME}': dropped {dropped} duplicate line item(s) "
            f"sharing the same (Shipment_ID, Material) across months."
        )

    df_deduped = df_deduped.drop(columns=['_refreshed_this_run', '_sort_key'], errors='ignore')

    # 3) NOW roll up to shipment-level: group by Shipment_ID alone and
    #    sum Quantity / Quantity in MT — same aggregation as
    #    create_shipment_report(), run once across the deduped set.
    if 'Quantity' in df_deduped.columns:
        df_deduped['Quantity'] = pd.to_numeric(df_deduped['Quantity'], errors='coerce').fillna(0)
    if 'Quantity in MT' in df_deduped.columns:
        df_deduped['Quantity in MT'] = pd.to_numeric(df_deduped['Quantity in MT'], errors='coerce').fillna(0)

    group_cols = ['Shipment_ID']
    agg_dict = {}
    for col in df_deduped.columns:
        if col in group_cols: continue
        elif col in ['Quantity', 'Quantity in MT']: agg_dict[col] = 'sum'
        else: agg_dict[col] = 'first'

    df_consolidated = df_deduped.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

    lead_cols = [c for c in ['Shipment_ID', 'Month'] if c in df_consolidated.columns]
    other_cols = [c for c in df_consolidated.columns if c not in lead_cols]
    df_consolidated = df_consolidated[lead_cols + other_cols]
    df_consolidated = df_consolidated.sort_values('Shipment_ID', kind='stable').reset_index(drop=True)

    ws = get_or_create_worksheet(sheet, CONSOLIDATE_TAB_NAME)
    export_dataframe(ws, df_consolidated, CONSOLIDATE_TAB_NAME)

    logger.info(
        f"'{CONSOLIDATE_TAB_NAME}': {len(df_consolidated)} shipment(s) after deduping "
        f"{dropped} duplicate line item(s) across {len(all_months_with_data)} month(s)."
    )

    return df_consolidated


# ==========================================
# Mirrors the (already fully upserted) consolidated DataFrame into the
# PRODUCTION Sheet's 'ZSDR_Abadi' tab — filtered down to only the rows
# that pass all three of these:
#   1) 'Is SA2,SA4,SB' == 'Yes'
#   2) 'Are blocked'   == 'Not Blocked'
#   3) 'Is Scrap'      == 'NOT SCRAP'
#
# NOTE: 'ZSDR002_Consolidate' on the monthly Sheet always keeps the FULL,
# unfiltered set — this filter only applies to what gets copied into
# production's 'ZSDR_Abadi'. That's deliberate: the monthly Sheet stays
# the complete record, while production only wants the subset that's
# actually SA2/SA4/SB, not blocked, and not scrap.
#
# The write itself is HEADER-DRIVEN (export_dataframe_header_driven),
# matching how the original production pipeline's 'ZSDR_Cur_Month'
# works: 'ZSDR_Abadi' must already have a header row set up by hand
# (exactly the columns/order wanted — e.g. without 'Material', if
# that's not wanted there), and only columns matching that header get
# written; everything else computed is silently dropped. Nothing here
# ever overwrites that header row.
#
# This is otherwise a straight full-replace copy of the DATA rows, not
# a second upsert pass — by the time this runs, `df_consolidated`
# returned by consolidate_shipment_reports() is already the complete,
# correct state of every month (the upsert already happened against
# the monthly Sheet's 'ZSDR002_Consolidate' tab).
# ==========================================
PRODUCTION_MIRROR_TAB_NAME = 'ZSDR_Abadi'

PRODUCTION_MIRROR_FILTERS = {
    'Is SA2,SA4,SB': 'Yes',
    'Are blocked': 'Not Blocked',
    'Is Scrap': 'NOT SCRAP',
}


def _apply_production_mirror_filter(df):
    mask = pd.Series(True, index=df.index)
    missing_cols = []
    for col, wanted_value in PRODUCTION_MIRROR_FILTERS.items():
        if col not in df.columns:
            missing_cols.append(col)
            continue
        mask &= (df[col] == wanted_value)
    if missing_cols:
        logger.warning(
            f"Production mirror filter: column(s) {missing_cols} not found in consolidated data — "
            f"filter on those column(s) skipped (rows not excluded on that basis)."
        )
    return df[mask].reset_index(drop=True)


def mirror_consolidate_to_production(gc, production_sheet_id, df_consolidated):
    if df_consolidated is None:
        logger.warning("mirror_consolidate_to_production(): nothing to mirror — skipping.")
        return

    total_rows = len(df_consolidated)
    df_filtered = _apply_production_mirror_filter(df_consolidated) if total_rows else df_consolidated
    logger.info(
        f"Mirroring '{CONSOLIDATE_TAB_NAME}' -> production Sheet tab '{PRODUCTION_MIRROR_TAB_NAME}' "
        f"(filtered: Is SA2,SA4,SB=Yes, Are blocked=Not Blocked, Is Scrap=NOT SCRAP — "
        f"{len(df_filtered)} of {total_rows} row(s) pass)..."
    )
    df_consolidated = df_filtered

    production_sheet = gc.open_by_key(production_sheet_id)
    ws = get_or_create_worksheet(production_sheet, PRODUCTION_MIRROR_TAB_NAME)
    export_dataframe_header_driven(ws, df_consolidated, PRODUCTION_MIRROR_TAB_NAME)
