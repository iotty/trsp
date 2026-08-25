"""
zsdr_monthly_upload_pipeline.py
-----------------------------------------------------------------------
Single script for the monthly ZSDR002/TMS upload interface. Replaces
the previous three-file split (workflows_common.py /
run_monthly_workflows.py / refresh_helper_columns.py) with one file and
two entry points, selected by the first command-line argument:

    python zsdr_monthly_upload_pipeline.py upload    -> Workflows 0-2 for a single
                                             month's freshly staged
                                             ZSDR002/TMS files.
    python zsdr_monthly_upload_pipeline.py refresh   -> re-derives helper columns
                                             for month(s) that already
                                             have data (e.g. after
                                             master data changes),
                                             without needing a new file.

Both modes end the same way: they decide whether the month(s) they
touched belong on the PRODUCTION Sheet's 'ZSDR_Abadi' tab (closed
months) or 'ZSDR_Cur_Month' tab (the month still in progress, compared
against today's actual calendar date) and mirror there accordingly.
'ZSDR_Abadi' can never receive the current month's rows, even
indirectly — that exclusion is enforced centrally so it can't regress
per-caller.

UPLOAD MODE HAS NO INTERMEDIATE MONTHLY SHEET. Earlier versions of this
pipeline wrote <Mon>_TMS / <Mon>_ZSDR002 / <MON>_ZSDR002_ShipLevel tabs
into a separate "monthly Sheet" and cross-month-deduped by reading them
all back. Upload mode no longer does this at all — it goes straight
from the staged Drive files to the production Sheet: a closed month's
freshly computed shipment report is upserted directly against whatever
'ZSDR_Abadi' already has (see upsert_month_into_abadi()); the current
month's is a straight full replace of 'ZSDR_Cur_Month', same as before.
REFRESH MODE STILL USES THE MONTHLY SHEET (see its own note below) —
that's a separate, not-yet-updated concern.

Required environment variables (both modes):
    GOOGLE_CREDENTIALS         Service account JSON (shared repo secret).
    TARGET_SHEET_ID            Spreadsheet ID of the PRODUCTION Sheet —
                                where Master_Truck_Input /
                                Master_Days_Input / Master_Sales_LGS are
                                read from, and where ZSDR_Abadi /
                                ZSDR_Cur_Month get written.

Upload mode also needs:
    STAGING_FOLDER_ID           Drive folder the Apps Script interface
                                stages ZSDR002_<Mon>_... and TMS_<Mon>_...
                                files into. Same name, same value as the
                                STAGING_FOLDER_ID Script Property on the
                                Apps Script side — this is a REPO SECRET
                                storing the identical folder ID, not a
                                different folder.
    UPLOAD_MONTH                Month key, e.g. "Aug".

Refresh mode also needs (UNCHANGED / STILL MONTHLY-SHEET-BASED — see note
above process_refresh() further down):
    MONTHLY_TARGET_SHEET_ID    Spreadsheet ID of the monthly Sheet —
                                where <Mon>_TMS / <Mon>_ZSDR002 /
                                <MON>_ZSDR002_ShipLevel / ZSDR002_Consolidate
                                live. Only months that still have data
                                there (from before upload mode stopped
                                writing to it) can be refreshed.
    REFRESH_MONTH               Optional. A single month key (e.g. "Aug")
                                to refresh just that month. If unset or
                                empty, EVERY month that currently has a
                                "<Mon>_ZSDR002" tab is refreshed.

ASSUMPTION: the ZSDR002 export has the same column layout as the ZSDR
export the original production pipeline processes (Delivery, SD
Document, Material, Block, DO Means of Pack, Material Group, District
Name, Sales District, Sales PIC Name, etc.). If ZSDR002's actual columns
differ, process_data() below will need adjusting.

DATE PARSING: the raw ZSDR002/TMS exports are MONTH-FIRST (M/D/YYYY),
confirmed against a real row (a raw cell reading "7/28/2026" — day 28
can't be a month, so that can only be July 28). _to_datetime_smart()
below parses text dates with dayfirst=False accordingly. If a future
export ever switches to a genuinely day-first locale, that needs
flipping back — verify against a raw cell with day > 12 before changing
it.
"""
import io
import os
import sys
import json
import logging
from datetime import time, date as _date, datetime as _datetime

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

# The timezone the Apps Script project uses (Project Settings > Time
# zone) to decide "what's the current month" — the upload interface's
# Code.gs does the same via Session.getScriptTimeZone(). GitHub Actions
# runners default to UTC, so without this, a run near a month boundary
# could disagree with the Apps Script UI about which month is current.
# Keep this in sync with the Apps Script project's actual timezone.
PIPELINE_TIMEZONE = 'Asia/Jakarta'


def current_month_abbr(tz_name=PIPELINE_TIMEZONE):
    """3-letter month abbreviation (e.g. 'Aug') for 'today' in tz_name."""
    from zoneinfo import ZoneInfo
    now = _datetime.now(ZoneInfo(tz_name))
    return MONTH_ABBR[now.month - 1]


def get_env(key):
    value = os.environ.get(key)
    if not value:
        logger.error(f"Missing required environment variable: {key}")
        sys.exit(1)
    return value


# ==========================================
# Robust date parsing.
#
# ROOT CAUSE of the "every date shows as 1970-01-01" bug: when the
# source Excel export stores a date cell as a plain number (Excel's
# internal date serial, e.g. 45500) instead of a cell openpyxl
# recognizes as a formatted date, pandas reads that column as int/float
# — not datetime64. Plain pd.to_datetime(...) on a numeric value with no
# unit= treats the number as nanoseconds since 1970-01-01, not as an
# Excel day-serial — 45500 nanoseconds after epoch is still, for all
# practical purposes, 1970-01-01 00:00:00.
#
# This helper checks, per column (and per cell, since a column can mix
# real numbers and text depending on how each row's cell was saved),
# whether the value is numeric — if so it's decoded as an Excel
# day-serial (origin 1899-12-30) — otherwise parsed as a normal date
# string. Already-parsed datetime64 columns pass through untouched.
#
# The text-date fallback uses dayfirst=False (month-first) — see the
# module docstring for why: the raw export format is M/D/YYYY, not
# day-first.
# ==========================================
_EXCEL_EPOCH = '1899-12-30'
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
        return pd.to_datetime(v, dayfirst=False, errors='coerce')

    return series.apply(parse_one)


TMS_DATE_COLUMNS = [
    'Shipment Date', 'Request Date', 'Delivery Date', 'Tender Date', 'Accept Date',
    'Assign Date', 'Confirm Date', 'Register Date', 'Inbound Date', 'Loading Date',
    'GI Date', 'Intransit Date', 'Arrive Date', 'Unloading Date', 'Delivered Date',
    'Gr Date', 'CancelDate', 'CreateDate'
]

BASE_HELPER_COLUMNS = [
    'Type of Truck', 'SM', 'Sales LGS', 'Is SA2,SA4,SB', 'Are blocked',
    'Is Scrap', 'Vendor', 'Truck Vendor Type', 'TRAIN/TRUCK', 'Last Status'
]

# Every column name process_data() can ever add — used by refresh mode
# to strip stale helper columns out of a previously-written ZSDR002 tab
# before recomputing, so it starts from just the original ZSDR002 data.
HELPER_COLUMN_NAMES = set(BASE_HELPER_COLUMNS) | set(TMS_DATE_COLUMNS) | {f'{c} (TMS)' for c in TMS_DATE_COLUMNS}


# ==========================================
# Auth / master data / Drive download
# ==========================================
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
    return df


# ==========================================
# Workflow 0 — TMS Data Prep
# ==========================================
def preprocess_tms(df_tms):
    logger.info("Workflow 0: standardizing TMS date columns...")
    existing_cols = [c for c in TMS_DATE_COLUMNS if c in df_tms.columns]
    df_tms[existing_cols] = df_tms[existing_cols].apply(_to_datetime_smart)
    return df_tms


# ==========================================
# Workflow 1 — ZSDR SKU-Level
# ==========================================
def process_data(df_zsdr, df_tms, df_masters):
    logger.info("Workflow 1: processing ZSDR002 SKU-level data...")

    # Capture the original ZSDR002 column order BEFORE anything is
    # added, so it can be restored unchanged at the end — every computed
    # helper column goes to the RIGHT of this original block, never
    # interleaved into it, and the original block is never reordered.
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

    # Merge in EVERY TMS date column. If a ZSDR002 column already has
    # that exact name, the TMS version is written under "<col> (TMS)"
    # instead of silently overwriting the ZSDR002 source column.
    tms_merged_map = {}  # original TMS column name -> actual column name written into df_zsdr
    for col in TMS_DATE_COLUMNS:
        if col not in tms_map.columns:
            continue
        target_col = col if col not in df_zsdr.columns else f'{col} (TMS)'
        if target_col != col:
            logger.warning(f"ZSDR002 already has a '{col}' column — TMS version written to '{target_col}' instead of overwriting it.")
        df_zsdr[target_col] = df_zsdr['Delivery'].map(tms_map[col])
        tms_merged_map[col] = target_col

    df_zsdr['Is SA2,SA4,SB'] = np.where(df_zsdr['SD Document'].astype(str).str.startswith(('SA2', 'SA4', 'SB')), 'Yes', 'No')

    mat_ok = df_zsdr['Material'].notna() & (df_zsdr['Material'] != '')

    # A blank cell read back from Sheets comes back as '' rather than
    # NaN — checking for both is what actually means "has something in
    # it," so a refresh doesn't collapse every blank 'Block' cell to
    # 'Blocked'.
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

    # Last Status: check every stage from the RIGHTMOST (latest, Gr
    # Date) back to the LEFTMOST (earliest, Tender Date) and use
    # whichever is the first one filled — i.e. the furthest-along stage
    # this delivery has actually reached.
    LAST_STATUS_SEQUENCE = [
        'Gr Date', 'Delivered Date', 'Unloading Date', 'Arrive Date', 'Intransit Date',
        'GI Date', 'Loading Date', 'Inbound Date', 'Register Date', 'Confirm Date',
        'Assign Date', 'Accept Date', 'Tender Date'
    ]
    conditions, choices = [], []
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
    return df_zsdr[original_cols + helper_cols]


# ==========================================
# Workflow 2 — ZSDR Shipment-Level
# ==========================================
_SHIPMENT_ID_COLS = ['SM', 'Delivery', 'SD Document', 'Contract Number']

# The actual roll-up key. Grouping directly by these three real columns
# — not a derived, backfilled Shipment_ID — means a shipment with
# multiple materials produces one row PER material instead of collapsing
# to a single row that only shows whichever material happened to sort
# first. Quantity/Quantity in MT still sum within each (Delivery, SM,
# Material) group; every other column still takes 'first'.
GROUP_KEY_COLS = ['Delivery', 'SM', 'Material']


def _assign_shipment_id(df):
    """Backfills Shipment_ID across SM, Delivery, SD Document, Contract
    Number (in that priority order) — kept as a plain identifying/display
    column on the output (e.g. for the Audit page), but no longer the
    roll-up key itself; see GROUP_KEY_COLS above."""
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


def _normalize_group_key_cols(df):
    """Same blank-normalization _assign_shipment_id() does for its bfill
    columns, applied to GROUP_KEY_COLS so '', 'nan', 'None', and
    whitespace-only cells all group together as genuinely blank instead
    of as distinct values."""
    df = df.copy()
    for col in GROUP_KEY_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({'': np.nan, 'nan': np.nan, 'None': np.nan})
    return df


def create_shipment_report(df_zsdr):
    logger.info("Workflow 2: rolling up by Delivery / SM / Material...")
    df_report = _assign_shipment_id(df_zsdr)
    df_report = _normalize_group_key_cols(df_report)

    if 'Quantity' in df_report.columns:
        df_report['Quantity'] = pd.to_numeric(df_report['Quantity'], errors='coerce').fillna(0)
    if 'Quantity in MT' in df_report.columns:
        df_report['Quantity in MT'] = pd.to_numeric(df_report['Quantity in MT'], errors='coerce').fillna(0)

    group_cols = [c for c in GROUP_KEY_COLS if c in df_report.columns]
    agg_dict = {}
    for col in df_report.columns:
        if col in group_cols: continue
        elif col in ['Quantity', 'Quantity in MT']: agg_dict[col] = 'sum'
        else: agg_dict[col] = 'first'

    return df_report.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()


# ==========================================
# Sheet export
# ==========================================
def get_or_create_worksheet(sheet, tab_name, rows=3000, cols=60):
    try:
        return sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Tab '{tab_name}' not found — creating it.")
        return sheet.add_worksheet(title=tab_name, rows=rows, cols=cols)


def _stringify_dates_and_times(df):
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Blank dates (NaT) become '' rather than the literal string
            # 'NaT'. Apostrophe-prefixed so Sheets stores this as literal
            # text instead of reinterpreting it as its own date value
            # (which would round-trip back through the API in an
            # ambiguous, locale-dependent format) — this is what lets
            # refresh mode read these columns back reliably.
            formatted = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(' 00:00:00', '')
            df[col] = formatted.where(df[col].notna(), '')
            df[col] = df[col].apply(lambda v: f"'{v}" if v else v)
        elif df[col].apply(lambda x: isinstance(x, time)).any():
            df[col] = df[col].astype(str).replace('NaT', '')
            df[col] = df[col].apply(lambda v: f"'{v}" if v and v != 'None' else '')
    return df


def export_dataframe(worksheet, df, tab_name):
    """Full replace: clears the tab, writes df's own columns as the header."""
    logger.info(f"Writing {len(df)} row(s) to '{tab_name}' (full replace)...")
    worksheet.clear()

    if df.empty:
        worksheet.update(values=[['(no data for this run)']], range_name='A1')
        logger.warning(f"'{tab_name}': no data to write.")
        return

    df_export = _stringify_dates_and_times(df).fillna("")
    headers = [str(c) for c in df_export.columns]
    values = [headers] + df_export.values.tolist()
    worksheet.update(values=values, range_name='A1', value_input_option='USER_ENTERED')
    logger.info(f"'{tab_name}': wrote {len(df_export)} row(s), {len(headers)} column(s).")


def export_dataframe_header_driven(worksheet, df, tab_name, header_row=1, start_col='A'):
    """Writes ONLY the columns matching the tab's EXISTING header row, in
    that header's order — the header row itself is never touched. If the
    tab has no header row yet, the export is skipped with a warning: a
    human sets that up once by hand."""
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

    df_final = _stringify_dates_and_times(pd.DataFrame(export_dict)).fillna("")
    values_to_write = df_final.values.tolist()

    data_start_row = header_row + 1
    worksheet.batch_clear([f'{start_col}{data_start_row}:ZZ'])

    if not values_to_write:
        logger.warning(f"'{tab_name}': no data to write.")
        return

    worksheet.update(values=values_to_write, range_name=f'{start_col}{data_start_row}', value_input_option='USER_ENTERED')
    logger.info(f"'{tab_name}': wrote {len(values_to_write)} row(s) across {len(existing_header)} header column(s) (header row preserved).")


def read_tab_as_df(sheet, tab_name):
    """Reads an existing tab back into a DataFrame, or None if it
    doesn't exist. UNFORMATTED_VALUE so numbers come back as plain
    numbers, not comma-formatted display strings."""
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        return None
    records = ws.get_all_records(value_render_option='UNFORMATTED_VALUE')
    return pd.DataFrame(records)


# ==========================================
# Consolidation — builds a single shipment-level 'ZSDR002_Consolidate'
# tab (monthly Sheet) from SKU-level data across EVERY month, deduped at
# the LINE-ITEM level.
#
# UPSERT KEY: (Delivery, SM, Material) — not (Month, Shipment_ID). The
# same delivery/SM/material line item showing up in two different
# months' ZSDR002 uploads (e.g. re-uploaded into the wrong month) used
# to produce two rows, each with the full quantity — double counting
# anywhere those rows got summed. Fixed by deduping at the SKU level,
# before any roll-up:
#   1. Read every month's SKU-level '<Mon>_ZSDR002' tab.
#   2. Normalize Delivery/SM/Material (blank variants all treated as
#      genuinely blank) — Shipment_ID is still computed too, kept as a
#      display/identifying column, but is no longer the key.
#   3. Upsert by (Delivery, SM, Material): a row from a month refreshed
#      THIS run always wins over one that wasn't; on a further tie, the
#      later calendar month wins.
#   4. THEN group the deduped SKU-level rows by (Delivery, SM, Material)
#      and sum Quantity / Quantity in MT — a shipment with more than one
#      material still gets one row per material, instead of collapsing
#      to a single row that only shows whichever material happened to
#      sort first.
# ==========================================
CONSOLIDATE_TAB_NAME = 'ZSDR002_Consolidate'


def consolidate_shipment_reports(sheet, months_to_process):
    """Returns the final consolidated, shipment-level DataFrame (tagged
    with a 'Month' column per row), or None if there was nothing to
    consolidate."""
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
    df_all_sku = _assign_shipment_id(df_all_sku)  # Shipment_ID kept as a display column, not the key

    for col in GROUP_KEY_COLS:
        if col not in df_all_sku.columns:
            logger.warning(f"'{col}' column not found in ZSDR002 data — upsert key falls back to whichever of {GROUP_KEY_COLS} are present.")
            df_all_sku[col] = ''
    df_all_sku = _normalize_group_key_cols(df_all_sku)

    upsert_key = [c for c in GROUP_KEY_COLS if c in df_all_sku.columns]

    month_order = {m: i for i, m in enumerate(MONTH_ABBR)}
    df_all_sku['_sort_key'] = (
        df_all_sku['_refreshed_this_run'].astype(int) * 100
        + df_all_sku['Month'].map(month_order).fillna(-1)
    )
    df_all_sku = df_all_sku.sort_values('_sort_key', kind='stable')

    before = len(df_all_sku)
    df_deduped = df_all_sku.drop_duplicates(subset=upsert_key, keep='last')
    dropped = before - len(df_deduped)
    if dropped:
        logger.warning(
            f"'{CONSOLIDATE_TAB_NAME}': dropped {dropped} duplicate line item(s) "
            f"sharing the same ({', '.join(upsert_key)}) across months."
        )

    df_deduped = df_deduped.drop(columns=['_refreshed_this_run', '_sort_key'], errors='ignore')

    if 'Quantity' in df_deduped.columns:
        df_deduped['Quantity'] = pd.to_numeric(df_deduped['Quantity'], errors='coerce').fillna(0)
    if 'Quantity in MT' in df_deduped.columns:
        df_deduped['Quantity in MT'] = pd.to_numeric(df_deduped['Quantity in MT'], errors='coerce').fillna(0)

    group_cols = upsert_key
    agg_dict = {}
    for col in df_deduped.columns:
        if col in group_cols: continue
        elif col in ['Quantity', 'Quantity in MT']: agg_dict[col] = 'sum'
        else: agg_dict[col] = 'first'

    df_consolidated = df_deduped.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

    lead_cols = [c for c in (GROUP_KEY_COLS + ['Shipment_ID', 'Month']) if c in df_consolidated.columns]
    other_cols = [c for c in df_consolidated.columns if c not in lead_cols]
    df_consolidated = df_consolidated[lead_cols + other_cols]
    sort_cols = [c for c in GROUP_KEY_COLS if c in df_consolidated.columns]
    df_consolidated = df_consolidated.sort_values(sort_cols, kind='stable').reset_index(drop=True)

    ws = get_or_create_worksheet(sheet, CONSOLIDATE_TAB_NAME)
    export_dataframe(ws, df_consolidated, CONSOLIDATE_TAB_NAME)

    logger.info(
        f"'{CONSOLIDATE_TAB_NAME}': {len(df_consolidated)} shipment(s) after deduping "
        f"{dropped} duplicate line item(s) across {len(all_months_with_data)} month(s)."
    )
    return df_consolidated


# ==========================================
# Production mirrors — closed months into 'ZSDR_Abadi', the current
# (in-progress) month into 'ZSDR_Cur_Month'. Both are header-driven,
# full-replace writes: each tab must already have its header row set up
# by hand on the production Sheet; only matching columns are written,
# and that header row is never touched.
# ==========================================
PRODUCTION_ABADI_TAB_NAME = 'ZSDR_Abadi'
PRODUCTION_CUR_MONTH_TAB_NAME = 'ZSDR_Cur_Month'

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
            f"Production mirror filter: column(s) {missing_cols} not found — "
            f"filter on those column(s) skipped (rows not excluded on that basis)."
        )
    return df[mask].reset_index(drop=True)


def mirror_to_abadi(gc, production_sheet_id, df_consolidated):
    """Mirrors the (already fully upserted) consolidated DataFrame into
    'ZSDR_Abadi' — CLOSED months only. The current month's rows are
    excluded here, centrally, regardless of what the caller passed in,
    so a month still in progress can never leak into the closed-months
    tab even indirectly through the cross-month consolidation."""
    if df_consolidated is None:
        logger.warning("mirror_to_abadi(): nothing to mirror — skipping.")
        return

    total_rows = len(df_consolidated)
    if total_rows and 'Month' in df_consolidated.columns:
        cur_month = current_month_abbr()
        df_consolidated = df_consolidated[df_consolidated['Month'] != cur_month].reset_index(drop=True)
        excluded = total_rows - len(df_consolidated)
        if excluded:
            logger.info(f"Excluded {excluded} row(s) belonging to the current month ({cur_month}) from '{PRODUCTION_ABADI_TAB_NAME}' — those belong in '{PRODUCTION_CUR_MONTH_TAB_NAME}' instead.")
        total_rows = len(df_consolidated)

    df_filtered = _apply_production_mirror_filter(df_consolidated) if total_rows else df_consolidated
    logger.info(
        f"Mirroring '{CONSOLIDATE_TAB_NAME}' -> production Sheet tab '{PRODUCTION_ABADI_TAB_NAME}' "
        f"(filtered: Is SA2,SA4,SB=Yes, Are blocked=Not Blocked, Is Scrap=NOT SCRAP — "
        f"{len(df_filtered)} of {total_rows} row(s) pass)..."
    )
    production_sheet = gc.open_by_key(production_sheet_id)
    ws = get_or_create_worksheet(production_sheet, PRODUCTION_ABADI_TAB_NAME)
    export_dataframe_header_driven(ws, df_filtered, PRODUCTION_ABADI_TAB_NAME)


def mirror_to_cur_month(gc, production_sheet_id, df_shipment_month):
    """Mirrors ONE month's own shipment report — the month still in
    progress — into 'ZSDR_Cur_Month', fully replacing it. Unlike
    mirror_to_abadi(), there's no cross-month upsert here: 'ZSDR_Cur_Month'
    represents exactly one month at a time."""
    if df_shipment_month is None:
        logger.warning("mirror_to_cur_month(): nothing to mirror — skipping.")
        return

    total_rows = len(df_shipment_month)
    df_filtered = _apply_production_mirror_filter(df_shipment_month) if total_rows else df_shipment_month
    logger.info(
        f"Mirroring this month's shipment report -> production Sheet tab '{PRODUCTION_CUR_MONTH_TAB_NAME}' "
        f"(filtered: Is SA2,SA4,SB=Yes, Are blocked=Not Blocked, Is Scrap=NOT SCRAP — "
        f"{len(df_filtered)} of {total_rows} row(s) pass)..."
    )
    production_sheet = gc.open_by_key(production_sheet_id)
    ws = get_or_create_worksheet(production_sheet, PRODUCTION_CUR_MONTH_TAB_NAME)
    export_dataframe_header_driven(ws, df_filtered, PRODUCTION_CUR_MONTH_TAB_NAME)


# ==========================================
# Upload-mode Abadi upsert. This is what replaced the monthly-Sheet-
# based consolidate_shipment_reports() + mirror_to_abadi() for the
# UPLOAD path specifically (refresh mode still uses that older pair —
# see the note above run_refresh() further down).
#
# No intermediate sheet at all: reads 'ZSDR_Abadi' back from the
# PRODUCTION Sheet itself, drops whatever rows it already has tagged
# with THIS month (so a re-upload of an already-uploaded month fully
# replaces that month's slice — no stale row left behind if the new
# upload's shape differs from the old one), appends this month's freshly
# computed rows, and re-applies the (Delivery, SM, Material) dedup
# safety net against every OTHER month's existing rows too — an
# accidental duplicate under a different month still resolves in favor
# of the freshest upload, exactly like the old cross-month consolidation
# did.
# ==========================================
UPSERT_DATE_COLUMNS = (
    ['Release Date', 'SO Create Date', 'DO Create Date', 'Schedule Date', 'Act. Gds Mvmnt Date']
    + TMS_DATE_COLUMNS + [f'{c} (TMS)' for c in TMS_DATE_COLUMNS]
)
UPSERT_TIME_COLUMNS = ['Release Time', 'SO Create Time', 'DO Create Time']


def _format_upsert_date_time_columns(df):
    """Formats real datetime64/time values into plain display strings —
    same format _stringify_dates_and_times produces, but driven by
    COLUMN NAME rather than current dtype. Needed because after
    concatenating freshly-computed rows (real datetime64 values) with
    rows read back from the Sheet (already plain strings), the column's
    dtype is no longer datetime64, so _stringify_dates_and_times's dtype
    check would silently skip it.

    Covers both the known UPSERT_DATE_COLUMNS/UPSERT_TIME_COLUMNS AND
    any OTHER column that happens to hold real datetime64/Timestamp/
    datetime.time values — e.g. 'Customer Ref. Date' or 'Latest Ship
    Date', which come straight from ZSDR002/openpyxl auto-parsing a
    date-formatted Excel cell into a native Timestamp without any
    explicit pd.to_datetime() call in this pipeline. Any such column
    left unformatted would still hold raw Timestamp objects at write
    time, which gspread/requests cannot JSON-serialize, crashing the
    upsert. Detecting by dtype here (not just by name) is what makes
    this safe against a future column silently gaining the same issue.

    Returns (formatted_df, date_time_col_names) — the second value is
    every column name actually formatted, for the caller to pass to
    _apostrophe_prefix_columns() afterward so post-concat prefixing
    covers exactly the same set, named columns or not.
    """
    df = df.copy()

    def fmt_date(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        if isinstance(v, (pd.Timestamp, _datetime)):
            if pd.isna(v):
                return ''
            s = v.strftime('%Y-%m-%d %H:%M:%S')
            return s[:-9] if s.endswith(' 00:00:00') else s
        s = str(v).strip()
        return '' if (not s or s.lower() == 'nat') else s

    def fmt_time(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        s = str(v).strip()
        return '' if (not s or s.lower() in ('nat', 'none')) else s

    def col_has_datetime_like_values(series):
        if pd.api.types.is_datetime64_any_dtype(series):
            return True
        if series.dtype == object:
            return series.map(lambda v: isinstance(v, (pd.Timestamp, _datetime, _date, time))).any()
        return False

    date_cols_done = set()
    for col in UPSERT_DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(fmt_date)
            date_cols_done.add(col)

    time_cols_done = set()
    for col in UPSERT_TIME_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(fmt_time)
            time_cols_done.add(col)

    # Safety net: any column not already handled above that still
    # carries real datetime64/Timestamp/date/time values gets the same
    # date-style formatting, and its name is reported back so it also
    # gets apostrophe-prefixed post-concat.
    extra_cols_done = set()
    for col in df.columns:
        if col in date_cols_done or col in time_cols_done:
            continue
        if col_has_datetime_like_values(df[col]):
            df[col] = df[col].apply(fmt_date)
            extra_cols_done.add(col)

    if extra_cols_done:
        logger.warning(
            f"'{PRODUCTION_ABADI_TAB_NAME}' upsert: found datetime-like values in column(s) not in "
            f"UPSERT_DATE_COLUMNS/UPSERT_TIME_COLUMNS — {sorted(extra_cols_done)}. Formatted them too "
            f"so the write doesn't crash, but consider adding them to that list explicitly."
        )

    return df, sorted(date_cols_done | time_cols_done | extra_cols_done)


def _apostrophe_prefix_columns(df, cols):
    """Same apostrophe-prefix trick export_dataframe_header_driven uses
    for real datetime64 columns (forces Sheets to store literal text
    instead of reinterpreting the string as its own ambiguous date
    value), applied by COLUMN NAME instead of current dtype — needed
    post-concat since these columns are plain strings by then, not
    datetime64, so the dtype-based version wouldn't catch them."""
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: f"'{v}" if v else v)
    return df


def upsert_month_into_abadi(gc, production_sheet_id, month_abbr, df_shipment_month):
    production_sheet = gc.open_by_key(production_sheet_id)
    ws = get_or_create_worksheet(production_sheet, PRODUCTION_ABADI_TAB_NAME)

    df_new = df_shipment_month.copy()
    df_new['Month'] = month_abbr
    df_new = _normalize_group_key_cols(df_new)
    df_new, date_time_cols_formatted = _format_upsert_date_time_columns(df_new)

    existing = read_tab_as_df(production_sheet, PRODUCTION_ABADI_TAB_NAME)
    if existing is not None and not existing.empty:
        existing = _normalize_group_key_cols(existing)
        if 'Month' in existing.columns:
            existing_other_months = existing[existing['Month'] != month_abbr].copy()
        else:
            logger.warning(f"'{PRODUCTION_ABADI_TAB_NAME}' has existing rows with no 'Month' column — treating all of them as other months (none dropped).")
            existing_other_months = existing.copy()
    else:
        existing_other_months = pd.DataFrame(columns=df_new.columns)

    upsert_key = [c for c in GROUP_KEY_COLS if c in df_new.columns]
    combined = pd.concat([existing_other_months, df_new], ignore_index=True, sort=False)

    before = len(combined)
    # df_new rows are appended LAST, so keep='last' means a freshly
    # uploaded row always wins any cross-month key collision.
    combined = combined.drop_duplicates(subset=upsert_key, keep='last')
    dropped = before - len(combined)
    if dropped:
        logger.warning(f"'{PRODUCTION_ABADI_TAB_NAME}': dropped {dropped} duplicate line item(s) sharing the same ({', '.join(upsert_key)}) across months during upsert.")

    total_rows = len(combined)
    combined = _apply_production_mirror_filter(combined) if total_rows else combined
    combined = _apostrophe_prefix_columns(combined, date_time_cols_formatted)

    logger.info(
        f"Upserting {month_abbr}'s {len(df_new)} row(s) into '{PRODUCTION_ABADI_TAB_NAME}' "
        f"(kept {len(existing_other_months)} other-month row(s) untouched, {total_rows} total before "
        f"the SA2/SA4/SB filter, {len(combined)} after)..."
    )
    export_dataframe_header_driven(ws, combined, PRODUCTION_ABADI_TAB_NAME)


# ==========================================
# Mode: upload — Workflows 0-2 for one month's freshly staged files.
# No monthly Sheet involved — see the module docstring. Goes straight
# from the staged Drive files to whichever production tab this month
# belongs on.
# ==========================================
def run_upload():
    month_abbr = get_env('UPLOAD_MONTH')
    if month_abbr not in MONTH_ABBR:
        logger.error(f"Unrecognized month key '{month_abbr}' — expected one of {MONTH_ABBR}.")
        sys.exit(1)

    staging_folder_id = get_env('STAGING_FOLDER_ID')
    production_sheet_id = get_env('TARGET_SHEET_ID')

    logger.info(f"================ UPLOAD: Workflows 0-2 for {month_abbr} ================")

    gc, drive_svc = authenticate_services()
    dict_masters = load_master_data(gc, production_sheet_id)

    df_tms_raw = download_staged_file(drive_svc, staging_folder_id, month_abbr, 'TMS', skip_rows=1)
    df_zsdr_raw = download_staged_file(drive_svc, staging_folder_id, month_abbr, 'ZSDR002', skip_rows=0)

    df_tms_clean = preprocess_tms(df_tms_raw)
    df_zsdr_sku = process_data(df_zsdr_raw, df_tms_clean, dict_masters)
    df_zsdr_shipment = create_shipment_report(df_zsdr_sku)

    if month_abbr == current_month_abbr():
        logger.info(f">>> {month_abbr} is the CURRENT month — replacing {PRODUCTION_CUR_MONTH_TAB_NAME} with this month's shipment report.")
        mirror_to_cur_month(gc, production_sheet_id, df_zsdr_shipment)
    else:
        logger.info(f">>> {month_abbr} is a CLOSED month — upserting this month's shipment report directly into {PRODUCTION_ABADI_TAB_NAME}.")
        upsert_month_into_abadi(gc, production_sheet_id, month_abbr, df_zsdr_shipment)

    logger.info(f"================ FINISHED upload for {month_abbr} ================")


# ==========================================
# Mode: refresh — re-derive helper columns for month(s) that already
# have data, using the CURRENT master data, without needing a new file.
# GUARANTEE: the original ZSDR002 columns are never touched — only
# columns in HELPER_COLUMN_NAMES are stripped and recomputed.
# ==========================================
def run_refresh():
    monthly_sheet_id = get_env('MONTHLY_TARGET_SHEET_ID')
    production_sheet_id = get_env('TARGET_SHEET_ID')
    requested_month = os.environ.get('REFRESH_MONTH', '').strip()

    gc, _drive_svc = authenticate_services()
    monthly_sheet = gc.open_by_key(monthly_sheet_id)

    if requested_month:
        if requested_month not in MONTH_ABBR:
            logger.error(f"Unrecognized REFRESH_MONTH '{requested_month}' — expected one of {MONTH_ABBR}.")
            sys.exit(1)
        months_to_process = [requested_month]
    else:
        existing_titles = {ws.title for ws in monthly_sheet.worksheets()}
        months_to_process = [m for m in MONTH_ABBR if f'{m}_ZSDR002' in existing_titles]
        if not months_to_process:
            logger.warning("No '<Mon>_ZSDR002' tabs found in the monthly Sheet — nothing to refresh.")
            return

    logger.info(f"================ REFRESH: helper columns for {', '.join(months_to_process)} ================")

    dict_masters = load_master_data(gc, production_sheet_id)

    refreshed = []
    skipped = []
    df_ship_current_month = None  # captured separately for the cur-month mirror below

    for month_abbr in months_to_process:
        sku_tab_name = f'{month_abbr}_ZSDR002'
        tms_tab_name = f'{month_abbr}_TMS'
        ship_tab_name = f'{month_abbr.upper()}_ZSDR002_ShipLevel'

        logger.info(f"--- {month_abbr} ---")

        df_tms_existing = read_tab_as_df(monthly_sheet, tms_tab_name)
        if df_tms_existing is None or df_tms_existing.empty:
            logger.warning(f"[{month_abbr}] '{tms_tab_name}' missing or empty — skipping (need TMS data to recompute TMS-derived helpers).")
            skipped.append(month_abbr)
            continue

        df_sku_existing = read_tab_as_df(monthly_sheet, sku_tab_name)
        if df_sku_existing is None or df_sku_existing.empty:
            logger.warning(f"[{month_abbr}] '{sku_tab_name}' missing or empty — skipping.")
            skipped.append(month_abbr)
            continue

        df_tms_clean = preprocess_tms(df_tms_existing)

        original_cols = [c for c in df_sku_existing.columns if c not in HELPER_COLUMN_NAMES]
        dropped_cols = [c for c in df_sku_existing.columns if c in HELPER_COLUMN_NAMES]
        logger.info(f"[{month_abbr}] Keeping {len(original_cols)} original column(s), dropping {len(dropped_cols)} stale helper column(s) before recompute.")
        df_zsdr_original_only = df_sku_existing[original_cols].copy()

        df_zsdr_refreshed = process_data(df_zsdr_original_only, df_tms_clean, dict_masters)
        df_ship_refreshed = create_shipment_report(df_zsdr_refreshed)

        export_dataframe(get_or_create_worksheet(monthly_sheet, sku_tab_name), df_zsdr_refreshed, sku_tab_name)
        export_dataframe(get_or_create_worksheet(monthly_sheet, ship_tab_name), df_ship_refreshed, ship_tab_name)

        if month_abbr == current_month_abbr():
            df_ship_current_month = df_ship_refreshed

        refreshed.append(month_abbr)

    if refreshed:
        closed_refreshed = [m for m in refreshed if m != current_month_abbr()]
        if closed_refreshed:
            df_consolidated = consolidate_shipment_reports(monthly_sheet, closed_refreshed)
            mirror_to_abadi(gc, production_sheet_id, df_consolidated)
        if df_ship_current_month is not None:
            logger.info(f">>> {current_month_abbr()} was refreshed this run — mirroring it into {PRODUCTION_CUR_MONTH_TAB_NAME} too.")
            mirror_to_cur_month(gc, production_sheet_id, df_ship_current_month)
    else:
        logger.warning("No months were successfully refreshed — skipping consolidation and production mirror steps.")

    logger.info("================ Refresh complete ================")
    logger.info(f"Refreshed: {', '.join(refreshed) if refreshed else '(none)'}")
    if skipped:
        logger.warning(f"Skipped (missing/empty data): {', '.join(skipped)}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ('upload', 'refresh'):
        logger.error('Usage: python zsdr_monthly_upload_pipeline.py [upload|refresh]')
        sys.exit(1)
    if sys.argv[1] == 'upload':
        run_upload()
    else:
        run_refresh()


if __name__ == "__main__":
    main()
