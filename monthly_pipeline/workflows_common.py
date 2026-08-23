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


def _to_datetime_smart(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return series

    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit='D', origin=_EXCEL_EPOCH, errors='coerce')

    def parse_one(v):
        if pd.isna(v):
            return pd.NaT
        if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool):
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


# ==========================================
# Consolidation — final step of a refresh run. Upserts the ShipLevel
# data for whichever month(s) were just refreshed into a single
# 'ZSDR002_Consolidate' tab, keyed by (Month, Shipment_ID).
#
# Behavior, per the "remove stale rows" choice: for every month included
# in `months_to_process`, that month's ENTIRE section of the
# consolidated tab is replaced by whatever its '<MON>_ZSDR002_ShipLevel'
# tab currently holds — matching shipments are updated in place, new
# ones are inserted, and any shipment that no longer appears in that
# month's ShipLevel tab is dropped rather than left stale. Every OTHER
# month's rows already in the consolidated tab are left completely
# untouched — this is what makes a single-month refresh safe to run
# without clobbering the rest of the year.
#
# Columns are aligned by NAME (via pd.concat), not by position, so
# months whose ShipLevel tabs have slightly different helper columns
# still line up correctly — any column a given row doesn't have is
# simply left blank for that row, instead of misaligning data into the
# wrong column.
# ==========================================
CONSOLIDATE_TAB_NAME = 'ZSDR002_Consolidate'


def consolidate_shipment_reports(sheet, months_to_process):
    """
    Returns the final consolidated DataFrame (after the upsert) so the
    caller can also mirror it elsewhere — see
    mirror_consolidate_to_production() below — without re-reading every
    ShipLevel tab a second time. Returns None if there was nothing to
    consolidate.
    """
    if not months_to_process:
        logger.warning("consolidate_shipment_reports() called with no months — skipping.")
        return None

    logger.info(f"Consolidating ShipLevel data into '{CONSOLIDATE_TAB_NAME}' for: {', '.join(months_to_process)}...")

    # 1) Fresh ShipLevel rows for the month(s) just refreshed.
    new_frames = []
    for month_abbr in months_to_process:
        ship_tab_name = f'{month_abbr.upper()}_ZSDR002_ShipLevel'
        df_ship = read_tab_as_df(sheet, ship_tab_name)
        if df_ship is None or df_ship.empty:
            logger.warning(f"[{month_abbr}] '{ship_tab_name}' missing or empty — nothing to consolidate for this month.")
            continue
        df_ship = df_ship.copy()
        df_ship.insert(0, 'Month', month_abbr)
        new_frames.append(df_ship)

    df_new = pd.concat(new_frames, ignore_index=True, sort=False) if new_frames else pd.DataFrame(columns=['Month'])

    # 2) Whatever the consolidated tab already holds.
    df_existing = read_tab_as_df(sheet, CONSOLIDATE_TAB_NAME)
    if df_existing is None or df_existing.empty:
        df_existing = pd.DataFrame(columns=['Month'])
    if 'Month' not in df_existing.columns:
        df_existing['Month'] = ''

    # 3) Drop this run's month(s) from the existing data — their section
    #    gets fully replaced by df_new. Every other month passes through
    #    untouched.
    df_kept = df_existing[~df_existing['Month'].isin(months_to_process)].copy()

    # 4) Union by column NAME (pd.concat aligns on labels automatically;
    #    a column missing from one side comes back blank/NaN for those
    #    rows rather than misaligning values).
    df_consolidated = pd.concat([df_kept, df_new], ignore_index=True, sort=False)

    lead_cols = [c for c in ['Month', 'Shipment_ID'] if c in df_consolidated.columns]
    other_cols = [c for c in df_consolidated.columns if c not in lead_cols]
    df_consolidated = df_consolidated[lead_cols + other_cols]

    # Stable, human-friendly ordering: calendar month order, then
    # Shipment_ID within each month.
    if 'Month' in df_consolidated.columns and len(df_consolidated):
        month_order = {m: i for i, m in enumerate(MONTH_ABBR)}
        sort_key = df_consolidated['Month'].map(month_order).fillna(999)
        sort_cols_df = pd.DataFrame({'_month_sort': sort_key})
        if 'Shipment_ID' in df_consolidated.columns:
            sort_cols_df['_shipment_id'] = df_consolidated['Shipment_ID'].astype(str)
        df_consolidated = (
            df_consolidated
            .assign(**{c: sort_cols_df[c] for c in sort_cols_df.columns})
            .sort_values(list(sort_cols_df.columns), kind='stable')
            .drop(columns=list(sort_cols_df.columns))
            .reset_index(drop=True)
        )

    ws = get_or_create_worksheet(sheet, CONSOLIDATE_TAB_NAME)
    export_dataframe(ws, df_consolidated, CONSOLIDATE_TAB_NAME)

    logger.info(
        f"'{CONSOLIDATE_TAB_NAME}': upserted {len(df_new)} row(s) for {', '.join(months_to_process)}; "
        f"preserved {len(df_kept)} row(s) from other months; {len(df_consolidated)} row(s) total."
    )

    return df_consolidated


# ==========================================
# Mirrors the (already fully upserted) consolidated DataFrame into the
# PRODUCTION Sheet's 'ZSDR_Abadi' tab.
#
# This is a straight full-replace copy, not a second upsert pass — by
# the time this runs, `df_consolidated` returned by
# consolidate_shipment_reports() is already the complete, correct state
# of every month (the upsert already happened against the monthly
# Sheet's 'ZSDR002_Consolidate' tab). Re-deriving another upsert here
# against whatever 'ZSDR_Abadi' currently holds would be redundant and
# just adds another place the two copies could drift apart — a plain
# mirror keeps 'ZSDR_Abadi' byte-for-byte in sync with
# 'ZSDR002_Consolidate' every run.
# ==========================================
PRODUCTION_MIRROR_TAB_NAME = 'ZSDR_Abadi'


def mirror_consolidate_to_production(gc, production_sheet_id, df_consolidated):
    if df_consolidated is None:
        logger.warning("mirror_consolidate_to_production(): nothing to mirror — skipping.")
        return

    logger.info(f"Mirroring '{CONSOLIDATE_TAB_NAME}' -> production Sheet tab '{PRODUCTION_MIRROR_TAB_NAME}'...")

    production_sheet = gc.open_by_key(production_sheet_id)
    ws = get_or_create_worksheet(production_sheet, PRODUCTION_MIRROR_TAB_NAME)
    export_dataframe(ws, df_consolidated, PRODUCTION_MIRROR_TAB_NAME)

    logger.info(f"'{PRODUCTION_MIRROR_TAB_NAME}': mirrored {len(df_consolidated)} row(s) from '{CONSOLIDATE_TAB_NAME}'.")
