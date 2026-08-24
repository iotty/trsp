"""
cur_month_pipeline.py
-----------------------------------------------------------------------
Upload-triggered pipeline for the TRSP dashboard's "Upload data" page
(UploadApi.gs, bound directly to the TRSP production spreadsheet).

UploadApi.gs stages an uploaded ZSDR002/TMS file into a new, timestamped
"Staging_ZSDR002_<timestamp>" / "Staging_TMS_<timestamp>" tab, then
dispatches this workflow (see dispatchCurMonthPipeline_() /
finalizeUpload() in UploadApi.gs). This script:

  1. Finds the MOST RECENTLY staged ZSDR002 and TMS tabs (the timestamp
     suffix in the tab name sorts chronologically as a plain string,
     so "most recent" is just the lexicographically greatest name).
  2. Runs Workflow 0 (TMS date prep) and Workflow 1 (ZSDR002 SKU-level
     enrichment) against them, using CURRENT master data
     (Master_Truck_Input / Master_Days_Input / Master_Sales_LGS) --
     all in the same spreadsheet now, no separate "monthly Sheet" vs
     "production Sheet" split.
  3. Runs Workflow 2 (shipment-level rollup).
  4. Tags every row with the current month (3-letter abbreviation,
     e.g. 'Aug') in a 'Month' column, and writes the result into
     ZSDR_Cur_Month -- a full replace of the DATA ROWS there (the
     header row is left untouched, matching how Config.gs describes
     ZSDR_Cur_Month: "month-to-date, open or closed").

TMS data is used only as processing input -- this script never writes
a standalone TMS output tab (per explicit instruction: "no need to
copy the TMS").

This does NOT touch ZSDR_Abadi. Moving closed (GI'd) rows out of
ZSDR_Cur_Month into ZSDR_Abadi once a month ends is a separate,
scheduled step -- see archive_closed_orders.py.

Required environment variables:
    GOOGLE_CREDENTIALS   Service account JSON.
    TARGET_SHEET_ID      The TRSP production spreadsheet's ID (the same
                          spreadsheet UploadApi.gs is bound to, and the
                          same secret already used for master data by
                          the earlier monthly-upload-interface scripts).

Optional:
    PIPELINE_TIMEZONE    IANA timezone name used to determine "current
                          month" (default 'Asia/Jakarta').
"""
import os
import sys
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

from workflows_common import (
    logger, MONTH_ABBR, get_env, authenticate_services, load_master_data,
    preprocess_tms, process_data, create_shipment_report, read_tab_as_df,
    get_or_create_worksheet, export_dataframe_header_driven,
)

CUR_MONTH_TAB_NAME = 'ZSDR_Cur_Month'
STAGING_ZSDR002_PREFIX = 'Staging_ZSDR002'
STAGING_TMS_PREFIX = 'Staging_TMS'


def current_month_abbr():
    tz_name = os.environ.get('PIPELINE_TIMEZONE', 'Asia/Jakarta')
    now = datetime.now(ZoneInfo(tz_name)) if ZoneInfo else datetime.utcnow()
    return MONTH_ABBR[now.month - 1]


def find_latest_staging_tab(sheet, prefix):
    matches = [ws.title for ws in sheet.worksheets() if ws.title.startswith(prefix + '_')]
    if not matches:
        return None
    # "Staging_ZSDR002_2026-08-24_1530[_n]" -- the yyyy-MM-dd_HHmm
    # timestamp sorts correctly as a plain string, so the most recently
    # staged upload is just the lexicographically greatest name.
    return sorted(matches)[-1]


def main():
    sheet_id = get_env('TARGET_SHEET_ID')
    gc, _drive_svc = authenticate_services()
    sheet = gc.open_by_key(sheet_id)

    zsdr_tab = find_latest_staging_tab(sheet, STAGING_ZSDR002_PREFIX)
    tms_tab = find_latest_staging_tab(sheet, STAGING_TMS_PREFIX)

    if not zsdr_tab:
        logger.error(f"No '{STAGING_ZSDR002_PREFIX}_*' tab found -- nothing to process.")
        sys.exit(1)
    if not tms_tab:
        logger.error(f"No '{STAGING_TMS_PREFIX}_*' tab found -- nothing to process.")
        sys.exit(1)

    logger.info(f"Using staged tabs: ZSDR002='{zsdr_tab}', TMS='{tms_tab}'")

    df_zsdr_raw = read_tab_as_df(sheet, zsdr_tab)
    df_tms_raw = read_tab_as_df(sheet, tms_tab)

    if df_zsdr_raw is None or df_zsdr_raw.empty:
        logger.error(f"'{zsdr_tab}' is missing or empty.")
        sys.exit(1)
    if df_tms_raw is None or df_tms_raw.empty:
        logger.error(f"'{tms_tab}' is missing or empty.")
        sys.exit(1)

    dict_masters = load_master_data(gc, sheet_id)

    df_tms_clean = preprocess_tms(df_tms_raw)
    df_zsdr_sku = process_data(df_zsdr_raw, df_tms_clean, dict_masters)
    df_shipment = create_shipment_report(df_zsdr_sku)

    month_abbr = current_month_abbr()
    df_shipment = df_shipment.copy()
    if 'Month' in df_shipment.columns:
        df_shipment = df_shipment.drop(columns=['Month'])
    df_shipment.insert(0, 'Month', month_abbr)

    ws_cur = get_or_create_worksheet(sheet, CUR_MONTH_TAB_NAME)
    export_dataframe_header_driven(ws_cur, df_shipment, CUR_MONTH_TAB_NAME)

    logger.info(f"'{CUR_MONTH_TAB_NAME}': {len(df_shipment)} shipment row(s) written for {month_abbr} (staged from '{zsdr_tab}' / '{tms_tab}').")


if __name__ == "__main__":
    main()
