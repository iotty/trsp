"""
cur_month_pipeline.py
-----------------------------------------------------------------------
Upload-triggered pipeline for the TRSP dashboard's "Upload data" page
(UploadApi.gs, bound directly to the TRSP production spreadsheet).

UploadApi.gs uploads a file straight to the production Drive staging
folder -- the SAME folder and 'ZSDR_'/'TMS_' naming convention your
original main.py/pipeline.yml already uses -- then dispatches this
workflow (see dispatchCurMonthPipeline_() / finalizeUpload() in
UploadApi.gs). This script:

  1. Downloads the most recently created 'ZSDR_*' and 'TMS_*' file from
     that staging folder (download_latest_file() in workflows_common.py,
     verbatim from the original main.py -- including skiprows=1 for TMS,
     skiprows=0 for ZSDR, matching the original export layouts exactly).
  2. Runs Workflow 0 (TMS date prep) and Workflow 1 (ZSDR002 SKU-level
     enrichment) against them, using CURRENT master data
     (Master_Truck_Input / Master_Days_Input / Master_Sales_LGS) from
     the same spreadsheet.
  3. Runs Workflow 2 (shipment-level rollup).
  4. Tags every row with the current month (3-letter abbreviation,
     e.g. 'Aug') in a 'Month' column, and writes the result into
     ZSDR_Cur_Month -- a full replace of the DATA ROWS there (the
     header row is left untouched, matching how Config.gs describes
     ZSDR_Cur_Month: "month-to-date, open or closed").

TMS data is used only as processing input -- this script never writes
a standalone TMS output tab (per explicit instruction: "no need to
copy the TMS").

This is a straight Drive-download read, not a Sheets-tab read -- a
prior version of this script read 'Staging_ZSDR002_*'/'Staging_TMS_*'
tabs that UploadApi.gs wrote via a slow, fragile Apps Script ->
throwaway-Sheets-conversion -> setValues() path (the source of the
transient "HTTP 503" failures seen in practice). Downloading the raw
file straight from Drive and reading it with pandas/openpyxl -- same
as your original production main.py -- avoids that whole class of
problem, and is considerably faster for a large file.

This does NOT touch ZSDR_Abadi. Moving closed (GI'd) rows out of
ZSDR_Cur_Month into ZSDR_Abadi once a month ends is a separate,
scheduled step -- see archive_closed_orders.py.

Required environment variables:
    GOOGLE_CREDENTIALS   Service account JSON.
    TARGET_SHEET_ID      The TRSP production spreadsheet's ID (the same
                          spreadsheet UploadApi.gs is bound to, and the
                          same secret already used for master data by
                          the earlier monthly-upload-interface scripts).
    STAGING_FOLDER_ID    The production Drive staging folder ID -- the
                          SAME folder your original pipeline.yml already
                          reads 'TMS_*'/'ZSDR_*' from (reuse that
                          secret's value here).

Optional:
    PIPELINE_TIMEZONE    IANA timezone name used to determine "current
                          month" (default 'Asia/Jakarta').
"""
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

from workflows_common import (
    logger, MONTH_ABBR, get_env, authenticate_services, load_master_data,
    download_latest_file, preprocess_tms, process_data, create_shipment_report,
    get_or_create_worksheet, export_dataframe_header_driven,
)

CUR_MONTH_TAB_NAME = 'ZSDR_Cur_Month'


def current_month_abbr():
    tz_name = os.environ.get('PIPELINE_TIMEZONE', 'Asia/Jakarta')
    now = datetime.now(ZoneInfo(tz_name)) if ZoneInfo else datetime.utcnow()
    return MONTH_ABBR[now.month - 1]


def main():
    sheet_id = get_env('TARGET_SHEET_ID')
    staging_folder_id = get_env('STAGING_FOLDER_ID')

    gc, drive_svc = authenticate_services()
    sheet = gc.open_by_key(sheet_id)

    # Same prefixes and skip_rows as the original production main.py:
    # TMS_* has a leading title row to skip, ZSDR_* doesn't.
    df_tms_raw, tms_mod_time = download_latest_file(drive_svc, staging_folder_id, 'TMS_', skip_rows=1)
    df_zsdr_raw, zsdr_mod_time = download_latest_file(drive_svc, staging_folder_id, 'ZSDR_', skip_rows=0)

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

    logger.info(
        f"'{CUR_MONTH_TAB_NAME}': {len(df_shipment)} shipment row(s) written for {month_abbr} "
        f"(ZSDR modified {zsdr_mod_time}, TMS modified {tms_mod_time})."
    )


if __name__ == "__main__":
    main()
