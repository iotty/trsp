"""
run_monthly_workflows.py
-----------------------------------------------------------------------
Implements Workflow 0-2 from the existing production pipeline (main.py),
scoped to a single month's staged files instead of "whatever is latest
in the shared staging folder":

    Workflow 0 — TMS Data Prep        -> preprocess_tms()
    Workflow 1 — ZSDR SKU-Level       -> process_data()
    Workflow 2 — ZSDR Shipment-Level  -> create_shipment_report()

The transformation logic lives in workflows_common.py, shared with
refresh_helper_columns.py (which re-derives helper columns for
already-uploaded months without needing a new file, e.g. after master
data changes). This script's own job is just: find this month's staged
files in Drive, run them through that shared logic, and write the
results out.

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
differ, process_data() in workflows_common.py will need adjusting.
"""
import io
import sys

import pandas as pd

from workflows_common import (
    logger, MONTH_ABBR, get_env, authenticate_services, load_master_data,
    preprocess_tms, process_data, create_shipment_report,
    get_or_create_worksheet, export_dataframe
)


# ==========================================
# Download this month's staged file for a given type/prefix.
# ==========================================
def download_staged_file(drive_svc, folder_id, month_abbr, file_type, skip_rows=0):
    from googleapiclient.http import MediaIoBaseDownload

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
