"""
cur_month_pipeline.py
-----------------------------------------------------------------------
Simple upload pipeline for the TRSP dashboard's "Upload data" page.
Reset to just this, per explicit instruction -- no shipment-level
rollup, no month tagging, no archiving, no column filtering:

  1. User uploads a ZSDR002 file and a TMS file.
  2. UploadApi.gs uploads them straight to the production Drive staging
     folder ('ZSDR_*' / 'TMS_*' naming -- same convention your original
     main.py already used).
  3. UploadApi.gs dispatches this GitHub Action (finalizeUpload() ->
     dispatchCurMonthPipeline_()), which downloads the latest 'ZSDR_*'
     and 'TMS_*' file from that folder.
  4. Each file is written straight back into the Sheet:
       ZSDR002 (with helper columns computed -- Type of Truck, SM,
         Sales LGS, merged TMS dates, Is SA2,SA4,SB, Are blocked,
         Is Scrap, Vendor, Truck Vendor Type, TRAIN/TRUCK, Last Status)
         -> 'ZSDR_Cur_Month'
       TMS (dates standardized) -> 'TMS_Cur_Month'
     Both are plain full-replace writes (export_dataframe: clears and
     rewrites the whole tab, header included) -- no filtering against
     an existing header row, no shipment-level grouping.

NOTE: this REPLACES a more elaborate version of this same script that
also ran create_shipment_report() (Workflow 2) and wrote a
shipment-level result. That's been removed here, per instruction to
reset back to this simple 4-step flow. create_shipment_report(),
consolidate_shipment_reports(), and archive_closed_orders.py still
exist in this repo but are no longer called from this pipeline --
archive_closed_orders.py in particular now assumes a 'Month'/
'Shipment_ID'-tagged ZSDR_Cur_Month that this script no longer
produces, so it's effectively orphaned unless you want it reconnected.

Required environment variables:
    GOOGLE_CREDENTIALS   Service account JSON.
    TARGET_SHEET_ID      The TRSP production spreadsheet's ID.
    STAGING_FOLDER_ID    The production Drive staging folder ID (same
                          folder your original pipeline.yml already
                          reads 'TMS_*'/'ZSDR_*' from).
"""
from workflows_common import (
    logger, get_env, authenticate_services, load_master_data,
    download_latest_file, preprocess_tms, process_data,
    get_or_create_worksheet, export_dataframe,
)

ZSDR_TAB_NAME = 'ZSDR_Cur_Month'
TMS_TAB_NAME = 'TMS_Cur_Month'


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

    ws_zsdr = get_or_create_worksheet(sheet, ZSDR_TAB_NAME)
    export_dataframe(ws_zsdr, df_zsdr_sku, ZSDR_TAB_NAME)
    logger.info(f"'{ZSDR_TAB_NAME}': wrote {len(df_zsdr_sku)} row(s) (source file modified {zsdr_mod_time}).")

    ws_tms = get_or_create_worksheet(sheet, TMS_TAB_NAME)
    export_dataframe(ws_tms, df_tms_clean, TMS_TAB_NAME)
    logger.info(f"'{TMS_TAB_NAME}': wrote {len(df_tms_clean)} row(s) (source file modified {tms_mod_time}).")


if __name__ == "__main__":
    main()
