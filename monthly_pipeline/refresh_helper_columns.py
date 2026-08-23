"""
refresh_helper_columns.py
-----------------------------------------------------------------------
Re-derives Workflow 1's helper columns (Type of Truck, SM, Sales LGS,
all TMS date columns, Is SA2/SA4/SB, Are blocked, Is Scrap, Vendor,
Truck Vendor Type, TRAIN/TRUCK, Last Status) and Workflow 2's
shipment-level rollup for month(s) that ALREADY have data — without
needing any new file upload.

This exists for the case where master data changes (Master_Truck_Input,
Master_Days_Input, Master_Sales_LGS on the production Sheet) after a
month's ZSDR002 has already been processed: rather than re-uploading
that month's files from scratch, this re-runs just the derivation logic
against the current masters and the month's already-stored TMS data.

GUARANTEE: the original ZSDR002 columns are never touched. This script:
  1) Reads the existing "<Mon>_ZSDR002" tab.
  2) Strips out every column that's a known HELPER column (see
     workflows_common.HELPER_COLUMN_NAMES), leaving only the original
     ZSDR002 source columns, in their original order.
  3) Re-runs process_data() on that original-only data against the
     month's existing "<Mon>_TMS" tab and the CURRENT master data.
  4) Writes the result back (original columns unchanged + freshly
     computed helper columns), and regenerates
     "<MON>_ZSDR002_ShipLevel" from the refreshed SKU-level data.
  5) Upserts every refreshed month's ShipLevel rows into a single
     "ZSDR002_Consolidate" tab (on the monthly Sheet), keyed by
     (Month, Shipment_ID). Only the month(s) refreshed this run are
     touched there — matching shipments are updated, new ones inserted,
     and shipments that no longer appear in that month's ShipLevel tab
     are dropped from the consolidated tab. Every other month's rows
     are left untouched.
  6) FINAL STEP: mirrors that now-current consolidated data into a
     "ZSDR_Abadi" tab on the PRODUCTION Sheet (full replace copy, since
     step 5 already produced the correct complete state).

If a month's "<Mon>_TMS" or "<Mon>_ZSDR002" tab doesn't exist (or is
empty), that month is skipped with a warning rather than failing the
whole run — every other requested month still gets refreshed.

Required environment variables:
    GOOGLE_CREDENTIALS       Service account JSON (shared repo secret).
    MONTHLY_TARGET_SHEET_ID  Spreadsheet ID of the monthly Sheet.
    TARGET_SHEET_ID           Spreadsheet ID of the production Sheet
                              (master data source).
    REFRESH_MONTH             Optional. A single month key (e.g. "Aug")
                              to refresh just that month. If unset or
                              empty, EVERY month that currently has a
                              "<Mon>_ZSDR002" tab is refreshed.
"""
import os
import sys

from workflows_common import (
    logger, MONTH_ABBR, HELPER_COLUMN_NAMES, get_env, authenticate_services,
    load_master_data, preprocess_tms, process_data, create_shipment_report,
    get_or_create_worksheet, export_dataframe, read_tab_as_df,
    consolidate_shipment_reports, mirror_consolidate_to_production
)


def main():
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

    logger.info(f"================ Refreshing helper columns: {', '.join(months_to_process)} ================")

    # Load master data ONCE, fresh, shared across every month refreshed
    # this run — this is the whole point: pick up whatever changed.
    dict_masters = load_master_data(gc, production_sheet_id)

    refreshed = []
    skipped = []

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

        # Strip any previously-computed helper columns so we start from
        # ONLY the original ZSDR002 data — this is what guarantees the
        # "regular data stays intact" requirement: nothing in this
        # script can alter an original value, it can only recompute the
        # helper columns derived from it.
        original_cols = [c for c in df_sku_existing.columns if c not in HELPER_COLUMN_NAMES]
        dropped_cols = [c for c in df_sku_existing.columns if c in HELPER_COLUMN_NAMES]
        logger.info(f"[{month_abbr}] Keeping {len(original_cols)} original column(s), dropping {len(dropped_cols)} stale helper column(s) before recompute.")
        df_zsdr_original_only = df_sku_existing[original_cols].copy()

        df_zsdr_refreshed = process_data(df_zsdr_original_only, df_tms_clean, dict_masters)
        df_ship_refreshed = create_shipment_report(df_zsdr_refreshed)

        ws_sku = get_or_create_worksheet(monthly_sheet, sku_tab_name)
        export_dataframe(ws_sku, df_zsdr_refreshed, sku_tab_name)

        ws_ship = get_or_create_worksheet(monthly_sheet, ship_tab_name)
        export_dataframe(ws_ship, df_ship_refreshed, ship_tab_name)

        refreshed.append(month_abbr)

    # Final steps:
    #   1) Upsert the freshly refreshed month(s)' ShipLevel data into the
    #      single 'ZSDR002_Consolidate' tab (monthly Sheet). Only the
    #      month(s) actually refreshed this run are touched — every
    #      other month's rows already there are left as-is.
    #   2) Mirror that now-current consolidated data into the 'ZSDR_Abadi'
    #      tab on the PRODUCTION Sheet, so it's available there too.
    if refreshed:
        df_consolidated = consolidate_shipment_reports(monthly_sheet, refreshed)
        mirror_consolidate_to_production(gc, production_sheet_id, df_consolidated)
    else:
        logger.warning("No months were successfully refreshed — skipping shipment-level consolidation and production mirror steps.")

    logger.info("================ Refresh complete ================")
    logger.info(f"Refreshed: {', '.join(refreshed) if refreshed else '(none)'}")
    if skipped:
        logger.warning(f"Skipped (missing/empty data): {', '.join(skipped)}")


if __name__ == "__main__":
    main()
