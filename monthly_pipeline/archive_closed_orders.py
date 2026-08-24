"""
archive_closed_orders.py
-----------------------------------------------------------------------
Scheduled, month-boundary step for the TRSP dashboard: moves CLOSED
rows out of ZSDR_Cur_Month and appends them into ZSDR_Abadi's history.

"Closed" = 'Act. Gds Mvmnt Date' is filled -- the exact same field
your original production main.py already used to split Shipped vs
Backlog (cond_gi = df['Act. Gds Mvmnt Date'].notna()). This is
deliberately NOT the TMS-derived 'GI Date' column that Config.gs's
ZCOL.GI_DATE happens to also name -- that's a different field used for
other purposes on the dashboard.

Only rows tagged with a Month OTHER than the current month are
candidates for archiving -- a row from the current month stays in
ZSDR_Cur_Month even if it's already closed, matching Config.gs's
description of ZSDR_Cur_Month as "month-to-date, open or closed".
Rows that are STILL OPEN are left in ZSDR_Cur_Month regardless of
their Month tag, so an order isn't lost just because the calendar
moved on -- it stays visible until it actually closes, then gets swept
into ZSDR_Abadi on a later run of this script.

Intended to run on a schedule (e.g. shortly after the start of each
new month), but it's idempotent and safe to re-run any time: rows
already moved out of ZSDR_Cur_Month simply won't be there to move
again on the next run.

ZSDR_Abadi is APPENDED to, never cleared/overwritten -- it's a running
history. Only columns that already exist in ZSDR_Abadi's own header
row are written (matching how its report shape has always been
defined by that header, not by the code -- see
export_dataframe_header_driven in workflows_common.py); anything else
computed is dropped. ZSDR_Abadi must already have its header row set
up by hand -- if it's empty, this script aborts with a warning rather
than guessing at a header.

Required environment variables:
    GOOGLE_CREDENTIALS   Service account JSON.
    TARGET_SHEET_ID      The TRSP production spreadsheet's ID.

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

import pandas as pd

from workflows_common import (
    logger, MONTH_ABBR, get_env, authenticate_services, read_tab_as_df,
    get_or_create_worksheet, export_dataframe_header_driven,
)

CUR_MONTH_TAB_NAME = 'ZSDR_Cur_Month'
ABADI_TAB_NAME = 'ZSDR_Abadi'
CLOSED_COLUMN = 'Act. Gds Mvmnt Date'


def current_month_abbr():
    tz_name = os.environ.get('PIPELINE_TIMEZONE', 'Asia/Jakarta')
    now = datetime.now(ZoneInfo(tz_name)) if ZoneInfo else datetime.utcnow()
    return MONTH_ABBR[now.month - 1]


def main():
    sheet_id = get_env('TARGET_SHEET_ID')
    gc, _drive_svc = authenticate_services()
    sheet = gc.open_by_key(sheet_id)

    df_cur = read_tab_as_df(sheet, CUR_MONTH_TAB_NAME)
    if df_cur is None or df_cur.empty:
        logger.warning(f"'{CUR_MONTH_TAB_NAME}' is missing or empty -- nothing to archive.")
        return

    if 'Month' not in df_cur.columns:
        logger.error(f"'{CUR_MONTH_TAB_NAME}' has no 'Month' column -- can't tell which rows are from a past month. Aborting.")
        return
    if CLOSED_COLUMN not in df_cur.columns:
        logger.error(f"'{CUR_MONTH_TAB_NAME}' has no '{CLOSED_COLUMN}' column -- can't tell which rows are closed. Aborting.")
        return

    this_month = current_month_abbr()

    is_past_month = df_cur['Month'].astype(str) != this_month
    is_closed = df_cur[CLOSED_COLUMN].notna() & (df_cur[CLOSED_COLUMN].astype(str).str.strip() != '')

    to_archive = df_cur[is_past_month & is_closed].copy()
    to_keep = df_cur[~(is_past_month & is_closed)].copy()

    logger.info(
        f"'{CUR_MONTH_TAB_NAME}': current month is '{this_month}'. "
        f"{len(to_archive)} closed row(s) from a past month -> archiving to '{ABADI_TAB_NAME}'; "
        f"{len(to_keep)} row(s) staying (current month, and/or still open)."
    )

    if to_archive.empty:
        logger.info("Nothing to archive this run.")
        return

    ws_abadi = get_or_create_worksheet(sheet, ABADI_TAB_NAME)
    existing_abadi_header = ws_abadi.row_values(1)
    if not existing_abadi_header:
        logger.warning(f"'{ABADI_TAB_NAME}' has no header row yet -- can't append. Set up its header row by hand first, then re-run.")
        return

    missing_from_archive = [c for c in existing_abadi_header if c not in to_archive.columns]
    if missing_from_archive:
        logger.warning(f"'{ABADI_TAB_NAME}': header column(s) {missing_from_archive} not found in the archived rows -- written blank.")
    dropped_cols = [c for c in to_archive.columns if c not in existing_abadi_header]
    if dropped_cols:
        logger.info(f"'{ABADI_TAB_NAME}': {len(dropped_cols)} column(s) from ZSDR_Cur_Month not in its header are not archived: {dropped_cols}")

    export_dict = {col: (to_archive[col] if col in to_archive.columns else "") for col in existing_abadi_header}
    df_append = pd.DataFrame(export_dict).fillna("")

    # RAW, not USER_ENTERED: these values are already the plain,
    # already-formatted text ZSDR_Cur_Month was written with (dates
    # round-tripped back as literal text, not real Sheets Date values --
    # see read_tab_as_df / export_dataframe_header_driven). USER_ENTERED
    # would let Sheets reinterpret a date-looking string as an actual
    # Date value on entry, which is exactly the ambiguous round-trip
    # problem the apostrophe-prefix trick exists to avoid in the first
    # place -- RAW just stores what's given, no reinterpretation.
    ws_abadi.append_rows(df_append.values.tolist(), value_input_option='RAW')
    logger.info(f"'{ABADI_TAB_NAME}': appended {len(df_append)} row(s).")

    ws_cur = get_or_create_worksheet(sheet, CUR_MONTH_TAB_NAME)
    export_dataframe_header_driven(ws_cur, to_keep, CUR_MONTH_TAB_NAME)
    logger.info(f"'{CUR_MONTH_TAB_NAME}': rewritten with {len(to_keep)} remaining row(s).")


if __name__ == "__main__":
    main()
