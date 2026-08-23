"""
check_staged_files.py
-----------------------------------------------------------------------
First real step of the "File Abadi" pipeline (previously a pure dummy).

This does NOT process/transform the ZSDR002 or TMS data itself yet. It
authenticates to Google Drive, looks for the expected file for the given
month, and writes the result directly into the "Log_File" tab of the
monthly Sheet — one row per month (matching column A, e.g. "August"),
with the ZSDR002 status in column B and the TMS status in column C.

Naming convention it looks for (set by Code.gs / prepareUpload()):
    ZSDR002_<MONTH>_<DATEUPLOADED yyyyMMdd>_<CODEUNIQUE>.<ext>
    TMS_<MONTH>_<DATEUPLOADED yyyyMMdd>_<CODEUNIQUE>.<ext>

Because Code.gs's finalizeUpload() always trashes older files for the
same month+type before a new one is confirmed, there should be at most
ONE matching file per type at any time. If more than one turns up here,
that's flagged in the sheet too, since it means the "only one file per
month+type" guarantee didn't hold (e.g. cleanup failed on a previous run).

Required environment variables:
    GOOGLE_CREDENTIALS         Service account JSON (same secret the
                                existing production pipeline already
                                uses — repo secrets are shared repo-wide).
    MONTHLY_STAGING_FOLDER_ID  Drive folder ID dedicated to this monthly
                                interface (a DIFFERENT repo secret from
                                the existing pipeline's STAGING_FOLDER_ID,
                                to avoid collisions).
    MONTHLY_TARGET_SHEET_ID    Spreadsheet ID of the NEW monthly Sheet
                                (its "Log_File" tab is what gets updated
                                here). Also a separate secret from the
                                existing pipeline's TARGET_SHEET_ID.
    UPLOAD_MONTH                Month key, e.g. "Aug" — passed through
                                from the workflow_dispatch `month` input.
"""
import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

FILE_TYPES = ['ZSDR002', 'TMS']
LOG_SHEET_NAME = 'Log_File'

# Column layout on Log_File: A = Month (full name), B = ZSDR002, C = TMS.
COLUMN_FOR_TYPE = {'ZSDR002': 'B', 'TMS': 'C'}

# Code.gs's MONTH_NAMES are 3-letter abbreviations (Jan, Feb, ...), but
# the Log_File sheet's Month column uses full names (January, February,
# ...) per the current sheet layout. Bridge the two here.
MONTH_ABBR_TO_FULL = {
    'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April',
    'May': 'May', 'Jun': 'June', 'Jul': 'July', 'Aug': 'August',
    'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'
}


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


def find_files(drive_svc, folder_id, prefix):
    query = f"'{folder_id}' in parents and name contains '{prefix}' and trashed = false"
    results = drive_svc.files().list(
        q=query, orderBy="createdTime desc",
        fields="files(id, name, modifiedTime)"
    ).execute()
    return results.get('files', [])


def check_file_status(drive_svc, folder_id, file_type, month_abbr):
    """Returns (status_text, is_ok) for one file type/month."""
    prefix = f"{file_type}_{month_abbr}_"
    try:
        matches = find_files(drive_svc, folder_id, prefix)
    except Exception as e:
        logger.error(f"[{file_type}] Drive access FAILED: {e}")
        return f"❌ Drive error: {e}", False

    if not matches:
        logger.warning(f"[{file_type}] NOT FOUND for {month_abbr} (prefix '{prefix}').")
        return "❌ Not found", False
    elif len(matches) > 1:
        names = ', '.join(m['name'] for m in matches)
        logger.warning(f"[{file_type}] Found {len(matches)} matches for {month_abbr} — expected 1: {names}")
        return f"⚠️ {len(matches)} files found (expected 1)", False
    else:
        f = matches[0]
        logger.info(f"[{file_type}] OK — found '{f['name']}'.")
        checked_wib = (datetime.now(timezone.utc) + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
        return f"✅ {f['name']} (checked {checked_wib} WIB)", True


def find_month_row(worksheet, month_full_name):
    """Returns the 1-indexed row number in Log_File matching this month, or None."""
    col_a = worksheet.col_values(1)  # includes header at index 0
    for idx, value in enumerate(col_a, start=1):
        if value.strip().lower() == month_full_name.lower():
            return idx
    return None


def main():
    month_abbr = get_env('UPLOAD_MONTH')
    folder_id = get_env('MONTHLY_STAGING_FOLDER_ID')
    sheet_id = get_env('MONTHLY_TARGET_SHEET_ID')

    month_full_name = MONTH_ABBR_TO_FULL.get(month_abbr)
    if not month_full_name:
        logger.error(f"Unrecognized month key '{month_abbr}' — expected one of {list(MONTH_ABBR_TO_FULL.keys())}.")
        sys.exit(1)

    logger.info(f"Checking staged files for {month_full_name} ({month_abbr})...")
    gc, drive_svc = authenticate_services()

    try:
        sheet = gc.open_by_key(sheet_id)
        worksheet = sheet.worksheet(LOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Tab '{LOG_SHEET_NAME}' not found in the target Sheet.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Could not open target Sheet: {e}")
        sys.exit(1)

    row = find_month_row(worksheet, month_full_name)
    if row is None:
        logger.error(f"Could not find a row for '{month_full_name}' in column A of '{LOG_SHEET_NAME}'.")
        sys.exit(1)

    all_ok = True
    for file_type in FILE_TYPES:
        status_text, is_ok = check_file_status(drive_svc, folder_id, file_type, month_abbr)
        all_ok = all_ok and is_ok

        col = COLUMN_FOR_TYPE[file_type]
        worksheet.update(values=[[status_text]], range_name=f'{col}{row}', value_input_option='USER_ENTERED')
        logger.info(f"Wrote {file_type} status to {col}{row}: {status_text}")

    if all_ok:
        logger.info(f"✅ All files for {month_full_name} accessed successfully and logged.")
    else:
        logger.warning(f"⚠️ One or more files for {month_full_name} could not be confirmed. See Log_File row {row}.")

    # Informational only for now — does not fail the job. Flip this to
    # sys.exit(1) on `not all_ok` once this check should block downstream
    # processing steps.
    sys.exit(0)


if __name__ == "__main__":
    main()
