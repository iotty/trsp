"""
check_staged_files.py
-----------------------------------------------------------------------
First real step of the "File Abadi" pipeline (previously a pure dummy).

This does NOT process any data yet. It only confirms that GitHub Actions
can authenticate to Google Drive and successfully locate the ZSDR002 and
TMS files staged by the Apps Script interface for a given month, then
echoes a clear PASS/FAIL per file type.

Naming convention it looks for (set by Code.gs / prepareUpload()):
    ZSDR002_<MONTH>_<DATEUPLOADED yyyyMMdd>_<CODEUNIQUE>.<ext>
    TMS_<MONTH>_<DATEUPLOADED yyyyMMdd>_<CODEUNIQUE>.<ext>

Because Code.gs's finalizeUpload() always trashes older files for the
same month+type before a new one is confirmed, there should be at most
ONE matching file per type at any time. If more than one turns up here,
that's flagged too, since it means the "only one file per month+type"
guarantee didn't hold (e.g. cleanup failed on a previous run).

Required environment variables:
    GOOGLE_CREDENTIALS         Service account JSON (same secret the
                                existing production pipeline already
                                uses — repo secrets are shared repo-wide).
    MONTHLY_STAGING_FOLDER_ID  Drive folder ID dedicated to this monthly
                                interface (deliberately a DIFFERENT repo
                                secret from the existing pipeline's
                                STAGING_FOLDER_ID, to avoid collisions).
    UPLOAD_MONTH                Month key, e.g. "Aug" — passed through
                                from the workflow_dispatch `month` input.
"""
import os
import sys
import json
import logging

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

FILE_TYPES = ['ZSDR002', 'TMS']


def get_env(key):
    value = os.environ.get(key)
    if not value:
        logger.error(f"Missing required environment variable: {key}")
        sys.exit(1)
    return value


def authenticate_drive():
    creds_json = json.loads(get_env('GOOGLE_CREDENTIALS'))
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return build('drive', 'v3', credentials=creds)


def find_files(drive_svc, folder_id, prefix):
    query = f"'{folder_id}' in parents and name contains '{prefix}' and trashed = false"
    results = drive_svc.files().list(
        q=query, orderBy="createdTime desc",
        fields="files(id, name, modifiedTime)"
    ).execute()
    return results.get('files', [])


def main():
    month = get_env('UPLOAD_MONTH')
    folder_id = get_env('MONTHLY_STAGING_FOLDER_ID')

    logger.info(f"Checking staged files for month: {month}")
    drive_svc = authenticate_drive()

    all_ok = True

    for file_type in FILE_TYPES:
        prefix = f"{file_type}_{month}_"
        try:
            matches = find_files(drive_svc, folder_id, prefix)
        except Exception as e:
            logger.error(f"[{file_type}] Drive access FAILED: {e}")
            all_ok = False
            continue

        if not matches:
            logger.warning(f"[{file_type}] NOT FOUND for {month} (looked for prefix '{prefix}').")
            all_ok = False
        elif len(matches) > 1:
            names = ', '.join(m['name'] for m in matches)
            logger.warning(f"[{file_type}] Found {len(matches)} matching files for {month} — expected exactly 1: {names}")
            all_ok = False
        else:
            f = matches[0]
            logger.info(f"[{file_type}] OK — found '{f['name']}' (modified {f.get('modifiedTime')}).")

    if all_ok:
        logger.info(f"✅ All files for {month} accessed successfully.")
    else:
        logger.warning(f"⚠️ One or more files for {month} could not be confirmed. See warnings above.")

    # Informational only for now — does not fail the job. Flip this to
    # sys.exit(1) on `not all_ok` once this check should block downstream
    # processing steps.
    sys.exit(0)


if __name__ == "__main__":
    main()
