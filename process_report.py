import os
import json
from datetime import datetime, timedelta
import pandas as pd  # Dipertahankan untuk fase kalkulasi data logistik selanjutnya
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build

def format_time_to_wib(iso_string):
    """Mengonversi waktu bawaan Google Drive (UTC) menjadi format WIB (UTC+7) yang rapi."""
    if not iso_string: 
        return "-"
    try:
        # Format bawaan Drive API: 2026-08-17T08:14:21.123Z
        dt = datetime.strptime(iso_string[:19], "%Y-%m-%dT%H:%M:%S")
        dt_wib = dt + timedelta(hours=7) 
        return dt_wib.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_string

def get_file_status(drive_service, file_id):
    """Menarik metadata file dan mengembalikan waktu update beserta status standar."""
    if not file_id or file_id == 'SIMULASI_ID_TMS_DARI_APPS_SCRIPT':
        return "-", "ERROR: Invalid File ID"
    
    try:
        # Hanya menarik metadata 'modifiedTime', sangat ringan dan cepat
        file_meta = drive_service.files().get(
            fileId=file_id, 
            fields="modifiedTime"
        ).execute()
        
        update_time = format_time_to_wib(file_meta.get("modifiedTime"))
        return update_time, "SUCCESS"
        
    except Exception as e:
        error_msg = str(e)
        if "HttpError 404" in error_msg:
            return "-", "ERROR: File Not Found (404) - Cek hak akses/Share"
        elif "HttpError 403" in error_msg:
            return "-", "ERROR: Permission Denied (403)"
        else:
            return "-", f"ERROR: {error_msg[:30]}..."

def main():
    print("🚀 Mulai pengecekan status file staging...")

    # 1. Menarik Environment Variables (Rahasia)
    tms_id = os.environ.get('TMS_FILE_ID')
    zsdr_id = os.environ.get('ZSDR_FILE_ID')
    target_sheet_id = os.environ.get('TARGET_SHEET_ID')  # Diambil melalui ID (Bukan URL)
    creds_json_str = os.environ.get('GOOGLE_CREDENTIALS')

    # Validasi awal
    if not creds_json_str:
        raise ValueError("❌ ERROR: Kredensial Service Account tidak ditemukan di Environment!")
    if not target_sheet_id:
        raise ValueError("❌ ERROR: TARGET_SHEET_ID tidak ditemukan di Environment!")

    # 2. Autentikasi Google API
    print("🔑 Autentikasi Service Account untuk Drive dan Sheets...")
    creds_info = json.loads(creds_json_str)
    scopes = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    
    drive_service = build('drive', 'v3', credentials=creds)
    gc = gspread.authorize(creds)

    # 3. Pengecekan Status File Drive
    print("Mengecek metadata TMS...")
    tms_time, tms_status = get_file_status(drive_service, tms_id)
    
    print("Mengecek metadata ZSDR...")
    zsdr_time, zsdr_status = get_file_status(drive_service, zsdr_id)

    # 4. Penulisan Log ke Google Sheets
    print(f"Menulis log status ke Spreadsheet ID: {target_sheet_id[:10]}...")
    try:
        # Menggunakan .open_by_key sesuai praktik terbaik
        sheet = gc.open_by_key(target_sheet_id) 
        worksheet = sheet.worksheet("Log_File") 
        
        # Format list-of-lists sesuai tabel pada image_4bb1a8.png
        # (Kolom A: File, Kolom B: Latest_Update, Kolom C: Status)
        update_data = [
            ['TMS', tms_time, tms_status],
            ['ZSDR', zsdr_time, zsdr_status]
        ]
        
        # Mengeksekusi penulisan langsung ke sel A2 sampai C3
        worksheet.update(update_data, 'A2:C3')
        print("✅ Status log berhasil diperbarui di tab 'Log_File'.")
        
    except gspread.exceptions.WorksheetNotFound:
        print("❌ ERROR: Tab bernama 'Log_File' tidak ditemukan di spreadsheet target.")
        import sys
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR saat menulis ke Sheets: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
