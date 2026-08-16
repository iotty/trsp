import os
import json
import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

def download_file_from_drive(service, file_id, file_name):
    print(f"📥 Memulai unduhan {file_name} (ID: {file_id})...")
    request = service.files().get_media(fileId=file_id)
    file_io = io.BytesIO()
    downloader = MediaIoBaseDownload(file_io, request)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        if status:
            print(f"   Progress {file_name}: {int(status.progress() * 100)}%")
            
    file_io.seek(0)
    return file_io

def main():
    print("🚀 Memulai proses pipeline logistik riil...")

    tms_id = os.environ.get('TMS_FILE_ID')
    zsdr_id = os.environ.get('ZSDR_FILE_ID')
    
    if not tms_id or not zsdr_id or tms_id == 'SIMULASI_ID_TMS_DARI_APPS_SCRIPT':
        raise ValueError("❌ Error: File ID tidak valid. Pastikan GAS mengirim ID yang benar!")

    print("🔑 Membaca kredensial Service Account...")
    creds_json_str = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json_str:
        raise ValueError("❌ Error: Secret GOOGLE_CREDENTIALS belum disetel di repositori GitHub!")
        
    creds_info = json.loads(creds_json_str)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, 
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    
    drive_service = build('drive', 'v3', credentials=creds)

    try:
        tms_io = download_file_from_drive(drive_service, tms_id, "Data TMS")
        zsdr_io = download_file_from_drive(drive_service, zsdr_id, "Data ZSDR")
    except Exception as e:
        print(f"❌ Error saat mengunduh file dari Drive: {e}")
        raise

    print("📊 Mengonversi file Excel biner ke Pandas DataFrame...")
    try:
        # Menggunakan pd.read_excel dengan engine openpyxl untuk file .xlsx
        df_tms = pd.read_excel(tms_io, engine='openpyxl')
        df_zsdr = pd.read_excel(zsdr_io, engine='openpyxl')
        
        print(f"✅ Sukses membaca TMS Excel: {len(df_tms)} baris, {len(df_tms.columns)} kolom.")
        print(f"✅ Sukses membaca ZSDR Excel: {len(df_zsdr)} baris, {len(df_zsdr.columns)} kolom.")
        
        # --- RUANG LOGIKA DATA LOGISTIK ---
        
        # Contoh kalkulasi sumifs-style untuk Quantity RET (IKU):
        # qty_ret = df_zsdr[
        #     (df_zsdr['SKU_Product'] == 'NAMA_SKU') & 
        #     (df_zsdr['Status'] == 'Pallet Loker/Bin')
        # ]['Quantity'].sum()
        
        # Contoh menghitung statistik Total PO berdasarkan unique fulfillment codes:
        # total_po = df_tms['Fulfillment_Code'].nunique()
        
        # ----------------------------------
        
        print("🎉 TAHAP PENGUNDUHAN DAN PEMBACAAN EXCEL SELESAI SEMPURNA!")

    except Exception as e:
        print(f"❌ Error saat membaca data Excel: {e}")
        raise

if __name__ == "__main__":
    main()
