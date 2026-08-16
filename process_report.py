import os
import json
import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

def download_file_from_drive(service, file_id, file_name):
    """
    Fungsi untuk mengunduh file dari Drive dalam bentuk chunk biner (sangat aman
    untuk file 8-10MB ke atas tanpa membuat memori crash).
    """
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

    # 1. Menangkap ID File dari Google Apps Script
    tms_id = os.environ.get('TMS_FILE_ID')
    zsdr_id = os.environ.get('ZSDR_FILE_ID')
    
    if not tms_id or not zsdr_id or tms_id == 'SIMULASI_ID_TMS_DARI_APPS_SCRIPT':
        raise ValueError("❌ Error: File ID tidak valid. Pastikan GAS mengirim ID yang benar!")

    # 2. Autentikasi Service Account menggunakan GitHub Secrets
    print("🔑 Membaca kredensial Service Account...")
    creds_json_str = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json_str:
        raise ValueError("❌ Error: Secret GOOGLE_CREDENTIALS belum disetel di repositori GitHub!")
        
    creds_info = json.loads(creds_json_str)
    # Scope ini hanya memberikan akses untuk membaca file Drive, bukan menghapus/mengedit
    creds = service_account.Credentials.from_service_account_info(
        creds_info, 
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    
    # 3. Inisialisasi API Google Drive
    drive_service = build('drive', 'v3', credentials=creds)

    # 4. Eksekusi Pengunduhan File
    try:
        tms_io = download_file_from_drive(drive_service, tms_id, "Data TMS")
        zsdr_io = download_file_from_drive(drive_service, zsdr_id, "Data ZSDR")
    except Exception as e:
        print(f"❌ Error saat mengunduh file dari Drive: {e}")
        print("💡 TIPS: Pastikan Anda sudah 'Share' (Bagikan) file tersebut ke email Service Account Anda!")
        raise

    # 5. Memuat Data ke Pandas
    print("📊 Mengonversi file biner ke Pandas DataFrame...")
    try:
        # Ubah menjadi pd.read_excel(tms_io) jika file aslinya .xlsx
        df_tms = pd.read_csv(tms_io)
        df_zsdr = pd.read_csv(zsdr_io)
        
        print(f"✅ Sukses membaca TMS: {len(df_tms)} baris, {len(df_tms.columns)} kolom.")
        print(f"✅ Sukses membaca ZSDR: {len(df_zsdr)} baris, {len(df_zsdr.columns)} kolom.")
        
        # --- RUANG UNTUK LOGIKA LOGISTIK ANDA ---
        # Nanti Anda bisa memasukkan logika rekap Quantity RET (IKU) berdasarkan SKU Product, 
        # filter Pallet Loker/Bin, hingga unique count kode fulfillment di bagian ini.
        # ----------------------------------------
        
        print("🎉 TAHAP PENGUNDUHAN DAN PEMBACAAN SELESAI SEMPURNA!")

    except Exception as e:
        print(f"❌ Error saat membaca data CSV: {e}")
        raise

if __name__ == "__main__":
    main()
