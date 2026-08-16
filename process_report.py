import sys
import os

def main():
    print("🚀 Memulai Pipeline Logistik (Mode Uji Coba)...")
    
    # Menyimulasikan penangkapan data dari parameter GitHub Actions
    tms_id = os.environ.get('TMS_FILE_ID', 'Tidak ada ID TMS')
    zsdr_id = os.environ.get('ZSDR_FILE_ID', 'Tidak ada ID ZSDR')
    
    print(f"📦 Menerima sinyal TMS ID  : {tms_id}")
    print(f"📦 Menerima sinyal ZSDR ID : {zsdr_id}")
    
    print("✅ Tahap 1: Koneksi ke Google Drive (Simulasi Sukses)")
    print("✅ Tahap 2: Pembacaan Master Data Google Sheets (Simulasi Sukses)")
    print("✅ Tahap 3: Pemrosesan Pandas (Simulasi Sukses)")
    print("🎉 SELURUH PROSES BERJALAN LANCAR!")
    
    # sys.exit(0) menginstruksikan kepada server bahwa program sukses tanpa error
    sys.exit(0) 

if __name__ == "__main__":
    main()
