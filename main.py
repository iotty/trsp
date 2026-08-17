import os
import io
import json
import logging
from datetime import datetime, timedelta, time
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ==========================================
# 0. KONFIGURASI PENGEMBANGAN
# ==========================================
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO, 
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 1. FUNGSI AUTENTIKASI DAN KREDENSIAL
# ==========================================
def get_secret(key):
    try:
        return os.environ[key]
    except KeyError:
        logger.error(f"Gagal memuat secret '{key}'. Pastikan sudah disetel di menu Settings > Secrets and variables > Actions.")
        raise

def authenticate_services():
    logger.info("Memulai autentikasi Google API...")
    try:
        creds_json = json.loads(get_secret('GOOGLE_CREDENTIALS'))
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets', 
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        
        gc = gspread.authorize(creds)
        drive_svc = build('drive', 'v3', credentials=creds)
        logger.info("Autentikasi berhasil.")
        return gc, drive_svc
    except Exception as e:
        logger.error(f"Error Autentikasi: {str(e)}")
        raise

# ==========================================
# 2. FUNGSI EKSTRAKSI MASTER DATA
# ==========================================
def load_master_data(gc, sheet_id):
    logger.info("Mengambil Master Data dari Google Sheets...")
    df_masters = {}
    master_names = ['Master_Truck_Input', 'Master_Days_Input', 'Master_Sales_LGS']
    
    try:
        sheet = gc.open_by_key(sheet_id)
        for name in master_names:
            try:
                ws = sheet.worksheet(name)
                df_masters[name] = pd.DataFrame(ws.get_all_records())
                logger.info(f"Master [{name}] berhasil dimuat ({len(df_masters[name])} baris).")
            except gspread.exceptions.WorksheetNotFound:
                logger.warning(f"Tab Master [{name}] tidak ditemukan. Dilewati.")
                df_masters[name] = pd.DataFrame() 
        return df_masters
    except Exception as e:
        logger.error(f"Gagal mengakses Spreadsheet ID. Error: {str(e)}")
        raise

# ==========================================
# 3A. FUNGSI FORMAT WAKTU KE WIB (UTC+7)
# ==========================================
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

# ==========================================
# 3B. FUNGSI UNDUH DRIVE & METADATA
# ==========================================
def download_latest_file(drive_svc, folder_id, prefix, skip_rows=0):
    logger.info(f"Mencari file berawalan '{prefix}' di Drive Staging...")
    try:
        query = f"'{folder_id}' in parents and name contains '{prefix}' and trashed = false"
        # Meminta id, name, dan modifiedTime sekaligus
        results = drive_svc.files().list(
            q=query, orderBy="createdTime desc", fields="files(id, name, modifiedTime)", pageSize=1
        ).execute()
        
        files = results.get('files', [])
        if not files:
            raise FileNotFoundError(f"File dengan awalan {prefix} tidak ditemukan.")
            
        file_id = files[0]['id']
        file_name = files[0]['name']
        modified_time = files[0].get('modifiedTime')
        
        logger.info(f"Ditemukan file terbaru: {file_name}. Memulai proses unduh...")
        
        bio = io.BytesIO()
        request = drive_svc.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(bio, request)
        
        while downloader.next_chunk()[1] is False: pass 
            
        bio.seek(0)
        df = pd.read_excel(bio, engine='openpyxl', skiprows=skip_rows)
        logger.info(f"Proses baca Excel '{prefix}' sukses: {len(df)} baris diekstrak.")
        
        # Mengembalikan DataFrame dan waktu update terakhir
        return df, modified_time
        
    except Exception as e:
        logger.error(f"Gagal mengunduh/membaca data {prefix}: {str(e)}")
        raise

# ==========================================
# 4A. FUNGSI PRE-PROCESSING TMS (WORKFLOW 0)
# ==========================================
def preprocess_tms(df_tms):
    logger.info("Memulai standarisasi format tanggal pada data TMS...")
    try:
        tms_date_cols = [
            'Shipment Date', 'Request Date', 'Delivery Date', 'Tender Date', 'Accept Date', 
            'Assign Date', 'Confirm Date', 'Register Date', 'Inbound Date', 'Loading Date', 
            'GI Date', 'Intransit Date', 'Arrive Date', 'Unloading Date', 'Delivered Date', 
            'Gr Date', 'CancelDate', 'CreateDate'
        ]
        
        existing_cols = [c for c in tms_date_cols if c in df_tms.columns]
        df_tms[existing_cols] = df_tms[existing_cols].apply(pd.to_datetime, dayfirst=True, errors='coerce')
        return df_tms
    except Exception as e:
        logger.error(f"Gagal saat standarisasi tanggal TMS: {str(e)}")
        raise

# ==========================================
# 4B. FUNGSI PEMROSESAN DATA ZSDR SKU-LEVEL
# ==========================================
def process_data(df_zsdr, df_tms, df_masters):
    logger.info("Memulai pembersihan format tanggal dan waktu ZSDR...")
    try:
        d_cols = [c for c in ['Release Date', 'SO Create Date', 'DO Create Date', 'Schedule Date', 'Act. Gds Mvmnt Date'] if c in df_zsdr.columns]
        t_cols = [c for c in ['Release Time', 'SO Create Time', 'DO Create Time'] if c in df_zsdr.columns]
        
        df_zsdr[d_cols] = df_zsdr[d_cols].apply(pd.to_datetime, dayfirst=True, errors='coerce')
        for c in t_cols: 
            df_zsdr[c] = pd.to_datetime(df_zsdr[c], format='%H:%M:%S', errors='coerce').dt.time
            
        logger.info("Mengkalkulasi Helper Columns (Vektorisasi)...")
        
        df_tms['Delivery No'] = df_tms['Delivery No'].astype(str).str.strip()
        df_zsdr['Delivery'] = df_zsdr['Delivery'].astype(str).str.strip()
        
        tms_map = df_tms.drop_duplicates('Delivery No').set_index('Delivery No')
        truck_cat_map = df_masters.get('Master_Truck_Input', pd.DataFrame()).drop_duplicates('Vendor').set_index('Vendor')['Cat'] if not df_masters.get('Master_Truck_Input', pd.DataFrame()).empty else {}
        
        df_sales_master = df_masters.get('Master_Sales_LGS', pd.DataFrame())
        if not df_sales_master.empty and 'Sales' in df_sales_master.columns and 'LGS' in df_sales_master.columns:
            df_sales_master['Sales_Clean'] = df_sales_master['Sales'].astype(str).str.strip().str.upper()
            sales_lgs_map = df_sales_master.drop_duplicates('Sales_Clean').set_index('Sales_Clean')['LGS']
        else:
            sales_lgs_map = {}

        df_zsdr['Type of Truck'] = df_zsdr['Delivery'].map(tms_map['Trucktype Name'] if 'Trucktype Name' in tms_map else {}).fillna('')
        df_zsdr['SM'] = df_zsdr['Delivery'].map(tms_map['Shipment No'] if 'Shipment No' in tms_map else {}).fillna('')
        
        pic_clean = df_zsdr.get('Sales PIC Name', pd.Series(dtype=str)).astype(str).str.strip().str.upper()
        pic_clean = pic_clean.replace('NAN', '')
        df_zsdr['Sales LGS'] = pic_clean.map(sales_lgs_map).fillna('Belum ada PIC') 
        
        for col in ['Tender Date', 'Accept Date', 'Register Date', 'Loading Date']:
            df_zsdr[col] = df_zsdr['Delivery'].map(tms_map[col] if col in tms_map else {})

        df_zsdr['Is SA2,SA4,SB'] = np.where(df_zsdr['SD Document'].astype(str).str.startswith(('SA2', 'SA4', 'SB')), 'Yes', 'No')
        
        mat_ok = df_zsdr['Material'].notna() & (df_zsdr['Material'] != '')
        df_zsdr['Are blocked'] = np.where(~mat_ok, '', np.where(df_zsdr['Block'].notna(), 'Blocked', 'Not Blocked'))

        do_pack = df_zsdr['DO Means of Pack'].fillna('').astype(str).str.lower()
        mat_group = df_zsdr['Material Group'].fillna('').astype(str).str.lower()
        is_scrap = do_pack.str.contains('scr') | mat_group.str.contains('bfr|mtbe')
        df_zsdr['Is Scrap'] = np.where((do_pack == '') & (mat_group == ''), '', np.where(is_scrap, 'SCRAP/BFR/MTBE', 'NOT SCRAP'))

        is_export = df_zsdr['District Name'].fillna('').astype(str).str.lower() == 'export'
        not_scrap = df_zsdr['Is Scrap'] == 'NOT SCRAP'
        in_tms = df_zsdr['Delivery'].isin(tms_map.index)
        
        mapped_carrier = df_zsdr['Delivery'].map(tms_map['Carrier Name'] if 'Carrier Name' in tms_map else {}).fillna('Not assigned yet').replace('', 'Not assigned yet')
        
        df_zsdr['Vendor'] = ''
        df_zsdr.loc[is_export, 'Vendor'] = 'Export'
        df_zsdr.loc[not_scrap & ~is_export & in_tms, 'Vendor'] = mapped_carrier[not_scrap & ~is_export & in_tms]

        def get_v_type(v): return '' if pd.isna(v) or v in ('', 'Not assigned yet') else truck_cat_map.get(v, 'Vendor Truck')
        df_zsdr['Truck Vendor Type'] = df_zsdr['Vendor'].map(get_v_type)

        truck_type = df_zsdr['Type of Truck'].str.lower()
        sales_dist = df_zsdr['Sales District'].fillna('').astype(str)
        
        df_zsdr['TRAIN/TRUCK'] = np.where(truck_type == '', '', 
                                    np.where(truck_type.str.contains('train'), 'TRAIN', 
                                        np.where((sales_dist == 'OTHERS') & truck_type.str.contains('container'), 'VESSEL', 'TRUCK')))

        df_zsdr['Last Status'] = np.select(
            [df_zsdr['Loading Date'].notna(), df_zsdr['Register Date'].notna(), df_zsdr['Accept Date'].notna(), df_zsdr['Tender Date'].notna()],
            ['Loading', 'Register', 'Accept', 'Tender'], default=''
        )
        
        return df_zsdr
    except Exception as e:
        logger.error(f"Gagal saat memproses data/kalkulasi ZSDR: {str(e)}")
        raise

# ==========================================
# 5A. FUNGSI PEMBUATAN SHIPMENT LEVEL REPORT
# ==========================================
def create_shipment_report(df_zsdr):
    logger.info("Membentuk Shipment Level Report (Workflow 2)...")
    try:
        df_report = df_zsdr.copy()
        cols_ref = ['SM', 'Delivery', 'SD Document', 'Contract Number']
        
        for col in cols_ref:
            if col in df_report.columns:
                df_report[col] = df_report[col].astype(str).str.strip().replace({'': np.nan, 'nan': np.nan, 'None': np.nan})

        bfill_cols = [c for c in cols_ref if c in df_report.columns]
        df_report['Shipment_ID'] = df_report[bfill_cols].bfill(axis=1).iloc[:, 0].fillna('UNASSIGNED')

        if 'Quantity' in df_report.columns:
            df_report['Quantity'] = pd.to_numeric(df_report['Quantity'], errors='coerce').fillna(0)
        if 'Quantity in MT' in df_report.columns:
            df_report['Quantity in MT'] = pd.to_numeric(df_report['Quantity in MT'], errors='coerce').fillna(0)

        group_cols = ['Shipment_ID'] + [c for c in cols_ref if c in df_report.columns]
        
        agg_dict = {}
        for col in df_report.columns:
            if col in group_cols: continue
            elif col in ['Quantity', 'Quantity in MT']: agg_dict[col] = 'sum'
            else: agg_dict[col] = 'first'

        df_grouped = df_report.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
        return df_grouped
        
    except Exception as e:
        logger.error(f"Gagal membentuk Shipment Level Report: {str(e)}")
        raise

# ==========================================
# 5B. FUNGSI FILTER SHIPPED REPORT (DOMESTICS & EXPORT)
# ==========================================
def create_shipped_domestic(df_sku):
    logger.info("Menerapkan filter untuk Workflow 3 (Shipped Domestics)...")
    try:
        df_dom = df_sku.copy()
        cond_sa = df_dom.get('Is SA2,SA4,SB', pd.Series(dtype=str)) == 'Yes'
        cond_block = df_dom.get('Are blocked', pd.Series(dtype=str)) == 'Not Blocked'
        cond_scrap = df_dom.get('Is Scrap', pd.Series(dtype=str)) == 'NOT SCRAP'
        cond_gi = df_dom['Act. Gds Mvmnt Date'].notna()
        cond_dom_dist = df_dom.get('District Name', pd.Series(dtype=str)).astype(str).str.upper() != 'EXPORT'
        
        df_filtered = df_dom[cond_sa & cond_block & cond_scrap & cond_gi & cond_dom_dist].copy()
        logger.info(f"Shipped Domestics: Tersisa {len(df_filtered)} baris.")
        return df_filtered
    except Exception as e:
        logger.error(f"Gagal memfilter Shipped Report Domestics: {str(e)}")
        raise

def create_shipped_export(df_sku):
    logger.info("Menerapkan filter untuk Workflow 4 (Shipped Export)...")
    try:
        df_exp = df_sku.copy()
        cond_sa = df_exp.get('Is SA2,SA4,SB', pd.Series(dtype=str)) == 'Yes'
        cond_block = df_exp.get('Are blocked', pd.Series(dtype=str)) == 'Not Blocked'
        cond_scrap = df_exp.get('Is Scrap', pd.Series(dtype=str)) == 'NOT SCRAP'
        cond_gi = df_exp['Act. Gds Mvmnt Date'].notna()
        cond_exp_dist = df_exp.get('District Name', pd.Series(dtype=str)).astype(str).str.upper() == 'EXPORT'
        
        df_filtered = df_exp[cond_sa & cond_block & cond_scrap & cond_gi & cond_exp_dist].copy()
        logger.info(f"Shipped Export: Tersisa {len(df_filtered)} baris.")
        return df_filtered
    except Exception as e:
        logger.error(f"Gagal memfilter Shipped Report Export: {str(e)}")
        raise

# ==========================================
# 5C. FUNGSI FILTER BACKLOG REPORT (DOMESTICS & EXPORT)
# ==========================================
def create_backlog_domestic(df_sku):
    logger.info("Menerapkan filter untuk Workflow 5 (Backlog Domestics)...")
    try:
        df_dom = df_sku.copy()
        cond_sa = df_dom.get('Is SA2,SA4,SB', pd.Series(dtype=str)) == 'Yes'
        cond_block = df_dom.get('Are blocked', pd.Series(dtype=str)) == 'Not Blocked'
        cond_scrap = df_dom.get('Is Scrap', pd.Series(dtype=str)) == 'NOT SCRAP'
        cond_gi = df_dom['Act. Gds Mvmnt Date'].isna()
        cond_sched = df_dom['Schedule Date'].notna()
        cond_dom_dist = df_dom.get('District Name', pd.Series(dtype=str)).astype(str).str.upper() != 'EXPORT'
        
        df_filtered = df_dom[cond_sa & cond_block & cond_scrap & cond_gi & cond_sched & cond_dom_dist].copy()
        logger.info(f"Backlog Domestics: Tersisa {len(df_filtered)} baris.")
        return df_filtered
    except Exception as e:
        logger.error(f"Gagal memfilter Backlog Report Domestics: {str(e)}")
        raise

def create_backlog_export(df_sku):
    logger.info("Menerapkan filter untuk Workflow 6 (Backlog Export)...")
    try:
        df_exp = df_sku.copy()
        cond_sa = df_exp.get('Is SA2,SA4,SB', pd.Series(dtype=str)) == 'Yes'
        cond_block = df_exp.get('Are blocked', pd.Series(dtype=str)) == 'Not Blocked'
        cond_scrap = df_exp.get('Is Scrap', pd.Series(dtype=str)) == 'NOT SCRAP'
        cond_gi = df_exp['Act. Gds Mvmnt Date'].isna()
        cond_sched = df_exp['Schedule Date'].notna()
        cond_exp_dist = df_exp.get('District Name', pd.Series(dtype=str)).astype(str).str.upper() == 'EXPORT'
        
        df_filtered = df_exp[cond_sa & cond_block & cond_scrap & cond_gi & cond_sched & cond_exp_dist].copy()
        logger.info(f"Backlog Export: Tersisa {len(df_filtered)} baris.")
        return df_filtered
    except Exception as e:
        logger.error(f"Gagal memfilter Backlog Report Export: {str(e)}")
        raise

# ==========================================
# 6A. FUNGSI PENCATATAN LOG KE SPREADSHEET
# ==========================================
def update_log_file(gc, sheet_id, tms_time_wib, zsdr_time_wib):
    logger.info("Menulis log pembaruan file ke tab 'Log_File'...")
    try:
        sheet = gc.open_by_key(sheet_id)
        worksheet = sheet.worksheet("Log_File")
        
        update_data = [
            ['TMS', tms_time_wib, 'SUCCESS'],
            ['ZSDR', zsdr_time_wib, 'SUCCESS']
        ]
        
        worksheet.update(values=update_data, range_name='A2:C3', value_input_option='USER_ENTERED')
        logger.info("✅ Status log berhasil diperbarui di tab 'Log_File'.")
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("Tab 'Log_File' tidak ditemukan! Lewati penulisan log.")
    except Exception as e:
        logger.error(f"Gagal menulis log ke Sheets: {str(e)}")

# ==========================================
# 6B. FUNGSI EKSPOR FLEKSIBEL & STATIS
# ==========================================
def export_to_sheets(gc, sheet_id, df_export, tab_name, header_row=1, start_col='A', clear_range=None, static_headers=None):
    logger.info(f"Menyiapkan ekspor '{tab_name}' mulai sel {start_col}{header_row}...")
    try:
        sheet = gc.open_by_key(sheet_id)
        ws_target = sheet.worksheet(tab_name)
        
        if static_headers:
            headers = static_headers
            ws_target.update(values=[headers], range_name=f'{start_col}{header_row}', value_input_option='USER_ENTERED')
        else:
            headers = ws_target.row_values(header_row)
            
        if not headers:
            logger.warning(f"Header di tab '{tab_name}' baris {header_row} kosong! Ekspor dibatalkan.")
            return

        export_dict = {}
        for col in headers:
            if col in df_export.columns:
                export_dict[col] = df_export[col]
            else:
                export_dict[col] = "" 
        
        df_final = pd.DataFrame(export_dict)
        
        for col in df_final.columns:
            if pd.api.types.is_datetime64_any_dtype(df_final[col]):
                df_final[col] = df_final[col].dt.strftime('%Y-%m-%d %H:%M:%S').str.replace(' 00:00:00', '')
            elif df_final[col].apply(lambda x: isinstance(x, time)).any():
                df_final[col] = df_final[col].astype(str)
        
        df_final = df_final.fillna("")
        values_to_write = df_final.values.tolist()
        
        data_start_row = header_row + 1
        clear_str = clear_range if clear_range else f'{start_col}{data_start_row}:ZZ'
        logger.info(f"[{tab_name}] Membersihkan data lama ({clear_str})...")
        ws_target.batch_clear([clear_str]) 
        
        if not values_to_write:
            logger.warning(f"Tidak ada data untuk diekspor ke '{tab_name}'.")
            return
            
        logger.info(f"[{tab_name}] Mengirim {len(values_to_write)} baris baru...")
        ws_target.update(values=values_to_write, range_name=f'{start_col}{data_start_row}', value_input_option='USER_ENTERED')
        logger.info(f"Ekspor ke '{tab_name}' Selesai!")
        
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Tab '{tab_name}' tidak ditemukan di Spreadsheet.")
    except Exception as e:
        logger.error(f"Gagal melakukan ekspor data ke '{tab_name}': {str(e)}")

# ==========================================
# 7. ORCHESTRATOR UTAMA
# ==========================================
def main_logistics_pipeline():
    logger.info("================ START PIPELINE ================")
    try:
        target_sheet_id = get_secret('TARGET_SHEET_ID')
        staging_folder_id = get_secret('STAGING_FOLDER_ID')
        
        gc, drive_svc = authenticate_services()
        dict_masters = load_master_data(gc, target_sheet_id)
        
        # Unduh data sekaligus mengambil metadata waktu file
        df_tms_raw, tms_mod_time = download_latest_file(drive_svc, staging_folder_id, 'TMS_', skip_rows=1)
        df_zsdr_raw, zsdr_mod_time = download_latest_file(drive_svc, staging_folder_id, 'ZSDR_', skip_rows=0)
        
        # Konversi waktu ke WIB
        tms_wib = format_time_to_wib(tms_mod_time)
        zsdr_wib = format_time_to_wib(zsdr_mod_time)
        
        # Tulis Log ke Spreadsheet
        update_log_file(gc, target_sheet_id, tms_wib, zsdr_wib)
        
        # Eksekusi Transformasi Data Utama
        df_tms_clean = preprocess_tms(df_tms_raw)
        df_zsdr_sku = process_data(df_zsdr_raw, df_tms_clean, dict_masters)
        df_zsdr_shipment = create_shipment_report(df_zsdr_sku)
        
        # Eksekusi Sub-Filter (Shipped & Backlog)
        df_shipped_dom = create_shipped_domestic(df_zsdr_sku)
        df_shipped_exp = create_shipped_export(df_zsdr_sku)
        df_backlog_dom = create_backlog_domestic(df_zsdr_sku)
        df_backlog_exp = create_backlog_export(df_zsdr_sku)
        
        # Workflow 0: TMS Raw Clean
        logger.info(">>> Menjalankan Workflow 0: Persiapan Data TMS")
        export_to_sheets(gc, target_sheet_id, df_tms_clean, "TMS_Cur_Month")

        # Workflow 1: ZSDR Master
        logger.info(">>> Menjalankan Workflow 1: Current Month ZSDR SKU-Level")
        export_to_sheets(gc, target_sheet_id, df_zsdr_sku, "ZSDR_Cur_Month_SKU")
        
        # Workflow 2: ZSDR Agregasi
        logger.info(">>> Menjalankan Workflow 2: ZSDR Current Month Report")
        export_to_sheets(gc, target_sheet_id, df_zsdr_shipment, "ZSDR_Cur_Month")

        # -----------------------------------------------------------------
        # DAFTAR KOLOM STATIS
        # -----------------------------------------------------------------
        static_cols_shipped = [
            'Master Contract Number', 'Delivery', 'SD Document', 'Contract Number', 
            'DO Means of Pack', 'SO Means of Pack', 'Material', 'Material Group', 
            'Quantity', 'Quantity in MT', 'Ship to Name', 'Schedule Date', 
            'Act. Gds Mvmnt Date', 'Sales District', 'Sold to Name', 
            'Sales PIC Name', 'Unit (MT)'
        ]
        
        static_cols_backlog = [
            'Master Contract Number', 'Delivery', 'SD Document', 'Contract Number', 
            'DO Means of Pack', 'SO Means of Pack', 'Material', 'Material Group', 
            'Quantity', 'Quantity in MT', 'Ship to Name', 'Schedule Date', 
            'Act. Gds Mvmnt Date', 'Sales District', 'Sold to Name', 
            'Sales PIC Name', 'Unit (MT)', 'Vendor', 'SM', 'Last Status', 'Sales LGS'
        ]

        # Workflow 3: Shipped Domestics
        logger.info(">>> Menjalankan Workflow 3: Shipped Report (Domestics)")
        export_to_sheets(gc, target_sheet_id, df_shipped_dom, "Shipped_Report", 
                         header_row=10, start_col='A', clear_range='A11:R', static_headers=static_cols_shipped)
        
        # Workflow 4: Shipped Export
        logger.info(">>> Menjalankan Workflow 4: Shipped Report (Export)")
        export_to_sheets(gc, target_sheet_id, df_shipped_exp, "Shipped_Report", 
                         header_row=10, start_col='S', clear_range='S11:ZZ', static_headers=static_cols_shipped)
        
        # Workflow 5: Backlog Domestics
        logger.info(">>> Menjalankan Workflow 5: Backlog Report (Domestics)")
        export_to_sheets(gc, target_sheet_id, df_backlog_dom, "Backlog_Report", 
                         header_row=11, start_col='A', clear_range='A12:V', static_headers=static_cols_backlog)

        # Workflow 6: Backlog Export
        logger.info(">>> Menjalankan Workflow 6: Backlog Report (Export)")
        export_to_sheets(gc, target_sheet_id, df_backlog_exp, "Backlog_Report", 
                         header_row=11, start_col='W', clear_range='W12:ZZ', static_headers=static_cols_backlog)
        
        logger.info("================ FINISH PIPELINE ===============")
        
    except Exception as e:
        logger.critical(f"PIPELINE GAGAL & DIHENTIKAN. Cek log error di atas. Detail kritis: {str(e)}")

if __name__ == "__main__":
    main_logistics_pipeline()
