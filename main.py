import streamlit as st
import pandas as pd
import numpy as np
import io
import time
import re

# --- FUNGSI-FUNGSI PEMROSESAN ---

def clean_and_convert_to_numeric(column):
    """Menghapus semua karakter non-digit (kecuali titik dan minus) dan mengubah kolom menjadi numerik."""
    if column.dtype == 'object':
        column = column.astype(str).str.replace(r'[^\d,\-]', '', regex=True)
        column = column.str.replace(',', '.', regex=False)
    return pd.to_numeric(column, errors='coerce').fillna(0)

def process_rekap(order_df, income_df, seller_conv_df):
    """
    Fungsi untuk memproses dan membuat sheet 'REKAP' dengan file 'income' sebagai data utama.
    """
    # --- PERUBAIKAN 1: Mengubah agregasi untuk memisahkan produk per pesanan ---
    # Agregasi data dari order-all berdasarkan No. Pesanan DAN Nama Produk
    order_agg = order_df.groupby(['No. Pesanan', 'Nama Produk']).agg({
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first',
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    # Pastikan tipe data 'No. Pesanan' sama untuk merge
    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
    
    # Gabungkan income_df dengan order_agg. Ini akan membuat duplikasi baris income untuk setiap produk.
    rekap_df = pd.merge(income_df, order_agg, on='No. Pesanan', how='left')

    # Gabungkan dengan data seller conversion
    iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
    rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
    rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0)

    # --- PERUBAIKAN 2: Distribusikan biaya per-pesanan HANYA ke baris produk pertama ---
    # Biaya per-pesanan (Voucher, Adm, Iklan, Proses) hanya boleh dihitung sekali per pesanan.
    # Kita akan menampilkannya di baris pertama dan 0 di baris berikutnya untuk pesanan yang sama.
    # 1. Hitung jumlah baris (produk) untuk setiap No. Pesanan.
    #    Ini akan membuat kolom baru berisi angka (misal: 2 jika ada 2 produk).
    jumlah_produk_per_pesanan = rekap_df.groupby('No. Pesanan')['Nama Produk'].transform('count')

    # 2. Bagi 'Biaya Proses Pesanan' asli dengan jumlah produk di atas.
    #    Kita langsung buat kolom 'Biaya Proses Pesanan (Per Produk)' dengan nilai yang sudah dibagi.
    #    .get('Biaya Proses Pesanan', 0) mengambil biaya asli sebelum di-nol-kan.
    rekap_df['Biaya Proses Pesanan (Per Produk)'] = rekap_df.get('Biaya Proses Pesanan', 0) / jumlah_produk_per_pesanan

    # 3. Lanjutkan logika untuk membuat biaya per-pesanan LAINNYA menjadi 0 di baris kedua dst.
    #    'Biaya Proses Pesanan' DIHAPUS dari list ini agar nilainya tidak diubah menjadi 0.
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    # Kolom biaya yang berlaku per pesanan
    order_level_costs = ['Voucher dari Penjual', 'Biaya Administrasi', 'Pengeluaran(Rp)'] # <-- 'Biaya Proses Pesanan' dihapus dari sini
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            # Biaya lain selain Biaya Proses Pesanan akan tetap 0 di baris duplikat
            rekap_df[col] = rekap_df[col] * is_first_item_mask

    # Buat kolom-kolom baru sesuai aturan
    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0)
    
    # Biaya per-produk dihitung untuk setiap baris
    rekap_df['Biaya Layanan 2%'] = rekap_df['Total Harga Produk'] * 0.02
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = rekap_df['Total Harga Produk'] * 0.045
    
    # --- PERUBAIKAN 3: Memastikan semua biaya bernilai positif (absolut) ---
    cost_columns_to_abs = [
        'Voucher dari Penjual', 'Pengeluaran(Rp)', 'Biaya Administrasi', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan (Per Produk)'
    ]
    for col in cost_columns_to_abs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].abs()

    # Kalkulasi Penjualan Netto per baris produk
    rekap_df['Penjualan Netto'] = (
        rekap_df.get('Total Harga Produk', 0) -
        rekap_df.get('Voucher dari Penjual', 0) -
        rekap_df.get('Pengeluaran(Rp)', 0) -
        rekap_df.get('Biaya Administrasi', 0) -
        rekap_df.get('Biaya Layanan 2%', 0) -
        rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0) -
        rekap_df.get('Biaya Proses Pesanan (Per Produk)', 0)
    )

    # Urutkan berdasarkan No. Pesanan untuk memastikan produk dalam pesanan yang sama berkelompok
    rekap_df.sort_values(by='No. Pesanan', inplace=True)
    rekap_df.reset_index(drop=True, inplace=True)
    
    # Buat DataFrame Final
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['No. Pesanan'],
        'Waktu Pesanan Dibuat': rekap_df['Waktu Pesanan Dibuat'],
        'Waktu Dana Dilepas': rekap_df['Tanggal Dana Dilepaskan'],
        'Nama Produk': rekap_df['Nama Produk'],
        'Jumlah Terjual': rekap_df['Jumlah Terjual'],
        'Harga Satuan': rekap_df['Harga Setelah Diskon'],
        'Total Harga Produk': rekap_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': rekap_df.get('Voucher dari Penjual', 0),
        'Biaya Komisi AMS + PPN Shopee': rekap_df.get('Pengeluaran(Rp)', 0),
        'Biaya Adm 8%': rekap_df.get('Biaya Administrasi', 0),
        'Biaya Layanan 2%': rekap_df['Biaya Layanan 2%'],
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': rekap_df['Biaya Proses Pesanan (Per Produk)'],
        'Penjualan Netto': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

    # --- PERUBAIKAN 4: Mengosongkan sel duplikat untuk pesanan multi-produk ---
    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''

    return rekap_final.fillna(0)

def process_iklan(iklan_df):
    """Fungsi untuk memproses dan membuat sheet 'IKLAN'."""
    iklan_df['Nama Iklan Clean'] = iklan_df['Nama Iklan'].str.replace(r'\s*baris\s*\[\d+\]$', '', regex=True).str.strip()
    iklan_df['Nama Iklan Clean'] = iklan_df['Nama Iklan Clean'].str.replace(r'\s*\[\d+\]$', '', regex=True).str.strip()
    
    iklan_agg = iklan_df.groupby('Nama Iklan Clean').agg({
        'Dilihat': 'sum',
        'Jumlah Klik': 'sum',
        'Biaya': 'sum',
        'Produk Terjual': 'sum',
        'Omzet Penjualan': 'sum'
    }).reset_index()
    iklan_agg.rename(columns={'Nama Iklan Clean': 'Nama Iklan'}, inplace=True)

    total_row = pd.DataFrame({
        'Nama Iklan': ['TOTAL'],
        'Dilihat': [iklan_agg['Dilihat'].sum()],
        'Jumlah Klik': [iklan_agg['Jumlah Klik'].sum()],
        'Biaya': [iklan_agg['Biaya'].sum()],
        'Produk Terjual': [iklan_agg['Produk Terjual'].sum()],
        'Omzet Penjualan': [iklan_agg['Omzet Penjualan'].sum()]
    })
    
    iklan_final = pd.concat([iklan_agg, total_row], ignore_index=True)
    return iklan_final

def get_harga_beli(nama_produk, katalog_df):
    """Mencocokkan nama produk dengan katalog untuk mendapatkan harga beli."""
    try:
        if not isinstance(nama_produk, str): return 0
        nama_produk_upper = nama_produk.upper()
        parts = nama_produk.split()
        judul_key = parts[0]
        
        kertas_key = next((k for k in ['HVS', 'KORAN', 'QPP'] if k in nama_produk_upper), None)
        ukuran_key = next((u for u in ['A4', 'A5', 'B5'] if u in nama_produk_upper), None)

        if not kertas_key or not ukuran_key or not judul_key:
            return 0

        match = katalog_df[
            (katalog_df["JUDUL AL QUR'AN"].str.startswith(judul_key, na=False)) &
            (katalog_df["JENIS KERTAS"] == kertas_key) &
            (katalog_df["UKURAN"].str.startswith(ukuran_key, na=False))
        ]
        
        if not match.empty:
            return match['KATALOG HARGA'].iloc[0]
        return 0
    except Exception:
        return 0

def process_summary(rekap_df, iklan_final_df, katalog_df):
    """Fungsi untuk memproses dan membuat sheet 'SUMMARY'."""
    # Karena rekap_df sudah dipecah per produk, groupby('Nama Produk') akan bekerja dengan benar.
    # Kita perlu membuat salinan rekap_df untuk diproses agar tidak mengubah data aslinya
    rekap_copy = rekap_df.copy()
    # Isi kembali No. Pesanan yang kosong agar groupby bisa bekerja jika diperlukan
    rekap_copy['No. Pesanan'] = rekap_copy['No. Pesanan'].replace('', np.nan).ffill()

    summary_df = rekap_copy.groupby('Nama Produk').agg({
        'Jumlah Terjual': 'sum',
        'Harga Satuan': 'first',
        'Total Harga Produk': 'sum',
        'Voucher Ditanggung Penjual': 'sum',
        'Biaya Komisi AMS + PPN Shopee': 'sum',
        'Biaya Adm 8%': 'sum',
        'Biaya Layanan 2%': 'sum',
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': 'sum',
        'Biaya Proses Pesanan': 'sum',
        'Penjualan Netto': 'sum'
    }).reset_index()

    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']]
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk', right_on='Nama Iklan', how='left')
    summary_df.rename(columns={'Biaya': 'Iklan Klik'}, inplace=True)
    summary_df['Iklan Klik'].fillna(0, inplace=True)
    summary_df.drop('Nama Iklan', axis=1, inplace=True, errors='ignore')

    summary_df['Penjualan Netto (Setelah Iklan)'] = summary_df['Penjualan Netto'] - summary_df['Iklan Klik']
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200
    summary_df['Biaya Ekspedisi'] = 0
    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(lambda x: get_harga_beli(x, katalog_df))
    summary_df['Harga Custom TLJ'] = 0
    summary_df['Total Pembelian'] = summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    
    summary_df['Margin Kotor'] = (
        summary_df['Penjualan Netto (Setelah Iklan)'] - 
        summary_df['Biaya Packing'] - 
        summary_df['Biaya Ekspedisi'] - 
        summary_df['Total Pembelian']
    )
    
    summary_df['Persentase'] = summary_df.apply(
        lambda row: row['Margin Kotor'] / row['Total Harga Produk'] if row['Total Harga Produk'] != 0 else 0, axis=1)
    
    summary_df['Jumlah Pesanan'] = summary_df.apply(
        lambda row: row['Biaya Proses Pesanan'] / 1250 if 1250 != 0 else 0, axis=1)
    
    summary_df['Penjualan Per Hari'] = summary_df['Penjualan Netto (Setelah Iklan)'] / 7
    
    summary_df['Jumlah buku per pesanan'] = summary_df.apply(
        lambda row: row['Jumlah Terjual'] / row['Jumlah Pesanan'] if row.get('Jumlah Pesanan', 0) != 0 else 0, axis=1)

    summary_final = pd.DataFrame({
        'No': np.arange(1, len(summary_df) + 1),
        'Nama Produk': summary_df['Nama Produk'],
        'Jumlah Terjual': summary_df['Jumlah Terjual'],
        'Harga Satuan': summary_df['Harga Satuan'],
        'Total Harga Produk': summary_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': summary_df['Voucher Ditanggung Penjual'],
        'Biaya Komisi AMS + PPN Shopee': summary_df['Biaya Komisi AMS + PPN Shopee'],
        'Biaya Adm 8%': summary_df['Biaya Adm 8%'],
        'Biaya Layanan 2%': summary_df['Biaya Layanan 2%'],
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': summary_df['Biaya Proses Pesanan'],
        'Iklan Klik': summary_df['Iklan Klik'],
        'Penjualan Netto': summary_df['Penjualan Netto (Setelah Iklan)'],
        'Biaya Packing': summary_df['Biaya Packing'],
        'Biaya Ekspedisi': summary_df['Biaya Ekspedisi'],
        'Harga Beli': summary_df['Harga Beli'],
        'Harga Custom TLJ': summary_df['Harga Custom TLJ'],
        'Total Pembelian': summary_df['Total Pembelian'],
        'Margin Kotor': summary_df['Margin Kotor'],
        'Persentase': summary_df['Persentase'],
        'Jumlah Pesanan': summary_df['Jumlah Pesanan'],
        'Penjualan Per Hari': summary_df['Penjualan Per Hari'],
        'Jumlah buku per pesanan': summary_df['Jumlah buku per pesanan']
    })
    
    return summary_final

# --- TAMPILAN STREAMLIT ---

st.set_page_config(layout="wide")
st.title("📊 Rekapanku - Sistem Otomatisasi Laporan")

try:
    katalog_df = pd.read_excel('HARGA ONLINE.xlsx')
except FileNotFoundError:
    st.error("Error: File 'HARGA ONLINE.xlsx' tidak ditemukan. Pastikan file tersebut berada di direktori yang sama dengan aplikasi ini.")
    st.stop()

st.header("1. Impor File Anda")
col1, col2 = st.columns(2)
with col1:
    uploaded_order = st.file_uploader("1. Import file order-all.xlsx", type="xlsx")
    uploaded_income = st.file_uploader("2. Import file income dilepas.xlsx", type="xlsx")
with col2:
    uploaded_iklan = st.file_uploader("3. Import file iklan produk", type="csv")
    uploaded_seller = st.file_uploader("4. Import file seller conversion", type="csv")

st.markdown("---")

if uploaded_order and uploaded_income and uploaded_iklan and uploaded_seller:
    st.header("2. Mulai Proses")
    if st.button("🚀 Mulai Proses"):
        progress_bar = st.progress(0, text="Mempersiapkan proses...")
        status_text = st.empty()
        
        try:
            status_text.text("Membaca dan membersihkan file...")
            order_all_df = pd.read_excel(uploaded_order)
            income_dilepas_df = pd.read_excel(uploaded_income, sheet_name='Income', skiprows=5)
            iklan_produk_df = pd.read_csv(uploaded_iklan, skiprows=7)
            seller_conversion_df = pd.read_csv(uploaded_seller)
            progress_bar.progress(20, text="File berhasil dimuat. Membersihkan format angka...")

            financial_data_to_clean = [
                (order_all_df, ['Harga Setelah Diskon', 'Total Harga Produk']),
                (income_dilepas_df, ['Voucher dari Penjual', 'Biaya Administrasi', 'Biaya Proses Pesanan']),
                (iklan_produk_df, ['Biaya', 'Omzet Penjualan']),
                (seller_conversion_df, ['Pengeluaran(Rp)'])
            ]

            for df, cols in financial_data_to_clean:
                for col in cols:
                    if col in df.columns:
                        df[col] = clean_and_convert_to_numeric(df[col])
            
            status_text.text("Menyusun sheet 'REKAP'...")
            rekap_processed = process_rekap(order_all_df, income_dilepas_df, seller_conversion_df)
            progress_bar.progress(40, text="Sheet 'REKAP' selesai.")
            
            status_text.text("Menyusun sheet 'IKLAN'...")
            iklan_processed = process_iklan(iklan_produk_df)
            progress_bar.progress(60, text="Sheet 'IKLAN' selesai.")

            status_text.text("Menyusun sheet 'SUMMARY'...")
            summary_processed = process_summary(rekap_processed, iklan_processed, katalog_df)
            progress_bar.progress(80, text="Sheet 'SUMMARY' selesai.")

            status_text.text("Menyiapkan file output untuk diunduh...")
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                summary_processed.to_excel(writer, sheet_name='SUMMARY', index=False)
                rekap_processed.to_excel(writer, sheet_name='REKAP', index=False)
                iklan_processed.to_excel(writer, sheet_name='IKLAN', index=False)
                order_all_df.to_excel(writer, sheet_name='sheet order-all', index=False)
                income_dilepas_df.to_excel(writer, sheet_name='sheet income dilepas', index=False)
                iklan_produk_df.to_excel(writer, sheet_name='sheet biaya iklan', index=False)
                seller_conversion_df.to_excel(writer, sheet_name='sheet seller conversion', index=False)
            
            output.seek(0)
            progress_bar.progress(100, text="Proses Selesai!")
            status_text.success("✅ Proses Selesai! File Anda siap diunduh.")

            st.header("3. Download Hasil")
            st.download_button(
                label="📥 Download File Output (Rekapanku.xlsx)",
                data=output,
                file_name="Rekapanku_Output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Terjadi kesalahan saat pemrosesan: {e}")
            st.exception(e)
else:
    st.info("Silakan unggah semua 4 file yang diperlukan untuk memulai proses.")
