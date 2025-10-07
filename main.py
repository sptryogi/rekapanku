import streamlit as st
import pandas as pd
import numpy as np
import io
import time
import re

# --- FUNGSI-FUNGSI PEMROSESAN ---

def clean_and_convert_to_numeric(column):
    """Menghapus semua karakter non-digit (kecuali titik dan minus) dan mengubah kolom menjadi numerik."""
    # Pastikan data adalah string sebelum menggunakan .str
    if column.dtype == 'object':
        # Menghapus 'Rp', spasi, dan titik ribuan. Mempertahankan koma desimal jika ada.
        column = column.astype(str).str.replace(r'[^\d,\-]', '', regex=True)
        # Mengganti koma desimal dengan titik
        column = column.str.replace(',', '.', regex=False)
    # Ubah ke numerik, ganti error (misal: sel kosong) dengan 0
    return pd.to_numeric(column, errors='coerce').fillna(0)

def process_rekap(order_df, income_df, seller_conv_df):
    """
    Fungsi untuk memproses dan membuat sheet 'REKAP' dengan file 'income' sebagai data utama.
    """
    # 1. Agregasi data dari order-all
    # Menggabungkan Nama Produk yang sama dalam satu No. Pesanan
    order_agg = order_df.groupby('No. Pesanan').agg({
        'Nama Produk': lambda x: ' | '.join(x.unique()), # Gabungkan nama produk jika beda
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first', # Ambil harga satuan pertama
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    # 2. Jadikan income_df sebagai tabel utama (LEFT table)
    # Pastikan tipe data 'No. Pesanan' sama untuk merge
    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
    
    # Gabungkan income_df dengan order_agg. Semua pesanan di income akan ada.
    rekap_df = pd.merge(income_df, order_agg, on='No. Pesanan', how='left')

    # 3. Gabungkan dengan data seller conversion
    iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
    rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
    rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0)
    
    # 4. Buat kolom-kolom baru sesuai aturan
    # Menggunakan .get() untuk keamanan jika kolom tidak ada setelah merge
    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0)
    
    # CATATAN: Rumus ini sesuai permintaan, tapi tidak umum.
    # Biasanya: Total Harga * Persentase.
    # Permintaan: (Total Harga * 6.5%) * 2%
    rekap_df['Biaya Layanan 2%'] = (rekap_df['Total Harga Produk'] * 0.065) * 0.02
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = (rekap_df['Total Harga Produk'] * 0.065) * 0.045
    
    # Menghindari pembagian dengan nol
    rekap_df['Biaya Proses Pesanan (Per Produk)'] = rekap_df.apply(
        lambda row: row.get('Biaya Proses Pesanan', 0) / row.get('Jumlah Terjual', 1) if row.get('Jumlah Terjual', 0) != 0 else 0,
        axis=1
    )

    # 5. Kalkulasi Penjualan Netto
    rekap_df['Penjualan Netto'] = (
        rekap_df.get('Total Harga Produk', 0) -
        rekap_df.get('Voucher dari Penjual', 0) -
        rekap_df.get('Pengeluaran(Rp)', 0) -
        rekap_df.get('Biaya Administrasi', 0) -
        rekap_df.get('Biaya Layanan 2%', 0) -
        rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0) -
        rekap_df.get('Biaya Proses Pesanan (Per Produk)', 0)
    )

    # 6. Pilih, ganti nama, dan urutkan kolom untuk output akhir
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['No. Pesanan'],
        'Waktu Pesanan Dibuat': rekap_df['Waktu Pesanan Dibuat'],
        'Waktu Dana Dilepas': rekap_df['Tanggal Dana Dilepaskan'],
        'Nama Produk': rekap_df['Nama Produk'],
        'Jumlah Terjual': rekap_df['Jumlah Terjual'],
        'Harga Satuan': rekap_df['Harga Setelah Diskon'],
        'Total Harga Produk': rekap_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': rekap_df['Voucher dari Penjual'],
        'Biaya Komisi AMS + PPN Shopee': rekap_df['Pengeluaran(Rp)'],
        'Biaya Adm 8%': rekap_df['Biaya Administrasi'],
        'Biaya Layanan 2%': rekap_df['Biaya Layanan 2%'],
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': rekap_df['Biaya Proses Pesanan (Per Produk)'],
        'Penjualan Netto': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df['Metode pembayaran pembeli']
    })

    return rekap_final.fillna(0) # Ganti semua NaN yang mungkin tersisa dengan 0

def process_iklan(iklan_df):
    """Fungsi untuk memproses dan membuat sheet 'IKLAN'."""
    # Membersihkan nama iklan dari akhiran 'baris [angka]'
    iklan_df['Nama Iklan Clean'] = iklan_df['Nama Iklan'].str.replace(r'\s*baris\s*\[\d+\]$', '', regex=True).str.strip()
    
    # Agregasi data berdasarkan nama iklan yang sudah dibersihkan
    iklan_agg = iklan_df.groupby('Nama Iklan Clean').agg({
        'Dilihat': 'sum',
        'Jumlah Klik': 'sum',
        'Biaya': 'sum',
        'Produk Terjual': 'sum',
        'Omzet Penjualan': 'sum'
    }).reset_index()
    iklan_agg.rename(columns={'Nama Iklan Clean': 'Nama Iklan'}, inplace=True)

    # Menambahkan baris Total
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
        nama_produk_upper = nama_produk.upper()
        parts = nama_produk.split()
        judul_key = parts[0]
        
        # Ekstraksi Jenis Kertas dan Ukuran dari Nama Produk
        kertas_key = next((k for k in ['HVS', 'KORAN', 'QPP'] if k in nama_produk_upper), None)
        ukuran_key = next((u for u in ['A4', 'A5', 'B5'] if u in nama_produk_upper), None)

        if not kertas_key or not ukuran_key or not judul_key:
            return 0

        # Filter katalog berdasarkan kriteria
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
    # Agregasi data dari sheet REKAP berdasarkan Nama Produk
    summary_df = rekap_df.groupby('Nama Produk').agg({
        'Jumlah Terjual': 'sum',
        'Harga Satuan': 'first', # Harga satuan diasumsikan sama untuk produk yang sama
        'Total Harga Produk': 'sum',
        'Voucher Ditanggung Penjual': 'sum',
        'Biaya Komisi AMS + PPN Shopee': 'sum',
        'Biaya Adm 8%': 'sum',
        'Biaya Layanan 2%': 'sum',
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': 'sum',
        'Biaya Proses Pesanan': 'sum',
        'Penjualan Netto': 'sum'
    }).reset_index()

    # Gabungkan dengan data iklan untuk mendapatkan 'Iklan Klik'
    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']]
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk', right_on='Nama Iklan', how='left')
    summary_df.rename(columns={'Biaya': 'Iklan Klik'}, inplace=True)
    summary_df['Iklan Klik'].fillna(0, inplace=True)
    summary_df.drop('Nama Iklan', axis=1, inplace=True, errors='ignore')

    # Kalkulasi ulang Penjualan Netto setelah dikurangi biaya iklan klik
    summary_df['Penjualan Netto (Setelah Iklan)'] = summary_df['Penjualan Netto'] - summary_df['Iklan Klik']

    # Kalkulasi kolom-kolom baru
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

    # Susun ulang kolom untuk output final
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

# Load dataset 'katalog' dari file lokal
try:
    katalog_df = pd.read_excel('HARGA ONLINE.xlsx')
except FileNotFoundError:
    st.error("Error: File 'HARGA ONLINE.xlsx' tidak ditemukan. Pastikan file tersebut berada di direktori yang sama dengan aplikasi ini.")
    st.stop()

# Area Upload File
st.header("1. Impor File Anda")
col1, col2 = st.columns(2)
with col1:
    uploaded_order = st.file_uploader("1. Import file order-all.xlsx", type="xlsx")
    uploaded_income = st.file_uploader("2. Import file income dilepas.xlsx", type="xlsx")
with col2:
    uploaded_iklan = st.file_uploader("3. Import file iklan produk", type="csv")
    uploaded_seller = st.file_uploader("4. Import file seller conversion", type="csv")

st.markdown("---")

# Tombol Proses
if uploaded_order and uploaded_income and uploaded_iklan and uploaded_seller:
    st.header("2. Mulai Proses")
    if st.button("🚀 Mulai Proses"):
        progress_bar = st.progress(0, text="Mempersiapkan proses...")
        status_text = st.empty()
        
        try:
            # Step 1: Memuat dan membersihkan data (20%)
            status_text.text("Membaca dan membersihkan file...")
            order_all_df = pd.read_excel(uploaded_order)
            income_dilepas_df = pd.read_excel(uploaded_income, skiprows=5)
            iklan_produk_df = pd.read_csv(uploaded_iklan, skiprows=7)
            seller_conversion_df = pd.read_csv(uploaded_seller)
            progress_bar.progress(20, text="File berhasil dimuat. Membersihkan format angka...")

            # --- Membersihkan semua kolom keuangan menjadi numerik ---
            financial_cols = {
                order_all_df: ['Harga Setelah Diskon', 'Total Harga Produk'],
                income_dilepas_df: ['Voucher dari Penjual', 'Biaya Administrasi', 'Biaya Proses Pesanan'],
                iklan_produk_df: ['Biaya', 'Omzet Penjualan'],
                seller_conversion_df: ['Pengeluaran(Rp)']
            }
            for df, cols in financial_cols.items():
                for col in cols:
                    if col in df.columns:
                        df[col] = clean_and_convert_to_numeric(df[col])
            
            # Step 2: Proses sheet REKAP (40%)
            status_text.text("Menyusun sheet 'REKAP'...")
            rekap_processed = process_rekap(order_all_df, income_dilepas_df, seller_conversion_df)
            progress_bar.progress(40, text="Sheet 'REKAP' selesai.")
            
            # Step 3: Proses sheet IKLAN (60%)
            status_text.text("Menyusun sheet 'IKLAN'...")
            iklan_processed = process_iklan(iklan_produk_df)
            progress_bar.progress(60, text="Sheet 'IKLAN' selesai.")

            # Step 4: Proses sheet SUMMARY (80%)
            status_text.text("Menyusun sheet 'SUMMARY'...")
            summary_processed = process_summary(rekap_processed, iklan_processed, katalog_df)
            progress_bar.progress(80, text="Sheet 'SUMMARY' selesai.")

            # Step 5: Membuat file Excel output (100%)
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

            # Area Download
            st.header("3. Download Hasil")
            st.download_button(
                label="📥 Download File Output (Rekapanku.xlsx)",
                data=output,
                file_name="Rekapanku_Output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Terjadi kesalahan saat pemrosesan: {e}")
            st.exception(e) # Menampilkan traceback untuk debugging
else:
    st.info("Silakan unggah semua 4 file yang diperlukan untuk memulai proses.")
