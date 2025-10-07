import streamlit as st
import pandas as pd
import numpy as np
import io
import time
import re

# --- FUNGSI-FUNGSI PEMROSESAN ---
# Tambahkan fungsi ini di bawah baris import Anda
def clean_and_convert_to_numeric(column):
    """Menghapus semua karakter non-digit dan mengubah kolom menjadi numerik."""
    # Pastikan data adalah string sebelum menggunakan .str
    if column.dtype == 'object':
        column = column.str.replace(r'[^\d]', '', regex=True)
    # Ubah ke numerik, ganti error (misal: sel kosong) dengan 0
    return pd.to_numeric(column, errors='coerce').fillna(0)
    
def process_rekap(order_df, income_df, seller_conv_df):
    """
    Fungsi untuk memproses dan membuat sheet 'REKAP'.
    """
    # Menggabungkan Nama Produk yang sama dalam satu No. Pesanan di file order-all
    order_agg = order_df.groupby(['No. Pesanan', 'Nama Produk']).agg({
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first',
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    # Menggabungkan data order yang sudah di-agregasi dengan data income
    # 'No. Pesanan' harus memiliki tipe data yang sama
    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
    
    rekap_df = pd.merge(order_agg, income_df, on='No. Pesanan', how='left')

    # Menggabungkan dengan data seller conversion
    # Buat ringkasan biaya iklan per pesanan
    iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
    rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
    
    # --- PERUBAIKAN 1: Mengganti inplace=True untuk menghindari warning ---
    # rekap_df['Pengeluaran(Rp)'].fillna(0, inplace=True) # Baris lama
    rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0) # Baris baru

    # --- PERUBAIKAN 2: Cek dan buat kolom yang mungkin hilang dari file income ---
    # Daftar kolom penting dari file income yang digunakan dalam perhitungan
    kolom_penting_income = [
        'Voucher Ditanggung Penjual', 
        'Biaya Administrasi', 
        'Biaya Proses Pesanan',
        'Waktu Pesanan Dibuat', # Tambahkan kolom lain yang mungkin hilang
        'Tanggal Dana Dilepaskan',
        'Metode Pembayaran Pembeli'
    ]
    
    for kolom in kolom_penting_income:
        if kolom not in rekap_df.columns:
            # Jika kolom tidak ada, buat kolom baru dan isi dengan 0 atau None
            if kolom in ['Waktu Pesanan Dibuat', 'Tanggal Dana Dilepaskan', 'Metode Pembayaran Pembeli']:
                 rekap_df[kolom] = None # Untuk kolom non-numerik
            else:
                 rekap_df[kolom] = 0 # Untuk kolom numerik/finansial
            st.warning(f"Peringatan: Kolom '{kolom}' tidak ditemukan. Diasumsikan bernilai 0 atau kosong.")
    # --- AKHIR PERBAIKAN 2 ---


    # Membuat kolom-kolom baru sesuai aturan
    rekap_df['Biaya Layanan 2%'] = rekap_df['Total Harga Produk'] * 0.02
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = rekap_df['Total Harga Produk'] * 0.045
    
    # Menghindari pembagian dengan nol
    rekap_df['Biaya Proses Pesanan (Per Produk)'] = rekap_df.apply(
        lambda row: row['Biaya Proses Pesanan'] / row['Jumlah Terjual'] if row['Jumlah Terjual'] != 0 else 0,
        axis=1
    )

    # Kalkulasi Penjualan Netto (Sekarang sudah aman dari KeyError)
    rekap_df['Penjualan Netto'] = (
        rekap_df['Total Harga Produk'] -
        rekap_df['Voucher Ditanggung Penjual'].fillna(0) -
        rekap_df['Pengeluaran(Rp)'].fillna(0) -
        rekap_df['Biaya Administrasi'].fillna(0) -
        rekap_df['Biaya Layanan 2%'].fillna(0) -
        rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'].fillna(0) -
        rekap_df['Biaya Proses Pesanan (Per Produk)'].fillna(0)
    )

    # Memilih dan menamai ulang kolom untuk output akhir
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['No. Pesanan'],
        'Waktu Pesanan Dibuat': rekap_df['Waktu Pesanan Dibuat'],
        'Waktu Dana Dilepas': rekap_df['Tanggal Dana Dilepaskan'],
        'Nama Produk': rekap_df['Nama Produk'],
        'Jumlah Terjual': rekap_df['Jumlah Terjual'],
        'Harga Satuan': rekap_df['Harga Setelah Diskon'],
        'Total Harga Produk': rekap_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': rekap_df['Voucher Ditanggung Penjual'],
        'Biaya Komisi AMS + PPN Shopee': rekap_df['Pengeluaran(Rp)'],
        'Biaya Adm 8%': rekap_df['Biaya Administrasi'],
        'Biaya Layanan 2%': rekap_df['Biaya Layanan 2%'],
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': rekap_df['Biaya Proses Pesanan (Per Produk)'],
        'Penjualan Netto': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df['Metode Pembayaran Pembeli']
    })

    return rekap_final

def process_iklan(iklan_df):
    """
    Fungsi untuk memproses dan membuat sheet 'IKLAN'.
    """
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
    """
    Mencocokkan nama produk dengan katalog untuk mendapatkan harga beli.
    Aturan: Cocokkan kata pertama dari JUDUL, JENIS KERTAS, dan UKURAN.
    """
    try:
        parts = nama_produk.split()
        judul_key = parts[0]
        
        # Ekstraksi Jenis Kertas dan Ukuran dari Nama Produk
        kertas_key = next((k for k in ['HVS', 'KORAN', 'QPP'] if k in nama_produk.upper()), None)
        ukuran_key = next((u for u in ['A4', 'A5', 'B5'] if u in nama_produk.upper()), None)

        if not kertas_key or not ukuran_key:
            return 0

        # Filter katalog berdasarkan kriteria
        match = katalog_df[
            (katalog_df["JUDUL AL QUR'AN"].str.startswith(judul_key)) &
            (katalog_df["JENIS KERTAS"] == kertas_key) &
            (katalog_df["UKURAN"].str.startswith(ukuran_key))
        ]
        
        if not match.empty:
            return match['KATALOG HARGA'].iloc[0]
        return 0 # Jika tidak ditemukan
    except Exception:
        return 0

def process_summary(rekap_df, iklan_final_df, katalog_df):
    """
    Fungsi untuk memproses dan membuat sheet 'SUMMARY'.
    """
    summary_df = rekap_df.groupby('Nama Produk').agg({
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

    # Gabungkan dengan data iklan
    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']]
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk', right_on='Nama Iklan', how='left')
    summary_df.rename(columns={'Biaya': 'Iklan Klik'}, inplace=True)
    summary_df['Iklan Klik'].fillna(0, inplace=True)
    summary_df.drop('Nama Iklan', axis=1, inplace=True)

    # Kalkulasi ulang Penjualan Netto setelah dikurangi biaya iklan
    summary_df['Penjualan Netto'] = summary_df['Penjualan Netto'] - summary_df['Iklan Klik']

    # Kalkulasi kolom baru
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200
    summary_df['Biaya Ekspedisi'] = 0
    
    # Dapatkan Harga Beli dari Katalog
    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(lambda x: get_harga_beli(x, katalog_df))
    
    summary_df['Harga Custom TLJ'] = 0
    summary_df['Total Pembelian'] = summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    summary_df['Margin Kotor'] = summary_df['Penjualan Netto'] - summary_df['Biaya Packing'] - summary_df['Biaya Ekspedisi'] - summary_df['Total Pembelian']
    
    summary_df['Persentase'] = summary_df.apply(
        lambda row: row['Margin Kotor'] / row['Total Harga Produk'] if row['Total Harga Produk'] != 0 else 0, axis=1)
    
    summary_df['Jumlah Pesanan'] = summary_df.apply(
        lambda row: row['Biaya Proses Pesanan'] / 1250 if 1250 != 0 else 0, axis=1)
    
    summary_df['Penjualan Per Hari'] = summary_df['Penjualan Netto'] / 7
    
    summary_df['Jumlah buku per pesanan'] = summary_df.apply(
        lambda row: row['Jumlah Terjual'] / row['Jumlah Pesanan'] if row['Jumlah Pesanan'] != 0 else 0, axis=1)

    # Re-order dan format kolom
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
        'Penjualan Netto': summary_df['Penjualan Netto'],
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

# Load dataset 'katalog'
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
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Memuat dan membersihkan data
        status_text.text("Membaca dan membersihkan file... (1/5)")
        time.sleep(1)
        order_all_df = pd.read_excel(uploaded_order)
        income_dilepas_df = pd.read_excel(uploaded_income, skiprows=5)
        iklan_produk_df = pd.read_csv(uploaded_iklan, skiprows=7)
        seller_conversion_df = pd.read_csv(uploaded_seller)
        
        # --- MULAI BAGIAN BARU ---
        # Bersihkan dan konversi semua kolom keuangan menjadi numerik
        status_text.text("Membersihkan format angka... (1.5/5)")
        
        # Kolom dari order_all_df
        cols_to_clean_order = ['Harga Setelah Diskon', 'Total Harga Produk']
        for col in cols_to_clean_order:
            if col in order_all_df.columns:
                order_all_df[col] = clean_and_convert_to_numeric(order_all_df[col])
        
        # Kolom dari income_dilepas_df
        cols_to_clean_income = ['Voucher Ditanggung Penjual', 'Biaya Administrasi', 'Biaya Proses Pesanan']
        for col in cols_to_clean_income:
            if col in income_dilepas_df.columns:
                income_dilepas_df[col] = clean_and_convert_to_numeric(income_dilepas_df[col])
        
        # Kolom dari iklan_produk_df
        cols_to_clean_iklan = ['Biaya', 'Omzet Penjualan']
        for col in cols_to_clean_iklan:
            if col in iklan_produk_df.columns:
                iklan_produk_df[col] = clean_and_convert_to_numeric(iklan_produk_df[col])
                
        # Kolom dari seller_conversion_df
        if 'Pengeluaran(Rp)' in seller_conversion_df.columns:
            seller_conversion_df['Pengeluaran(Rp)'] = clean_and_convert_to_numeric(seller_conversion_df['Pengeluaran(Rp)'])
        
        # --- AKHIR BAGIAN BARU ---
        
        progress_bar.progress(20)

        # Step 2: Proses sheet REKAP
        status_text.text("Menyusun sheet 'REKAP'... (2/5)")
        rekap_processed = process_rekap(order_all_df, income_dilepas_df, seller_conversion_df)
        progress_bar.progress(40)
        
        # Step 3: Proses sheet IKLAN
        status_text.text("Menyusun sheet 'IKLAN'... (3/5)")
        iklan_processed = process_iklan(iklan_produk_df)
        progress_bar.progress(60)

        # Step 4: Proses sheet SUMMARY
        status_text.text("Menyusun sheet 'SUMMARY'... (4/5)")
        summary_processed = process_summary(rekap_processed, iklan_processed, katalog_df)
        progress_bar.progress(80)

        # Step 5: Membuat file Excel output
        status_text.text("Menyiapkan file output untuk diunduh... (5/5)")
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
        progress_bar.progress(100)
        status_text.success("✅ Proses Selesai! File Anda siap diunduh.")

        # Area Download
        st.header("3. Download Hasil")
        st.download_button(
            label="📥 Download File Output (Rekapanku.xlsx)",
            data=output,
            file_name="Rekapanku_Output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("Silakan unggah semua 4 file yang diperlukan untuk memulai proses.")
