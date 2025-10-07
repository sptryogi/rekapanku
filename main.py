import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

st.title("📊 Rekapanku - Otomatisasi Rekapan Shopee")

st.markdown("Unggah keempat file laporan Shopee berikut untuk diproses menjadi satu file Excel rekap otomatis.")

# Upload section
col1, col2 = st.columns(2)
with col1:
    order_all = st.file_uploader("1️⃣ Upload file order-all.xlsx", type=["xlsx"])
    income_file = st.file_uploader("2️⃣ Upload file income dilepas.xlsx", type=["xlsx"])
with col2:
    iklan_file = st.file_uploader("3️⃣ Upload file iklan produk (CSV atau Excel)", type=["csv", "xlsx"])
    seller_file = st.file_uploader("4️⃣ Upload file seller conversion (CSV atau Excel)", type=["csv", "xlsx"])

katalog_file = st.file_uploader("📁 Upload file katalog.xlsx (untuk lookup Harga Beli)", type=["xlsx"])

# Fungsi helper universal untuk membaca CSV/XLSX
def read_flexible(file, skiprows_guess=0):
    if not file:
        return None
    try:
        if file.name.lower().endswith('.csv'):
            # Deteksi encoding umum biar gak error
            try:
                df = pd.read_csv(file)
            except UnicodeDecodeError:
                df = pd.read_csv(file, encoding='latin1')
        else:
            df = pd.read_excel(file, skiprows=skiprows_guess)
            unnamed_cols = [c for c in df.columns if 'Unnamed' in str(c)]
            if len(unnamed_cols) > len(df.columns) / 2:
                df = pd.read_excel(file, skiprows=skiprows_guess + 1)
        return df
    except Exception as e:
        st.warning(f"Gagal membaca {file.name}: {e}")
        return None


# Button to start processing
if st.button("🚀 Mulai Proses"):
    progress = st.progress(0)
    status = st.empty()

    if not all([order_all, income_file, iklan_file, seller_file, katalog_file]):
        st.error("Mohon unggah semua file terlebih dahulu.")
        st.stop()

    # Step 1: Load all data
    status.text("Memuat semua file...")

    df_order = read_flexible(order_all)
    df_income = read_flexible(income_file, skiprows_guess=5)
    df_iklan = read_flexible(iklan_file, skiprows_guess=7)      # bisa CSV atau XLSX
    df_seller = read_flexible(seller_file)                      # bisa CSV atau XLSX
    df_katalog = read_flexible(katalog_file)

    progress.progress(10)

    # Step 2: Create REKAP sheet (simplified core)
    status.text("Memproses sheet REKAP...")
    rekap = pd.DataFrame()
    rekap['No'] = range(1, len(df_income) + 1)
    rekap['No. Pesanan'] = df_income['No. Pesanan']
    rekap['Waktu Pesanan Dibuat'] = df_income['Waktu Pesanan Dibuat']
    rekap['Waktu Dana Dilepas'] = df_income['Tanggal Dana Dilepaskan']
    rekap['Metode Pembayaran'] = df_income['Metode pembayaran pembeli']
    
    # Merge with order file for product info
    merged = df_order.groupby(['No. Pesanan','Nama Produk'], as_index=False).agg({
        'Jumlah Terjual':'sum',
        'Harga Setelah Diskon':'mean',
        'Total Harga Produk':'sum'
    })

    rekap = pd.merge(rekap, merged, on='No. Pesanan', how='left')

    # Tambah kolom biaya dari file lain
    rekap['Voucher Ditanggung Penjual'] = df_income['Voucher dari Penjual']
    rekap['Biaya Adm 8%'] = df_income['Biaya Administrasi']
    rekap['Biaya Layanan 2%'] = rekap['Total Harga Produk'] * 0.02
    rekap['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = rekap['Total Harga Produk'] * 0.045
    rekap['Biaya Proses Pesanan'] = df_income['Biaya Proses Pesanan'] / rekap['Jumlah Terjual'].replace(0, np.nan)

    # Biaya Komisi AMS + PPN Shopee dari seller conversion
    df_seller_sum = df_seller.groupby(['Kode Pesanan','Nama Produk'], as_index=False)['Pengeluaran(Rp)'].sum()
    rekap = pd.merge(rekap, df_seller_sum, left_on=['No. Pesanan','Nama Produk'], right_on=['Kode Pesanan','Nama Produk'], how='left')
    rekap.rename(columns={'Pengeluaran(Rp)':'Biaya Komisi AMS + PPN Shopee'}, inplace=True)

    # Hitung Penjualan Netto
    rekap['Penjualan Netto'] = (
        rekap['Total Harga Produk'] - rekap['Voucher Ditanggung Penjual'] - rekap['Biaya Komisi AMS + PPN Shopee'] -
        rekap['Biaya Adm 8%'] - rekap['Biaya Layanan 2%'] - rekap['Biaya Layanan Gratis Ongkir Xtra 4,5%'] - rekap['Biaya Proses Pesanan']
    )
    progress.progress(50)

    # Step 3: Sheet IKLAN
    status.text("Memproses sheet IKLAN...")
    df_iklan['Nama Iklan'] = df_iklan['Nama Iklan'].str.replace(r'baris \d+', '', regex=True).str.strip()
    iklan_group = df_iklan.groupby('Nama Iklan', as_index=False).agg({
        'Dilihat':'sum','Jumlah Klik':'sum','Biaya':'sum','Produk Terjual':'sum','Omzet Penjualan':'sum'
    })
    total_row = pd.DataFrame([{
        'Nama Iklan':'TOTAL',
        'Dilihat':iklan_group['Dilihat'].sum(),
        'Jumlah Klik':iklan_group['Jumlah Klik'].sum(),
        'Biaya':iklan_group['Biaya'].sum(),
        'Produk Terjual':iklan_group['Produk Terjual'].sum(),
        'Omzet Penjualan':iklan_group['Omzet Penjualan'].sum()
    }])
    iklan_final = pd.concat([iklan_group, total_row], ignore_index=True)
    progress.progress(70)

    # Step 4: Sheet SUMMARY dengan sistem lookup Harga Beli dari katalog
    status.text("Membangun sheet SUMMARY dengan lookup katalog...")
    summary = rekap.groupby('Nama Produk', as_index=False).agg({
        'Jumlah Terjual':'sum',
        'Harga Setelah Diskon':'mean',
        'Total Harga Produk':'sum',
        'Voucher Ditanggung Penjual':'sum',
        'Biaya Komisi AMS + PPN Shopee':'sum',
        'Biaya Adm 8%':'sum',
        'Biaya Layanan 2%':'sum',
        'Biaya Layanan Gratis Ongkir Xtra 4,5%':'sum',
        'Biaya Proses Pesanan':'sum',
        'Penjualan Netto':'sum'
    })
    summary.insert(0,'No',range(1,len(summary)+1))

    # ==== Sistem Lookup Harga Beli (multi kolom) ====
    def lookup_harga_beli(nama_produk):
        if pd.isna(nama_produk):
            return 0
        nama_upper = str(nama_produk).upper()
        kata_depan = nama_upper.split()[0]

        # Filter awal berdasar kata depan
        match = df_katalog[df_katalog['JUDUL AL QUR\'AN'].str.upper().str.contains(kata_depan, na=False)]
        if match.empty:
            return 0

        # Filter tambahan: cek JENIS KERTAS dan UKURAN di nama produk
        jenis_filter = match['JENIS KERTAS'].apply(lambda x: str(x).upper() in nama_upper)
        ukuran_filter = match['UKURAN'].apply(lambda x: str(x).upper() in nama_upper)

        match_final = match[jenis_filter & ukuran_filter]
        if match_final.empty:
            # fallback ke match awal jika tidak ketemu spesifik
            match_final = match

        # Ambil harga pertama yang valid
        val = match_final['KATALOG HARGA'].iloc[0]
        try:
            return float(val)
        except:
            return 0

    summary['Harga Beli'] = summary['Nama Produk'].apply(lookup_harga_beli)

    # Hitung kolom tambahan
    summary['Harga Custom TLJ'] = 0
    summary['Total Pembelian'] = summary['Jumlah Terjual'] * summary['Harga Beli']
    summary['Margin Kotor'] = summary['Penjualan Netto'] - (summary['Jumlah Terjual']*200) - summary['Total Pembelian']
    summary['Persentase'] = summary['Margin Kotor'] / summary['Total Harga Produk'].replace(0,np.nan)
    summary['Jumlah Pesanan'] = summary['Biaya Proses Pesanan'] / 1250
    summary['Penjualan Per Hari'] = summary['Penjualan Netto'] / 7
    summary['Jumlah buku per pesanan'] = summary['Jumlah Terjual'] / summary['Jumlah Pesanan'].replace(0,np.nan)

    progress.progress(90)

    # Step 5: Save all sheets to Excel
    status.text("Menyimpan hasil akhir ke Excel...")
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary.to_excel(writer, index=False, sheet_name='SUMMARY')
        rekap.to_excel(writer, index=False, sheet_name='REKAP')
        iklan_final.to_excel(writer, index=False, sheet_name='IKLAN')
        df_order.to_excel(writer, index=False, sheet_name='sheet order-all')
        df_income.to_excel(writer, index=False, sheet_name='sheet income dilepas')
        df_iklan.to_excel(writer, index=False, sheet_name='sheet biaya iklan')
        df_seller.to_excel(writer, index=False, sheet_name='sheet seller conversion')

    progress.progress(100)
    status.text("✅ Selesai! File siap diunduh.")

    st.download_button(
        label="📥 Download Hasil Rekapan (Excel)",
        data=output.getvalue(),
        file_name="Rekapanku_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
