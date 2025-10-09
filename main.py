import streamlit as st
st.set_page_config(
    page_title="Rekapanku",           # judul di tab browser
    page_icon="📊",                   # emoji atau file ikon (.png/.ico)
    layout="wide"
)
from datetime import datetime
import pandas as pd
import numpy as np
import io
import time
import re
from rapidfuzz import fuzz


# --- FUNGSI-FUNGSI PEMROSESAN ---

def clean_and_convert_to_numeric(column):
    """Menghapus semua karakter non-digit (kecuali titik dan minus) dan mengubah kolom menjadi numerik."""
    if column.dtype == 'object':
        column = column.astype(str).str.replace(r'[^\d,\-]', '', regex=True)
        column = column.str.replace(',', '.', regex=False)
    return pd.to_numeric(column, errors='coerce').fillna(0)

def process_rekap(order_df, income_df, seller_conv_df, service_fee_df):
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

    # 1. Siapkan data dari 'Service Fee Details'
    # Pilih kolom yang relevan dan pastikan tipe data No. Pesanan cocok
    service_fee_data = service_fee_df[['No. Pesanan', 'Biaya Layanan Promo XTRA', 'Biaya Layanan Gratis Ongkir XTRA']].copy()
    service_fee_data['No. Pesanan'] = service_fee_data['No. Pesanan'].astype(str)
    
    # Bersihkan dan ubah nama kolom agar sesuai target
    service_fee_data['Biaya Layanan 2%'] = clean_and_convert_to_numeric(service_fee_data['Biaya Layanan Promo XTRA'])
    service_fee_data['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = clean_and_convert_to_numeric(service_fee_data['Biaya Layanan Gratis Ongkir XTRA'])

    # 2. Gabungkan data biaya layanan baru ini ke rekap_df
    rekap_df = pd.merge(rekap_df, service_fee_data[['No. Pesanan', 'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%']], on='No. Pesanan', how='left')

    # 3. Kembalikan logika biaya per-pesanan ke awal (tidak dibagi)
    #    Daftar ini sekarang berisi SEMUA biaya yang hanya berlaku sekali per pesanan.
    order_level_costs = [
        'Voucher dari Penjual', 
        'Biaya Administrasi', 
        'Pengeluaran(Rp)', 
        'Biaya Proses Pesanan', # <-- Dikembalikan ke sini
        'Biaya Layanan 2%', # <-- Kolom baru
        'Biaya Layanan Gratis Ongkir Xtra 4,5%' # <-- Kolom baru
    ]
    
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            # Isi nilai NaN dengan 0 sebelum perkalian
            rekap_df[col] = rekap_df[col].fillna(0)
            # Jadikan 0 untuk baris produk kedua, ketiga, dst. dalam satu pesanan
            rekap_df[col] = rekap_df[col] * is_first_item_mask

    # 4. Hapus logika perhitungan lama yang tidak lagi digunakan
    #    (perkalian 2% dan 4.5% serta pembagian Biaya Proses Pesanan sudah tidak relevan)
    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0)

    # 5. Pastikan semua biaya bernilai positif (menghilangkan tanda minus)
    cost_columns_to_abs = [
        'Voucher dari Penjual', 'Pengeluaran(Rp)', 'Biaya Administrasi', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan' # <-- Cukup kolom asli
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
        rekap_df.get('Biaya Proses Pesanan', 0) # <-- Diubah kembali ke kolom asli
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
        'Biaya Layanan 2%': rekap_df.get('Biaya Layanan 2%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan', 0), # <-- Diubah ke kolom asli
        'Penjualan Netto': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

    # --- PERUBAIKAN 4: Mengosongkan sel duplikat untuk pesanan multi-produk ---
    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''

    return rekap_final.fillna(0)

def process_rekap_pacific(order_df, income_df, seller_conv_df):
    """
    Fungsi untuk memproses sheet 'REKAP' KHUSUS untuk PacificStore.
    Perbedaan utama: Biaya Layanan dihitung dari Total Harga Produk.
    """
    # Bagian ini sama persis dengan fungsi rekap sebelumnya
    order_agg = order_df.groupby(['No. Pesanan', 'Nama Produk']).agg({
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first',
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
    
    rekap_df = pd.merge(income_df, order_agg, on='No. Pesanan', how='left')

    iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
    rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
    rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0)

    # --- LOGIKA BARU UNTUK PACIFICSTORE ---
    # Hitung biaya layanan langsung dari Total Harga Produk.
    # Biaya ini bersifat per-produk, bukan per-pesanan.
    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0)
    rekap_df['Biaya Layanan 2%'] = 0
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = 0
    # --- AKHIR LOGIKA BARU ---

    # Logika untuk men-nol-kan biaya per-pesanan di baris duplikat
    order_level_costs = [
        'Voucher dari Penjual', 
        'Biaya Administrasi', 
        'Pengeluaran(Rp)', 
        'Biaya Proses Pesanan'
        # Biaya Layanan 2% dan 4,5% DIHAPUS dari sini karena dihitung per produk
    ]
    
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].fillna(0)
            rekap_df[col] = rekap_df[col] * is_first_item_mask

    # Pastikan semua biaya bernilai positif
    cost_columns_to_abs = [
        'Voucher dari Penjual', 'Pengeluaran(Rp)', 'Biaya Administrasi', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan'
    ]
    for col in cost_columns_to_abs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].abs()

    # Kalkulasi Penjualan Netto (sama seperti sebelumnya)
    rekap_df['Penjualan Netto'] = (
        rekap_df.get('Total Harga Produk', 0) -
        rekap_df.get('Voucher dari Penjual', 0) -
        rekap_df.get('Pengeluaran(Rp)', 0) -
        rekap_df.get('Biaya Administrasi', 0) -
        rekap_df.get('Biaya Layanan 2%', 0) -
        rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0) -
        rekap_df.get('Biaya Proses Pesanan', 0)
    )

    # Sisa kodenya sama persis dengan fungsi rekap sebelumnya
    rekap_df.sort_values(by='No. Pesanan', inplace=True)
    rekap_df.reset_index(drop=True, inplace=True)
    
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
        'Biaya Layanan 2%': rekap_df.get('Biaya Layanan 2%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan', 0),
        'Penjualan Netto': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

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

def get_harga_beli_fuzzy(nama_produk, katalog_df, score_threshold_primary=80, score_threshold_fallback=75):
    """
    Cari harga beli dari katalog menggunakan kombinasi filter ukuran + jenis kertas
    dan fuzzy matching pada judul. Kembalikan angka (0 jika tidak menemukan).
    """
    try:
        if not isinstance(nama_produk, str) or nama_produk.strip() == "":
            return 0

        s = nama_produk.upper()
        # bersihkan tanda baca untuk matching
        s_clean = re.sub(r'[^A-Z0-9\s×xX\-]', ' ', s)
        s_clean = re.sub(r'\s+', ' ', s_clean).strip()

        # 1) deteksi ukuran (pattern umum)
        ukuran_found = None
        ukuran_patterns = [
            r'\bA[0-9]\b',         # A3 A4 A5
            r'\bB[0-9]\b',         # B4 B5
            r'\b\d{1,3}\s*[x×X]\s*\d{1,3}\b',  # e.g. 21x29, 30 x 21
            r'\b\d{1,3}\s*CM\b'    # e.g. 21 CM
        ]
        for pat in ukuran_patterns:
            m = re.search(pat, s_clean)
            if m:
                ukuran_found = m.group(0).replace(' ', '').upper()
                break

        # 2) deteksi jenis kertas dari kata kunci umum
        jenis_kertas_tokens = ['HVS','KORAN','GLOSSY','DUPLEX','ART','COVER','MATT','MATTE','CTP','BOOK PAPER']
        jenis_found = None
        for jt in jenis_kertas_tokens:
            if jt in s_clean:
                jenis_found = jt
                break

        # 3) filter kandidat katalog: coba filter ukuran dulu, lalu jenis kertas
        candidates = katalog_df.copy()
        if ukuran_found:
            # matching kasar: apakah UKURAN_NORM mengandung ukuran_found (contoh A4 atau 21x29)
            candidates = candidates[candidates['UKURAN_NORM'].str.contains(re.escape(ukuran_found), na=False)]
        if jenis_found and not candidates.empty:
            candidates = candidates[candidates['JENIS_KERTAS_NORM'].str.contains(jenis_found, na=False)]

        # jika tidak ada kandidat setelah filter, fallback ke seluruh katalog
        if candidates.empty:
            candidates = katalog_df.copy()

        # 4) fuzzy matching di kandidat (token_set_ratio lebih toleran terhadap urutan)
        best_score = 0
        best_price = 0
        best_title = ""
        for _, row in candidates.iterrows():
            title = str(row['JUDUL_NORM'])
            score = fuzz.token_set_ratio(s_clean, title)
            # prefer skor tertinggi; tiebreaker: judul lebih panjang (lebih spesifik)
            if score > best_score or (score == best_score and len(title) > len(best_title)):
                best_score = score
                best_price = row.get('KATALOG_HARGA_NUM', 0)
                best_title = title

        # 5) keputusan: terima bila skor cukup tinggi
        if best_score >= score_threshold_primary and best_price and best_price > 0:
            return float(best_price)

        # 6) fallback: scan seluruh katalog untuk skor terbaik jika belum cukup
        best_score2 = best_score
        best_price2 = best_price
        for _, row in katalog_df.iterrows():
            title = str(row['JUDUL_NORM'])
            score = fuzz.token_set_ratio(s_clean, title)
            if score > best_score2 or (score == best_score2 and len(title) > len(best_title)):
                best_score2 = score
                best_price2 = row.get('KATALOG_HARGA_NUM', 0)
                best_title = title

        if best_score2 >= score_threshold_fallback and best_price2 and best_price2 > 0:
            return float(best_price2)

        # kalau masih tidak cukup, kembalikan 0
        return 0
    except Exception:
        return 0

def process_summary(rekap_df, iklan_final_df, katalog_df, store_type): # <-- Tambahkan parameter 'store_type'
    """Fungsi untuk memproses dan membuat sheet 'SUMMARY'."""
    rekap_copy = rekap_df.copy()
    rekap_copy['No. Pesanan'] = rekap_copy['No. Pesanan'].replace('', np.nan).ffill()

    summary_df = rekap_copy.groupby('Nama Produk').agg({
        'Jumlah Terjual': 'sum', 'Harga Satuan': 'first', 'Total Harga Produk': 'sum',
        'Voucher Ditanggung Penjual': 'sum', 'Biaya Komisi AMS + PPN Shopee': 'sum',
        'Biaya Adm 8%': 'sum', 'Biaya Layanan 2%': 'sum',
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': 'sum', 'Biaya Proses Pesanan': 'sum',
        'Penjualan Netto': 'sum'
    }).reset_index()

    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']]
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk', right_on='Nama Iklan', how='left')
    summary_df.rename(columns={'Biaya': 'Iklan Klik'}, inplace=True)
    summary_df['Iklan Klik'].fillna(0, inplace=True)
    summary_df.drop('Nama Iklan', axis=1, inplace=True, errors='ignore')

    summary_df['Penjualan Netto (Setelah Iklan)'] = summary_df['Penjualan Netto'] - summary_df['Iklan Klik']
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200

    # --- LOGIKA BARU UNTUK BIAYA KIRIM ---
    if store_type == 'PacificStore':
        summary_df['Biaya Kirim ke Sby'] = summary_df['Jumlah Terjual'] * 733
        biaya_ekspedisi_final = summary_df['Biaya Kirim ke Sby']
    else: # Default untuk HumanStore
        summary_df['Biaya Ekspedisi'] = 0
        biaya_ekspedisi_final = summary_df['Biaya Ekspedisi']
    # --- AKHIR LOGIKA BARU ---

    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(lambda x: get_harga_beli_fuzzy(x, katalog_df))
    summary_df['Harga Custom TLJ'] = 0
    summary_df['Total Pembelian'] = summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    
    summary_df['Margin Kotor'] = (
        summary_df['Penjualan Netto (Setelah Iklan)'] - 
        summary_df['Biaya Packing'] - 
        biaya_ekspedisi_final - # <-- Gunakan variabel hasil logika di atas
        summary_df['Total Pembelian']
    )
    
    summary_df['Persentase'] = summary_df.apply(lambda row: row['Margin Kotor'] / row['Total Harga Produk'] if row['Total Harga Produk'] != 0 else 0, axis=1)
    summary_df['Jumlah Pesanan'] = summary_df.apply(lambda row: row['Biaya Proses Pesanan'] / 1250 if 1250 != 0 else 0, axis=1)
    summary_df['Penjualan Per Hari'] = summary_df['Penjualan Netto (Setelah Iklan)'] / 7
    summary_df['Jumlah buku per pesanan'] = summary_df.apply(lambda row: row['Jumlah Terjual'] / row['Jumlah Pesanan'] if row.get('Jumlah Pesanan', 0) != 0 else 0, axis=1)

    # --- MEMBUAT DATAFRAME FINAL SECARA DINAMIS ---
    summary_final_data = {
        'No': np.arange(1, len(summary_df) + 1), 'Nama Produk': summary_df['Nama Produk'],
        'Jumlah Terjual': summary_df['Jumlah Terjual'], 'Harga Satuan': summary_df['Harga Satuan'],
        'Total Harga Produk': summary_df['Total Harga Produk'], 'Voucher Ditanggung Penjual': summary_df['Voucher Ditanggung Penjual'],
        'Biaya Komisi AMS + PPN Shopee': summary_df['Biaya Komisi AMS + PPN Shopee'], 'Biaya Adm 8%': summary_df['Biaya Adm 8%'],
        'Biaya Layanan 2%': summary_df['Biaya Layanan 2%'], 'Biaya Layanan Gratis Ongkir Xtra 4,5%': summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': summary_df['Biaya Proses Pesanan'], 'Iklan Klik': summary_df['Iklan Klik'],
        'Penjualan Netto': summary_df['Penjualan Netto (Setelah Iklan)'], 'Biaya Packing': summary_df['Biaya Packing'],
    }
    # Tambahkan kolom ekspedisi sesuai pilihan toko
    if store_type == 'PacificStore':
        summary_final_data['Biaya Kirim ke Sby'] = biaya_ekspedisi_final
    else:
        summary_final_data['Biaya Ekspedisi'] = biaya_ekspedisi_final
        
    summary_final_data.update({
        'Harga Beli': summary_df['Harga Beli'], 'Harga Custom TLJ': summary_df['Harga Custom TLJ'],
        'Total Pembelian': summary_df['Total Pembelian'], 'Margin Kotor': summary_df['Margin Kotor'],
        'Persentase': summary_df['Persentase'], 'Jumlah Pesanan': summary_df['Jumlah Pesanan'],
        'Penjualan Per Hari': summary_df['Penjualan Per Hari'], 'Jumlah buku per pesanan': summary_df['Jumlah buku per pesanan']
    })
    
    return pd.DataFrame(summary_final_data)

# --- TAMPILAN STREAMLIT ---

st.set_page_config(layout="wide")
st.title("📊 Rekapanku - Sistem Otomatisasi Laporan")

# --- LANGKAH 1: PILIH TOKO ---
store_choice = st.selectbox(
    "Pilih Toko untuk Diproses:",
    ("", "HumanStore", "PacificStore")
)

# Hanya tampilkan uploader jika toko sudah dipilih
if store_choice:
    try:
        katalog_df = pd.read_excel('HARGA ONLINE.xlsx')
        # ... (Kode preprocessing katalog Anda tetap di sini) ...
        katalog_df.columns = [str(c).strip().upper() for c in katalog_df.columns]
        for col in ["JUDUL AL QUR'AN","JENIS KERTAS","UKURAN","KATALOG HARGA"]:
            if col not in katalog_df.columns:
                katalog_df[col] = ""
        katalog_df['JUDUL_NORM'] = katalog_df["JUDUL AL QUR'AN"].astype(str).str.upper().str.replace(r'[^A-Z0-9\s]', ' ', regex=True)
        katalog_df['JENIS_KERTAS_NORM'] = katalog_df['JENIS KERTAS'].astype(str).str.upper().str.replace(r'[^A-Z0-9\s]', ' ', regex=True)
        katalog_df['UKURAN_NORM'] = katalog_df['UKURAN'].astype(str).str.upper().str.replace(r'\s+', '', regex=True)
        katalog_df['KATALOG_HARGA_NUM'] = pd.to_numeric(katalog_df['KATALOG HARGA'].astype(str).str.replace(r'[^0-9\.]', '', regex=True), errors='coerce').fillna(0)

    except FileNotFoundError:
        st.error("Error: File 'HARGA ONLINE.xlsx' tidak ditemukan.")
        st.stop()
    
    st.header("1. Import File Anda")
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
        if st.button(f"🚀 Mulai Proses untuk {store_choice}"):
            progress_bar = st.progress(0, text="Mempersiapkan proses...")
            status_text = st.empty()
            
            try:
                # --- LOGIKA PEMBACAAN FILE ---
                status_text.text("Membaca file...")
                order_all_df = pd.read_excel(uploaded_order)
                income_dilepas_df = pd.read_excel(uploaded_income, sheet_name='Income', skiprows=5)
                # Baca 'Service Fee' HANYA untuk HumanStore
                if store_choice == "HumanStore":
                    service_fee_df = pd.read_excel(uploaded_income, sheet_name='Service Fee Details', skiprows=1)
                
                iklan_produk_df = pd.read_csv(uploaded_iklan, skiprows=7)
                seller_conversion_df = pd.read_csv(uploaded_seller)
                progress_bar.progress(20, text="File dimuat. Membersihkan format angka...")

                # ... (Kode pembersihan data keuangan Anda tetap di sini) ...
                financial_data_to_clean = [
                    (order_all_df, ['Harga Setelah Diskon', 'Total Harga Produk']),
                    (income_dilepas_df, ['Voucher dari Penjual', 'Biaya Administrasi', 'Biaya Proses Pesanan', 'Harga Awal', 'Harga Setelah Diskon', 'Total Harga Produk']),
                    (iklan_produk_df, ['Biaya', 'Omzet Penjualan']),
                    (seller_conversion_df, ['Pengeluaran(Rp)'])
                ]
                for df, cols in financial_data_to_clean:
                    for col in cols:
                        if col in df.columns:
                            df[col] = clean_and_convert_to_numeric(df[col])
                
                # --- LOGIKA PEMROSESAN BERDASARKAN TOKO ---
                status_text.text("Menyusun sheet 'REKAP'...")
                if store_choice == "HumanStore":
                    rekap_processed = process_rekap(order_all_df, income_dilepas_df, seller_conversion_df, service_fee_df)
                    file_name_output = f"Rekapanku_HumanStore.xlsx"
                else: # PacificStore
                    rekap_processed = process_rekap_pacific(order_all_df, income_dilepas_df, seller_conversion_df)
                    file_name_output = f"Rekapanku_PacificStore.xlsx"
                
                progress_bar.progress(40, text="Sheet 'REKAP' selesai.")
                
                status_text.text("Menyusun sheet 'IKLAN'...")
                iklan_processed = process_iklan(iklan_produk_df)
                progress_bar.progress(60, text="Sheet 'IKLAN' selesai.")

                status_text.text("Menyusun sheet 'SUMMARY'...")
                # Kirim pilihan toko ke fungsi summary
                summary_processed = process_summary(rekap_processed, iklan_processed, katalog_df, store_type=store_choice)
                progress_bar.progress(80, text="Sheet 'SUMMARY' selesai.")

                # ... (Sisa kode untuk membuat file Excel dan tombol download tetap sama) ...
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
                    # Simpan service fee HANYA jika HumanStore
                    if store_choice == "HumanStore":
                        service_fee_df.to_excel(writer, sheet_name='sheet service fee', index=False)
                
                output.seek(0)
                progress_bar.progress(100, text="Proses Selesai!")
                status_text.success("✅ Proses Selesai! File Anda siap diunduh.")

                st.header("3. Download Hasil")
                st.download_button(
                    label=f"📥 Download File Output ({file_name_output})",
                    data=output,
                    file_name=file_name_output,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Terjadi kesalahan saat pemrosesan: {e}")
                st.exception(e)
else:
    st.info("Silakan pilih toko terlebih dahulu untuk melanjutkan.")

