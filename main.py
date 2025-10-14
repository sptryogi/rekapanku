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
import pdfplumber

# --- FUNGSI-FUNGSI PEMROSESAN ---

def clean_and_convert_to_numeric(column):
    """Menghapus semua karakter non-digit (kecuali titik dan minus) dan mengubah kolom menjadi numerik."""
    if column.dtype == 'object':
        column = column.astype(str).str.replace(r'[^\d,\-]', '', regex=True)
        column = column.str.replace(',', '.', regex=False)
    return pd.to_numeric(column, errors='coerce').fillna(0)

def clean_order_all_numeric(column):
    """
    Fungsi khusus untuk membersihkan kolom di file order-all.
    Menghapus semua karakter non-digit dari string.
    """
    # Karena kita akan memastikan kolom dibaca sebagai string,
    # kita bisa langsung membersihkannya dengan aman.
    # Regex `\D` berarti "karakter apa pun yang bukan digit".
    # Ini akan menghapus '.' , ',' , spasi, 'Rp', dll.
    cleaned_column = column.astype(str).str.replace(r'\D', '', regex=True)
    
    # Ubah string angka yang sudah bersih (misal: "35750") ke tipe data numerik.
    return pd.to_numeric(cleaned_column, errors='coerce').fillna(0)

def clean_columns(df):
    """Menghapus spasi di awal dan akhir dari semua nama kolom DataFrame."""
    df.columns = df.columns.str.strip()
    return df
    
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

    # 1. Pastikan Total Harga Produk ada dan numerik
    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0).fillna(0)
    
    # 2. Hitung biaya baru berdasarkan Total Harga Produk (ini berlaku per-baris/per-produk)
    rekap_df['Biaya Adm 8%'] = rekap_df['Total Harga Produk'] * 0.08
    rekap_df['Biaya Layanan 2%'] = rekap_df['Total Harga Produk'] * 0.02
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = rekap_df['Total Harga Produk'] * 0.045
    
    # 3. Hitung Biaya Proses Pesanan yang dibagi rata
    #    Hitung dulu ada berapa produk dalam satu pesanan
    product_count_per_order = rekap_df.groupby('No. Pesanan')['No. Pesanan'].transform('size')
    #    Bagi 1250 dengan jumlah produk tersebut
    rekap_df['Biaya Proses Pesanan Dibagi'] = 1250 / product_count_per_order
    
    # 4. Terapkan logika "hanya di baris pertama" HANYA untuk biaya yang benar-benar per-pesanan
    order_level_costs = [
        'Voucher dari Penjual', 
        'Pengeluaran(Rp)',
        'Total Penghasilan' 
        # 'Biaya Administrasi', 'Biaya Layanan', dan 'Biaya Proses Pesanan' DIHAPUS dari sini
    ]
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].fillna(0)
            rekap_df[col] = rekap_df[col] * is_first_item_mask

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
    rekap_df['Penjualan Netto'] = rekap_df.get('Total Penghasilan', 0)

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
        'Biaya Adm 8%': rekap_df.get('Biaya Adm 8%', 0),
        'Biaya Layanan 2%': rekap_df.get('Biaya Layanan 2%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan Dibagi', 0),
        'Total Penghasilan': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

    # --- PERUBAIKAN 4: Mengosongkan sel duplikat untuk pesanan multi-produk ---
    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''

    return rekap_final.fillna(0)

def process_rekap_pacific(order_df, income_df, seller_conv_df):
    """
    Fungsi untuk memproses sheet 'REKAP' KHUSUS untuk PacificBookStore.
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

    # --- LOGIKA BARU UNTUK PACIFICBOOKSTORE ---
    # 1. Pastikan Total Harga Produk ada dan numerik
    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0).fillna(0)
    
    # 2. Hitung biaya baru berdasarkan Total Harga Produk (ini berlaku per-baris/per-produk)
    # rekap_df['Biaya Adm 8%'] = rekap_df['Total Harga Produk'] * 0.08
    # rekap_df['Biaya Layanan 2%'] = rekap_df['Total Harga Produk'] * 0.02
    # rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = rekap_df['Total Harga Produk'] * 0.045
    rekap_df['Biaya Adm 8%'] = 0
    rekap_df['Biaya Layanan 2%'] = 0
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = 0
    
    # 3. Hitung Biaya Proses Pesanan yang dibagi rata
    #    Hitung dulu ada berapa produk dalam satu pesanan
    product_count_per_order = rekap_df.groupby('No. Pesanan')['No. Pesanan'].transform('size')
    #    Bagi 1250 dengan jumlah produk tersebut
    # rekap_df['Biaya Proses Pesanan Dibagi'] = 1250 / product_count_per_order
    rekap_df['Biaya Proses Pesanan Dibagi'] = 0
    
    # 4. Terapkan logika "hanya di baris pertama" HANYA untuk biaya yang benar-benar per-pesanan
    order_level_costs = [
        'Voucher dari Penjual', 
        'Pengeluaran(Rp)',
        'Total Penghasilan'
        # 'Biaya Administrasi' dan 'Biaya Proses Pesanan' DIHAPUS dari sini
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
    rekap_df['Penjualan Netto'] = rekap_df.get('Total Penghasilan', 0)

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
        'Biaya Adm 8%': rekap_df.get('Biaya Adm 8%', 0),
        'Biaya Layanan 2%': rekap_df.get('Biaya Layanan 2%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan Dibagi', 0), # <-- Gunakan kolom yang sudah dibagi
        'Total Penghasilan': rekap_df['Penjualan Netto'],
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
        'Total Penghasilan': 'sum'
    }).reset_index()

    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']]
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk', right_on='Nama Iklan', how='left')
    summary_df.rename(columns={'Biaya': 'Iklan Klik'}, inplace=True)
    # summary_df['Iklan Klik'].fillna(0, inplace=True)
    summary_df['Iklan Klik'] = summary_df['Iklan Klik'].fillna(0)
    summary_df.drop('Nama Iklan', axis=1, inplace=True, errors='ignore')

    summary_df['Penjualan Netto'] = (
        summary_df['Total Harga Produk'] -
        summary_df['Voucher Ditanggung Penjual'] -
        summary_df['Biaya Komisi AMS + PPN Shopee'] -
        summary_df['Biaya Adm 8%'] -
        summary_df['Biaya Layanan 2%'] -
        summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] -
        summary_df['Biaya Proses Pesanan'] -
        summary_df['Iklan Klik']
    )
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200

    # --- LOGIKA BARU UNTUK BIAYA KIRIM ---
    if store_type == 'PacificBookStore':
        summary_df['Biaya Kirim ke Sby'] = summary_df['Jumlah Terjual'] * 733
        biaya_ekspedisi_final = summary_df['Biaya Kirim ke Sby']
    else: # Default untuk HumanStore
        summary_df['Biaya Ekspedisi'] = 0
        biaya_ekspedisi_final = summary_df['Biaya Ekspedisi']
    # --- AKHIR LOGIKA BARU ---

    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(lambda x: get_harga_beli_fuzzy(x, katalog_df))
    summary_df['Harga Custom TLJ'] = 0
    summary_df['Total Pembelian'] = summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    
    summary_df['M1'] = (
        summary_df['Penjualan Netto'] - 
        summary_df['Biaya Packing'] - 
        biaya_ekspedisi_final - # <-- Gunakan variabel hasil logika di atas
        summary_df['Total Pembelian']
    )
    
    summary_df['Persentase'] = (summary_df.apply(lambda row: row['M1'] / row['Total Harga Produk'] if row['Total Harga Produk'] != 0 else 0, axis=1))
    summary_df['Jumlah Pesanan'] = summary_df.apply(lambda row: row['Biaya Proses Pesanan'] / 1250 if 1250 != 0 else 0, axis=1)
    summary_df['Penjualan Per Hari'] = round(summary_df['Penjualan Netto'] / 7, 1)
    summary_df['Jumlah buku per pesanan'] = round(summary_df.apply(lambda row: row['Jumlah Terjual'] / row['Jumlah Pesanan'] if row.get('Jumlah Pesanan', 0) != 0 else 0, axis=1), 1)

    # --- MEMBUAT DATAFRAME FINAL SECARA DINAMIS ---
    summary_final_data = {
        'No': np.arange(1, len(summary_df) + 1), 'Nama Produk': summary_df['Nama Produk'],
        'Jumlah Terjual': summary_df['Jumlah Terjual'], 'Harga Satuan': summary_df['Harga Satuan'],
        'Total Harga Produk': summary_df['Total Harga Produk'], 'Voucher Ditanggung Penjual': summary_df['Voucher Ditanggung Penjual'],
        'Biaya Komisi AMS + PPN Shopee': summary_df['Biaya Komisi AMS + PPN Shopee'], 'Biaya Adm 8%': summary_df['Biaya Adm 8%'],
        'Biaya Layanan 2%': summary_df['Biaya Layanan 2%'], 'Biaya Layanan Gratis Ongkir Xtra 4,5%': summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': summary_df['Biaya Proses Pesanan'], 'Iklan Klik': summary_df['Iklan Klik'],
        'Penjualan Netto': summary_df['Penjualan Netto'], 'Biaya Packing': summary_df['Biaya Packing'],
    }
    # Tambahkan kolom ekspedisi sesuai pilihan toko
    if store_type == 'PacificBookStore':
        summary_final_data['Biaya Kirim ke Sby'] = biaya_ekspedisi_final
    else:
        summary_final_data['Biaya Ekspedisi'] = biaya_ekspedisi_final
        
    summary_final_data.update({
        'Harga Beli': summary_df['Harga Beli'], 'Harga Custom TLJ': summary_df['Harga Custom TLJ'],
        'Total Pembelian': summary_df['Total Pembelian'], 'M1': summary_df['M1'],
        'Persentase': summary_df['Persentase'], 'Jumlah Pesanan': summary_df['Jumlah Pesanan'],
        'Penjualan Per Hari': summary_df['Penjualan Per Hari'], 'Jumlah buku per pesanan': summary_df['Jumlah buku per pesanan']
    })

    summary_final = pd.DataFrame(summary_final_data)

    # --- PERUBAHAN: Menambahkan baris Total ---
    # 1. Buat baris total dengan menjumlahkan semua kolom numerik sebagai dasar
    total_row = pd.DataFrame(summary_final.sum(numeric_only=True)).T
    total_row['Nama Produk'] = 'Total'

    # 2. Ambil nilai total yang sudah dijumlahkan untuk perhitungan baru
    total_penjualan_netto = total_row['Penjualan Netto'].iloc[0]
    total_biaya_packing = total_row['Biaya Packing'].iloc[0]
    total_pembelian = total_row['Total Pembelian'].iloc[0]
    total_harga_produk = total_row['Total Harga Produk'].iloc[0]
    total_biaya_proses_pesanan = total_row['Biaya Proses Pesanan'].iloc[0]
    total_jumlah_terjual = total_row['Jumlah Terjual'].iloc[0]
    
    # Tentukan nama kolom biaya kirim dan ambil nilainya
    biaya_ekspedisi_col_name = 'Biaya Kirim ke Sby' if store_type == 'PacificBookStore' else 'Biaya Ekspedisi'
    total_biaya_ekspedisi = total_row[biaya_ekspedisi_col_name].iloc[0]
    
    # 3. Hitung ulang kolom spesifik berdasarkan rumus yang Anda berikan
    # Hitung ulang M1
    total_m1 = total_penjualan_netto - total_biaya_packing - total_biaya_ekspedisi - total_pembelian
    total_row['M1'] = total_m1
    
    # Hitung ulang Persentase
    total_row['Persentase'] = (total_m1 / total_harga_produk) if total_harga_produk != 0 else 0
    
    # Hitung ulang Jumlah Pesanan
    total_jumlah_pesanan = (total_biaya_proses_pesanan / 1250) if 1250 != 0 else 0
    total_row['Jumlah Pesanan'] = total_jumlah_pesanan
    
    # Hitung ulang Penjualan Per Hari
    total_row['Penjualan Per Hari'] = round(total_penjualan_netto / 7, 1)
    
    # Hitung ulang Jumlah buku per pesanan
    total_row['Jumlah buku per pesanan'] = round(total_jumlah_terjual / total_jumlah_pesanan if total_jumlah_pesanan != 0 else 0, 1)

    # 4. Kosongkan kolom yang tidak seharusnya dijumlahkan
    for col in ['Harga Satuan', 'Harga Beli', 'No', 'Harga Custom TLJ']:
        if col in total_row.columns:
            total_row[col] = None

    # 5. Gabungkan dataframe asli dengan baris total yang sudah dihitung ulang
    summary_with_total = pd.concat([summary_final, total_row], ignore_index=True)
    # --- AKHIR PERUBAHAN ---

    return summary_with_total

def parse_pdf_receipt(pdf_file):
    """Mengekstrak tanggal dan total nominal dari satu file PDF nota Lalamove."""
    try:
        full_text = ""
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"

        # Pola untuk tanggal (misal: 02 Okt 2025)
        date_match = re.search(r'(\d{2})\s+(\w+)\s+(\d{4})', full_text)
        # Pola untuk total harga (misal: Rp9.402)
        total_match = re.search(r'Total Harga\s*\(IDR\)\s*Rp([\d\.]+)', full_text)

        if not total_match: # Fallback jika format sedikit berbeda
             total_match = re.search(r'Total Harga\s*Rp([\d\.]+)', full_text)

        tanggal = None
        if date_match:
            day, month_str, year = date_match.groups()
            month_map = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'Mei': '05', 'Jun': '06',
                'Jul': '07', 'Agu': '08', 'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12'
            }
            month = month_map.get(month_str[:3], '00')
            tanggal = f"{day}-{month}-{year}"

        nominal = float(total_match.group(1).replace('.', '')) if total_match else 0
        
        return {'Tanggal Kirim Paket': tanggal, 'Nominal': nominal}
    except Exception as e:
        st.warning(f"Gagal memproses PDF: {pdf_file.name}. Error: {e}")
        return None

def process_rekap_tiktok(order_details_df, semua_pesanan_df):
    """Fungsi untuk memproses dan membuat sheet 'REKAP' untuk TikTok."""
    # Pastikan tipe data kunci untuk merge sama
    order_details_df['Order/adjustment ID'] = order_details_df['Order/adjustment ID'].astype(str)
    semua_pesanan_df['Order ID'] = semua_pesanan_df['Order ID'].astype(str)

    # Gabungkan data utama (order_details) dengan detail produk (semua_pesanan)
    rekap_df = pd.merge(
        order_details_df,
        semua_pesanan_df,
        left_on='Order/adjustment ID',
        right_on='Order ID',
        how='left'
    )
    
    # Ekstrak ukuran dari variasi
    rekap_df['Variasi'] = rekap_df['Variation'].str.extract(r'\b(A\d{1,2}|B\d{1,2})\b', expand=False).fillna('')
    
    # Hitung kolom finansial
    rekap_df['Total Harga Setelah Diskon'] = rekap_df['SKU Subtotal Before Discount'] - rekap_df['SKU Seller Discount']
    rekap_df['Biaya Komisi Platform 8%'] = rekap_df['Total Harga Setelah Diskon'] * 0.08
    rekap_df['Komisi Dinamis 5%'] = rekap_df['Total Harga Setelah Diskon'] * 0.05
    rekap_df['Biaya Layanan Cashback Bonus 1,5%'] = rekap_df['Total Harga Setelah Diskon'] * 0.015

    # Hitung Biaya Proses Pesanan yang dibagi rata
    product_count = rekap_df.groupby('Order ID')['Order ID'].transform('size')
    rekap_df['Biaya Proses Pesanan'] = 1250 / product_count

    # Buat DataFrame Final
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['Order ID'],
        'Waktu Pesanan Dibuat': rekap_df['Order created time(UTC)'],
        'Waktu Dana Dilepas': rekap_df['Order settled time(UTC)'],
        'Nama Produk': rekap_df['Product Name'],
        'Variasi': rekap_df['Variasi'],
        'Jumlah Terjual': rekap_df['Quantity'],
        'Harga Satuan': rekap_df['SKU Unit Original Price'],
        'Total Harga Sebelum Diskon': rekap_df['SKU Subtotal Before Discount'],
        'Diskon Penjual': rekap_df['SKU Seller Discount'],
        'Total Harga Setelah Diskon': rekap_df['Total Harga Setelah Diskon'],
        'Komisi Affiliate': rekap_df['Affiliate commission'],
        'Biaya Komisi Platform 8%': rekap_df['Biaya Komisi Platform 8%'],
        'Komisi Dinamis 5%': rekap_df['Komisi Dinamis 5%'],
        'Biaya Layanan Cashback Bonus 1,5%': rekap_df['Biaya Layanan Cashback Bonus 1,5%'],
        'Biaya Layanan Voucher Xtra': rekap_df['Voucher Xtra Service Fee'],
        'Biaya Proses Pesanan': rekap_df['Biaya Proses Pesanan'],
        'Total Penghasilan': rekap_df['Total settlement amount']
    })
    
    # Kosongkan sel duplikat untuk pesanan multi-produk
    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas', 'Total Penghasilan']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''
    
    return rekap_final.fillna(0)

def process_summary_tiktok(rekap_df, katalog_df, ekspedisi_df):
    """Fungsi untuk memproses dan membuat sheet 'SUMMARY' untuk TikTok."""
    # Agregasi data dari REKAP berdasarkan Nama Produk dan Variasi
    summary_df = rekap_df.groupby(['Nama Produk', 'Variasi']).agg({
        'Jumlah Terjual': 'sum',
        'Harga Satuan': 'first',
        'Diskon Penjual': 'sum',
        'Total Harga Setelah Diskon': 'sum',
        'Komisi Affiliate': 'sum',
        'Biaya Komisi Platform 8%': 'sum',
        'Komisi Dinamis 5%': 'sum',
        'Biaya Layanan Cashback Bonus 1,5%': 'sum',
        'Biaya Layanan Voucher Xtra': 'sum',
        'Biaya Proses Pesanan': 'sum',
    }).reset_index()

    # Hitung Penjualan Netto
    summary_df['Penjualan Netto'] = (
        summary_df['Total Harga Setelah Diskon'] -
        summary_df['Komisi Affiliate'] -
        summary_df['Biaya Komisi Platform 8%'] -
        summary_df['Komisi Dinamis 5%'] -
        summary_df['Biaya Layanan Cashback Bonus 1,5%'] -
        summary_df['Biaya Layanan Voucher Xtra'] -
        summary_df['Biaya Proses Pesanan']
    )
    
    # Ambil data Biaya Ekspedisi dari sheet EKSPEDISI (kolom 'Jumlah')
    ekspedisi_cost = ekspedisi_df[['Nama Produk', 'Jumlah']].rename(columns={'Jumlah': 'Biaya Ekspedisi'})
    summary_df = pd.merge(summary_df, ekspedisi_cost, on='Nama Produk', how='left')
    summary_df['Biaya Ekspedisi'] = summary_df['Biaya Ekspedisi'].fillna(0)

    # Hitung kolom lainnya
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200
    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(lambda x: get_harga_beli_fuzzy(x, katalog_df))
    summary_df['Harga Custom TLJ'] = 0
    summary_df['Total Pembelian'] = summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    
    summary_df['M1'] = (
        summary_df['Penjualan Netto'] -
        summary_df['Biaya Packing'] -
        summary_df['Biaya Ekspedisi'] -
        summary_df['Total Pembelian']
    )
    
    summary_df['Persentase'] = summary_df.apply(lambda row: row['M1'] / row['Total Harga Setelah Diskon'] if row['Total Harga Setelah Diskon'] != 0 else 0, axis=1)
    summary_df['Jumlah Pesanan'] = summary_df['Biaya Proses Pesanan'] / 1250
    summary_df['Penjualan Per Hari'] = round(summary_df['Penjualan Netto'] / 7, 1)
    summary_df['Jumlah buku per pesanan'] = summary_df.apply(lambda row: row['Jumlah Terjual'] / row['Jumlah Pesanan'] if row.get('Jumlah Pesanan', 0) != 0 else 0, axis=1)

    # Buat DataFrame Final
    summary_final = pd.DataFrame({
        'No': np.arange(1, len(summary_df) + 1), 'Nama Produk': summary_df['Nama Produk'], 'Variasi': summary_df['Variasi'],
        'Jumlah Terjual': summary_df['Jumlah Terjual'], 'Harga Satuan': summary_df['Harga Satuan'],
        'Total Diskon Penjual': summary_df['Diskon Penjual'], 'Total Harga Sesudah Diskon': summary_df['Total Harga Setelah Diskon'],
        'Komisi Affiliate': summary_df['Komisi Affiliate'], 'Biaya Komisi Platform 8%': summary_df['Biaya Komisi Platform 8%'],
        'Komisi Dinamis 5%': summary_df['Komisi Dinamis 5%'], 'Biaya Layanan Cashback Bonus 1,5%': summary_df['Biaya Layanan Cashback Bonus 1,5%'],
        'Biaya Layanan Voucher Xtra': summary_df['Biaya Layanan Voucher Xtra'], 'Biaya Proses Pesanan': summary_df['Biaya Proses Pesanan'],
        'Penjualan Netto': summary_df['Penjualan Netto'], 'Biaya Packing': summary_df['Biaya Packing'],
        'Biaya Ekspedisi': summary_df['Biaya Ekspedisi'], 'Harga Beli': summary_df['Harga Beli'],
        'Harga Custom TLJ': summary_df['Harga Custom TLJ'], 'Total Pembelian': summary_df['Total Pembelian'],
        'M1': summary_df['M1'], 'Persentase': summary_df['Persentase'], 'Jumlah Pesanan': summary_df['Jumlah Pesanan'],
        'Penjualan Per Hari': summary_df['Penjualan Per Hari'], 'Jumlah buku per pesanan': summary_df['Jumlah buku per pesanan']
    })

    # Tambahkan baris Total (logika mirip Shopee)
    total_row = pd.DataFrame(summary_final.sum(numeric_only=True)).T
    total_row['Nama Produk'] = 'Total'
    total_m1 = total_row['Penjualan Netto'].iloc[0] - total_row['Biaya Packing'].iloc[0] - total_row['Biaya Ekspedisi'].iloc[0] - total_row['Total Pembelian'].iloc[0]
    total_row['M1'] = total_m1
    total_harga_diskon = total_row['Total Harga Sesudah Diskon'].iloc[0]
    total_row['Persentase'] = (total_m1 / total_harga_diskon) if total_harga_diskon != 0 else 0
    total_row['Penjualan Per Hari'] = round(total_row['Penjualan Netto'].iloc[0] / 7, 1)
    total_jumlah_pesanan = total_row['Jumlah Pesanan'].iloc[0]
    total_jumlah_terjual = total_row['Jumlah Terjual'].iloc[0]
    total_row['Jumlah buku per pesanan'] = round(total_jumlah_terjual / total_jumlah_pesanan if total_jumlah_pesanan != 0 else 0, 1)
    for col in ['Harga Satuan', 'Harga Beli', 'No', 'Harga Custom TLJ', 'Variasi']:
        if col in total_row.columns: total_row[col] = None
    
    summary_with_total = pd.concat([summary_final, total_row], ignore_index=True)
    return summary_with_total.fillna(0)

def process_ekspedisi_tiktok(summary_df, pdf_data_list):
    """Membuat sheet EKSPEDISI berdasarkan data summary dan nota PDF."""
    # Bagian Kiri: Data Produk
    kiri_df = summary_df[summary_df['Nama Produk'] != 'Total'][['Nama Produk', 'Jumlah Terjual']].copy()
    kiri_df.rename(columns={'Jumlah Terjual': 'QTY'}, inplace=True)
    
    # Bagian Kanan: Data dari PDF
    kanan_df = pd.DataFrame(pdf_data_list)
    
    # Kalkulasi Biaya Ekspedisi
    total_qty = kiri_df['QTY'].sum()
    total_nominal = kanan_df['Nominal'].sum()
    biaya_per_produk = total_nominal / total_qty if total_qty > 0 else 0
    
    kiri_df['Biaya Ekspedisi per produk'] = biaya_per_produk
    kiri_df['Jumlah'] = kiri_df['QTY'] * biaya_per_produk
    
    # Tambah baris total
    kiri_total = pd.DataFrame([{'Nama Produk': 'Total', 'QTY': total_qty, 'Biaya Ekspedisi per produk': None, 'Jumlah': kiri_df['Jumlah'].sum()}])
    kiri_df = pd.concat([kiri_df, kiri_total], ignore_index=True)

    kanan_total = pd.DataFrame([{'Tanggal Kirim Paket': 'Total', 'Nominal': total_nominal}])
    kanan_df = pd.concat([kanan_df, kanan_total], ignore_index=True)
    
    # Gabungkan dengan kolom kosong di tengah
    final_df = pd.concat([kiri_df, pd.DataFrame(columns=[' ']), kanan_df], axis=1)
    return final_df.fillna('')
    
# --- TAMPILAN STREAMLIT ---

st.set_page_config(layout="wide")
st.title("📊 Rekapanku - Sistem Otomatisasi Laporan")

marketplace_choice = st.selectbox(
    "Pilih Marketplace:",
    ("", "Shopee", "TikTok")
)

store_choice = ""
if marketplace_choice == "Shopee":
    store_choice = st.selectbox(
        "Pilih Toko Shopee:",
        ("HumanStore", "PacificBookStore"),
        key='shopee_store'
    )
elif marketplace_choice == "TikTok":
    # Untuk sekarang, TikTok hanya untuk HumanStore
    store_choice = "HumanStore"
    st.info("Marketplace TikTok saat ini hanya tersedia untuk HumanStore.")

# Hanya tampilkan uploader jika marketplace sudah dipilih
if marketplace_choice:
    try:
        # ... (kode untuk membaca HARGA ONLINE.xlsx tetap sama) ...
        katalog_df = pd.read_excel('HARGA ONLINE.xlsx')
        # ... (kode preprocessing katalog Anda tetap di sini) ...
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

    if marketplace_choice == "Shopee":
        col1, col2 = st.columns(2)
        with col1:
            uploaded_order = st.file_uploader("1. Import file order-all.xlsx", type="xlsx")
            uploaded_income = st.file_uploader("2. Import file income dilepas.xlsx", type="xlsx")
        with col2:
            uploaded_iklan = st.file_uploader("3. Import file iklan produk", type="csv")
            uploaded_seller = st.file_uploader("4. Import file seller conversion", type="csv")
        # Inisialisasi variabel lain agar tidak error
        uploaded_income_tiktok = None
        uploaded_semua_pesanan = None
        uploaded_pdfs = None

    elif marketplace_choice == "TikTok":
        col1, col2 = st.columns(2)
        with col1:
            uploaded_income_tiktok = st.file_uploader("1. Import file Income (Order details & Reports)", type="xlsx")
            uploaded_semua_pesanan = st.file_uploader("2. Import file semua pesanan.xlsx", type="xlsx")
        with col2:
            uploaded_pdfs = st.file_uploader(
                "3. Import Nota Resi Ekspedisi (bisa lebih dari satu)",
                type="pdf",
                accept_multiple_files=True
            )
        # Inisialisasi variabel lain agar tidak error
        uploaded_order = None
        uploaded_income = None
        uploaded_iklan = None
        uploaded_seller = None

    st.markdown("---")
    
    # Kondisi untuk menampilkan tombol proses
    show_shopee_button = marketplace_choice == "Shopee" and uploaded_order and uploaded_income and uploaded_iklan and uploaded_seller
    show_tiktok_button = marketplace_choice == "TikTok" and uploaded_income_tiktok and uploaded_semua_pesanan and uploaded_pdfs

    if show_shopee_button or show_tiktok_button:
        button_label = f"🚀 Mulai Proses untuk {marketplace_choice} - {store_choice}"
        if st.button(button_label):
            progress_bar = st.progress(0, text="Mempersiapkan proses...")
            status_text = st.empty()
            
            try:
                # --- LOGIKA PEMBACAAN FILE ---
                if marketplace_choice == "Shopee":
                    # --- ALUR PROSES SHOPEE (KODE LAMA ANDA) ---
                    status_text.text("Membaca file Shopee...")
                    order_all_df = pd.read_excel(uploaded_order, dtype={'Harga Setelah Diskon': str, 'Total Harga Produk': str})
                    income_dilepas_df = pd.read_excel(uploaded_income, sheet_name='Income', skiprows=5)
                    if store_choice == "HumanStore":
                        service_fee_df = pd.read_excel(uploaded_income, sheet_name='Service Fee Details', skiprows=1)
                    iklan_produk_df = pd.read_csv(uploaded_iklan, skiprows=7)
                    seller_conversion_df = pd.read_csv(uploaded_seller)
                    progress_bar.progress(20, text="File dimuat. Membersihkan format angka...")

                    # ... (Kode pembersihan data keuangan Anda tetap di sini) ...
                    # --- Langkah 1: Bersihkan file order-all secara khusus ---
                    cols_to_clean_order = ['Harga Setelah Diskon', 'Total Harga Produk']
                    for col in cols_to_clean_order:
                      if col in order_all_df.columns:
                          # Gunakan fungsi baru yang spesifik
                          order_all_df[col] = clean_order_all_numeric(order_all_df[col])
    
                    # --- Langkah 2: Bersihkan file-file lainnya dengan fungsi lama ---
                    other_financial_data_to_clean = [
                        (income_dilepas_df, ['Voucher dari Penjual', 'Biaya Administrasi', 'Biaya Proses Pesanan', 'Total Penghasilan']),
                        (iklan_produk_df, ['Biaya', 'Omzet Penjualan']),
                        (seller_conversion_df, ['Pengeluaran(Rp)'])
                    ]
    
                    for df, cols in other_financial_data_to_clean:
                        for col in cols:
                            if col in df.columns:
                              # Gunakan fungsi lama yang umum
                              df[col] = clean_and_convert_to_numeric(df[col])
                
                    # --- LOGIKA PEMROSESAN BERDASARKAN TOKO ---
                    status_text.text("Menyusun sheet 'REKAP' (Shopee)...")
                    if store_choice == "HumanStore":
                        rekap_processed = process_rekap(order_all_df, income_dilepas_df, seller_conversion_df, service_fee_df)
                    else: # PacificBookStore
                        rekap_processed = process_rekap_pacific(order_all_df, income_dilepas_df, seller_conversion_df)
                    progress_bar.progress(40, text="Sheet 'REKAP' selesai.")
                    
                    status_text.text("Menyusun sheet 'IKLAN' (Shopee)...")
                    iklan_processed = process_iklan(iklan_produk_df)
                    progress_bar.progress(60, text="Sheet 'IKLAN' selesai.")
    
                    status_text.text("Menyusun sheet 'SUMMARY' (Shopee)...")
                    summary_processed = process_summary(rekap_processed, iklan_processed, katalog_df, store_type=store_choice)
                    progress_bar.progress(80, text="Sheet 'SUMMARY' selesai.")
                    
                    file_name_output = f"Rekapanku_Shopee_{store_choice}.xlsx"
                    sheets = {
                        'SUMMARY': summary_processed, 'REKAP': rekap_processed, 'IKLAN': iklan_processed,
                        'sheet order-all': order_all_df, 'sheet income dilepas': income_dilepas_df,
                        'sheet biaya iklan': iklan_produk_df, 'sheet seller conversion': seller_conversion_df
                    }
                    if store_choice == "HumanStore": sheets['sheet service fee'] = service_fee_df
    
                elif marketplace_choice == "TikTok":
                    # --- ALUR PROSES TIKTOK BARU ---
                    status_text.text("Membaca file TikTok...")
                    # Baca sheet 'Order details' dan langsung bersihkan kolomnya
                    order_details_df = pd.read_excel(uploaded_income_tiktok, sheet_name='Order details', header=0)
                    order_details_df = clean_columns(order_details_df)
                    # Baca sheet 'Reports' dan langsung bersihkan kolomnya
                    reports_df = pd.read_excel(uploaded_income_tiktok, sheet_name='Reports', header=0)
                    reports_df = clean_columns(reports_df)
                    # Baca 'semua pesanan' dan langsung bersihkan kolomnya
                    # 1. Baca file tanpa header, sehingga semua baris (termasuk header asli) menjadi data
                    semua_pesanan_df = pd.read_excel(uploaded_semua_pesanan)
                    semua_pesanan_df = semua_pesanan_df.iloc[[0] + list(range(2, len(semua_pesanan_df)))]
                    semua_pesanan_df = clean_columns(semua_pesanan_df)
                    progress_bar.progress(20, text="File Excel TikTok dimuat dan kolom dibersihkan.")
                    
                    status_text.text(f"Memproses {len(uploaded_pdfs)} file PDF nota resi...")
                    pdf_data = [parse_pdf_receipt(pdf) for pdf in uploaded_pdfs if pdf is not None]
                    pdf_data = [data for data in pdf_data if data is not None] # Hapus hasil yang gagal
                    progress_bar.progress(40, text="File PDF selesai diproses.")
    
                    status_text.text("Menyusun sheet 'REKAP' (TikTok)...")
                    rekap_processed = process_rekap_tiktok(order_details_df, semua_pesanan_df)
                    progress_bar.progress(60, text="Sheet 'REKAP' selesai.")
                    
                    # Untuk SUMMARY, kita perlu EKSPEDISI dulu, tapi EKSPEDISI perlu agregasi dari SUMMARY.
                    # Jadi, kita buat summary sementara dulu.
                    summary_temp_for_ekspedisi = rekap_processed.groupby(['Nama Produk', 'Variasi']).agg({'Jumlah Terjual': 'sum'}).reset_index()
                    
                    status_text.text("Menyusun sheet 'EKSPEDISI'...")
                    ekspedisi_processed = process_ekspedisi_tiktok(summary_temp_for_ekspedisi, pdf_data)
                    progress_bar.progress(70, text="Sheet 'EKSPEDISI' selesai.")
    
                    status_text.text("Menyusun sheet 'SUMMARY' (TikTok)...")
                    summary_processed = process_summary_tiktok(rekap_processed, katalog_df, ekspedisi_processed)
                    progress_bar.progress(85, text="Sheet 'SUMMARY' selesai.")
    
                    file_name_output = f"Rekapanku_TikTok_{store_choice}.xlsx"
                    sheets = {
                        'SUMMARY': summary_processed,
                        'REKAP': rekap_processed,
                        'EKSPEDISI': ekspedisi_processed,
                        'sheet Order details': order_details_df,
                        'sheet Reports': reports_df,
                        'sheet semua pesanan': semua_pesanan_df
                    }

                # ... (Sisa kode untuk membuat file Excel dan tombol download tetap sama) ...
                status_text.text("Menyiapkan file output untuk diunduh...")
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    
                    # --- SEMUA FORMATTING VISUAL DIDEFINISIKAN DI SINI ---
                    workbook = writer.book
                    
                    # --- PERUBAHAN 1: Format Judul diubah menjadi rata kiri (align: 'left') ---
                    title_format = workbook.add_format({'bold': True, 'fg_color': '#4472C4', 'font_color': 'white', 'align': 'left', 'valign': 'vcenter', 'font_size': 14})
                    
                    # Format Header Kolom (biru muda, bold, border)
                    header_format = workbook.add_format({'bold': True, 'fg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
                    
                    # --- PERUBAHAN 2: Tambahkan format border untuk sel data ---
                    cell_border_format = workbook.add_format({'border': 1})
                    
                    # Format Persen (0.00%) DENGAN BORDER
                    percent_format = workbook.add_format({'num_format': '0.00%', 'border': 1})
                    
                    # Format 1 Angka Desimal (0.0) DENGAN BORDER
                    one_decimal_format = workbook.add_format({'num_format': '0.0', 'border': 1})
                    
                    # Format Baris Total (kuning, bold)
                    total_fmt = workbook.add_format({'bold': True, 'fg_color': '#FFFF00', 'border': 1})
                    total_fmt_percent = workbook.add_format({'bold': True, 'fg_color': '#FFFF00', 'num_format': '0.00%', 'border': 1})
                    total_fmt_decimal = workbook.add_format({'bold': True, 'fg_color': '#FFFF00', 'num_format': '0.0', 'border': 1})

                    # --- PROSES SETIAP SHEET ---
                    for sheet_name, df in sheets.items():
                        # --- PERUBAHAN 3: Ubah startrow menjadi 3 untuk memberi ruang 2 baris header ---
                        start_row_data = 3 if sheet_name in ['SUMMARY', 'REKAP', 'IKLAN'] else 1
                        
                        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row_data, header=False)
                        worksheet = writer.sheets[sheet_name]
                        
                        start_row_header = 0
                        if sheet_name in ['SUMMARY', 'REKAP', 'IKLAN']:
                            # --- PERUBAHAN 4: Buat judul dinamis dan merge 2 baris ---
                            judul_sheet = f"{sheet_name} {store_choice.upper()} {marketplace_choice.upper()}"
                            worksheet.merge_range(0, 0, 1, len(df.columns) - 1, judul_sheet, title_format) # merge dari baris 0 hingga 1
                            start_row_header = 2 # Header kolom sekarang mulai di baris ke-3 (index 2)
                        
                        for col_num, value in enumerate(df.columns.values):
                            worksheet.write(start_row_header, col_num, value, header_format)

                        # Terapkan formatting KHUSUS untuk sheet SUMMARY, REKAP, dan IKLAN
                        if sheet_name in ['SUMMARY', 'REKAP', 'IKLAN']:
                            # --- PERUBAHAN 5: Terapkan border ke semua sel data ---
                            # (row_start, col_start, row_end, col_end, format)
                            worksheet.conditional_format(start_row_data, 0, start_row_data + len(df) - 1, len(df.columns) - 1, 
                                                         {'type': 'no_blanks', 'format': cell_border_format})

                        if sheet_name == 'SUMMARY':
                            persen_col = df.columns.get_loc('Persentase')
                            penjualan_hari_col = df.columns.get_loc('Penjualan Per Hari')
                            buku_pesanan_col = df.columns.get_loc('Jumlah buku per pesanan')
                            
                            # --- PERUBAHAN 6: Terapkan format persen ke seluruh kolom, bukan hanya baris total ---
                            # Terapkan format mulai dari baris data pertama hingga baris sebelum total
                            # (worksheet.set_column(col_start, col_end, width, format))
                            # worksheet.set_column(persen_col, persen_col, 12, percent_format) # Format ini sudah termasuk border
                            for row_idx in range(len(df) - 1): # -1 agar tidak menyentuh baris 'Total'
                                excel_row = start_row_data + row_idx
                                cell_value = df.iloc[row_idx, persen_col]
                                worksheet.write(excel_row, persen_col, cell_value, percent_format)
                            
                            # Atur lebar kolomnya secara terpisah
                            worksheet.set_column(persen_col, persen_col, 12)
                            worksheet.set_column(penjualan_hari_col, penjualan_hari_col, 18, one_decimal_format)
                            worksheet.set_column(buku_pesanan_col, buku_pesanan_col, 22, one_decimal_format)
                            
                            last_row = len(df) + start_row_header
                            for col_num in range(len(df.columns)):
                                cell_value = df.iloc[-1, col_num]
                                current_fmt = total_fmt
                                if col_num == persen_col:
                                    current_fmt = total_fmt_percent
                                elif col_num in [penjualan_hari_col, buku_pesanan_col]:
                                    current_fmt = total_fmt_decimal
                                
                                if pd.notna(cell_value):
                                    worksheet.write(last_row, col_num, cell_value, current_fmt)
                                else:
                                    worksheet.write_blank(last_row, col_num, None, current_fmt)

                        # TAMBAHKAN BLOK BARU INI
                        if sheet_name == 'IKLAN':
                            # Cek jika baris terakhir adalah baris TOTAL
                            last_row_idx = len(df) - 1
                            if not df.empty and df.iloc[last_row_idx]['Nama Iklan'] == 'TOTAL':
                                # Terapkan format total (kuning, bold, border) ke setiap sel di baris ini
                                for col_num in range(len(df.columns)):
                                    cell_value = df.iloc[last_row_idx, col_num]
                                    worksheet.write(start_row_data + last_row_idx, col_num, cell_value, total_fmt)
                        
                        # Atur lebar kolom otomatis untuk semua sheet
                        for i, col in enumerate(df.columns):
                            column_len = max(df[col].astype(str).map(len).max(), len(col))
                            worksheet.set_column(i, i, column_len + 2)
                
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
