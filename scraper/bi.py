import os
import io
import re
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LKPP_DIR = BASE_DIR.parent / "lkpp"

from dotenv import load_dotenv

load_dotenv(override=True)

import pandas as pd

excel_path = Path(r"D:\data-project\TABEL4_2.xls")

if not excel_path.exists():
    raise FileNotFoundError(f"File Excel tidak ditemukan: {excel_path}")

xls = pd.ExcelFile(excel_path)
print(xls.sheet_names)

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.api import (
    create_session,
    get_bytes,
    get_json,
)

from utils.data import (
    normalize_dataframe,
    find_duplicates,
    concat_dataframes,
    filter_not_null,
)

from utils.file import (
    read_parquet,
    save_csv,
    save_parquet,
)

from utils.storage import (
    get_minio_client,
    upload_file,
)

BI_SESSION = create_session()
MINIO_BUCKET = "maganghub"


# ============================================================
# BI CONFIG
# ============================================================

BI_CONFIG = {

    "sknbi_regional": {

        "storage_name": "sknbi_regional",

        "frekuensi": "bulanan",

        "sumber": "Bank Indonesia (BI)",

        "indikator": (
            "Nilai Transaksi "
            "(Kliring Kredit)"
        ),

        "satuan": "Rp miliar",

        "nama_data": {

            "Kota Asal":
                "Nilai Transaksi "
                "(Kliring Kredit) "
                "Menurut Kota Asal "
                "Menurut Provinsi",

            "Kota Tujuan":
                "Nilai Transaksi "
                "(Kliring Kredit) "
                "Menurut Kota Tujuan "
                "Menurut Provinsi",
        },

        "note": (
            "Data SKNBI Regional "
            "berdasarkan SPIP "
            "September 2025"
        ),

        "nama_data_import": (
            "https://www.bi.go.id/"
            "id/statistik/ekonomi-keuangan/"
            "spip/Default.aspx"
        ),

        "raw_file": (
            "bi_sknbi_regional_raw_"
            "Maret_September_2025.parquet"
        ),
    },


    # ========================================================
    # ATM
    # ========================================================

    "atm": {
        "storage_name": "atm",
        "idnamadata": 18452,

    "indikator": [
        "Jumlah Kartu/Instrumen",
        "Kartu ATM",
        "Kartu ATM+Debet",
        "Volume Transaksi",
        "Nilai Transaksi",
    ],

        "nama_data": (
            "Jumlah Mesin ATM "
            "Menurut Provinsi"
        ),

        "satuan": "unit",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "bulanan",

        "nama_data_import": (
            "https://www.bi.go.id/"
        ),

        "note": None,

        "raw_file": "bi_atm_raw.parquet",

        "source_file": PROJECT_ROOT / "TABEL_5b.xls",

        "source_url": None,
    },

    "kredit_perbankan": {
        "storage_name": "kredit_perbankan",
        "idnamadata": 9767,
        "indikator": "Total Kredit Perbankan Menurut Provinsi",
        "nama_data": "Total Kredit Perbankan Menurut Provinsi",
        "satuan": "Rp miliar",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "bulanan",
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/Default.aspx"
        ),
        "note": (
            "Posisi pinjaman/kredit Rupiah dan Valas "
            "Bank Umum dan BPR menurut lokasi proyek provinsi, "
            "tabel I.12"
        ),
        "raw_file": "bi_kredit_perbankan_raw.parquet",
        "source_url": "https://www.bi.go.id/SEKI/tabel/TABEL1_12.xls",
    },

    "belanja_barang_pemerintah_pusat": {
        "storage_name": "belanja_barang_pemerintah_pusat",
        "idnamadata": 15868,
        "indikator": "Belanja Barang Pemerintah Pusat",
        "nama_data": "Belanja Barang Pemerintah Pusat",
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
        "source_url": "https://www.bi.go.id/SEKI/tabel/TABEL4_2.xls",
        "raw_file": "bi_belanja_barang_pemerintah_pusat_raw.parquet",
        "tahun_akhir": 2025,
    },

    "belanja_bantuan_sosial_pemerintah_pusat": {
        "storage_name": "belanja_bantuan_sosial_pemerintah_pusat",
        "idnamadata": 15880,  # Ganti dengan ID XFlask yang benar
        "indikator": "Belanja Bantuan Sosial Pemerintah Pusat",
        "nama_data": "Belanja Bantuan Sosial Pemerintah Pusat",
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
        "source_url": "https://www.bi.go.id/SEKI/tabel/TABEL4_2.xls",
        "raw_file": "bi_belanja_bantuan_sosial_pemerintah_pusat_raw.parquet",
        "tahun_akhir": 2025,
    },

    "belanja_dana_alokasi_khusus_pemerintah": {
        "storage_name": "belanja_dana_alokasi_khusus_pemerintah",
        "idnamadata": 15895,
        "indikator": "Belanja Dana Alokasi Khusus Pemerintah",
        "nama_data": "Belanja Dana Alokasi Khusus Pemerintah",
        "satuan": "Rupiah",
        "sumber": "LKPP/LKBUN",
        "frekuensi": "tahunan",
        "nama_data_import": "https://djpb.kemenkeu.go.id/portal/publikasi/lkpp.php",
        "raw_file": "bi_belanja_dana_alokasi_khusus_pemerintah_raw.parquet",
        "tahun_awal": 2019,
        "tahun_akhir": 2025,
    },

    "belanja_dana_alokasi_umum_pemerintah": {
        "storage_name": "belanja_dana_alokasi_umum_pemerintah",
        "idnamadata": 15894,
        "indikator": "Belanja Dana Alokasi Umum (DAU) Pemerintah",
        "nama_data": "Belanja Dana Alokasi Umum (DAU) Pemerintah",
        "satuan": "Rupiah",
        "sumber": "LKPP/LKBUN",
        "frekuensi": "tahunan",
        "nama_data_import": "https://djpb.kemenkeu.go.id/portal/publikasi/lkpp.php",
        "raw_file": "bi_belanja_dana_alokasi_umum_pemerintah_raw.parquet",
        "tahun_awal": 2019,
        "tahun_akhir": 2025,
    },

    "belanja_pegawai_pemerintah_pusat": {
        "storage_name": "belanja_pegawai_pemerintah_pusat",
        "idnamadata": None,  # Isi dengan ID XFlask yang benar
        "indikator": "Belanja Pegawai Pemerintah Pusat",
        "nama_data": "Belanja Pegawai Pemerintah Pusat",
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
        "source_url": "https://www.bi.go.id/SEKI/tabel/TABEL4_2.xls",
        "raw_file": "bi_belanja_pegawai_pemerintah_pusat_raw.parquet",
        "tahun_awal": 2022,
        "tahun_akhir": 2025,
    },
    "belanja_pemerintah_pusat": {
        "storage_name": "belanja_pemerintah_pusat",
        "nama_data": "Belanja Pemerintah Pusat",
        "nama_item": "Realisasi",
        "indikator": "Belanja Pemerintah Pusat",
        "idnamadata": 6773,
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "tahun_awal": 2023,
        "tahun_akhir": 2025,
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
    },

    "belanja_negara_total": {
        "storage_name": "belanja_negara_total",
        "nama_data": "Belanja Negara: Total",
        "indikator": "Belanja Negara: Total",
        "idnamadata": 6773,  # GANTI dengan ID khusus Belanja Negara: Total
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "tahun_awal": 2023,
        "tahun_akhir": 2025,
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
    },

    "belanja_hibah_pemerintah_pusat": {
        "storage_name": "belanja_hibah_pemerintah_pusat",
        "idnamadata": 15879,
        "indikator": "Belanja Hibah Pemerintah Pusat",
        "nama_data": "Belanja Hibah Pemerintah Pusat",
        "negara": "Indonesia",
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "tahun_awal": 2020,
        "tahun_akhir": 2025,
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
        "source_url": "https://www.bi.go.id/SEKI/tabel/TABEL4_2.xls",
    },

    "belanja_subsidi_pemerintah": {
        "storage_name": "belanja_subsidi_pemerintah",
        "idnamadata": 15876,
        "indikator": "Belanja Subsidi Pemerintah",
        "nama_data": "Belanja Subsidi Pemerintah",
        "negara": "Indonesia",
        "satuan": "Rupiah",
        "sumber": "Bank Indonesia (BI)",
        "frekuensi": "tahunan",
        "tahun_awal": 2023,
        "tahun_akhir": 2025,
        "nama_data_import": (
            "https://www.bi.go.id/id/statistik/"
            "ekonomi-keuangan/seki/default.aspx"
        ),
        "source_url": "https://www.bi.go.id/SEKI/tabel/TABEL4_2.xls",
    },
}

# ============================================================
# PARSER KREDIT PERBANKAN BI
# ============================================================

PROVINSI_BI = {
    "aceh": "Aceh",
    "sumatera utara": "Sumatera Utara",
    "sumatera barat": "Sumatera Barat",
    "riau": "Riau",
    "kepulauan riau": "Kep. Riau",
    "jambi": "Jambi",
    "sumatera selatan": "Sumatera Selatan",
    "kep. bangka belitung": "Kep. Bangka Belitung",
    "kepulauan bangka belitung": "Kep. Bangka Belitung",
    "bengkulu": "Bengkulu",
    "lampung": "Lampung",
    "dki jakarta": "DKI Jakarta",
    "jawa barat": "Jawa Barat",
    "banten": "Banten",
    "jawa tengah": "Jawa Tengah",
    "d.i. yogyakarta": "DI Yogyakarta",
    "di yogyakarta": "DI Yogyakarta",
    "d.i yogyakarta": "DI Yogyakarta",
    "jawa timur": "Jawa Timur",
    "bali": "Bali",
    "nusa tenggara barat": "Nusa Tenggara Barat",
    "nusa tenggara timur": "Nusa Tenggara Timur",
    "kalimantan barat": "Kalimantan Barat",
    "kalimantan tengah": "Kalimantan Tengah",
    "kalimantan selatan": "Kalimantan Selatan",
    "kalimantan timur": "Kalimantan Timur",
    "kalimantan utara": "Kalimantan Utara",
    "sulawesi utara": "Sulawesi Utara",
    "sulawesi tengah": "Sulawesi Tengah",
    "sulawesi selatan": "Sulawesi Selatan",
    "sulawesi tenggara": "Sulawesi Tenggara",
    "gorontalo": "Gorontalo",
    "sulawesi barat": "Sulawesi Barat",
    "maluku": "Maluku",
    "maluku utara": "Maluku Utara",
    "papua": "Papua",
    "papua barat": "Papua Barat",
}

BULAN_BI = {
    "jan": 1, "januari": 1, "january": 1,
    "feb": 2, "februari": 2, "february": 2,
    "mar": 3, "maret": 3, "march": 3,
    "apr": 4, "april": 4,
    "mei": 5, "may": 5,
    "jun": 6, "juni": 6, "june": 6,
    "jul": 7, "juli": 7, "july": 7,
    "agu": 8, "ags": 8, "agustus": 8, "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "okt": 10, "oktober": 10, "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "des": 12, "desember": 12, "dec": 12, "december": 12,
}


def normalisasi_teks_bi(value):
    if pd.isna(value):
        return ""

    value = str(value).strip().lower()
    value = re.sub(r"^\s*\d+\s*[\.\)]\s*", "", value)
    value = re.sub(r"\s+", " ", value)

    return value


def cari_provinsi_bi(row):
    # Nama provinsi biasanya ada di beberapa kolom pertama.
    for value in row.iloc[:5]:
        text = normalisasi_teks_bi(value)

        if text in PROVINSI_BI:
            return PROVINSI_BI[text]

    return None


def parse_tanggal_header_bi(header_values):
    """
    Mencari tanggal bulanan dari gabungan nilai header
    yang mungkin tersebar di beberapa baris.
    """
    values = [
        value for value in header_values
        if not pd.isna(value)
    ]

    # Kasus header berupa tanggal Excel.
    for value in values:
        if isinstance(value, (datetime, pd.Timestamp)):
            return pd.Timestamp(value).to_period("M").to_timestamp("M")

    text = " ".join(str(value).strip() for value in values)
    text_lower = text.lower()

    # Kasus header memiliki tanggal seperti 31/01/2024.
    for value in values:
        parsed = pd.to_datetime(
            value,
            errors="coerce",
            dayfirst=True,
        )

        if not pd.isna(parsed) and re.search(
            r"\d{1,2}[/\-]\d{1,2}[/\-]\d{4}",
            str(value),
        ):
            return parsed.to_period("M").to_timestamp("M")

    # Tahun bisa berada pada baris header yang berbeda.
    years = re.findall(r"\b(?:19|20)\d{2}\b", text)
    if not years:
        return None

    year = int(years[-1])

    # Bulan dicari berdasarkan nama, bukan posisi kolom.
    month = None
    for month_name, month_number in BULAN_BI.items():
        if re.search(rf"\b{re.escape(month_name)}\b", text_lower):
            month = month_number
            break

    if month is None:
        return None

    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)



def parse_sheet_kredit_bi(df_sheet, sheet_name):
    """
    Parse sheet BI I.12.
    Tahun berada di baris indeks 3 dan bulan di baris indeks 4.
    Data provinsi dimulai dari baris indeks 5.
    """
    import calendar
    import re

    bulan_map = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }

    provinsi_map = {
        "Nanggroe Aceh Darussalam": "Aceh",
        "Aceh": "Aceh",
        "Bangka Belitung": "Kep. Bangka Belitung",
        "Bangka Belitung Islands": "Kep. Bangka Belitung",
        "D.K.I. Jakarta": "DKI Jakarta",
        "D.I. Yogyakarta": "DI Yogyakarta",
        "DI Yogyakarta": "DI Yogyakarta",
        "Kepulauan Riau": "Kep. Riau",
        "Kep. Riau": "Kep. Riau",
        "Banten": "Banten",
    }

    # Header tahun dan bulan berdasarkan struktur Excel yang ditemukan
    tahun_header = df_sheet.iloc[3]
    bulan_header = df_sheet.iloc[4]

    kolom_tanggal = {}
    tahun_aktif = None

    for col in range(3, len(df_sheet.columns)):
        nilai_tahun = tahun_header.iloc[col]
        nilai_bulan = bulan_header.iloc[col]

        # Tahun pada Excel hanya muncul di awal kelompok kolom.
        if pd.notna(nilai_tahun):
            try:
                tahun_int = int(float(nilai_tahun))
                if 2000 <= tahun_int <= 2100:
                    tahun_aktif = tahun_int
            except (ValueError, TypeError):
                pass

        if tahun_aktif is None or pd.isna(nilai_bulan):
            continue

        nama_bulan = str(nilai_bulan).strip().lower()
        bulan = bulan_map.get(nama_bulan)

        if bulan is None:
            continue

        # Abaikan kolom di luar rentang yang diminta.
        if tahun_aktif < 2023:
            continue

        # Sheet historis hanya sampai 2024.
        if sheet_name == "Th 2010-2024" and tahun_aktif > 2024:
            continue

        # Hanya ambil kolom data numerik.
        kolom_tanggal[col] = (tahun_aktif, bulan)

    records = []

    for row_idx in range(5, len(df_sheet)):
        nama_asli = df_sheet.iloc[row_idx, 2]

        if pd.isna(nama_asli):
            continue

        nama_asli = str(nama_asli).strip()

        # Hapus nomor urut di akhir nama provinsi, misalnya "Nusa Tenggara Timur 1)"
        nama_asli = re.sub(r"\s+\d+\)?$", "", nama_asli).strip()

        # Lewati baris yang bukan provinsi.
        if nama_asli.lower() in {
            "province", "provinsi", "jumlah", "total",
            "indonesia", "sumber", "source"
        }:
            continue

        nama_provinsi = provinsi_map.get(
            nama_asli,
            nama_asli,
        )

        for col, (tahun, bulan) in kolom_tanggal.items():
            nilai = pd.to_numeric(
                df_sheet.iloc[row_idx, col],
                errors="coerce",
            )

            if pd.isna(nilai):
                continue

            tanggal = pd.Timestamp(
                year=tahun,
                month=bulan,
                day=calendar.monthrange(tahun, bulan)[1],
            )

            records.append({
                "data_x": tanggal,
                "data_y": float(nilai),
                "provinsi": nama_provinsi,
            })

    hasil = pd.DataFrame(
        records,
        columns=["data_x", "data_y", "provinsi"],
    )

    if not hasil.empty:
        hasil = hasil.drop_duplicates(
            subset=["data_x", "provinsi"],
            keep="last",
        ).reset_index(drop=True)

    print(
        f"{sheet_name}: {len(hasil)} baris, "
        f"{hasil['provinsi'].nunique() if not hasil.empty else 0} provinsi"
    )

    return hasil

# ============================================================
# CONFIG HELPER
# ============================================================

def get_bi_config(
    config_name
):

    if config_name not in BI_CONFIG:

        raise ValueError(
            f"Config BI tidak ditemukan: "
            f"{config_name}"
        )

    return BI_CONFIG[
        config_name
    ]


def get_storage_paths(
    config
):

    storage_name = (
        config["storage_name"]
    )

    raw_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "bi"
        / storage_name
    )

    final_dir = (
        PROJECT_ROOT
        / "final"
        / "bi"
        / storage_name
    )

    return {
        "raw_dir": raw_dir,
        "final_dir": final_dir,
    }

class BIScraper:

    def __init__(
        self,
        config,
    ):

        self.config = config

        self.paths = (
            get_storage_paths(
                config
            )
        )

        self.RAW_DATA_DIR = (
            self.paths["raw_dir"]
        )

        self.RAW_FILE = (
            self.RAW_DATA_DIR
            / config["raw_file"]
            if config.get("raw_file")
            else None
        )

        self.minio_client = (
            get_minio_client()
        )


    def load_raw_data(self):
        print("\n==============================")
        print("LOAD RAW DATA BI")
        print("==============================")

        if self.config["storage_name"] == "kredit_perbankan":
            raw_files = sorted(
                self.RAW_DATA_DIR.glob(
                    "bi_kredit_perbankan_*_raw.parquet"
                )
            )
        else:
            raw_files = (
                [self.RAW_FILE]
                if self.RAW_FILE and self.RAW_FILE.exists()
                else []
            )

        if not raw_files:
            raise FileNotFoundError(
                f"Raw file tidak ditemukan di {self.RAW_DATA_DIR}"
            )

        frames = []

        for raw_file in raw_files:
            print("Membaca:", raw_file.name)
            frames.append(read_parquet(raw_file))

        df = concat_dataframes(frames)

        print("TOTAL RAW ROWS:", len(df))
        print("KOLOM:", df.columns.tolist())

        return df

    def load_raw_data_belanja_bantuan_sosial_pemerintah_pusat(self):
        print("\n==============================")
        print("LOAD RAW DATA BANTUAN SOSIAL")
        print("==============================")

        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_bantuan_sosial_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw file Bantuan Sosial tidak ditemukan di "
                f"{self.RAW_DATA_DIR}"
            )

        frames = []

        for raw_file in raw_files:
            print("Membaca:", raw_file.name)
            frames.append(read_parquet(raw_file))

        df = concat_dataframes(frames)

        print("TOTAL RAW ROWS:", len(df))
        print("KOLOM:", df.columns.tolist())

        return df

    def load_raw_data_belanja_barang_pemerintah_pusat(self):
        print("\n==============================")
        print("LOAD RAW DATA BELANJA BARANG PEMERINTAH PUSAT")
        print("==============================")

        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_barang_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw file Belanja Barang Pemerintah Pusat "
                f"tidak ditemukan di {self.RAW_DATA_DIR}"
            )

        frames = []

        for raw_file in raw_files:
            print("Membaca:", raw_file.name)
            frames.append(read_parquet(raw_file))

        df = concat_dataframes(frames)

        print("TOTAL RAW ROWS:", len(df))
        print("KOLOM:", df.columns.tolist())

        return df

    def load_raw_data_belanja_pegawai_pemerintah_pusat(self):
        """Membaca raw Parquet yang sudah tersimpan."""

        print("\n=== LOAD RAW BELANJA PEGAWAI ===")

        raw_files = sorted(
            file
            for file in self.RAW_DATA_DIR.glob(
                "bi_belanja_pegawai_pemerintah_pusat_*_raw.parquet"
            )
            if file.stem.rsplit("_", 2)[-2].isdigit()
            and 2022 <= int(file.stem.rsplit("_", 2)[-2]) <= 2025
        )

        if not raw_files:
            raise FileNotFoundError(
                f"Raw Parquet tidak ditemukan di {self.RAW_DATA_DIR}"
            )

        df = concat_dataframes(
            [read_parquet(path) for path in raw_files]
        )

        df = normalize_dataframe(
            df,
            date_columns=["data_x"],
            numeric_columns=["data_y"],
        )

        df = (
            df.dropna(subset=["data_x", "data_y"])
            .drop_duplicates(subset=["data_x", "sumber"], keep="last")
            .sort_values("data_x")
            .reset_index(drop=True)
        )

        print("TOTAL RAW ROWS:", len(df))
        print(df.to_string(index=False))

        return df


    def load_raw_data_belanja_pemerintah_pusat(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "File RAW Belanja Pemerintah Pusat tidak ditemukan."
            )

        dfs = []

        for raw_file in raw_files:
            df = read_parquet(raw_file)
            print(f"RAW DIBACA: {raw_file.name} | {len(df)} baris")
            dfs.append(df)

        result = concat_dataframes(dfs)

        print("\nTOTAL RAW YANG DIBACA:", len(result))
        print(result[["data_x", "data_y", "sumber"]].to_string(index=False))

        return result


    def load_raw_data_belanja_negara_total(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_negara_total_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "File RAW Belanja Negara: Total tidak ditemukan."
            )

        dfs = []

        for raw_file in raw_files:
            df = read_parquet(raw_file)
            print(f"RAW DIBACA: {raw_file.name} | {len(df)} baris")
            dfs.append(df)

        result = concat_dataframes(dfs)

        print("TOTAL RAW:", len(result))
        return result

    def load_raw_data_belanja_hibah_pemerintah_pusat(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_hibah_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw Parquet Belanja Hibah tidak ditemukan."
            )

        dfs = []

        for raw_file in raw_files:
            print("Membaca:", raw_file.name)

            df = read_parquet(raw_file)
            dfs.append(df)

        result = concat_dataframes(dfs)

        print("TOTAL RAW ROWS:", len(result))
        print("KOLOM:", result.columns.tolist())

        return result

    def load_raw_data_belanja_subsidi_pemerintah(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_subsidi_pemerintah_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "RAW Belanja Subsidi tidak ditemukan."
            )

        dfs = []

        for raw_file in raw_files:
            print("Membaca RAW:", raw_file.name)
            dfs.append(read_parquet(raw_file))

        result = concat_dataframes(dfs)

        result["tahun"] = pd.to_datetime(
            result["data_x"],
            errors="coerce",
        ).dt.year

        result = result[
            result["tahun"].between(2023, 2025)
        ].drop(columns=["tahun"])

        print("TOTAL RAW LOADED:", len(result))

        return result


    def upload_raw_belanja_subsidi_pemerintah(self):
        storage_name = "belanja_subsidi_pemerintah"

        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_subsidi_pemerintah_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "RAW Belanja Subsidi tidak ditemukan untuk upload."
            )

        for file_path in raw_files:
            object_name = (
                f"bi/raw/{storage_name}/{file_path.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                str(file_path),
                object_name,
            )

            print("Uploaded RAW:", object_name)

    def load_raw_data_belanja_dana_alokasi_khusus_pemerintah(self):
        print("\n==============================")
        print("LOAD RAW DATA DAK LKPP/LKBUN")
        print("==============================")

        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_dana_alokasi_khusus_pemerintah_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw Parquet DAK tidak ditemukan di "
                f"{self.RAW_DATA_DIR}"
            )

        frames = []

        for raw_file in raw_files:
            print("Membaca:", raw_file.name)
            frames.append(read_parquet(raw_file))

        df = concat_dataframes(frames)

        df = normalize_dataframe(
            df,
            date_columns=["data_x"],
            numeric_columns=["data_y"],
        )

        df = df.dropna(subset=["data_x", "data_y"])
        df = df.sort_values(["data_x", "item"]).reset_index(drop=True)

        print("TOTAL RAW ROWS:", len(df))
        print("KOLOM:", df.columns.tolist())

        return df


    def load_raw_data_belanja_dana_alokasi_umum_pemerintah(self):
        """
        Membaca dan menggabungkan raw Parquet DAU
        yang sudah tersimpan. Tidak melakukan scraping ulang.
        """
        print("\n==============================")
        print("LOAD RAW DATA DAU")
        print("==============================")

        tahun_awal = int(self.config.get("tahun_awal", 2019))
        tahun_akhir = int(self.config.get("tahun_akhir", 2025))

        dataframes = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            filepath = (
                self.RAW_DATA_DIR
                / f"bi_belanja_dana_alokasi_umum_pemerintah_{tahun}_raw.parquet"
            )

            if not filepath.exists():
                print(f"File tidak ditemukan, dilewati: {filepath}")
                continue

            df_tahun = read_parquet(filepath)

            if df_tahun.empty:
                print(f"File kosong, dilewati: {filepath}")
                continue

            dataframes.append(df_tahun)
            print(f"Tahun {tahun}: {len(df_tahun)} baris")

        if not dataframes:
            raise FileNotFoundError(
                "Tidak ada file raw Parquet DAU yang dapat dibaca."
            )

        df = concat_dataframes(dataframes)

        df = normalize_dataframe(
            df,
            date_columns=["data_x"],
            numeric_columns=["data_y"],
        )

        df = df.dropna(subset=["data_x", "data_y"])
        df = df.sort_values(["data_x", "item"]).reset_index(drop=True)

        print("\nTOTAL RAW ROWS:", len(df))
        print(df.head().to_string(index=False))

        return df


    def prepare_dataframe(
        self,
        df,
    ):

        text_columns = [
            "indikator",
            "provinsi",
            "satuan",
            "sumber",
        ]

        if "blok" in df.columns:
            text_columns.append(
                "blok"
            )

        if "wilayah" in df.columns:
            text_columns.append(
                "wilayah"
            )

        df = normalize_dataframe(
            df,
            text_columns=text_columns,
            date_columns=[
                "data_x"
            ],
            numeric_columns=[
                "data_y"
            ],
        )

        return df


    def scrape_belanja_bantuan_sosial_pemerintah_pusat(
        self,
        tahun_awal=2022,
        tahun_akhir=2025,
    ):
        """
        Scrape data APBN dan Realisasi Bantuan Sosial
        Pemerintah Pusat dari Excel BI SEKI.
        Raw Parquet disimpan terpisah per tahun.
        """
        tahun_akhir = int(
            self.config.get("tahun_akhir", 2025)
            if tahun_akhir is None
            else tahun_akhir
        )

        tahun_awal = int(tahun_awal)

        source_url = self.config["source_url"]

        excel_content = get_bytes(
            session=BI_SESSION,
            url=source_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/130.0.0.0 Safari/537.36"
                ),
                "Referer": self.config["nama_data_import"],
            },
            timeout=120,
        )

        if not excel_content:
            raise ValueError("File Excel BI yang diterima kosong.")

        excel_bytes = io.BytesIO(excel_content)
        xls = pd.ExcelFile(excel_path)

        df = pd.read_excel(
            xls,
            sheet_name="4.2",
            header=None,
        )

        sheet_name = next(
            (
                name for name in xls.sheet_names
                if name.strip() == "4.2"
            ),
            None,
        )

        if sheet_name is None:
            raise ValueError(
                f"Sheet 4.2 tidak ditemukan. "
                f"Sheet tersedia: {xls.sheet_names}"
            )

        excel_bytes.seek(0)
        df = pd.read_excel(
            excel_bytes,
            sheet_name=sheet_name,
            header=None,
        )

        # Identifikasi kolom tahun.
        kolom_tahun = {}

        for row_idx in range(len(df)):
            nilai_baris = pd.to_numeric(
                df.iloc[row_idx, 3:],
                errors="coerce",
            )

            if nilai_baris.between(1990, 2100).sum() < 2:
                continue

            for col_idx in range(3, len(df.columns)):
                nilai = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(nilai)
                    and nilai == int(nilai)
                    and tahun_awal <= int(nilai) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(int(nilai), col_idx)

        tahun_diminta = set(range(tahun_awal, tahun_akhir + 1))
        tahun_tidak_ditemukan = tahun_diminta - set(kolom_tahun)

        if tahun_tidak_ditemukan:
            raise ValueError(
                "Kolom tahun tidak ditemukan: "
                f"{sorted(tahun_tidak_ditemukan)}"
            )

        # Identifikasi baris APBN dan Realisasi.
        bagian_aktif = None
        baris_bantuan_sosial = {}

        for row_idx in range(len(df)):
            teks_baris = [
                str(value).strip().lower()
                for value in df.iloc[row_idx].tolist()
                if pd.notna(value)
            ]

            for teks in teks_baris:
                teks_bersih = re.sub(
                    r"^\d+\s*[.)]?\s*", "", teks
                ).strip()

                if teks_bersih == "apbn":
                    bagian_aktif = "APBN"
                    break

                if teks_bersih in {"realisasi", "actual"}:
                    bagian_aktif = "Realisasi"
                    break

            label_baris = [
                re.sub(
                    r"^\d+\s*[.)]?\s*",
                    "",
                    str(value).strip(),
                ).lower()
                for value in df.iloc[row_idx, :3].tolist()
                if pd.notna(value)
            ]

            if (
                bagian_aktif in {"APBN", "Realisasi"}
                and any(
                    "bantuan sosial" in label
                    for label in label_baris
                )
            ):
                baris_bantuan_sosial[bagian_aktif] = row_idx

        if not {"APBN", "Realisasi"}.issubset(baris_bantuan_sosial):
            raise ValueError(
                "Baris Bantuan Sosial APBN/Realisasi tidak lengkap. "
                f"Baris ditemukan: {baris_bantuan_sosial}"
            )

        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        hasil_semua_tahun = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            records = []

            for jenis in ["APBN", "Realisasi"]:
                nilai = pd.to_numeric(
                    pd.Series([
                        df.iat[
                            baris_bantuan_sosial[jenis],
                            kolom_tahun[tahun],
                        ]
                    ]),
                    errors="coerce",
                ).iloc[0]

                if pd.isna(nilai):
                    raise ValueError(
                        f"Nilai {jenis} tahun {tahun} kosong."
                    )

                records.append({
                    "data_x": pd.Timestamp(tahun, 12, 31),
                    "data_y": float(nilai) * 1_000_000_000,
                    "indikator": self.config["indikator"],
                    "satuan": self.config["satuan"],
                    "sumber": jenis,
                    "frekuensi": self.config["frekuensi"],
                })

            hasil_tahun = pd.DataFrame(records)

            filename = (
                "bi_belanja_bantuan_sosial_pemerintah_pusat_"
                f"{tahun}_raw.parquet"
            )
            filepath = self.RAW_DATA_DIR / filename

            save_parquet(
                hasil_tahun,
                filepath,
            )

            print(f"\nRAW PARQUET TERSIMPAN: {filepath}")
            print(hasil_tahun.to_string(index=False))

            hasil_semua_tahun.append(hasil_tahun)

        hasil = concat_dataframes(hasil_semua_tahun)

        print("\nRINGKASAN SCRAPING BANTUAN SOSIAL")
        print("Rentang tahun:", tahun_awal, "-", tahun_akhir)
        print("Total baris:", len(hasil))

        return hasil


    def scrape_belanja_barang_pemerintah_pusat(
        self,
        tahun_awal=2023,
        tahun_akhir=None,
    ):
        """
        Scrape APBN dan Realisasi Belanja Barang
        Pemerintah Pusat dari Excel BI SEKI untuk 2023-2025.
        Raw Parquet disimpan terpisah per tahun.
        """
        tahun_akhir = int(
            self.config.get("tahun_akhir", 2025)
            if tahun_akhir is None
            else tahun_akhir
        )

        source_url = self.config["source_url"]

        excel_content = get_bytes(
            session=BI_SESSION,
            url=source_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/130.0.0.0 Safari/537.36"
                ),
                "Referer": self.config["nama_data_import"],
            },
            timeout=120,
        )

        if not excel_content:
            raise ValueError("File Excel BI yang diterima kosong.")

        excel_bytes = io.BytesIO(excel_content)
        xls = pd.ExcelFile(excel_bytes)

        sheet_name = next(
            (
                name for name in xls.sheet_names
                if name.strip() == "4.2"
            ),
            None,
        )

        if sheet_name is None:
            raise ValueError(
                f"Sheet 4.2 tidak ditemukan. "
                f"Sheet tersedia: {xls.sheet_names}"
            )

        excel_bytes.seek(0)
        df = pd.read_excel(
            excel_bytes,
            sheet_name=sheet_name,
            header=None,
        )

        # Cari kolom tahun pada header Excel.
        kolom_tahun = {}

        for row_idx in range(len(df)):
            nilai_baris = pd.to_numeric(
                df.iloc[row_idx, 3:],
                errors="coerce",
            )

            jumlah_tahun = nilai_baris.between(
                1990, 2100
            ).sum()

            if jumlah_tahun < 2:
                continue

            for col_idx in range(3, len(df.columns)):
                nilai = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(nilai)
                    and nilai == int(nilai)
                    and tahun_awal <= int(nilai) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(
                        int(nilai), col_idx
                    )

        tahun_diminta = set(
            range(tahun_awal, tahun_akhir + 1)
        )
        tahun_tidak_ditemukan = (
            tahun_diminta - set(kolom_tahun)
        )

        if tahun_tidak_ditemukan:
            raise ValueError(
                "Kolom tahun tidak ditemukan: "
                f"{sorted(tahun_tidak_ditemukan)}"
            )

        # Cari baris Belanja Barang untuk APBN dan Realisasi.
        bagian_aktif = None
        baris_belanja = {}

        for row_idx in range(len(df)):
            teks_baris = [
                str(value).strip().lower()
                for value in df.iloc[row_idx].tolist()
                if pd.notna(value)
            ]

            for teks in teks_baris:
                teks_bersih = re.sub(
                    r"^\d+\s*[.)]?\s*", "", teks
                ).strip()

                if teks_bersih == "apbn":
                    bagian_aktif = "APBN"
                    break

                if teks_bersih in {"realisasi", "actual"}:
                    bagian_aktif = "Realisasi"
                    break

            label_baris = [
                re.sub(
                    r"^\d+\s*[.)]?\s*",
                    "",
                    str(value).strip(),
                ).lower()
                for value in df.iloc[row_idx, :3].tolist()
                if pd.notna(value)
            ]

            if (
                bagian_aktif in {"APBN", "Realisasi"}
                and "belanja barang" in label_baris
            ):
                baris_belanja[bagian_aktif] = row_idx

        jenis_wajib = {"APBN", "Realisasi"}

        if not jenis_wajib.issubset(baris_belanja):
            raise ValueError(
                "Baris Belanja Barang APBN/Realisasi "
                f"tidak lengkap: {baris_belanja}"
            )

        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        hasil_semua_tahun = []

        # Ekstrak dan simpan data per tahun.
        for tahun in range(tahun_awal, tahun_akhir + 1):
            records = []

            for jenis in ["APBN", "Realisasi"]:
                nilai = pd.to_numeric(
                    pd.Series([
                        df.iat[
                            baris_belanja[jenis],
                            kolom_tahun[tahun],
                        ]
                    ]),
                    errors="coerce",
                ).iloc[0]

                if pd.isna(nilai):
                    raise ValueError(
                        f"Nilai {jenis} tahun {tahun} kosong."
                    )

                records.append({
                    "data_x": pd.Timestamp(tahun, 12, 31),
                    "data_y": float(nilai) * 1_000_000_000,
                    "indikator": self.config["indikator"],
                    "satuan": self.config["satuan"],
                    "sumber": jenis,
                    "frekuensi": self.config["frekuensi"],
                })

            hasil_tahun = pd.DataFrame(records)

            filename = (
                "bi_belanja_barang_pemerintah_pusat_"
                f"{tahun}_raw.parquet"
            )
            filepath = self.RAW_DATA_DIR / filename

            save_parquet(
                hasil_tahun,
                filepath,
            )

            print(
                f"\nRAW PARQUET TERSIMPAN: {filepath}"
            )
            print(f"Tahun: {tahun}")
            print(hasil_tahun.to_string(index=False))

            hasil_semua_tahun.append(hasil_tahun)

        hasil = concat_dataframes(hasil_semua_tahun)

        print("\n==============================")
        print("RINGKASAN SCRAPING BELANJA BARANG")
        print("==============================")
        print("Rentang tahun:", tahun_awal, "-", tahun_akhir)
        print("Total baris:", len(hasil))
        print(
            hasil.groupby(
                hasil["data_x"].dt.year
            ).size().to_string()
        )

        return hasil

    def scrape_belanja_pegawai_pemerintah_pusat(
        self,
        tahun_awal=None,
        tahun_akhir=None,
    ):
        """Ambil realisasi Belanja Pegawai dari Excel BI SEKI 4.2."""

        tahun_awal = int(
            self.config.get("tahun_awal", 2020)
            if tahun_awal is None else tahun_awal
        )
        tahun_akhir = int(
            self.config.get("tahun_akhir", 2025)
            if tahun_akhir is None else tahun_akhir
        )

        excel_path = Path(r"D:\data-project\TABEL4_2.xls")

        if not excel_path.exists():
            raise FileNotFoundError(f"File Excel tidak ditemukan: {excel_path}")

        xls = pd.ExcelFile(excel_path)

        sheet_name = next(
            (s for s in xls.sheet_names if s.strip() == "4.2"),
            None,
        )
        if sheet_name is None:
            raise ValueError(
                f"Sheet 4.2 tidak ditemukan: {xls.sheet_names}"
            )

        df = pd.read_excel(
            xls,
            sheet_name=sheet_name,
            header=None,
        )

        # Cari kolom tahun berdasarkan header Excel.
        kolom_tahun = {}

        for row_idx in range(len(df)):
            for col_idx in range(3, len(df.columns)):
                value = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(value)
                    and value == int(value)
                    and tahun_awal <= int(value) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(int(value), col_idx)

        tahun_tidak_ditemukan = (
            set(range(tahun_awal, tahun_akhir + 1))
            - set(kolom_tahun)
        )
        if tahun_tidak_ditemukan:
            raise ValueError(
                f"Kolom tahun tidak ditemukan: "
                f"{sorted(tahun_tidak_ditemukan)}"
            )

        # Temukan baris Belanja Pegawai di bagian Realisasi.
        bagian_aktif = None
        baris_realisasi = None

        for row_idx in range(len(df)):
            row = df.iloc[row_idx].tolist()

            for value in row:
                if pd.isna(value):
                    continue

                label = re.sub(
                    r"^\s*\d+\s*[.)]?\s*",
                    "",
                    str(value).strip().lower(),
                ).strip()

                if label in {"realisasi", "actual"}:
                    bagian_aktif = "Realisasi"
                elif label == "apbn":
                    bagian_aktif = "APBN"

            label_awal = [
                re.sub(
                    r"^\s*\d+\s*[.)]?\s*",
                    "",
                    str(value).strip().lower(),
                ).strip()
                for value in row[:3]
                if pd.notna(value)
            ]

            if (
                bagian_aktif == "Realisasi"
                and any(label == "belanja pegawai" for label in label_awal)
            ):
                baris_realisasi = row_idx
                break

        if baris_realisasi is None:
            raise ValueError(
                "Baris Belanja Pegawai pada bagian Realisasi "
                "tidak ditemukan di sheet 4.2."
            )

        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

        hasil_semua_tahun = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            nilai = pd.to_numeric(
                pd.Series([
                    df.iat[baris_realisasi, kolom_tahun[tahun]]
                ]),
                errors="coerce",
            ).iloc[0]

            if pd.isna(nilai):
                raise ValueError(
                    f"Nilai realisasi Belanja Pegawai {tahun} kosong."
                )

            hasil_tahun = pd.DataFrame([{
                "data_x": pd.Timestamp(tahun, 12, 31),
                "data_y": float(nilai) * 1_000_000_000,
                "indikator": self.config["indikator"],
                "satuan": self.config["satuan"],
                "sumber": self.config["sumber"],
                "frekuensi": self.config["frekuensi"],
            }])

            filepath = (
                self.RAW_DATA_DIR
                / f"bi_belanja_pegawai_pemerintah_pusat_{tahun}_raw.parquet"
            )

            save_parquet(
                hasil_tahun,
                filepath,
            )

            print(f"\nRAW TERSIMPAN: {filepath}")
            print(hasil_tahun.to_string(index=False))

            hasil_semua_tahun.append(hasil_tahun)

        return concat_dataframes(hasil_semua_tahun)


    def scrape_belanja_pemerintah_pusat(
        self,
        tahun_awal=None,
        tahun_akhir=None,
    ):
        """Scrape realisasi Belanja Pemerintah Pusat dari Excel BI."""

        tahun_awal = int(
            self.config.get("tahun_awal", 2023)
            if tahun_awal is None
            else tahun_awal
        )
        tahun_akhir = int(
            self.config.get("tahun_akhir", 2025)
            if tahun_akhir is None
            else tahun_akhir
        )

        excel_path = Path(r"D:\data-project\TABEL4_2.xls")

        if not excel_path.exists():
            raise FileNotFoundError(
                f"Excel tidak ditemukan: {excel_path}"
            )

        xls = pd.ExcelFile(excel_path)
        print("SHEET EXCEL:", xls.sheet_names)

        df = pd.read_excel(
            xls,
            sheet_name="4.2",
            header=None,
        )

        # Cari kolom tahun pada seluruh header Excel.
        kolom_tahun = {}

        for row_idx in range(len(df)):
            for col_idx in range(3, len(df.columns)):
                value = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(value)
                    and value == int(value)
                    and tahun_awal <= int(value) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(int(value), col_idx)

        print("TAHUN AWAL:", tahun_awal)
        print("TAHUN AKHIR:", tahun_akhir)
        print("KOLOM TAHUN TERDETEKSI:", kolom_tahun)

        missing = (
            set(range(tahun_awal, tahun_akhir + 1))
            - set(kolom_tahun)
        )

        if missing:
            raise ValueError(
                f"Kolom tahun tidak ditemukan: {sorted(missing)}. "
                "Periksa header tahun pada Excel BI."
            )

        # Cari baris Belanja Pemerintah Pusat pada bagian Realisasi.
        bagian_aktif = None
        baris_realisasi = None

        for row_idx in range(len(df)):
            labels = []

            for value in df.iloc[row_idx, :3]:
                if pd.isna(value):
                    continue

                label = str(value).strip().lower()
                label = re.sub(r"^\s*\d+\s*[.)]?\s*", "", label)
                labels.append(label.strip())

            if any(
                value in {"realisasi", "actual"}
                for value in labels
            ):
                bagian_aktif = "Realisasi"

            elif any(value == "apbn" for value in labels):
                bagian_aktif = "APBN"

            if (
                bagian_aktif == "Realisasi"
                and any(
                    value == "belanja pemerintah pusat"
                    for value in labels
                )
            ):
                baris_realisasi = row_idx
                break

        if baris_realisasi is None:
            raise ValueError(
                "Baris Belanja Pemerintah Pusat pada bagian "
                "Realisasi tidak ditemukan."
            )

        print("BARIS REALISASI:", baris_realisasi)

        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        hasil = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            nilai_asli = df.iat[
                baris_realisasi,
                kolom_tahun[tahun],
            ]

            nilai = pd.to_numeric(
                pd.Series([nilai_asli]),
                errors="coerce",
            ).iloc[0]

            print(f"TAHUN {tahun} - NILAI EXCEL:", repr(nilai_asli))

            if pd.isna(nilai):
                raise ValueError(
                    f"Nilai Realisasi tahun {tahun} kosong "
                    f"atau bukan angka: {nilai_asli!r}"
                )

            raw_df = pd.DataFrame([{
                "data_x": pd.Timestamp(tahun, 12, 31),
                "data_y": float(nilai) * 1_000_000_000,
                "indikator": self.config["indikator"],
                "satuan": self.config["satuan"],
                "sumber": (
                    "realisasi (APBN 2026)"
                    if tahun == 2025
                    else "realisasi"
                ),
                "frekuensi": self.config["frekuensi"],
            }])

            filepath = (
                self.RAW_DATA_DIR
                / f"bi_belanja_pemerintah_pusat_{tahun}_raw.parquet"
            )

            save_parquet(
                raw_df,
                filepath,
            )

            print(f"\nRAW TERSIMPAN: {filepath}")
            print(raw_df.to_string(index=False))

            hasil.append(raw_df)

        return concat_dataframes(hasil)


    def scrape_belanja_negara_total(
        self,
        tahun_awal=None,
        tahun_akhir=None,
    ):
        """Scrape realisasi Belanja Negara: Total dari Excel BI."""

        tahun_awal = int(
            self.config.get("tahun_awal", 2023)
            if tahun_awal is None
            else tahun_awal
        )
        tahun_akhir = int(
            self.config.get("tahun_akhir", 2025)
            if tahun_akhir is None
            else tahun_akhir
        )

        excel_path = Path(r"D:\data-project\TABEL4_2.xls")

        if not excel_path.exists():
            raise FileNotFoundError(
                f"Excel tidak ditemukan: {excel_path}"
            )

        df = pd.read_excel(
            excel_path,
            sheet_name="4.2",
            header=None,
        )

        # Deteksi kolom tahun.
        kolom_tahun = {}

        for row_idx in range(len(df)):
            for col_idx in range(3, len(df.columns)):
                value = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(value)
                    and value == int(value)
                    and tahun_awal <= int(value) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(int(value), col_idx)

        missing = (
            set(range(tahun_awal, tahun_akhir + 1))
            - set(kolom_tahun)
        )

        if missing:
            raise ValueError(
                f"Kolom tahun tidak ditemukan: {sorted(missing)}"
            )

        # Cari baris Belanja Negara pada bagian Realisasi.
        bagian_aktif = None
        baris_target = None

        for row_idx in range(len(df)):
            labels = []

            for value in df.iloc[row_idx, :3]:
                if pd.isna(value):
                    continue

                label = str(value).strip().lower()
                label = re.sub(r"^\s*\d+\s*[.)]?\s*", "", label)
                labels.append(label.strip())

            if any(
                value in {"realisasi", "actual"}
                for value in labels
            ):
                bagian_aktif = "Realisasi"

            elif any(value == "apbn" for value in labels):
                bagian_aktif = "APBN"

            if (
                bagian_aktif == "Realisasi"
                and any(value == "belanja negara" for value in labels)
            ):
                baris_target = row_idx
                break

        if baris_target is None:
            raise ValueError(
                "Baris Belanja Negara pada bagian Realisasi tidak ditemukan."
            )

        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        hasil = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            nilai_asli = df.iat[
                baris_target,
                kolom_tahun[tahun],
            ]

            nilai = pd.to_numeric(
                pd.Series([nilai_asli]),
                errors="coerce",
            ).iloc[0]

            if pd.isna(nilai):
                raise ValueError(
                    f"Nilai Belanja Negara tahun {tahun} kosong."
                )

            raw_df = pd.DataFrame([{
                "data_x": pd.Timestamp(tahun, 12, 31),
                "data_y": float(nilai) * 1_000_000_000,
                "indikator": self.config["indikator"],
                "satuan": self.config["satuan"],
                "sumber": "realisasi",
                "frekuensi": self.config["frekuensi"],
            }])

            filepath = (
                self.RAW_DATA_DIR
                / f"bi_belanja_negara_total_{tahun}_raw.parquet"
            )

            save_parquet(
                raw_df,
                filepath,
            )

            print(f"\nRAW TERSIMPAN: {filepath}")
            print(raw_df.to_string(index=False))

            hasil.append(raw_df)

        return concat_dataframes(hasil)

    def scrape_belanja_hibah_pemerintah_pusat(
        self,
        tahun_awal=2020,
        tahun_akhir=None,
    ):
        """
        Scrape Realisasi Belanja Hibah Pemerintah Pusat
        dari Excel BI SEKI tabel 4.2 periode 2020-2025.
        Raw Parquet disimpan terpisah per tahun.
        """

        tahun_akhir = int(
            self.config.get("tahun_akhir", 2025)
            if tahun_akhir is None
            else tahun_akhir
        )

        source_url = self.config["source_url"]

        print("\n==============================")
        print("SCRAPE BELANJA HIBAH BI")
        print("==============================")
        print("URL:", source_url)

        excel_content = get_bytes(
            session=BI_SESSION,
            url=source_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/130.0.0.0 Safari/537.36"
                ),
                "Referer": self.config["nama_data_import"],
            },
            timeout=120,
        )

        if not excel_content:
            raise ValueError("File Excel BI yang diterima kosong.")


        excel_bytes = io.BytesIO(excel_content)
        xls = pd.ExcelFile(excel_bytes)

        sheet_name = next(
            (
                name for name in xls.sheet_names
                if name.strip() == "4.2"
            ),
            None,
        )

        if sheet_name is None:
            raise ValueError(
                f"Sheet 4.2 tidak ditemukan. "
                f"Sheet tersedia: {xls.sheet_names}"
            )

        excel_bytes.seek(0)

        df = pd.read_excel(
            excel_bytes,
            sheet_name=sheet_name,
            header=None,
        )

        # Cari kolom tahun
        kolom_tahun = {}

        for row_idx in range(len(df)):
            nilai_baris = pd.to_numeric(
                df.iloc[row_idx, 3:],
                errors="coerce",
            )

            if nilai_baris.between(1990, 2100).sum() < 2:
                continue

            for col_idx in range(3, len(df.columns)):
                nilai = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(nilai)
                    and nilai == int(nilai)
                    and tahun_awal <= int(nilai) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(
                        int(nilai),
                        col_idx,
                    )

        tahun_diminta = set(
            range(tahun_awal, tahun_akhir + 1)
        )

        tahun_tidak_ditemukan = (
            tahun_diminta - set(kolom_tahun)
        )

        if tahun_tidak_ditemukan:
            raise ValueError(
                "Kolom tahun tidak ditemukan: "
                f"{sorted(tahun_tidak_ditemukan)}"
            )

        # Cari baris Realisasi Belanja Hibah
        bagian_aktif = None
        baris_hibah = None

        for row_idx in range(len(df)):
            teks_baris = [
                str(value).strip().lower()
                for value in df.iloc[row_idx].tolist()
                if pd.notna(value)
            ]

            label_baris = [
                re.sub(
                    r"^\d+\s*[.)]?\s*",
                    "",
                    str(value).strip(),
                ).lower()
                for value in df.iloc[row_idx, :3].tolist()
                if pd.notna(value)
            ]

            if any(
                teks in {"realisasi", "actual"}
                for teks in teks_baris
            ):
                bagian_aktif = "Realisasi"

            elif any(
                teks == "apbn"
                for teks in teks_baris
            ):
                bagian_aktif = "APBN"

            if (
                bagian_aktif == "Realisasi"
                and any(
                    "belanja hibah" in teks
                    for teks in label_baris
                )
            ):
                baris_hibah = row_idx
                break

        if baris_hibah is None:
            raise ValueError(
                "Baris Realisasi Belanja Hibah tidak ditemukan."
            )

        print("Baris Belanja Hibah:", baris_hibah)
        print("Kolom tahun:", kolom_tahun)

        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        hasil_semua_tahun = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            nilai = pd.to_numeric(
                pd.Series([
                    df.iat[
                        baris_hibah,
                        kolom_tahun[tahun],
                    ]
                ]),
                errors="coerce",
            ).iloc[0]

            if pd.isna(nilai):
                raise ValueError(
                    f"Nilai Belanja Hibah tahun {tahun} kosong."
                )

            hasil_tahun = pd.DataFrame([{
                "data_x": pd.Timestamp(tahun, 12, 31),
                "data_y": float(nilai) * 1_000_000_000,
                "indikator": self.config["indikator"],
                "satuan": self.config["satuan"],
                "sumber": self.config["sumber"],
                "frekuensi": self.config["frekuensi"],
            }])

            filename = (
                "bi_belanja_hibah_pemerintah_pusat_"
                f"{tahun}_raw.parquet"
            )

            filepath = self.RAW_DATA_DIR / filename

            save_parquet(
                hasil_tahun,
                filepath,
            )

            print(f"\nRAW PARQUET TERSIMPAN: {filepath}")
            print(hasil_tahun.to_string(index=False))

            hasil_semua_tahun.append(hasil_tahun)

        hasil = concat_dataframes(hasil_semua_tahun)

        print("\n==============================")
        print("RINGKASAN BELANJA HIBAH")
        print("==============================")
        print("Rentang:", tahun_awal, "-", tahun_akhir)
        print("Total baris:", len(hasil))
        print(hasil.to_string(index=False))

        return hasil

    def scrape_belanja_subsidi_pemerintah(
        self,
        tahun_awal=2023,
        tahun_akhir=2025,
    ):
        """
        Scrape Realisasi Belanja Subsidi Pemerintah
        dari BI SEKI Tabel 4.2.
        RAW disimpan terpisah per tahun.
        """

        source_url = self.config["source_url"]

        print("\n==============================")
        print("SCRAPE BELANJA SUBSIDI BI")
        print("==============================")
        print("URL:", source_url)
        print("PERIODE:", tahun_awal, "-", tahun_akhir)

        excel_content = get_bytes(
            session=BI_SESSION,
            url=source_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/130.0.0.0 Safari/537.36"
                ),
                "Referer": self.config["nama_data_import"],
            },
            timeout=120,
        )

        if not excel_content:
            raise ValueError("File Excel BI yang diterima kosong.")

        excel_bytes = io.BytesIO(excel_content)
        xls = pd.ExcelFile(excel_bytes)

        sheet_name = next(
            (
                name for name in xls.sheet_names
                if name.strip() == "4.2"
            ),
            None,
        )

        if sheet_name is None:
            raise ValueError(
                f"Sheet 4.2 tidak ditemukan: {xls.sheet_names}"
            )

        excel_bytes.seek(0)

        df = pd.read_excel(
            excel_bytes,
            sheet_name=sheet_name,
            header=None,
        )

        # Cari kolom tahun
        kolom_tahun = {}

        for row_idx in range(len(df)):
            for col_idx in range(3, len(df.columns)):
                nilai = pd.to_numeric(
                    pd.Series([df.iat[row_idx, col_idx]]),
                    errors="coerce",
                ).iloc[0]

                if (
                    pd.notna(nilai)
                    and nilai == int(nilai)
                    and tahun_awal <= int(nilai) <= tahun_akhir
                ):
                    kolom_tahun.setdefault(
                        int(nilai),
                        col_idx,
                    )

        tahun_diminta = set(
            range(tahun_awal, tahun_akhir + 1)
        )

        tahun_tidak_ditemukan = (
            tahun_diminta - set(kolom_tahun)
        )

        if tahun_tidak_ditemukan:
            raise ValueError(
                "Kolom tahun tidak ditemukan: "
                f"{sorted(tahun_tidak_ditemukan)}"
            )

        # Cari baris Realisasi -> Subsidi
        bagian_aktif = None
        baris_subsidi = None

        for row_idx in range(len(df)):
            teks_baris = [
                str(value).strip().lower()
                for value in df.iloc[row_idx].tolist()
                if pd.notna(value)
            ]

            label_baris = [
                re.sub(
                    r"^\d+\s*[.)]?\s*",
                    "",
                    str(value).strip(),
                ).lower()
                for value in df.iloc[row_idx, :3].tolist()
                if pd.notna(value)
            ]

            if any(
                teks in {"realisasi", "actual"}
                for teks in teks_baris
            ):
                bagian_aktif = "Realisasi"

            elif any(
                teks == "apbn"
                for teks in teks_baris
            ):
                bagian_aktif = "APBN"

            if (
                bagian_aktif == "Realisasi"
                and "subsidi" in label_baris
            ):
                baris_subsidi = row_idx
                break

        if baris_subsidi is None:
            raise ValueError(
                "Baris Realisasi Subsidi tidak ditemukan."
            )

        print("Baris Realisasi Subsidi:", baris_subsidi)
        print("Kolom tahun:", kolom_tahun)

        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        hasil_semua_tahun = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            nilai = pd.to_numeric(
                pd.Series([
                    df.iat[
                        baris_subsidi,
                        kolom_tahun[tahun],
                    ]
                ]),
                errors="coerce",
            ).iloc[0]

            if pd.isna(nilai):
                raise ValueError(
                    f"Nilai Subsidi tahun {tahun} kosong."
                )

            hasil_tahun = pd.DataFrame([{
                "data_x": pd.Timestamp(tahun, 12, 31),
                "data_y": float(nilai) * 1_000_000_000,
                "indikator": self.config["indikator"],
                "satuan": self.config["satuan"],
                "sumber": self.config["sumber"],
                "frekuensi": self.config["frekuensi"],
            }])

            filename = (
                "bi_belanja_subsidi_pemerintah_"
                f"{tahun}_raw.parquet"
            )

            filepath = self.RAW_DATA_DIR / filename

            save_parquet(
                hasil_tahun,
                filepath,
            )

            print(f"\nRAW TERSIMPAN: {filepath}")
            print(hasil_tahun.to_string(index=False))

            hasil_semua_tahun.append(hasil_tahun)

        hasil = concat_dataframes(hasil_semua_tahun)

        print("\nTOTAL RAW:", len(hasil))
        print(hasil.to_string(index=False))

        return hasil


    def scrape_dak_lkpp(self, tahun=None):
        """
        Menyimpan hasil ekstraksi DAK LKPP/LKBUN
        yang sudah diperiksa dari PDF ke raw Parquet.
        """
        print("\n==============================")
        print("RAW DATA DAK LKPP/LKBUN")
        print("==============================")

        data_dak = {
            2019: {
                "label": "Dana Transfer Khusus",
                "APBN": 200_368_843_603_000,
                "Realisasi": 186_394_672_896_195,
            },
            2020: {
                "label": "Dana Transfer Khusus",
                "APBN": 182_558_695_000_000,
                "Realisasi": 176_578_831_927_772,
            },
            2021: {
                "label": "Dana Transfer Khusus",
                "APBN": 196_423_545_000_000,
                "Realisasi": 184_638_297_985_284,
            },
            2022: {
                "label": "Dana Transfer Khusus",
                "APBN": 189_593_843_066_000,
                "Realisasi": 173_164_641_054_831,
            },
            2023: {
                "label": "Dana Alokasi Khusus",
                "APBN": 185_797_257_584_000,
                "Realisasi": 181_357_561_783_241,
            },
            2024: {
                "label": "Dana Alokasi Khusus",
                "APBN": 188_101_019_444_000,
                "Realisasi": 183_538_473_113_006,
            },
            2025: {
                "label": "Dana Alokasi Khusus",
                "APBN": 166_934_762_675_000,
                "Realisasi": 162_932_253_327_901,
            },
        }

        tahun_awal = int(self.config.get("tahun_awal", 2019))
        tahun_akhir = int(self.config.get("tahun_akhir", 2025))

        if tahun is not None:
            tahun_list = [int(tahun)]
        else:
            tahun_list = list(range(tahun_awal, tahun_akhir + 1))

        tahun_invalid = [
            t for t in tahun_list if t not in data_dak
        ]
        if tahun_invalid:
            raise ValueError(f"Tahun tidak tersedia: {tahun_invalid}")

        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        hasil_semua_tahun = []

        for t in tahun_list:
            info = data_dak[t]
            records = []

            for jenis in ["APBN", "Realisasi"]:
                records.append({
                    "data_x": pd.Timestamp(t, 12, 31),
                    "data_y": info[jenis],
                    "indikator": self.config["indikator"],
                    "provinsi": pd.NA,
                    "wilayah": "Indonesia",
                    "item": jenis,
                    "label_laporan": info["label"],
                    "satuan": self.config["satuan"],
                    "sumber": self.config["sumber"],
                    "frekuensi": self.config["frekuensi"],
                })

            df_tahun = pd.DataFrame(records)

            filepath = (
                self.RAW_DATA_DIR
                / f"bi_belanja_dana_alokasi_khusus_pemerintah_{t}_raw.parquet"
            )

            save_parquet(
                df_tahun,
                filepath,
            )

            print(f"\nTahun {t}: {len(df_tahun)} baris")
            print("Raw tersimpan:", filepath)
            print(df_tahun[["item", "data_y", "label_laporan"]].to_string(index=False))

            hasil_semua_tahun.append(df_tahun)

        hasil = concat_dataframes(hasil_semua_tahun)

        print("\nTOTAL RAW ROWS:", len(hasil))
        return hasil

    def scrape_dau_lkpp(self, tahun=None):
        """
        Menyimpan data DAU LKPP/LKBUN yang sudah diverifikasi
        ke raw Parquet per tahun.
        """
        print("\n==============================")
        print("RAW DATA DAU LKPP/LKBUN")
        print("==============================")

        data_dau = {
            2019: {
                "label": "Dana Alokasi Umum",
                "APBN": 417_873_582_692_000,
                "Realisasi": 420_910_238_556_000,
            },
            2020: {
                "label": "Dana Alokasi Umum",
                "APBN": 384_381_524_227_000,
                "Realisasi": 381_612_451_495_551,
            },
            2021: {
                "label": "Dana Alokasi Umum",
                "APBN": 390_291_390_288_000,
                "Realisasi": 377_791_390_288_000,
            },
            2022: {
                "label": "Dana Alokasi Umum",
                "APBN": 378_000_000_000_000,
                "Realisasi": 378_000_000_000_000,
            },
            2023: {
                "label": "Dana Alokasi Umum",
                "APBN": 396_000_000_000_000,
                "Realisasi": 398_029_628_594_542,
            },
            2024: {
                "label": "Dana Alokasi Umum",
                "APBN": 427_699_303_543_000,
                "Realisasi": 429_210_611_278_998,
            },
            2025: {
                "label": "Dana Alokasi Umum",
                "APBN": 438_625_121_056_000,
                "Realisasi": 433_292_088_063_189,
            },
        }

        # Jangan lanjutkan proses jika data tahunan belum lengkap.
        tahun_awal = int(self.config.get("tahun_awal", 2019))
        tahun_akhir = int(self.config.get("tahun_akhir", 2025))

        if tahun is not None:
            tahun_list = [int(tahun)]
        else:
            tahun_list = list(range(tahun_awal, tahun_akhir + 1))

        tahun_invalid = [t for t in tahun_list if t not in data_dau]
        if tahun_invalid:
            raise ValueError(
                f"Data DAU belum lengkap atau belum diverifikasi "
                f"untuk tahun: {tahun_invalid}"
            )

        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        hasil_semua_tahun = []

        for t in tahun_list:
            info = data_dau[t]
            records = []

            for jenis in ["APBN", "Realisasi"]:
                records.append({
                    "data_x": pd.Timestamp(t, 12, 31),
                    "data_y": info[jenis],
                    "indikator": self.config["indikator"],
                    "provinsi": pd.NA,
                    "wilayah": "Indonesia",
                    "item": jenis,
                    "label_laporan": info["label"],
                    "satuan": self.config["satuan"],
                    "sumber": self.config["sumber"],
                    "frekuensi": self.config["frekuensi"],
                })

            df_tahun = pd.DataFrame(records)

            filepath = (
                self.RAW_DATA_DIR
                / f"bi_belanja_dana_alokasi_umum_pemerintah_{t}_raw.parquet"
            )

            save_parquet(
                df_tahun,
                filepath,
            )

            print(f"\nTahun {t}: {len(df_tahun)} baris")
            print("Raw tersimpan:", filepath)
            print(
                df_tahun[["item", "data_y", "label_laporan"]]
                .to_string(index=False)
            )

            hasil_semua_tahun.append(df_tahun)

        hasil = concat_dataframes(hasil_semua_tahun)

        print("\nTOTAL RAW ROWS:", len(hasil))
        return hasil

    def scrape_kredit_perbankan(self):
        """
        Download Excel BI, parsing sheet historis dan terbaru,
        mengambil data mulai 2023, lalu menyimpan raw Parquet
        secara terpisah per tahun.
        """
        source_url = self.config.get("source_url")

        if not source_url:
            raise ValueError("source_url belum dikonfigurasi.")

        print("\n==============================")
        print("SCRAPE KREDIT PERBANKAN BI")
        print("==============================")
        print("URL:", source_url)

        # ==========================================
        # DOWNLOAD EXCEL BI
        # ==========================================
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()

        retry_strategy = Retry(
            total=4,
            connect=4,
            read=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/130.0.0.0 Safari/537.36"
            ),
            "Referer": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/Default.aspx"
            ),
        }

        response = session.get(
            source_url,
            headers=headers,
            timeout=(20, 120),
        )
        response.raise_for_status()

        if not response.content:
            raise ValueError("File Excel BI yang diterima kosong.")

        excel_bytes = io.BytesIO(response.content)
        xls = pd.ExcelFile(excel_bytes)

        # ==========================================
        # VALIDASI SHEET
        # ==========================================
        required_sheets = ["Th 2010-2024", "1.12"]

        missing = [
            sheet for sheet in required_sheets
            if sheet not in xls.sheet_names
        ]

        if missing:
            raise ValueError(
                f"Sheet Excel tidak ditemukan: {missing}. "
                f"Sheet tersedia: {xls.sheet_names}"
            )

        # ==========================================
        # PARSING SHEET
        # ==========================================
        parsed_sheets = []

        for sheet_name in required_sheets:
            print(f"\nParsing sheet: {sheet_name}")

            df_sheet = pd.read_excel(
                excel_bytes,
                sheet_name=sheet_name,
                header=None,
            )

            parsed = parse_sheet_kredit_bi(
                df_sheet,
                sheet_name,
            )

            print("Jumlah baris hasil:", len(parsed))
            print(
                "Rentang tanggal:",
                parsed["data_x"].min(),
                "-",
                parsed["data_x"].max(),
            )

            parsed_sheets.append(parsed)

        # ==========================================
        # GABUNG DATA
        # PRIORITASKAN SHEET 1.12
        # ==========================================
        historical = parsed_sheets[0].copy()
        historical["_source_order"] = 0

        latest = parsed_sheets[1].copy()
        latest["_source_order"] = 1

        df = pd.concat(
            [historical, latest],
            ignore_index=True,
        )

        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce",
        )

        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce",
        )

        df = (
            df.dropna(subset=["data_x", "data_y", "provinsi"])
            .sort_values("_source_order")
            .drop_duplicates(
                subset=["data_x", "provinsi"],
                keep="last",
            )
            .drop(columns="_source_order")
        )

        # ==========================================
        # FILTER MULAI JANUARI 2023
        # ==========================================
        df = df[
            df["data_x"] >= pd.Timestamp("2023-01-01")
        ].copy()

        if df.empty:
            raise ValueError(
                "Tidak ada data kredit perbankan sejak Januari 2023."
            )

        # ==========================================
        # VALIDASI TAHUN DATA
        # ==========================================
        tahun_sekarang = pd.Timestamp.today().year

        df = df[
            df["data_x"].dt.year.between(2023, tahun_sekarang)
        ].copy()

        if df.empty:
            raise ValueError(
                "Tidak ada data dengan tahun valid mulai 2023."
            )

        # ==========================================
        # STANDARDISASI RAW DATA
        # ==========================================
        df["indikator"] = self.config["indikator"]
        df["wilayah"] = df["provinsi"]
        df["satuan"] = self.config["satuan"]
        df["sumber"] = self.config["sumber"]
        df["frekuensi"] = self.config["frekuensi"]

        df = df[
            [
                "data_x",
                "data_y",
                "indikator",
                "provinsi",
                "wilayah",
                "satuan",
                "sumber",
                "frekuensi",
            ]
        ].sort_values(
            ["data_x", "provinsi"]
        ).reset_index(drop=True)

        print("\nDEBUG TAHUN DATA:")
        print(
            df["data_x"]
            .dt.year
            .value_counts()
            .sort_index()
            .to_string()
        )

        print("\nDEBUG SAMPLE TANGGAL:")
        print(
            df[["data_x", "provinsi", "data_y"]]
            .head(20)
            .to_string(index=False)
        )

        # ==========================================
        # SIMPAN RAW PARQUET PER TAHUN
        # ==========================================
        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        for tahun, df_tahun in df.groupby(df["data_x"].dt.year):
            filename = f"bi_kredit_perbankan_{tahun}_raw.parquet"
            filepath = self.RAW_DATA_DIR / filename

            save_parquet(
                df_tahun,
                filepath,
            )

            print(
                f"Tahun {tahun}: {len(df_tahun)} baris "
                f"| {df_tahun['provinsi'].nunique()} provinsi "
                f"| {filepath}"
            )

        # ==========================================
        # RINGKASAN HASIL
        # ==========================================
        print("\n==============================")
        print("RAW KREDIT PERBANKAN TERSIMPAN")
        print("==============================")
        print("Total baris:", len(df))
        print("Jumlah provinsi:", df["provinsi"].nunique())
        print(
            "Rentang tanggal:",
            df["data_x"].min(),
            "-",
            df["data_x"].max(),
        )

        print("\nContoh Desember 2023:")
        print(
            df.loc[
                df["data_x"].eq(pd.Timestamp("2023-12-31")),
                ["data_x", "provinsi", "data_y"],
            ].head(10).to_string(index=False)
        )

        return df

    def scrape_atm(self, source_file):
        """
        Scrape Tabel 5b - ATM dan ATM+Debet Regional
        menjadi raw dataframe standar BI.
        """

        print("\n==============================")
        print("SCRAPE ATM REGIONAL")
        print("==============================")

        if source_file is None:
            raise ValueError(
                "Source file ATM belum dikonfigurasi."
            )

        source_file = Path(source_file)

        if not source_file.exists():
            raise FileNotFoundError(
                f"Source file ATM tidak ditemukan: "
                f"{source_file}"
            )

        print("SOURCE FILE:", source_file)

        df_raw = pd.read_excel(
            source_file,
            sheet_name=0,
            header=None,
        )

        print(
            "RAW EXCEL SHAPE:",
            df_raw.shape
        )

        print(
            "\n=============================="
        )
        print(
            "DEBUG RAW EXCEL"
        )
        print(
            "=============================="
        )

        print(
            "\n=============================="
        )
        print(
            "DEBUG ATM STRUCTURE"
        )
        print(
            "=============================="
        )

        print(
            df_raw.iloc[
                25:156,
                0:3
            ].to_string(
                index=True,
                header=False
            )
        )

        # ==========================================
        # HEADER PERIODE
        # ==========================================

        tahun_row = df_raw.iloc[4]
        bulan_row = df_raw.iloc[5]

        tahun = tahun_row.ffill()

        records = []

        bulan_mapping = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }

        # ==========================================
        # LOOP PERIODE
        # ==========================================

        for col in range(
            3,
            df_raw.shape[1]
        ):

            tahun_value = tahun.iloc[col]
            bulan_value = bulan_row.iloc[col]

            if pd.isna(tahun_value):
                continue

            if pd.isna(bulan_value):
                continue

            try:
                tahun_value = int(
                    float(tahun_value)
                )
            except (
                ValueError,
                TypeError,
            ):
                continue

            bulan_value = str(
                bulan_value
            ).strip()

            bulan_num = bulan_mapping.get(
                bulan_value[:3]
            )

            if bulan_num is None:
                continue

            tanggal = (
                pd.Timestamp(
                    year=tahun_value,
                    month=bulan_num,
                    day=1,
                )
                + pd.offsets.MonthEnd(0)
            )

            # ======================================
            # LOOP WILAYAH / INDIKATOR
            # ======================================

            for row in range(
                6,
                df_raw.shape[0]
            ):

                # Kolom 0 = provinsi/wilayah
                wilayah = df_raw.iloc[
                    row, 0
                ]

                # Kolom 1 = indikator
                indikator = df_raw.iloc[
                    row, 1
                ]

                # Kolom 2 = satuan
                satuan = df_raw.iloc[
                    row, 2
                ]

                # Nilai sesuai periode
                nilai = df_raw.iloc[
                    row, col
                ]

                # ----------------------------------
                # VALIDASI WILAYAH
                # ----------------------------------

                if pd.isna(wilayah):
                    continue

                # ----------------------------------
                # VALIDASI INDIKATOR
                # ----------------------------------

                if pd.isna(indikator):
                    continue

                # ----------------------------------
                # VALIDASI NILAI
                # ----------------------------------

                if pd.isna(nilai):
                    continue

                try:
                    nilai = float(nilai)
                except (
                    ValueError,
                    TypeError,
                ):
                    continue

                records.append({
                    "indikator": str(
                        indikator
                    ).strip(),

                    "provinsi": str(
                        wilayah
                    ).strip(),

                    "satuan": str(
                        satuan
                    ).strip(),

                    "sumber": (
                        "Bank Indonesia (BI)"
                    ),

                    "data_x": tanggal,

                    "data_y": nilai,
                })

        df = pd.DataFrame(records)

        print(
            "TOTAL SCRAPED ROWS:",
            len(df)
        )

        # ==========================================
        # DEBUG ATM
        # ==========================================

        if not df.empty:

            print(
                "\n=============================="
            )
            print(
                "DEBUG ATM RAW"
            )
            print(
                "=============================="
            )

            print(
                "INDIKATOR:",
                df["indikator"]
                .dropna()
                .unique()
                .tolist()
            )

            print(
                "\nPROVINSI SAMPLE:"
            )

            print(
                df["provinsi"]
                .dropna()
                .unique()
                .tolist()[:20]
            )

        return df

    def scrape(self, source_file=None):

        if self.config["storage_name"] == "atm":

            if source_file is None:
                raise ValueError(
                    "source_file wajib diisi "
                    "untuk scraping ATM"
                )

            df = self.scrape_atm(
                source_file
            )

            self.RAW_DATA_DIR.mkdir(
                parents=True,
                exist_ok=True
            )

            save_parquet(
                df,
                self.RAW_FILE,
            )

            print(
                "RAW ATM SAVED:",
                self.RAW_FILE
            )

            return df

        raise ValueError(
            "Scraping belum dikonfigurasi "
            f"untuk dataset: "
            f"{self.config['storage_name']}"
        )


    def upload_existing_raw_to_minio(self):
        if self.config["storage_name"] == "kredit_perbankan":
            raw_files = sorted(
                self.RAW_DATA_DIR.glob(
                    "bi_kredit_perbankan_*_raw.parquet"
                )
            )
        else:
            raw_files = (
                [self.RAW_FILE]
                if self.RAW_FILE and self.RAW_FILE.exists()
                else []
            )

        if not raw_files:
            raise FileNotFoundError(
                f"File raw tidak ditemukan di {self.RAW_DATA_DIR}"
            )

        for raw_file in raw_files:
            object_name = (
                f"bi/raw/{self.config['storage_name']}/"
                f"{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(
                f"Raw data di-upload ke MinIO: "
                f"{MINIO_BUCKET}/{object_name}"
            )


    def upload_raw_belanja_bantuan_sosial_pemerintah_pusat(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_bantuan_sosial_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw file Bantuan Sosial tidak ditemukan di "
                f"{self.RAW_DATA_DIR}"
            )

        for raw_file in raw_files:
            object_name = (
                "bi/raw/belanja_bantuan_sosial_pemerintah_pusat/"
                f"{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(
                "Raw data di-upload ke MinIO: "
                f"{MINIO_BUCKET}/{object_name}"
            )


    def upload_raw_belanja_barang_pemerintah_pusat(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_barang_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw file Belanja Barang Pemerintah Pusat "
                f"tidak ditemukan di {self.RAW_DATA_DIR}"
            )

        for raw_file in raw_files:
            object_name = (
                "bi/raw/belanja_barang_pemerintah_pusat/"
                f"{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(
                f"Raw data di-upload ke MinIO: "
                f"{MINIO_BUCKET}/{object_name}"
            )


    def upload_raw_belanja_pegawai_pemerintah_pusat(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_pegawai_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw file Belanja Pegawai Pemerintah Pusat "
                f"tidak ditemukan di {self.RAW_DATA_DIR}"
            )

        for raw_file in raw_files:
            # Upload hanya data tahun 2022–2025.
            object_name = (
                "bi/raw/belanja_pegawai_pemerintah_pusat/"
                f"{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(
                f"Raw data di-upload ke MinIO: "
                f"{MINIO_BUCKET}/{object_name}"
            )

    def upload_raw_belanja_pemerintah_pusat(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "File RAW Belanja Pemerintah Pusat tidak ditemukan."
            )

        for raw_file in raw_files:
            object_name = (
                f"bi/raw/belanja_pemerintah_pusat/{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(f"RAW BERHASIL DIUPLOAD: {object_name}")

    def upload_raw_belanja_negara_total(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_negara_total_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "File RAW Belanja Negara: Total tidak ditemukan."
            )

        for raw_file in raw_files:
            object_name = (
                f"bi/raw/belanja_negara_total/{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(f"RAW BERHASIL DIUPLOAD: {object_name}")

    def upload_raw_belanja_hibah_pemerintah_pusat(self):
        """
        Upload seluruh file RAW Belanja Hibah Pemerintah Pusat ke MinIO.
        """

        storage_name = "belanja_hibah_pemerintah_pusat"

        raw_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "bi"
            / storage_name
        )

        raw_files = sorted(
            raw_dir.glob(
                "bi_belanja_hibah_pemerintah_pusat_*_raw.parquet"
            )
        )

        if not raw_files:
            print("Tidak ada file RAW Belanja Hibah Pemerintah Pusat.")
            return

        minio_client = get_minio_client()

        for file_path in raw_files:
            object_name = (
                f"bi/raw/{storage_name}/{file_path.name}"
            )

            upload_file(
                minio_client,
                MINIO_BUCKET,
                str(file_path),
                object_name,
            )

            print(f"Uploaded RAW: {object_name}")

        print(
            f"Total {len(raw_files)} file RAW "
            "Belanja Hibah berhasil diproses."
        )

    def upload_raw_belanja_dana_alokasi_khusus_pemerintah(self):
        raw_files = sorted(
            self.RAW_DATA_DIR.glob(
                "bi_belanja_dana_alokasi_khusus_pemerintah_*_raw.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "Raw Parquet DAK tidak ditemukan di "
                f"{self.RAW_DATA_DIR}"
            )

        for raw_file in raw_files:
            object_name = (
                "bi/raw/belanja_dana_alokasi_khusus_pemerintah/"
                f"{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            print(
                "Raw DAK di-upload ke MinIO: "
                f"{MINIO_BUCKET}/{object_name}"
            )


    def upload_raw_belanja_dana_alokasi_umum_pemerintah(self):
        """
        Upload file raw Parquet DAU yang sudah tersimpan
        ke MinIO.
        """
        print("\n==============================")
        print("UPLOAD RAW DATA DAU KE MINIO")
        print("==============================")

        tahun_awal = int(self.config.get("tahun_awal", 2019))
        tahun_akhir = int(self.config.get("tahun_akhir", 2025))

        uploaded = 0

        for tahun in range(tahun_awal, tahun_akhir + 1):
            raw_file = (
                self.RAW_DATA_DIR
                / f"bi_belanja_dana_alokasi_umum_pemerintah_{tahun}_raw.parquet"
            )

            if not raw_file.exists():
                print(f"File tidak ditemukan, dilewati: {raw_file}")
                continue

            object_name = (
                "bi/raw/belanja_dana_alokasi_umum_pemerintah/"
                f"{raw_file.name}"
            )

            upload_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_file,
                object_name,
                "application/octet-stream",
            )

            uploaded += 1
            print(f"Uploaded: {object_name}")

        print(f"\nTOTAL FILE DIUPLOAD: {uploaded}")


class BIETL:

    def __init__(self, config_name):

        self.config_name = config_name

        self.config = get_bi_config(
            config_name
        )

        self.scraper = BIScraper(
            self.config
        )

        self.minio_client = (
            self.scraper.minio_client
        )

        self.FINAL_DATA_DIR = (
            self.scraper.paths["final_dir"]
        )

        self.FINAL_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )


    def filter_data(
        self,
        df,
    ):

        indikator = (
            self.config["indikator"]
        )

        if isinstance(indikator, str):
            return df[
                df["indikator"].astype(str).str.strip()
                == indikator.strip()
            ].copy()

        return df[
            df["indikator"].isin(indikator)
        ].copy()


    def create_data(
        self,
        df,
        blok=None,
    ):

        print(
            "\n=============================="
        )

        print(
            "CREATE DATA:",
            self.config["storage_name"],
        )

        print(
            "=============================="
        )

        df = self.filter_data(
            df
        )

        # ----------------------------------------------------
        # KHUSUS DATA YANG MEMILIKI BLOK
        # ----------------------------------------------------

        if blok is not None:

            if "blok" not in df.columns:

                raise ValueError(
                    "Kolom 'blok' tidak "
                    "tersedia."
                )

            df = df[
                df["blok"] == blok
            ].copy()

        if df.empty:

            print(
                "Tidak ada data."
            )

            return pd.DataFrame()

        # ----------------------------------------------------
        # NAMA INDIKATOR
        # ----------------------------------------------------

        nama_data = self.config["nama_data"]

        if isinstance(nama_data, dict):

            nama_indikator = nama_data.get(
                blok,
                self.config["indikator"],
            )

        else:

            nama_indikator = nama_data

        # ----------------------------------------------------
        # STANDARD DATA
        # ----------------------------------------------------

        final_df = pd.DataFrame({

            "kabkota": pd.NA,

            "provinsi": (
                df["wilayah"]
                if "wilayah" in df.columns
                else df.get(
                    "provinsi",
                    pd.Series(
                        pd.NA,
                        index=df.index
                    )
                )
            ),

            "nama_indikator": nama_indikator,

            "nama_item": pd.NA,

            "idnamadata": (
                self.config.get(
                    "idnamadata"
                )
            ),

            "data_x": df["data_x"],

            "data_y": pd.to_numeric(
                df["data_y"],
                errors="coerce",
            ),

            "satuan": (
                self.config["satuan"]
            ),

            "sumber": (
                self.config["sumber"]
            ),

            "nama_data_import": (
                self.config[
                    "nama_data_import"
                ]
            ),

            "note": (
                self.config.get(
                    "note"
                )
            ),
        })

        final_df = final_df[
            final_df["data_y"].notna()
        ].copy()

        final_df = (
            final_df
            .sort_values(
                [
                    "data_x",
                    "provinsi",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        final_df["data_x"] = (
            final_df["data_x"]
            .dt.strftime(
                "%d-%m-%Y"
            )
        )

        duplicates = find_duplicates(
            final_df,
            subset=[
                "provinsi",
                "data_x",
            ],
        )

        if not duplicates.empty:

            print(
                "\nWARNING: "
                "Ditemukan duplicate!"
            )

        else:

            print(
                "Tidak ada duplicate."
            )

        return final_df


    def create_data_belanja_bantuan_sosial_pemerintah_pusat(self, df):
        # Ambil data APBN saja.
        df = df[
            df["sumber"]
            .astype(str)
            .str.strip()
            .str.upper()
            .eq("APBN")
        ].copy()

        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "nama_indikator": (
                "Belanja Bantuan Sosial Pemerintah Pusat"
            ),
            "item": df["sumber"].astype(str).str.strip(),
            "idnamadata": self.config.get("idnamadata"),
            "data_x": pd.to_datetime(
                df["data_x"],
                errors="coerce",
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"],
                errors="coerce",
            ),
            "satuan": "Rupiah",
            "sumber": "Bank Indonesia (BI)",
            "nama_data_import": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/default.aspx"
            ),
            "note": pd.NA,
        })

        final_df = final_df.dropna(
            subset=["data_x", "data_y"]
        ).reset_index(drop=True)

        duplicates = find_duplicates(
            final_df,
            subset=["data_x", "item"],
        )

        if not duplicates.empty:
            print("WARNING: Ditemukan duplicate:")
            print(duplicates.to_string(index=False))
        else:
            print("Tidak ada duplicate.")

        return final_df


    def create_data_belanja_barang_pemerintah_pusat(self, df):
        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "nama_indikator": "Belanja Barang Pemerintah Pusat",
            "item": df["sumber"].astype(str).str.strip(),
            "idnamadata": 15868,
            "data_x": pd.to_datetime(
                df["data_x"], errors="coerce"
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"], errors="coerce"
            ),
            "satuan": "Rupiah",
            "sumber": "Bank Indonesia (BI)",
            "nama_data_import": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/default.aspx"
            ),
            "note": pd.NA,
        })

        final_df = (
            final_df
            .dropna(subset=["data_x", "data_y"])
            .reset_index(drop=True)
        )

        # Cek duplikat berdasarkan tanggal dan kategori item.
        duplicates = find_duplicates(
            final_df,
            subset=["data_x", "item"],
        )

        if not duplicates.empty:
            print("WARNING: Ditemukan duplicate!")
            print(duplicates.to_string(index=False))
        else:
            print("Tidak ada duplicate.")

        return final_df


    def create_data_belanja_pegawai_pemerintah_pusat(self, df):
        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "nama_indikator": "Belanja Pegawai Pemerintah Pusat",
            "item": "Realisasi",
            "idnamadata": self.config.get("idnamadata"),
            "data_x": pd.to_datetime(
                df["data_x"],
                errors="coerce",
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"],
                errors="coerce",
            ),
            "satuan": "Rupiah",
            "sumber": "Bank Indonesia (BI)",
            "nama_data_import": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/default.aspx"
            ),
            "note": pd.NA,
        })

        final_df = final_df.dropna(
            subset=["data_x", "data_y"]
        ).reset_index(drop=True)

        duplicates = find_duplicates(
            final_df,
            subset=["data_x", "item"],
        )

        if not duplicates.empty:
            print("WARNING: Ditemukan duplicate:")
            print(duplicates.to_string(index=False))
        else:
            print("Tidak ada duplicate.")

        return final_df

    def create_data_belanja_pemerintah_pusat(self, df):
        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "nama_indikator": "Belanja Pemerintah Pusat",
            "item": df["sumber"].astype(str).str.strip(),
            "idnamadata": self.config.get("idnamadata"),
            "data_x": pd.to_datetime(
                df["data_x"], errors="coerce"
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"], errors="coerce"
            ),
            "satuan": "Rupiah",
            "sumber": "Bank Indonesia (BI)",
            "nama_data_import": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/default.aspx"
            ),
            "note": pd.NA,
        })

        print("\nDEBUG SEBELUM DROPNA:")
        print(final_df.to_string())
        print("\nDEBUG NILAI KOSONG:")
        print(final_df.isna().sum())

        final_df = final_df.dropna(subset=["data_x", "data_y"])

        print("\nDEBUG SETELAH DROPNA:", final_df.shape)

        return final_df


    def create_data_belanja_negara_total(self, df):
        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "nama_indikator": "Belanja Negara: Total",
            "item": df["sumber"].astype(str).str.strip(),
            "idnamadata": self.config.get("idnamadata"),
            "data_x": pd.to_datetime(
                df["data_x"], errors="coerce"
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"], errors="coerce"
            ),
            "satuan": "Rupiah",
            "sumber": "Bank Indonesia (BI)",
            "nama_data_import": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/default.aspx"
            ),
            "note": pd.NA,
        })

        final_df = final_df.dropna(subset=["data_x", "data_y"])

        final_df = find_duplicates(
            final_df,
            subset=["data_x", "item"],
        )

        return final_df


    def create_data_belanja_negara_total(self, df):
        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "nama_indikator": "Belanja Negara: Total",
            "item": df["sumber"].astype(str).str.strip(),
            "idnamadata": self.config.get("idnamadata"),
            "data_x": pd.to_datetime(
                df["data_x"], errors="coerce"
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"], errors="coerce"
            ),
            "satuan": "Rupiah",
            "sumber": "Bank Indonesia (BI)",
            "nama_data_import": (
                "https://www.bi.go.id/id/statistik/"
                "ekonomi-keuangan/seki/default.aspx"
            ),
            "note": pd.NA,
        })

        print("\nDEBUG FINAL SEBELUM DROPNA:")
        print(final_df.to_string(index=False))
        print("\nDEBUG NILAI KOSONG:")
        print(final_df.isna().sum())

        final_df = final_df.dropna(
            subset=["data_x", "data_y"]
        )

        print("\nDEBUG SETELAH DROPNA:", final_df.shape)

        return final_df

    def create_data_belanja_hibah_pemerintah_pusat(self, df):
        print("\n==============================")
        print("CREATE DATA BELANJA HIBAH")
        print("==============================")

        df = df[
            df["indikator"] == self.config["indikator"]
        ].copy()

        if df.empty:
            print("Data Belanja Hibah kosong.")
            return pd.DataFrame()

        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": self.config.get("negara", "Indonesia"),
            "nama_indikator": self.config["nama_data"],
            "nama_item": self.config.get("nama_item", pd.NA),
            "idnamadata": self.config["idnamadata"],
            "data_x": pd.to_datetime(
                df["data_x"], errors="coerce"
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"], errors="coerce"
            ),
            "satuan": self.config["satuan"],
            "sumber": self.config["sumber"],
            "nama_data_import": self.config["nama_data_import"],
            "note": pd.NA,
        })

        print("\nDEBUG FINAL SEBELUM DROPNA:")
        print(final_df.to_string(index=False))

        print("\nDEBUG NILAI KOSONG:")
        print(final_df.isna().sum())

        final_df = final_df.dropna(
            subset=["data_x", "data_y"]
        ).copy()

        print("\nDEBUG SETELAH DROPNA:", final_df.shape)
        print(final_df.to_string(index=False))

        return final_df

    def create_data_belanja_subsidi_pemerintah(self, df):
        print("\n==============================")
        print("CREATE DATA BELANJA SUBSIDI")
        print("==============================")

        if df is None or df.empty:
            raise ValueError("RAW Belanja Subsidi kosong.")

        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": pd.NA,
            "negara": self.config.get("negara", "Indonesia"),
            "nama_indikator": self.config["nama_data"],
            "nama_item": pd.NA,
            "idnamadata": self.config["idnamadata"],
            "data_x": pd.to_datetime(
                df["data_x"],
                errors="coerce",
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                df["data_y"],
                errors="coerce",
            ),
            "satuan": self.config["satuan"],
            "sumber": self.config["sumber"],
            "nama_data_import": self.config["nama_data_import"],
            "note": pd.NA,
        })

        final_df = final_df.dropna(
            subset=["data_x", "data_y"]
        ).copy()

        # Pastikan hanya 2023–2025
        tahun = pd.to_datetime(
            final_df["data_x"],
            format="%d-%m-%Y",
            errors="coerce",
        ).dt.year

        final_df = final_df[
            tahun.between(2023, 2025)
        ].copy()

        final_df = final_df.sort_values(
            "data_x"
        ).reset_index(drop=True)

        print("\nFINAL DATA:")
        print(final_df.to_string(index=False))
        print("\nTOTAL FINAL ROWS:", len(final_df))

        return final_df


    def create_data_belanja_dana_alokasi_umum_pemerintah(self, df):
        """
        Membentuk dataset final DAU dari raw DataFrame.
        """
        print("\n==============================")
        print("CREATE FINAL DATA DAU")
        print("==============================")

        if df is None or df.empty:
            raise ValueError("Data raw DAU kosong.")

        final_df = pd.DataFrame({
            "kabkota": pd.NA,
            "provinsi": df["provinsi"],
            "negara": "Indonesia",
            "nama_indikator": self.config["indikator"],
            "item": df["item"],
            "idnamadata": self.config["idnamadata"],
            "data_x": pd.to_datetime(
                df["data_x"], errors="coerce"
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(df["data_y"], errors="coerce"),
            "satuan": df["satuan"],
            "sumber": df["sumber"],
            "nama_data_import": self.config["nama_data_import"],
            "note": df["label_laporan"],
        })

        final_df = final_df.dropna(subset=["data_x", "data_y"])

        duplicates = find_duplicates(
            final_df,
            subset=["data_x", "item"],
        )

        if not duplicates.empty:
            print("PERINGATAN: Ditemukan baris duplikat berdasarkan data_x dan item.")
            print(
                duplicates[["data_x", "item", "data_y"]]
                .to_string(index=False)
            )

        final_df = final_df.reset_index(drop=True)

        print("\nTOTAL FINAL ROWS:", len(final_df))
        print(final_df.head().to_string(index=False))

        return final_df


    def create_atm(self, df):
        df = df[
            df["indikator"].astype(str).str.strip()
            == "Jumlah Mesin ATM"
        ].copy()

        if df.empty:
            print(
                "WARNING: Data indikator "
                "'Jumlah Mesin ATM' tidak ditemukan."
            )
            return pd.DataFrame()

        final_df = pd.DataFrame({
            "kabkota": pd.NA,

            "provinsi": df["provinsi"],

            "nama_indikator": self.config[
                "nama_data"
            ],

            "nama_item": pd.NA,

            "idnamadata": self.config[
                "idnamadata"
            ],

            "data_x": pd.to_datetime(
                df["data_x"],
                errors="coerce",
            ).dt.strftime(
                "%d-%m-%Y"
            ),

            "data_y": pd.to_numeric(
                df["data_y"],
                errors="coerce",
            ),

            "satuan": self.config[
                "satuan"
            ],

            "sumber": self.config[
                "sumber"
            ],

            "nama_data_import": self.config[
                "nama_data_import"
            ],

            "note": self.config[
                "note"
            ],
        })

        return filter_not_null(
            final_df,
            "data_y",
        )

    def generate_nama_data(self, blok=None):

        nama_data = self.config["nama_data"]

        if isinstance(nama_data, dict):

            return nama_data.get(
                blok,
                self.config["indikator"],
            )

        return nama_data


    def save_final_data(
        self,
        df,
        filename,
        nama_data,
    ):

        filepath = (
            self.FINAL_DATA_DIR
            / filename
        )

        save_csv(
            df,
            filepath,
        )

        print(
            "\nFinal data disimpan:"
        )

        print(
            filepath
        )

        object_name = (
            f"bi/final/"
            f"{self.config['storage_name']}/"
            f"{filename}"
        )

        upload_file(
            self.minio_client,
            MINIO_BUCKET,
            filepath,
            object_name,
            "text/csv",
        )

        print(
            "Final data di-upload "
            "ke MinIO:"
        )

        print(
            f"{MINIO_BUCKET}/"
            f"{object_name}"
        )

        print(
            "Nama data:",
            nama_data,
        )

        print(
            "Total rows:",
            len(df),
        )

        return filepath


    def run_belanja_bantuan_sosial_pemerintah_pusat(self):
        # 1. Scrape Excel BI dan simpan raw Parquet.
        self.scraper.scrape_belanja_bantuan_sosial_pemerintah_pusat()

        # 2. Upload raw Parquet ke MinIO.
        self.scraper.upload_raw_belanja_bantuan_sosial_pemerintah_pusat()

        # 3. Baca raw data.
        df = (
            self.scraper
            .load_raw_data_belanja_bantuan_sosial_pemerintah_pusat()
        )

        # 4. Normalisasi mengikuti pipeline BI yang sudah ada.
        df = self.scraper.prepare_dataframe(df)

        # 5. Transformasi final, APBN saja.
        final_df = (
            self
            .create_data_belanja_bantuan_sosial_pemerintah_pusat(df)
        )

        if final_df.empty:
            raise ValueError(
                "Final data Bantuan Sosial APBN kosong."
            )

        print("\nPreview final data Bantuan Sosial APBN:")
        print(
            final_df[["item", "data_x", "data_y"]]
            .to_string(index=False)
        )

        # 6. Upload CSV final ke MinIO.
        filename = (
            "bi_belanja_bantuan_sosial_pemerintah_pusat.csv"
        )

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        return final_df


    def run_belanja_barang_pemerintah_pusat(self):
        # 1. Load raw Parquet yang sudah tersedia
        df = (
            self.scraper
            .load_raw_data_belanja_barang_pemerintah_pusat()
        )

        # 2. Normalisasi
        df = self.scraper.prepare_dataframe(df)

        # 3. Transformasi final
        final_df = (
            self.create_data_belanja_barang_pemerintah_pusat(df)
        )

        if final_df.empty:
            print("WARNING: Final data Belanja Barang kosong.")
            return final_df

        # 4. Validasi jumlah data dan duplikat sebelum menyimpan
        print("\nPreview final data:")
        print(
            final_df[
                ["item", "data_x", "data_y"]
            ].to_string(index=False)
        )

        # 5. Simpan CSV final dan upload ke MinIO
        filename = "bi_belanja_barang_pemerintah_pusat.csv"

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        return final_df

    def run_belanja_pegawai_pemerintah_pusat(self):
        # 1. Baca Excel lokal dan simpan RAW Parquet (2022–2025).
        self.scraper.scrape_belanja_pegawai_pemerintah_pusat(
            tahun_awal=2022,
            tahun_akhir=2025,
        )

        # 2. Upload RAW Parquet ke MinIO.
        self.scraper.upload_raw_belanja_pegawai_pemerintah_pusat()

        # 3. Baca RAW data.
        df = (
            self.scraper
            .load_raw_data_belanja_pegawai_pemerintah_pusat()
        )

        # 4. Normalisasi mengikuti pipeline BI yang sudah ada.
        df = self.scraper.prepare_dataframe(df)

        # 5. Transformasi final.
        final_df = (
            self
            .create_data_belanja_pegawai_pemerintah_pusat(df)
        )

        if final_df.empty:
            raise ValueError(
                "Final data Belanja Pegawai kosong."
            )

        print("\nPreview final data Belanja Pegawai:")
        print(
            final_df[["item", "data_x", "data_y"]]
            .to_string(index=False)
        )

        # 6. Upload CSV final ke MinIO.
        filename = "bi_belanja_pegawai_pemerintah_pusat.csv"

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        return final_df


    def run_belanja_pemerintah_pusat(self):
        # 1. Scrape Excel lokal dan simpan RAW Parquet.
        self.scraper.scrape_belanja_pemerintah_pusat(
            tahun_awal=self.config.get("tahun_awal", 2023),
            tahun_akhir=self.config.get("tahun_akhir", 2025),
        )

        # 2. Upload RAW ke MinIO.
        self.scraper.upload_raw_belanja_pemerintah_pusat()

        # 3. Load RAW.
        df = self.scraper.load_raw_data_belanja_pemerintah_pusat()

        # 4. Normalisasi.
        df = self.scraper.prepare_dataframe(df)

        # 5. Transformasi final.
        final_df = self.create_data_belanja_pemerintah_pusat(df)

        if final_df.empty:
            raise ValueError("Final data Belanja Pemerintah Pusat kosong.")

        print("\nPreview final data:")
        print(
            final_df[["item", "data_x", "data_y"]]
            .to_string(index=False)
        )

        # 6. Simpan dan upload CSV final.
        filename = "bi_belanja_pemerintah_pusat.csv"

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        return final_df


    def run_belanja_negara_total(self):
        self.scraper.scrape_belanja_negara_total(
            tahun_awal=self.config.get("tahun_awal", 2023),
            tahun_akhir=self.config.get("tahun_akhir", 2025),
        )

        self.scraper.upload_raw_belanja_negara_total()

        df = self.scraper.load_raw_data_belanja_negara_total()
        df = self.scraper.prepare_dataframe(df)

        final_df = self.create_data_belanja_negara_total(df)

        if final_df.empty:
            raise ValueError(
                "Final data Belanja Negara: Total kosong."
            )

        print("\nPREVIEW FINAL:")
        print(final_df[["item", "data_x", "data_y"]].to_string(index=False))

        filename = "bi_belanja_negara_total.csv"

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        print("TOTAL ROWS FINAL:", len(final_df))

        return final_df

    def run_belanja_hibah_pemerintah_pusat(self):
        print("\n========================================")
        print("BI ETL: BELANJA HIBAH PEMERINTAH PUSAT")
        print("========================================")

        # 1. Upload RAW ke MinIO
        self.scraper.upload_raw_belanja_hibah_pemerintah_pusat()

        # 2. Load seluruh RAW Parquet 2020-2025
        df = (
            self.scraper
            .load_raw_data_belanja_hibah_pemerintah_pusat()
        )

        # 3. Normalize
        df = self.scraper.prepare_dataframe(df)

        # 4. Transform
        final_df = (
            self.create_data_belanja_hibah_pemerintah_pusat(df)
        )

        if final_df.empty:
            print("Tidak ada data final.")
            return final_df

        # 5. Save CSV dan upload final ke MinIO
        filename = (
            "bi_belanja_hibah_pemerintah_pusat.csv"
        )

        filepath = self.FINAL_DATA_DIR / filename

        save_csv(
            final_df,
            filepath,
        )

        print("\nFinal data disimpan:")
        print(filepath)

        object_name = (
            "bi/final/belanja_hibah_pemerintah_pusat/"
            f"{filename}"
        )

        upload_file(
            self.minio_client,
            MINIO_BUCKET,
            filepath,
            object_name,
            "text/csv",
        )

        print("Final data di-upload ke MinIO:")
        print(f"{MINIO_BUCKET}/{object_name}")
        print("TOTAL FINAL ROWS:", len(final_df))

        return final_df

    def run_belanja_subsidi_pemerintah(self):
        print("\n========================================")
        print("BI ETL: BELANJA SUBSIDI PEMERINTAH")
        print("========================================")

        # 1. Scrape Excel BI untuk 2023–2025
        self.scraper.scrape_belanja_subsidi_pemerintah(
            tahun_awal=2023,
            tahun_akhir=2025,
        )

        # 2. Upload RAW ke MinIO
        self.scraper.upload_raw_belanja_subsidi_pemerintah()

        # 3. Load RAW
        df = (
            self.scraper
            .load_raw_data_belanja_subsidi_pemerintah()
        )

        # 4. Normalize
        df = self.scraper.prepare_dataframe(df)

        # 5. Transform
        final_df = (
            self.create_data_belanja_subsidi_pemerintah(df)
        )

        if final_df.empty:
            raise ValueError("Final data Belanja Subsidi kosong.")

        # 6. Simpan CSV final
        filename = "bi_belanja_subsidi_pemerintah.csv"

        filepath = self.FINAL_DATA_DIR / filename

        save_csv(
            final_df,
            filepath,
        )

        print("\nFINAL CSV TERSIMPAN:")
        print(filepath)

        # 7. Upload final CSV ke MinIO
        object_name = (
            "bi/final/belanja_subsidi_pemerintah/"
            f"{filename}"
        )

        upload_file(
            self.minio_client,
            MINIO_BUCKET,
            filepath,
            object_name,
            "text/csv",
        )

        print("FINAL DIUPLOAD KE MINIO:")
        print(f"{MINIO_BUCKET}/{object_name}")

        return final_df

    def run_belanja_dana_alokasi_khusus_pemerintah(self):
        # 1. Simpan hasil ekstraksi yang sudah diperiksa ke raw Parquet.
        self.scraper.scrape_dak_lkpp()

        # 2. Upload raw Parquet ke MinIO.
        self.scraper.upload_raw_belanja_dana_alokasi_khusus_pemerintah()

        # 3. Baca seluruh raw Parquet.
        df = (
            self.scraper
            .load_raw_data_belanja_dana_alokasi_khusus_pemerintah()
        )

        # 4. Transformasi menjadi format CSV final.
        final_df = (
            self.create_data_belanja_dana_alokasi_khusus_pemerintah(df)
        )

        if final_df.empty:
            raise ValueError("Final data DAK kosong.")

        # 5. Validasi ringkasan.
        print("\nPREVIEW CSV FINAL DAK:")
        print(
            final_df[["item", "data_x", "data_y", "note"]]
            .to_string(index=False)
        )

        print("\nJumlah baris:", len(final_df))
        print("Jumlah tahun:", final_df["data_x"].nunique())

        # 6. Simpan CSV final dan upload ke MinIO.
        filename = (
            "bi_belanja_dana_alokasi_khusus_pemerintah.csv"
        )

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        return final_df


    def run_belanja_dana_alokasi_umum_pemerintah(self):
        """
        Menjalankan proses ETL DAU dan menyimpan CSV final.
        """
        print("\n========================================")
        print("ETL BELANJA DANA ALOKASI UMUM PEMERINTAH")
        print("========================================")

        # 1. Buat raw data
        self.scraper.scrape_dau_lkpp()

        # 2. Upload raw Parquet ke MinIO
        self.scraper.upload_raw_belanja_dana_alokasi_umum_pemerintah()

        # 3. Baca raw Parquet
        df = self.scraper.load_raw_data_belanja_dana_alokasi_umum_pemerintah()

        # 4. Bentuk data final
        final_df = (
            self.create_data_belanja_dana_alokasi_umum_pemerintah(df)
        )

        # 5. Simpan CSV final
        filename = "bi_belanja_dana_alokasi_umum_pemerintah.csv"

        self.save_final_data(
            final_df,
            filename,
            self.config["nama_data"],
        )

        print("\nETL DAU SELESAI")
        print("File final:", filename)
        print("Total baris:", len(final_df))

        return final_df


    def run(self):

        print(
            "\n========================================"
        )

        print(
            "BI ETL:",
            self.config["storage_name"],
        )

        print(
            "========================================"
        )

        # ----------------------------------------------------
        # RAW → MINIO
        # ----------------------------------------------------

        self.scraper.\
            upload_existing_raw_to_minio()

        # ----------------------------------------------------
        # LOAD RAW
        # ----------------------------------------------------

        df = (
            self.scraper
            .load_raw_data()
        )

        # ----------------------------------------------------
        # NORMALIZE
        # ----------------------------------------------------

        df = (
            self.scraper
            .prepare_dataframe(df)
        )

        # ----------------------------------------------------
        # KHUSUS SKNBI
        # ----------------------------------------------------

        if (
            self.config[
                "storage_name"
            ]
            == "sknbi_regional"
        ):

            for blok in [
                "Kota Asal",
                "Kota Tujuan",
            ]:

                final_df = (
                    self.create_data(
                        df,
                        blok,
                    )
                )

                if final_df.empty:
                    continue

                if blok == "Kota Asal":

                    filename = (
                        "bi_sknbi_"
                        "nilai_transaksi_"
                        "kliring_kredit_"
                        "kota_asal_"
                        "provinsi.csv"
                    )

                else:

                    filename = (
                        "bi_sknbi_"
                        "nilai_transaksi_"
                        "kliring_kredit_"
                        "kota_tujuan_"
                        "provinsi.csv"
                    )

                self.save_final_data(
                    final_df,
                    filename,
                    self.config[
                        "nama_data"
                    ][blok],
                )

        # ----------------------------------------------------
        # DATA LAIN / ATM
        # ----------------------------------------------------

        else:

            final_df = (
                self.create_data(
                    df
                )
            )

            if final_df.empty:
                return final_df

            filename = (
                f"bi_"
                f"{self.config['storage_name']}"
                f".csv"
            )

            self.save_final_data(
                final_df,
                filename,
                self.config[
                    "nama_data"
                ],
            )

            return final_df

def get_existing_latest_period(
    id_nama_data,
):

    df_existing = (
        get_xflask_api_data(
            id_nama_data
        )
    )

    if df_existing.empty:
        return None

    if "data_x" not in (
        df_existing.columns
    ):
        return None

    df_existing["data_x"] = (
        pd.to_datetime(
            df_existing["data_x"],
            errors="coerce",
            dayfirst=True,
        )
    )

    return (
        df_existing["data_x"]
        .max()
    )

def get_existing_latest_period(
    id_nama_data,
):

    df_existing = (
        get_xflask_api_data(
            id_nama_data
        )
    )

    if df_existing.empty:
        return None

    if "data_x" not in (
        df_existing.columns
    ):
        return None

    df_existing["data_x"] = (
        pd.to_datetime(
            df_existing["data_x"],
            errors="coerce",
            dayfirst=True,
        )
    )

    return (
        df_existing["data_x"]
        .max()
    )

def get_xflask_api_data(id_nama_data):
    url = f"https://xflask.databoks.id/api/getdata/{id_nama_data}"

    token = os.getenv("KATADATA_API_KEY")

    headers = {
        "Authorization": f"Bearer {token}",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=60,
    )

    response.raise_for_status()

    result = response.json()

    if isinstance(result, dict):
        data = result.get("data", [])
    else:
        data = result

    return pd.DataFrame(data)

def update_bi_dataset(
    config,
    idnamadata=None,
    iditem=None,
    backfill=True,
):
    if idnamadata is None:
        idnamadata = config.get("idnamadata")

    if idnamadata is None:
        raise ValueError(
            "idnamadata belum dikonfigurasi."
        )

    config_name = next(
        (
            name
            for name, item in BI_CONFIG.items()
            if item is config
        ),
        None,
    )

    if config_name is None:
        raise ValueError(
            "Config BI tidak dikenali."
        )

    etl = BIETL(config_name)

    # ==========================================
    # AMBIL DATA EXISTING XFLASK
    # ==========================================

    existing_latest = None

    if not backfill:
        try:
            existing_latest = (
                get_existing_latest_period(
                    idnamadata
                )
            )
        except Exception as exc:
            print(
                "WARNING: gagal mengambil "
                "data existing:",
                exc,
            )

    # ==========================================
    # ATM
    # ==========================================

    if config_name == "atm":

        # ------------------------------------------
        # VALIDASI SOURCE
        # ------------------------------------------

        source_file = config.get(
            "source_file"
        )

        if source_file is None:
            raise FileNotFoundError(
                "Source ATM belum dikonfigurasi. "
                "Gunakan source Tabel 5i untuk "
                "Jumlah Mesin ATM Menurut Provinsi."
            )

        # ------------------------------------------
        # SOURCE → RAW
        # ------------------------------------------

        df = etl.scraper.scrape(
            source_file=source_file
        )

        # ------------------------------------------
        # CURRENT UPDATE
        # DATA TERBARU + 2 TAHUN SEBELUMNYA
        # ------------------------------------------

        if (
            not backfill
            and existing_latest is not None
            and "data_x" in df.columns
        ):

            raw_dates = pd.to_datetime(
                df["data_x"],
                errors="coerce",
                dayfirst=True,
            )

            # Misalnya latest XFlask = 2022
            # maka mulai dari 2020-01-01
            start_date = pd.Timestamp(
                year=existing_latest.year - 2,
                month=1,
                day=1,
            )

            df = df[
                raw_dates >= start_date
            ].copy()

            print(
                "\n=============================="
            )
            print(
                "CURRENT UPDATE ATM"
            )
            print(
                "=============================="
            )
            print(
                "EXISTING LATEST:",
                existing_latest,
            )
            print(
                "START DATE:",
                start_date,
            )
            print(
                "RAW ROWS AFTER FILTER:",
                len(df),
            )

            # --------------------------------------
            # SIMPAN RAW YANG SUDAH DIFILTER
            # --------------------------------------

            save_parquet(
                df,
                etl.scraper.RAW_FILE,
            )

        # Dataset kredit perbankan: download dan parsing Excel
        # sebelum raw diunggah ke MinIO.
        if config_name == "kredit_perbankan":
            etl.scraper.scrape_kredit_perbankan()

        # Raw -> MinIO
        etl.scraper.upload_existing_raw_to_minio()

        # Load -> normalize
        df = etl.scraper.load_raw_data()
        df = etl.scraper.prepare_dataframe(df)

        # ------------------------------------------
        # CREATE ATM
        # ------------------------------------------

        final_df = etl.create_atm(
            df
        )

        if final_df.empty:
            return final_df

        filename = (
            "jumlah_mesin_atm_menurut_provinsi.csv"
        )

        etl.save_final_data(
            final_df,
            filename,
            config["nama_data"],
        )

        return final_df

    # ==========================================
    # KREDIT PERBANKAN
    # ==========================================

    if config_name == "kredit_perbankan":

        # 1. Download Excel BI dan simpan sebagai raw Parquet
        df = etl.scraper.scrape_kredit_perbankan()

        # 2. Upload raw ke MinIO
        etl.scraper.upload_existing_raw_to_minio()

        # 3. Load ulang dan normalisasi data
        df = etl.scraper.load_raw_data()
        df = etl.scraper.prepare_dataframe(df)

        # 4. Transformasi ke format final
        final_df = etl.create_data(df)

        if final_df.empty:
            print("WARNING: Final data kredit perbankan kosong.")
            return final_df

        # 5. Simpan final CSV dan upload ke MinIO
        filename = "bi_kredit_perbankan.csv"

        etl.save_final_data(
            final_df,
            filename,
            config["nama_data"],
        )

        return final_df

    # ==========================================
    # SKNBI / KLIRING
    # ==========================================

    if config_name == "sknbi_regional":

        results = []

        for blok in [
            "Kota Asal",
            "Kota Tujuan",
        ]:

            final_df = etl.create_data(
                df,
                blok,
            )

            if final_df.empty:
                continue

            if blok == "Kota Asal":

                filename = (
                    "bi_sknbi_"
                    "nilai_transaksi_"
                    "kliring_kredit_"
                    "kota_asal_"
                    "provinsi.csv"
                )

            else:

                filename = (
                    "bi_sknbi_"
                    "nilai_transaksi_"
                    "kliring_kredit_"
                    "kota_tujuan_"
                    "provinsi.csv"
                )

            etl.save_final_data(
                final_df,
                filename,
                etl.generate_nama_data(
                    blok
                ),
            )

            results.append(
                final_df
            )

        if not results:
            return pd.DataFrame()

        return concat_dataframes(results)

    # ==========================================
    # BELANJA PEMERINTAH PUSAT
    # ==========================================

    if config_name == "belanja_barang_pemerintah_pusat":
        return etl.run_belanja_barang_pemerintah_pusat()

    # ==========================================
    # BELANJA BANTUAN SOSIAL PEMERINTAH PUSAT
    # ==========================================

    if config_name == "belanja_bantuan_sosial_pemerintah_pusat":
        return etl.run_belanja_bantuan_sosial_pemerintah_pusat()

    # ==========================================
    # BELANJA PEGAWAI PEMERINTAH PUSAT
    # ==========================================

    if config_name == "belanja_pegawai_pemerintah_pusat":
        return etl.run_belanja_pegawai_pemerintah_pusat()

    # ==========================================
    # BELANJA PEMERINTAH PUSAT
    # ==========================================

    if config_name == "belanja_pemerintah_pusat":
        return etl.run_belanja_pemerintah_pusat()

    # ==========================================
    # BELANJA NEGARA: TOTAL
    # ==========================================

    if config_name == "belanja_negara_total":
        return etl.run_belanja_negara_total()

    # ==========================================
    # BELANJA HIBAH PEMERINTAH PUSAT
    # ==========================================

    if config_name == "belanja_hibah_pemerintah_pusat":
        return etl.run_belanja_hibah_pemerintah_pusat()

    # ==========================================
    # BELANJA SUBSIDI PEMERINTAH
    # ==========================================

    if config_name == "belanja_subsidi_pemerintah":
        return etl.run_belanja_subsidi_pemerintah()

    # ==========================================
    # BELANJA DANA ALOKASI KHUSUS PEMERINTAH
    # ==========================================

    if config_name == "belanja_dana_alokasi_khusus_pemerintah":
        return etl.run_belanja_dana_alokasi_khusus_pemerintah()

    # ==========================================
    # BELANJA DANA ALOKASI UMUM PEMERINTAH
    # ==========================================
    if config_name == "belanja_dana_alokasi_umum_pemerintah":
        return etl.run_belanja_dana_alokasi_umum_pemerintah()

    # ==========================================
    # DATASET BI LAIN
    # ==========================================

    final_df = etl.create_data(
        df
    )

    if final_df.empty:
        return final_df

    filename = (
        "jumlah_mesin_atm_menurut_provinsi.csv"
    )

    etl.save_final_data(
        final_df,
        filename,
        config["nama_data"],
    )

    return final_df

def update_idnamadata_atm(
    idnamadata=18452,
    iditem=None,
    backfill=True,
):

    config = get_bi_config(
        "atm"
    )

    return update_bi_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def update_curr_atm():

    return update_idnamadata_atm(
        backfill=False
    )

def update_idnamadata_kredit_perbankan(
    idnamadata=9767,
    iditem=None,
    backfill=True,
):
    config = get_bi_config("kredit_perbankan")

    return update_bi_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def update_curr_kredit_perbankan():
    return update_idnamadata_kredit_perbankan(
        backfill=False
    )

def fetch(params=None):

    params = params or {}

    idnamadata = params.get(
        "idnamadata"
    )

    backfill = params.get(
        "backfill",
        False,
    )

    if isinstance(
        backfill,
        str,
    ):

        backfill = (
            backfill
            .strip()
            .lower()
            in {
                "true",
                "1",
                "yes",
                "y",
            }
        )

    # ========================================================
    # BELANJA BARANG PEMERINTAH PUSAT
    # ========================================================

    if idnamadata == 15868:
        config = get_bi_config(
            "belanja_barang_pemerintah_pusat"
        )

        return update_bi_dataset(
            config=config,
            idnamadata=15868,
            backfill=backfill,
        )

    # ========================================================
    # ATM
    # ========================================================

    if idnamadata == 18452:

        return update_idnamadata_atm(
            idnamadata=18452,
            backfill=backfill,
        )

    # ========================================================
    # KREDIT PERBANKAN
    # ========================================================

    if idnamadata == 9767:

        return update_idnamadata_kredit_perbankan(
            idnamadata=idnamadata,
            backfill=backfill,
        )
    # ========================================================
    # BELANJA BANTUAN SOSIAL PEMERINTAH PUSAT
    # ========================================================

    if idnamadata == 15880:
        config = get_bi_config(
            "belanja_bantuan_sosial_pemerintah_pusat"
        )

        return update_bi_dataset(
            config=config,
            idnamadata=15880,
            backfill=backfill,
        )

    # ========================================================
    # BELANJA PEGAWAI PEMERINTAH PUSAT
    # ========================================================

    pegawai_config = get_bi_config(
        "belanja_pegawai_pemerintah_pusat"
    )

    pegawai_id = pegawai_config.get("idnamadata")

    if (
        pegawai_id is not None
        and str(idnamadata) == str(pegawai_id)
    ):
        return update_bi_dataset(
            config=pegawai_config,
            idnamadata=pegawai_id,
            backfill=backfill,
        )

    # ========================================================
    # BELANJA PEMERINTAH PUSAT
    # ========================================================

    belanja_pusat_config = get_bi_config(
        "belanja_pemerintah_pusat"
    )

    belanja_pusat_id = belanja_pusat_config.get("idnamadata")

    if (
        belanja_pusat_id is not None
        and str(idnamadata) == str(belanja_pusat_id)
    ):
        return update_bi_dataset(
            config=belanja_pusat_config,
            idnamadata=belanja_pusat_id,
            backfill=backfill,
        )

    # ========================================================
    # BELANJA NEGARA: TOTAL
    # ========================================================

    negara_total_config = get_bi_config("belanja_negara_total")
    negara_total_id = negara_total_config.get("idnamadata")

    if (
        negara_total_id is not None
        and str(idnamadata) == str(negara_total_id)
    ):
        return update_bi_dataset(
            config=negara_total_config,
            idnamadata=negara_total_id,
            backfill=backfill,
        )

    # ==========================================
    # BELANJA HIBAH PEMERINTAH PUSAT
    # ==========================================

    if idnamadata == 15879:

        config = get_bi_config(
            "belanja_hibah_pemerintah_pusat"
        )

        return update_bi_dataset(
            config=config,
            idnamadata=15879,
            backfill=True,
        )

    # ========================================================
    # BELANJA SUBSIDI PEMERINTAH
    # ========================================================

    if str(idnamadata) == "15876":
        config = get_bi_config(
            "belanja_subsidi_pemerintah"
        )

        return update_bi_dataset(
            config=config,
            idnamadata=15876,
            backfill=True,
        )

    # ========================================================
    # BELANJA DANA ALOKASI KHUSUS PEMERINTAH
    # ========================================================
    if idnamadata == 15895:
        config = get_bi_config(
            "belanja_dana_alokasi_khusus_pemerintah"
        )

        return update_bi_dataset(
            config=config,
            idnamadata=15895,
            backfill=backfill,
        )

    # ========================================================
    # BELANJA DANA ALOKASI UMUM PEMERINTAH
    # ========================================================
    if idnamadata == 15894:
        config = get_bi_config("belanja_dana_alokasi_umum_pemerintah")
        return update_bi_dataset(
            config=config,
            idnamadata=15894,
            backfill=backfill,
        )

    # ========================================================
    # EXISTING SKNBI
    # ========================================================

    blok = params.get(
        "blok",
        "Kota Asal",
    )

    etl = BIETL(
        "sknbi_regional"
    )

    df = (
        etl.scraper
        .load_raw_data()
    )

    df = (
        etl.scraper
        .prepare_dataframe(df)
    )

    return etl.create_data(
        df,
        blok,
    )