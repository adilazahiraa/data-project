import os
import argparse
import io
import re
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from utils.api import create_session
from utils.file import read_parquet, save_parquet, save_csv
from utils.data import normalize_dataframe
from utils.storage import get_minio_client, upload_bytes


# ============================================================
# ENV
# ============================================================

load_dotenv(override=True)


PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)

MINIO_BUCKET = "maganghub"

KATADATA_API_KEY = os.getenv(
    "KATADATA_API_KEY"
)

DATABASE_BASE_URL = (
    "https://xflask.databoks.id/database"
)

KEMENDAG_BASE_URL = (
    "https://satudata.kemendag.go.id"
)


# ============================================================
# CONFIG
# ============================================================

KEMENDAG_CONFIG = {

    # ========================================================
    # 14194 - NERACA PERDAGANGAN
    # ========================================================

    "neraca_perdagangan": {

        "idnamadata": 14194,

        "indikator": (
            "Neraca Perdagangan (total) "
            "Menurut Provinsi"
        ),

        "default_satuan": "US$",

        "sumber": (
            "Kementerian Perdagangan"
        ),

        "storage_name": (
            "neraca_perdagangan"
        ),

        "file_name": (
            "neraca_perdagangan_total_menurut_provinsi"
        ),

        "note": (
            "Neraca Perdagangan (total) "
            "Menurut Provinsi berdasarkan "
            "Kementerian Perdagangan"
        ),

        "nama_data_import": (
            "s3://maganghub/kemendag/"
            "neraca_perdagangan/final/"
            "neraca_perdagangan_total_menurut_provinsi.csv"
        ),

        "tahun_awal": 2022,

        "tahun_terbaru": 2025,

        # ----------------------------------------------------
        # KODE PROVINSI KEMENDAG
        # ----------------------------------------------------

        "provinsi": {
            "51": "BALI",
            "19": "BANGKA BELITUNG",
            "36": "BANTEN",
            "17": "BENGKULU",
            "34": "D.I. YOGYAKARTA",
            "31": "DKI JAKARTA",
            "75": "GORONTALO",
            "15": "JAMBI",
            "32": "JAWA BARAT",
            "33": "JAWA TENGAH",
            "35": "JAWA TIMUR",
            "61": "KALIMANTAN BARAT",
            "63": "KALIMANTAN SELATAN",
            "62": "KALIMANTAN TENGAH",
            "64": "KALIMANTAN TIMUR",
            "65": "KALIMANTAN UTARA",
            "21": "KEPULAUAN RIAU",
            "18": "LAMPUNG",
            "81": "MALUKU",
            "82": "MALUKU UTARA",
            "11": "NANGROE ACEH DARUSALAM",
            "52": "NUSA TENGGARA BARAT",
            "53": "NUSA TENGGARA TIMUR",
            "94": "PAPUA",
            "91": "PAPUA BARAT",
            "92": "PAPUA BARAT DAYA",
            "97": "PAPUA PEGUNUNGAN",
            "95": "PAPUA SELATAN",
            "96": "PAPUA TENGAH",
            "14": "RIAU",
            "76": "SULAWESI BARAT",
            "73": "SULAWESI SELATAN",
            "72": "SULAWESI TENGAH",
            "74": "SULAWESI TENGGARA",
            "71": "SULAWESI UTARA",
            "13": "SUMATERA BARAT",
            "16": "SUMATERA SELATAN",
            "12": "SUMATERA UTARA",
        },
    },

    # ========================================================
    # 14195 - NERACA PERDAGANGAN MIGAS
    # ========================================================

    "neraca_perdagangan_migas": {

        "idnamadata": 14195,

        "indikator": (
            "Neraca Perdagangan Migas "
            "Menurut Provinsi"
        ),

        "default_satuan": "US$",

        "sumber": (
            "Kementerian Perdagangan"
        ),

        "storage_name": (
            "neraca_perdagangan_migas"
        ),

        "file_name": (
            "neraca_perdagangan_migas_menurut_provinsi"
        ),

        "note": (
            "Neraca Perdagangan Migas "
            "Menurut Provinsi berdasarkan "
            "Kementerian Perdagangan"
        ),

        "nama_data_import": (
            "s3://maganghub/kemendag/"
            "neraca_perdagangan_migas/final/"
            "neraca_perdagangan_migas_menurut_provinsi.csv"
        ),

        "tahun_awal": 2022,

        "tahun_terbaru": 2025,

        # ----------------------------------------------------
        # TARGET DATA EXCEL
        # ----------------------------------------------------

        "sheet_name": "Neraca Perdagangan",

        "section": "NERACA PERDAGANGAN",

        "row_name": "MIGAS",

        # ----------------------------------------------------
        # KODE PROVINSI KEMENDAG
        # ----------------------------------------------------

        "provinsi": {
            "51": "BALI",
            "19": "BANGKA BELITUNG",
            "36": "BANTEN",
            "17": "BENGKULU",
            "34": "D.I. YOGYAKARTA",
            "31": "DKI JAKARTA",
            "75": "GORONTALO",
            "15": "JAMBI",
            "32": "JAWA BARAT",
            "33": "JAWA TENGAH",
            "35": "JAWA TIMUR",
            "61": "KALIMANTAN BARAT",
            "63": "KALIMANTAN SELATAN",
            "62": "KALIMANTAN TENGAH",
            "64": "KALIMANTAN TIMUR",
            "65": "KALIMANTAN UTARA",
            "21": "KEPULAUAN RIAU",
            "18": "LAMPUNG",
            "81": "MALUKU",
            "82": "MALUKU UTARA",
            "11": "NANGROE ACEH DARUSALAM",
            "52": "NUSA TENGGARA BARAT",
            "53": "NUSA TENGGARA TIMUR",
            "94": "PAPUA",
            "91": "PAPUA BARAT",
            "92": "PAPUA BARAT DAYA",
            "97": "PAPUA PEGUNUNGAN",
            "95": "PAPUA SELATAN",
            "96": "PAPUA TENGAH",
            "14": "RIAU",
            "76": "SULAWESI BARAT",
            "73": "SULAWESI SELATAN",
            "72": "SULAWESI TENGAH",
            "74": "SULAWESI TENGGARA",
            "71": "SULAWESI UTARA",
            "13": "SUMATERA BARAT",
            "16": "SUMATERA SELATAN",
            "12": "SUMATERA UTARA",
        },
    },

    # ========================================================
    # 14187 - NILAI PERDAGANGAN EKSPOR IMPOR
    # ========================================================

    "nilai_perdagangan_ekspor_impor": {

        "idnamadata": 14187,

        "indikator": (
            "Nilai Perdagangan Ekspor Impor (total) "
            "Menurut Provinsi"
        ),

        "default_satuan": "US$",

        "sumber": (
            "Kementerian Perdagangan"
        ),

        "storage_name": (
            "nilai_perdagangan_ekspor_impor"
        ),

        "file_name": (
            "nilai_perdagangan_ekspor_impor_total_menurut_provinsi"
        ),

        "note": (
            "Nilai Perdagangan Ekspor Impor (total) "
            "Menurut Provinsi berdasarkan "
            "Kementerian Perdagangan"
        ),

        "nama_data_import": (
            "s3://maganghub/kemendag/"
            "nilai_perdagangan_ekspor_impor/final/"
            "nilai_perdagangan_ekspor_impor_total_menurut_provinsi.csv"
        ),

        "tahun_awal": 2022,

        "tahun_terbaru": 2025,

        # ----------------------------------------------------
        # TARGET DATA EXCEL
        # ----------------------------------------------------

        "sheet_name": "Neraca Perdagangan",

        "section": "",

        "row_name": "TOTAL PERDAGANGAN",

        # ----------------------------------------------------
        # KODE PROVINSI KEMENDAG
        # ----------------------------------------------------

        "provinsi": {
            "51": "BALI",
            "19": "BANGKA BELITUNG",
            "36": "BANTEN",
            "17": "BENGKULU",
            "34": "D.I. YOGYAKARTA",
            "31": "DKI JAKARTA",
            "75": "GORONTALO",
            "15": "JAMBI",
            "32": "JAWA BARAT",
            "33": "JAWA TENGAH",
            "35": "JAWA TIMUR",
            "61": "KALIMANTAN BARAT",
            "63": "KALIMANTAN SELATAN",
            "62": "KALIMANTAN TENGAH",
            "64": "KALIMANTAN TIMUR",
            "65": "KALIMANTAN UTARA",
            "21": "KEPULAUAN RIAU",
            "18": "LAMPUNG",
            "81": "MALUKU",
            "82": "MALUKU UTARA",
            "11": "NANGROE ACEH DARUSALAM",
            "52": "NUSA TENGGARA BARAT",
            "53": "NUSA TENGGARA TIMUR",
            "94": "PAPUA",
            "91": "PAPUA BARAT",
            "92": "PAPUA BARAT DAYA",
            "97": "PAPUA PEGUNUNGAN",
            "95": "PAPUA SELATAN",
            "96": "PAPUA TENGAH",
            "14": "RIAU",
            "76": "SULAWESI BARAT",
            "73": "SULAWESI SELATAN",
            "72": "SULAWESI TENGAH",
            "74": "SULAWESI TENGGARA",
            "71": "SULAWESI UTARA",
            "13": "SUMATERA BARAT",
            "16": "SUMATERA SELATAN",
            "12": "SUMATERA UTARA",
        },
    },
}


# ============================================================
# CONFIG HELPER
# ============================================================

def get_kemendag_config(indikator):

    if indikator not in KEMENDAG_CONFIG:
        raise ValueError(
            f"Indikator Kemendag tidak ditemukan: "
            f"{indikator}"
        )

    return KEMENDAG_CONFIG[indikator]


def get_storage_paths(config):

    storage_name = config["storage_name"]
    file_name = config["file_name"]

    raw_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "kemendag"
        / storage_name
    )

    final_dir = (
        PROJECT_ROOT
        / "data"
        / "final"
        / "kemendag"
        / storage_name
    )

    raw_file = (
        raw_dir
        / f"{file_name}_raw.parquet"
    )

    final_file = (
        final_dir
        / f"{file_name}.csv"
    )

    return {
        "raw_dir": raw_dir,
        "raw_file": raw_file,
        "final_dir": final_dir,
        "final_file": final_file,
    }

def get_raw_year_file(config, tahun):
    paths = get_storage_paths(config)

    raw_dir = paths["raw_dir"]

    return (
        raw_dir
        / f"{config['file_name']}_{tahun}_raw.parquet"
    )

# ============================================================
# HELPER
# ============================================================

def normalize_province_name(value):

    if pd.isna(value):
        return pd.NA

    value = (
        str(value)
        .strip()
        .lower()
    )

    replacements = {
        ".": "",
        ",": "",
        "'": "",
        '"': "",
        "-": " ",
    }

    for old, new in replacements.items():
        value = value.replace(
            old,
            new,
        )

    value = " ".join(
        value.split()
    )

    aliases = {

        "bali": "bali",

        "bangka belitung":
            "kepulauan bangka belitung",

        "kep bangka belitung":
            "kepulauan bangka belitung",

        "kepulauan bangka belitung":
            "kepulauan bangka belitung",

        "kep riau":
            "kepulauan riau",

        "kepulauan riau":
            "kepulauan riau",

        "d i yogyakarta":
            "di yogyakarta",

        "di yogyakarta":
            "di yogyakarta",

        "dki jakarta":
            "dki jakarta",

        "jakarta":
            "dki jakarta",

        "nangroe aceh darusalam":
            "aceh",

        "aceh":
            "aceh",
    }

    return aliases.get(
        value,
        value,
    )


def parse_number(value):

    if pd.isna(value):
        return pd.NA

    value = str(value).strip()

    if value in {
        "",
        "-",
        "–",
        "—",
        "...",
        "…",
        "NA",
        "N/A",
    }:
        return pd.NA

    # Kemendag menggunakan angka desimal
    # dengan titik sebagai decimal separator.
    #
    # Contoh:
    # 6582.2
    # -97050.6

    value = value.replace(
        ",",
        "",
    )

    try:

        number = float(value)

        if number.is_integer():
            return int(number)

        return number

    except (
        ValueError,
        TypeError,
    ):

        return pd.NA


# ============================================================
# CLASS SCRAPER
# ============================================================

class KemendagScraper:

    def __init__(
        self,
        kode_provinsi,
        provinsi,
    ):

        self.kode_provinsi = str(
            kode_provinsi
        )

        self.provinsi = provinsi

        self.session = create_session()


    # ========================================================
    # DOWNLOAD EXCEL
    # ========================================================

    def download_excel(self):

        url = (
            f"{KEMENDAG_BASE_URL}"
            "/data-informasi/"
            "perdagangan-luar-negeri/"
            "neraca-perdagangan-provinsi/"
            f"download/{self.kode_provinsi}"
        )

        print(
            "DOWNLOAD:",
            self.provinsi,
        )

        print(
            "URL:",
            url,
        )

        response = self.session.get(
            url,
            timeout=60,
        )

        print(
            "STATUS:",
            response.status_code,
        )

        response.raise_for_status()

        return response.content


    # ========================================================
    # PARSE EXCEL
    # ========================================================

    def parse_excel(
        self,
        content,
        tahun_awal,
        tahun_akhir,
        section,
        row_name,
    ):
        excel = pd.ExcelFile(
            io.BytesIO(content)
        )

        print(
            "SHEET:",
            excel.sheet_names,
        )

        rows = []

        for sheet_name in excel.sheet_names:

            try:
                df = pd.read_excel(
                    io.BytesIO(content),
                    sheet_name=sheet_name,
                    header=None,
                )

            except Exception as e:
                print(
                    "⚠️ Gagal baca sheet:",
                    sheet_name,
                    e,
                )
                continue

            if df.empty:
                continue

            # --------------------------------------------------------
            # CARI HEADER YANG MENGANDUNG TAHUN
            # --------------------------------------------------------

            header_row = None

            for idx in range(min(len(df), 30)):

                row = df.iloc[idx]

                year_count = 0

                for value in row.tolist():

                    # WAJIB ubah ke string sebelum regex
                    if pd.isna(value):
                        continue

                    value_str = str(value).strip()

                    match = re.search(
                        r"\b(20\d{2})\b",
                        value_str,
                    )

                    if match:
                        year_count += 1

                # Header dianggap valid kalau
                # menemukan minimal 1 tahun
                if year_count > 0:
                    header_row = idx
                    break

            if header_row is None:
                print(
                    "⚠️ Header tahun tidak ditemukan:",
                    self.provinsi,
                )
                continue

            print(
                "HEADER ROW:",
                header_row,
            )

            # --------------------------------------------------------
            # MAP KOLOM TAHUN
            # --------------------------------------------------------

            header = df.iloc[header_row]

            year_columns = {}

            for col_idx in range(len(header)):

                value = header.iloc[col_idx]

                if pd.isna(value):
                    continue

                value_str = str(value).strip()

                match = re.search(
                    r"\b(20\d{2})\b",
                    value_str,
                )

                if match is None:
                    continue

                year = int(
                    match.group(1)
                )

                if (
                    tahun_awal
                    <= year
                    <= tahun_akhir
                ):
                    year_columns[year] = col_idx

            if not year_columns:
                print(
                    "⚠️ Tidak ada kolom tahun:",
                    self.provinsi,
                )
                continue

            print(
                "TAHUN TERDETEKSI:",
                sorted(year_columns.keys()),
            )

            # --------------------------------------------------------
            # CARI SECTION DAN BARIS TARGET
            # --------------------------------------------------------

            section_found = False
            target_row = None

            for idx in range(
                header_row + 1,
                len(df),
            ):

                row = df.iloc[idx]

                row_values = []

                for value in row.tolist():

                    if pd.isna(value):
                        continue

                    value_str = str(value)

                    cleaned = re.sub(
                        r"\s+",
                        " ",
                        value_str,
                    ).strip().upper()

                    row_values.append(cleaned)

                if not section:
                    if row_name.upper() in row_values:
                        target_row = idx
                        break

                # Cari section terlebih dahulu
                if section.upper() in row_values:

                    section_found = True

                    # Kalau section sekaligus merupakan
                    # target row, langsung gunakan baris ini
                    if row_name.upper() in row_values:
                        target_row = idx
                        break

                    continue

                # Setelah section ditemukan,
                # cari row target di bawahnya
                if section_found:

                    if row_name.upper() in row_values:
                        target_row = idx
                        break

            if target_row is None:

                print(
                    f"⚠️ Baris {row_name} "
                    f"di section {section} "
                    f"tidak ditemukan:",
                    self.provinsi,
                )

                continue

            print(
                "SECTION:",
                section,
            )

            print(
                "TARGET ROW:",
                target_row,
                row_name,
            )

            # --------------------------------------------------------
            # AMBIL DATA
            # --------------------------------------------------------

            for tahun in sorted(
                year_columns.keys()
            ):

                col_idx = year_columns[tahun]

                raw_value = df.iloc[
                    target_row,
                    col_idx,
                ]

                data_y = parse_number(
                    raw_value
                )

                rows.append({
                    "kode_wilayah":
                        self.kode_provinsi,

                    "provinsi":
                        self.provinsi,

                    "data_x":
                        pd.Timestamp(
                            year=tahun,
                            month=12,
                            day=31,
                        ),

                    "data_y":
                        data_y,
                })

            # Sudah menemukan data
            break

        if not rows:
            raise RuntimeError(
                f"Baris NERACA PERDAGANGAN "
                f"tahun {tahun_awal}-{tahun_akhir} "
                f"tidak ditemukan. "
                f"Provinsi: {self.provinsi}"
            )

        return pd.DataFrame(rows)


    # ========================================================
    # RUN
    # ========================================================

    def run(
        self,
        tahun_awal,
        tahun_akhir,
        section,
        row_name,
    ):

        print("\n")
        print(
            "--------------------------------"
        )

        print(
            self.provinsi
        )

        print(
            "--------------------------------"
        )

        print(
            "TAHUN:",
            f"{tahun_awal}-{tahun_akhir}",
        )

        content = (
            self.download_excel()
        )

        df = self.parse_excel(
            content=content,
            tahun_awal=tahun_awal,
            tahun_akhir=tahun_akhir,
            section=section,
            row_name=row_name,
        )

        print(
            "JUMLAH DATA:",
            len(df),
        )

        return df


# ============================================================
# SCRAPE ALL
# ============================================================

def scrape_all(
    config,
    tahun_awal,
    tahun_akhir,
    save_raw=True,
    raw_file=None,
):

    configs = config["provinsi"]

    section = config.get(
        "section",
        "NERACA PERDAGANGAN",
    )

    row_name = config.get(
        "row_name",
        "NERACA PERDAGANGAN",
    )

    raw_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "kemendag"
        / config["storage_name"]
    )

    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    all_data = []

    # ========================================================
    # SCRAPE PER TAHUN
    # ========================================================

    for tahun in range(
        tahun_awal,
        tahun_akhir + 1,
    ):

        print("\n")
        print(
            "========================================"
        )
        print(
            f"SCRAPE TAHUN {tahun}"
        )
        print(
            "========================================"
        )

        year_data = []

        for kode_provinsi, provinsi in (
            configs.items()
        ):

            scraper = KemendagScraper(
                kode_provinsi=kode_provinsi,
                provinsi=provinsi,
            )

            try:

                df = scraper.run(
                    tahun_awal=tahun,
                    tahun_akhir=tahun,
                    section=section,
                    row_name=row_name,
                )

                if df.empty:
                    print(
                        "⚠️ Data kosong:",
                        provinsi,
                        tahun,
                    )
                    continue

                year_data.append(df)

                print(
                    "✅ Berhasil:",
                    provinsi,
                    tahun,
                    len(df),
                    "row",
                )

            except Exception as e:

                print(
                    "❌ Gagal:",
                    provinsi,
                    tahun,
                )

                print(
                    "   ",
                    str(e),
                )

                continue

        if not year_data:
            print(
                f"⚠️ Tidak ada data "
                f"untuk tahun {tahun}"
            )
            continue

        df_year = pd.concat(
            year_data,
            ignore_index=True,
        )

        # ----------------------------------------------------
        # RAW ONLY
        # ----------------------------------------------------

        df_year = df_year[
            [
                "kode_wilayah",
                "provinsi",
                "data_x",
                "data_y",
            ]
        ].copy()

        # ----------------------------------------------------
        # SAVE PARQUET PER TAHUN
        # ----------------------------------------------------

        if save_raw:

            raw_file_year = (
                raw_dir
                / (
                    f"{config['file_name']}_"
                    f"{tahun}_raw.parquet"
                )
            )

            save_parquet(
                df_year,
                raw_file_year,
            )

            print(
                "RAW TAHUN:",
                raw_file_year,
            )

        all_data.append(df_year)

    if not all_data:
        raise RuntimeError(
            "Tidak ada data Kemendag "
            "yang berhasil di-scrape."
        )

    df_raw = pd.concat(
        all_data,
        ignore_index=True,
    )

    print(
        "\nTOTAL RAW:",
        len(df_raw),
    )

    print(
        "KOLOM RAW:",
        df_raw.columns.tolist(),
    )

    return df_raw


# ============================================================
# XFLASK
# ============================================================

def get_existing_xflask_data(
    id_nama_data,
):

    if not KATADATA_API_KEY:

        raise RuntimeError(
            "KATADATA_API_KEY "
            "tidak ditemukan di .env"
        )

    session = create_session()

    session.headers.update({
        "X-API-Key":
            KATADATA_API_KEY
    })

    response = session.get(
        f"{DATABASE_BASE_URL}/data",
        params={
            "id_nama_data":
                id_nama_data
        },
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "Response XFlask "
            "bukan list."
        )

    return pd.DataFrame(data)


def get_xflask_api_data(
    id_nama_data,
):

    if not KATADATA_API_KEY:

        raise RuntimeError(
            "KATADATA_API_KEY "
            "tidak ditemukan di .env"
        )

    session = create_session()

    session.headers.update({
        "X-Api-Key":
            KATADATA_API_KEY
    })

    response = session.get(
        f"https://xflask.databoks.id"
        f"/api/getdata/{id_nama_data}",
        timeout=60,
    )

    print("\n")
    print(
        "=============================="
    )
    print(
        "GET DATA XFLASK API"
    )
    print(
        "=============================="
    )

    print(
        "STATUS:",
        response.status_code,
    )

    response.raise_for_status()

    result = response.json()

    if not isinstance(
        result,
        dict,
    ):

        raise RuntimeError(
            f"Response XFlask "
            f"tidak sesuai: {result}"
        )

    if not result.get(
        "success"
    ):

        raise RuntimeError(
            f"XFlask API gagal: "
            f"{result}"
        )

    df = pd.DataFrame(
        result.get(
            "data",
            [],
        )
    )

    print(
        "JUMLAH ROW:",
        len(df),
    )

    print(
        "KOLOM:",
        df.columns.tolist(),
    )

    return df


def get_existing_latest_year(
    id_nama_data,
):

    try:

        df = get_xflask_api_data(
            id_nama_data
        )

    except Exception as e:

        print(
            "⚠️ Tidak bisa mengambil "
            "tahun existing XFlask:",
            e,
        )

        return None

    if df.empty:
        return None

    if "data_x" not in df.columns:
        return None

    dates = pd.to_datetime(
        df["data_x"],
        errors="coerce",
    )

    dates = dates.dropna()

    if dates.empty:
        return None

    return int(
        dates.dt.year.max()
    )


# ============================================================
# ETL CLASS
# ============================================================

class KemendagETL:

    def __init__(
        self,
        config_name="neraca_perdagangan",
    ):

        config = get_kemendag_config(
            config_name
        )

        paths = get_storage_paths(
            config
        )

        self.config = config

        self.raw_file = (
            paths["raw_file"]
        )

        self.final_file = (
            paths["final_file"]
        )

        self.minio_client = (
            get_minio_client()
        )

        if not KATADATA_API_KEY:

            raise RuntimeError(
                "KATADATA_API_KEY "
                "tidak ditemukan di .env"
            )

        self.session = (
            create_session()
        )

        self.session.headers.update({
            "X-API-Key":
                KATADATA_API_KEY
        })


    # ========================================================
    # LOAD RAW
    # ========================================================

    def load_raw(self):

        print("\n")
        print(
            "=============================="
        )
        print(
            "LOAD RAW PARQUET"
        )
        print(
            "=============================="
        )

        df = read_parquet(
            self.raw_file
        )

        print(
            "JUMLAH RAW DATA:",
            len(df),
        )

        print(
            "KOLOM RAW:",
            df.columns.tolist(),
        )

        return df


    # ========================================================
    # GET NAMA DATA
    # ========================================================

    def get_nama_data(self):

        print("\n")
        print(
            "=============================="
        )
        print(
            "GET NAMA DATA"
        )
        print(
            "=============================="
        )

        response = self.session.get(
            f"{DATABASE_BASE_URL}/nama_data",
            timeout=30,
        )

        print(
            "STATUS:",
            response.status_code,
        )

        response.raise_for_status()

        df_nama = pd.DataFrame(
            response.json()
        )

        print(
            "JUMLAH DATASET:",
            len(df_nama),
        )

        return df_nama


    # ========================================================
    # MATCH NAMA DATA
    # ========================================================

    def match_nama_data(
        self,
        df_nama,
        indikator,
    ):

        print("\n")
        print(
            "=============================="
        )
        print(
            "MATCH NAMA DATA"
        )
        print(
            "=============================="
        )

        print(
            "TARGET:",
            indikator,
        )

        df_nama = df_nama.copy()

        df_nama = normalize_dataframe(
            df_nama,
            text_columns=[
                "nama",
            ],
        )

        df_nama["nama_clean"] = (
            df_nama["nama"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        target = (
            indikator
            .strip()
            .lower()
        )

        matched = df_nama[
            df_nama["nama_clean"]
            == target
        ]

        if matched.empty:

            print(
                "❌ NAMA DATA "
                "TIDAK DITEMUKAN"
            )

            return None

        row = matched.iloc[0]

        id_nama_data = int(
            row["id"]
        )

        print(
            "✅ MATCH"
        )

        print(
            "NAMA:",
            row["nama"],
        )

        print(
            "ID:",
            id_nama_data,
        )

        return id_nama_data


    # ========================================================
    # TRANSFORM
    # ========================================================

    def transform(
        self,
        df_raw,
        id_nama_data,
        config,
    ):

        indikator = config[
            "indikator"
        ]

        default_satuan = config.get(
            "default_satuan",
            pd.NA,
        )

        sumber = config.get(
            "sumber",
            "Kementerian Perdagangan",
        )

        note = config.get(
            "note",
            pd.NA,
        )

        nama_data_import = (
            config.get(
                "nama_data_import",
                pd.NA,
            )
        )

        print("\n")
        print(
            "=============================="
        )
        print(
            "TRANSFORM DATA"
        )
        print(
            "=============================="
        )

        df = normalize_dataframe(
            df_raw,
            text_columns=[
                "kode_wilayah",
                "provinsi",
            ],
            date_columns=[
                "data_x",
            ],
        )

        # ----------------------------------------------------
        # NORMALISASI NAMA PROVINSI UNTUK XFLASK
        # ----------------------------------------------------

        df["provinsi"] = (
            df["provinsi"]
            .replace({
                "NANGROE ACEH DARUSALAM": "ACEH"
            })
        )

        # ----------------------------------------------------
        # FINAL
        #
        # metadata baru ditambahkan DI SINI.
        # ----------------------------------------------------

        df_final = pd.DataFrame({

            "kota":
                pd.NA,

            "provinsi":
                df["provinsi"],

            "nama_indikator":
                indikator,

            "nama_item":
                pd.NA,

            "idnamadata":
                id_nama_data,

            "data_x":
                (
                    pd.to_datetime(
                        df["data_x"],
                        errors="coerce",
                    )
                    .dt.strftime(
                        "%d-%m-%Y"
                    )
                ),

            "data_y":
                (
                    pd.to_numeric(
                        df["data_y"],
                        errors="coerce",
                    )
                    * 1_000_000
                ),

            "satuan":
                default_satuan,

            "sumber":
                sumber,

            "note":
                note,

            "nama_data_import":
                nama_data_import,
        })

        df_final = df_final[
            df_final["data_y"].notna()
        ].copy()

        print(
            "JUMLAH FINAL ROW:",
            len(df_final),
        )

        return df_final


    # ========================================================
    # SAVE FINAL
    # ========================================================

    def save_final(
        self,
        df_final,
    ):

        print("\n")
        print(
            "=============================="
        )
        print(
            "SAVE FINAL CSV"
        )
        print(
            "=============================="
        )

        save_csv(
            df_final,
            self.final_file,
        )

        print(
            "FINAL FILE:",
            self.final_file,
        )

        return self.final_file


    # ========================================================
    # UPLOAD RAW MINIO
    # ========================================================

    def upload_raw_minio(self, raw_file, tahun):

        storage_name = (
            self.config[
                "storage_name"
            ]
        )

        file_name = (
            self.config[
                "file_name"
            ]
        )

        with open(
            raw_file,
            "rb",
        ) as file:

            raw_bytes = file.read()

        object_name = (
            f"kemendag/"
            f"{storage_name}/raw/"
            f"{file_name}_{tahun}_raw.parquet"
        )

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            raw_bytes,
            object_name,
            "application/octet-stream",
        )

        print(
            "RAW MinIO upload berhasil:"
        )

        print(
            f"{MINIO_BUCKET}/"
            f"{object_name}"
        )

    # ========================================================
    # UPLOAD FINAL MINIO
    # ========================================================

    def upload_minio(self):

        storage_name = (
            self.config[
                "storage_name"
            ]
        )

        file_name = (
            self.config[
                "file_name"
            ]
        )

        with open(
            self.final_file,
            "rb",
        ) as file:

            csv_bytes = file.read()

        object_name = (
            f"kemendag/"
            f"{storage_name}/final/"
            f"{file_name}.csv"
        )

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            csv_bytes,
            object_name,
            "text/csv",
        )

        print(
            "FINAL MinIO upload berhasil:"
        )

        print(
            f"{MINIO_BUCKET}/"
            f"{object_name}"
        )


    # ========================================================
    # RUN
    # ========================================================

    def run(
        self,
        df_raw,
        id_nama_data,
    ):

        df_final = self.transform(
            df_raw=df_raw,
            id_nama_data=id_nama_data,
            config=self.config,
        )

        self.save_final(
            df_final
        )

        for tahun in sorted(
            pd.to_datetime(
                df_raw["data_x"],
                errors="coerce",
            ).dt.year.dropna().unique()
        ):
            raw_file_year = get_raw_year_file(
                self.config,
                int(tahun),
            )

            if not raw_file_year.exists():
                continue

            self.upload_raw_minio(
                raw_file=raw_file_year,
                tahun=int(tahun),
            )

        self.upload_minio()

        return df_final


# ============================================================
# GENERIC UPDATE
# ============================================================

def update_kemendag_dataset(
    config,
    idnamadata=None,
    iditem=None,
    backfill=True,
):

    """
    Generic update untuk dataset Kemendag.

    backfill=True:
        Ambil seluruh histori.

    backfill=False:
        Ambil data terbaru
        + maksimal dua periode sebelumnya.

    Output:
        - raw parquet
        - final csv
        - upload raw ke MinIO
        - upload final ke MinIO
    """

    # ========================================================
    # RESOLVE ID NAMA DATA
    # ========================================================

    if idnamadata is None:

        etl = KemendagETL(
            config_name=(
                config["storage_name"]
                if config["storage_name"]
                in KEMENDAG_CONFIG
                else "neraca_perdagangan"
            )
        )

        df_nama = (
            etl.get_nama_data()
        )

        idnamadata = (
            etl.match_nama_data(
                df_nama,
                config["indikator"],
            )
        )

        if idnamadata is None:

            raise RuntimeError(
                "Gagal mendapatkan "
                "idnamadata untuk "
                f"{config['indikator']}."
            )

    # ========================================================
    # STORAGE
    # ========================================================

    paths = get_storage_paths(
        config
    )

    raw_file = paths[
        "raw_file"
    ]

    final_file = paths[
        "final_file"
    ]

    print("\n")
    print(
        "=============================="
    )
    print(
        "STORAGE"
    )
    print(
        "=============================="
    )

    print(
        "RAW:",
        raw_file,
    )

    print(
        "FINAL:",
        final_file,
    )

    # ========================================================
    # TENTUKAN RENTANG TAHUN
    # ========================================================

    if backfill:

        tahun_awal = config[
            "tahun_awal"
        ]

        tahun_akhir = config[
            "tahun_terbaru"
        ]

        print("\n")
        print(
            "========================================"
        )
        print(
            "KEMENDAG DATASET - BACKFILL"
        )
        print(
            "========================================"
        )

    else:

        tahun_existing = (
            get_existing_latest_year(
                idnamadata
            )
        )

        tahun_terbaru = int(
            config[
                "tahun_terbaru"
            ]
        )

        if tahun_existing is None:

            tahun_awal = (
                tahun_terbaru - 1
            )

        else:

            tahun_awal = (
                tahun_existing - 1
            )

        tahun_akhir = (
            tahun_terbaru
        )

        print("\n")
        print(
            "TAHUN TERAKHIR EXISTING:",
            tahun_existing,
        )

        print(
            "TAHUN TERBARU KEMENDAG:",
            tahun_terbaru,
        )

        print(
            "RENTANG UPDATE:",
            f"{tahun_awal}-{tahun_akhir}",
        )

    # ========================================================
    # SCRAPING
    # ========================================================

    df_raw = scrape_all(
        config=config,
        tahun_awal=tahun_awal,
        tahun_akhir=tahun_akhir,
        save_raw=True,
    )

    if df_raw.empty:

        raise RuntimeError(
            "Tidak ada data yang "
            "berhasil diambil untuk "
            f"{config['indikator']}."
        )

    # ========================================================
    # ETL
    # ========================================================

    config_name = None

    for name, cfg in (
        KEMENDAG_CONFIG.items()
    ):

        if cfg is config:
            config_name = name
            break

    if config_name is None:

        raise RuntimeError(
            "Config Kemendag tidak "
            "ditemukan."
        )

    etl = KemendagETL(
        config_name=config_name
    )

    df_final = etl.transform(
        df_raw=df_raw,
        id_nama_data=idnamadata,
        config=config,
    )

    # ========================================================
    # SAVE FINAL
    # ========================================================

    etl.save_final(
        df_final
    )

    # ========================================================
    # UPLOAD MINIO
    # ========================================================

    for tahun in range(
        int(tahun_awal),
        int(tahun_akhir) + 1,
    ):
        raw_file_year = (
            get_raw_year_file(
                config,
                tahun,
            )
        )

        if not raw_file_year.exists():
            continue

        print("UPLOAD RAW TAHUN:", tahun)
        print("FILE RAW:", raw_file_year)
        print("EXISTS:", raw_file_year.exists())

        etl.upload_raw_minio(
            raw_file=raw_file_year,
            tahun=tahun,
        )

    etl.upload_minio()

    return df_final


# ============================================================
# DATASET 14194
# ============================================================

def update_idnamadata_14194(
    idnamadata=14194,
    iditem=None,
    backfill=True,
):

    """
    Update dataset:

    14194 -
    Neraca Perdagangan (total)
    Menurut Provinsi.
    """

    config = get_kemendag_config(
        "neraca_perdagangan"
    )

    return update_kemendag_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )


def update_curr_14194():

    """
    Ambil data terbaru
    + dua periode sebelumnya
    untuk update rutin.
    """

    return update_idnamadata_14194(
        backfill=False,
    )

# ============================================================
# DATASET 14195
# ============================================================

def update_idnamadata_14195(
    idnamadata=14195,
    iditem=None,
    backfill=True,
):

    """
    Update dataset:

    14195 -
    Neraca Perdagangan Migas
    Menurut Provinsi.
    """

    config = get_kemendag_config(
        "neraca_perdagangan_migas"
    )

    return update_kemendag_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )


def update_curr_14195():

    """
    Ambil data terbaru
    + dua periode sebelumnya
    untuk update rutin.
    """

    return update_idnamadata_14195(
        backfill=False,
    )

# ============================================================
# FETCH
# ============================================================

def fetch(params=None):

    params = params or {}

    idnamadata = params.get(
        "idnamadata"
    )

    iditem = params.get(
        "iditem"
    )

    backfill = params.get(
        "backfill",
        False,
    )

    if idnamadata is not None:

        idnamadata = int(
            idnamadata
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
    # 14194
    # ========================================================

    if (
        idnamadata == 14194
        or idnamadata is None
    ):

        return update_idnamadata_14194(
            idnamadata=(
                14194
                if idnamadata is None
                else idnamadata
            ),
            iditem=iditem,
            backfill=backfill,
        )

    # ========================================================
    # 14195
    # ========================================================

    if idnamadata == 14195:

        return update_idnamadata_14195(
            idnamadata=idnamadata,
            iditem=iditem,
            backfill=backfill,
        )

    raise ValueError(
        f"idnamadata Kemendag "
        f"belum dikonfigurasi: "
        f"{idnamadata}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--idnamadata",
        type=int,
        default=14194,
        help=(
            "ID nama data yang "
            "ingin di-update"
        ),
    )

    parser.add_argument(
        "--backfill",
        action="store_true",
        help=(
            "Ambil seluruh histori "
            "data Kemendag"
        ),
    )

    args = parser.parse_args()

    print("\n")
    print(
        "========================================"
    )
    print(
        "KEMENDAG PIPELINE"
    )
    print(
        "========================================"
    )

    print(
        "ID NAMA DATA:",
        args.idnamadata,
    )

    print(
        "BACKFILL:",
        args.backfill,
    )

    df_final = fetch({
        "idnamadata":
            args.idnamadata,

        "backfill":
            args.backfill,
    })

    print("\n")
    print(
        "========================================"
    )
    print(
        "PIPELINE SELESAI"
    )
    print(
        "========================================"
    )

    print(
        "FINAL ROW:",
        len(df_final),
    )


if __name__ == "__main__":
    main()