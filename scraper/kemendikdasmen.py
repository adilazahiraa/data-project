import argparse
import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from utils.api import create_session, get
from utils.file import (
    read_parquet,
    save_csv,
    save_parquet,
)
from utils.storage import (
    MINIO_BUCKET,
    get_minio_client,
    upload_local_file,
)
from utils.data import (
    concat_dataframes,
    find_duplicates,
    filter_not_null,
)

load_dotenv(override=True)


# ============================================================
# GLOBAL CONFIG
# ============================================================

KATADATA_API_KEY = os.getenv("KATADATA_API_KEY")

XFLASK_API_BASE_URL = "https://xflask.databoks.id/api"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_CONFIG = {
    9828: {
        "indikator": "Jumlah Guru SD Menurut Kabupaten Kota",
        "storage_name": "jumlah_guru_sd",
        "file_name": "jumlah_guru_sd_menurut_kabupaten_kota",
        "default_satuan": "guru",
        "leveldata": "kabkota",
        "source_years": [2021, 2022, 2023, 2024, 2025],
        "source_file_name": (
            "Dataset - gambaran-umum-keadaan-sekolah-dasar-sd-2025-"
            "semua-wilayah-sd-mi-sederajat-1 - kabupaten - ASC.xlsx"
        ),
        "province_column": "Provinsi",
        "city_column": "Kota/Kab",
        "province_code_column": "Kode Provinsi",
        "city_code_column": "Kode Kota/Kab",
        "status_column": "Status Sekolah",
        "status_value": "Negeri dan Swasta",
        "year_column": "Periode",
        "value_column": "Kepala Sekolah dan Guru",
        "sumber": (
            "Kementerian Pendidikan Dasar dan Menengah (Kemendikdasmen)"
        ),
        "note": (
            "Data jumlah guru SD menurut kabupaten/kota berdasarkan dataset "
            "Gambaran Umum Keadaan Sekolah Dasar (SD). Data menggunakan "
            "kategori Negeri dan Swasta dan mengecualikan Sekolah Indonesia "
            "di Luar Negeri (SILN)."
        ),
        "nama_data_import": (
            "s3://maganghub/kemendikdasmen/jumlah_guru_sd/final/"
            "jumlah_guru_sd_menurut_kabupaten_kota.csv"
        ),
    },

    9829: {
        "indikator": "Jumlah Guru SMP Menurut Kabupaten Kota",
        "storage_name": "jumlah_guru_smp",
        "file_name": "jumlah_guru_smp_menurut_kabupaten_kota",
        "default_satuan": "guru",
        "leveldata": "kabkota",
        "source_years": [2023, 2024, 2025],
        "source_file_name": (
            "Dataset - gambaran-umum-keadaan-sekolah-menengah-pertama-"
            "smp-2025-semua-wilayah-smp-mts-sederajat-2 - kabupaten - ASC.xlsx"
        ),
        "province_column": "Provinsi",
        "city_column": "Kota/Kab",
        "province_code_column": "Kode Provinsi",
        "city_code_column": "Kode Kota/Kab",
        "status_column": "Status Sekolah",
        "status_value": "Negeri dan Swasta",
        "year_column": "Periode",
        "value_column": "Kepala Sekolah dan Guru",
        "sumber": (
            "Kementerian Pendidikan Dasar dan Menengah "
            "(Kemendikdasmen)"
        ),
        "note": (
            "Data jumlah guru SMP menurut kabupaten/kota berdasarkan "
            "dataset Gambaran Umum Keadaan Sekolah Menengah Pertama "
            "(SMP). Data menggunakan kategori Negeri dan Swasta dan "
            "mengecualikan Sekolah Indonesia di Luar Negeri (SILN)."
        ),
        "nama_data_import": (
            "s3://maganghub/kemendikdasmen/jumlah_guru_smp/final/"
            "jumlah_guru_smp_menurut_kabupaten_kota.csv"
        ),
    },

    9830: {
        "indikator": "Jumlah Guru SMA Menurut Kabupaten Kota",
        "storage_name": "jumlah_guru_sma",
        "file_name": "jumlah_guru_sma_menurut_kabupaten_kota",
        "default_satuan": "guru",
        "leveldata": "kabkota",
        "source_years": [2023, 2024, 2025],
        "source_file_name": (
            "Dataset - gambaran-umum-keadaan-sekolah-menengah-atas-"
            "sma-2025-semua-wilayah-sma-ma-sederajat-3 - kabupaten - ASC.xlsx"
        ),
        "province_column": "Provinsi",
        "city_column": "Kota/Kab",
        "province_code_column": "Kode Provinsi",
        "city_code_column": "Kode Kota/Kab",
        "status_column": "Status Sekolah",
        "status_value": "Negeri dan Swasta",
        "year_column": "Periode",
        "value_column": "Kepala Sekolah dan Guru",
        "sumber": (
            "Kementerian Pendidikan Dasar dan Menengah "
            "(Kemendikdasmen)"
        ),
        "note": (
            "Data jumlah guru SMA menurut kabupaten/kota berdasarkan "
            "dataset Gambaran Umum Keadaan Sekolah Menengah Atas "
            "(SMA). Data menggunakan kategori Negeri dan Swasta dan "
            "mengecualikan Sekolah Indonesia di Luar Negeri (SILN)."
        ),
        "nama_data_import": (
            "s3://maganghub/kemendikdasmen/jumlah_guru_sma/final/"
            "jumlah_guru_sma_menurut_kabupaten_kota.csv"
        ),
    },
}


# ============================================================
# HELPERS
# ============================================================

def get_data_config(id_nama_data):
    id_nama_data = int(id_nama_data)

    if id_nama_data not in DATA_CONFIG:
        raise ValueError(
            f"Dataset Kemendikdasmen ID {id_nama_data} belum dikonfigurasi."
        )

    return DATA_CONFIG[id_nama_data]


def normalize_geo(value):
    """Normalisasi nama wilayah hanya untuk kebutuhan matching."""

    if pd.isna(value):
        return ""

    value = str(value).strip().upper()

    replacements = {
        "PROV.": "",
        "PROV": "",
        "D.K.I.": "DKI",
        "D.K.I": "DKI",
        "D.I.": "DI",
        "D.I": "DI",
        "KOTA ADM.": "KOTA",
        "KAB. ADM.": "KAB.",
        "KAB ADM.": "KAB.",
        "KAB ADM": "KAB.",
        "KEP. SERIBU": "KEPULAUAN SERIBU",
        "KEP SERIBU": "KEPULAUAN SERIBU",
    }

    for old, new in replacements.items():
        value = value.replace(old, new)

    value = re.sub(r"\bADM\.?\b", "", value)
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()

    return value


def clean_province_display(value):
    if pd.isna(value):
        return None

    value = str(value).strip()
    value = re.sub(r"^PROV\.?\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value)

    upper = value.upper()

    aliases = {
        "D.K.I. JAKARTA": "DKI Jakarta",
        "D.K.I JAKARTA": "DKI Jakarta",
        "D.I. YOGYAKARTA": "D.I. Yogyakarta",
        "D.I YOGYAKARTA": "D.I. Yogyakarta",
    }

    if upper in aliases:
        return aliases[upper]

    return value.title()


def clean_city_display(value):
    if pd.isna(value):
        return None

    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)

    upper = value.upper()

    # Bentuk khusus dari file Kemendikdasmen.
    upper = upper.replace("KOTA ADM. ", "KOTA ")
    upper = upper.replace("KAB. ADM. ", "KAB. ")
    upper = upper.replace("KAB ADM. ", "KAB. ")
    upper = upper.replace("KAB ADM ", "KAB. ")

    # Samakan nama Kepulauan Seribu.
    if upper in {
        "KAB. ADM. KEP. SERIBU",
        "KAB. KEP. SERIBU",
    }:
        upper = "KAB. KEPULAUAN SERIBU"

    if upper == "KAB. TOBA":
        upper = "KAB. TOBA SAMOSIR"

    if upper == "KAB. MAHAKAM ULU":
        upper = "KAB. MAHAKAM HULU"

    if upper.startswith("KAB. "):
        return "Kab. " + upper[5:].title()

    if upper.startswith("KOTA "):
        return "Kota " + upper[5:].title()

    return upper.title()


def parse_numeric(value):
    if pd.isna(value):
        return pd.NA

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()

    if not text:
        return pd.NA

    # Angka Indonesia: 2.526 -> 2526, 2.526,5 -> 2526.5
    if "." in text and "," in text:
        text = text.replace(".", "").replace(",", ".")
    elif "." in text:
        # File utama dibaca Excel sehingga umumnya sudah numerik.
        # Untuk string bertitik, anggap titik sebagai pemisah ribuan
        # bila seluruh bagian setelah titik berjumlah 3 digit.
        parts = text.split(".")
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            text = "".join(parts)
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return pd.NA


# ============================================================
# KEMENDIKDASMEN SCRAPER
# ============================================================

class KemendikdasmenScraper:

    def load_excel(self, file_path, id_nama_data, tahun=2025):
        config = get_data_config(id_nama_data)
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(
                f"File source tidak ditemukan: {file_path}"
            )

        print("\n" + "=" * 70)
        print("KEMENDIKDASMEN SCRAPER")
        print("=" * 70)
        print("SOURCE:", file_path)
        print("ID NAMA DATA:", id_nama_data)
        print("INDIKATOR:", config["indikator"])

        # Header sebenarnya berada pada baris ke-3 Excel.
        df = pd.read_excel(
            file_path,
            sheet_name=0,
            header=2,
        )

        print("RAW SHAPE:", df.shape)
        print("RAW COLUMNS:", df.columns.tolist())

        required_columns = [
            config["year_column"],
            config["province_column"],
            config["province_code_column"],
            config["city_column"],
            config["city_code_column"],
            config["status_column"],
            config["value_column"],
        ]

        missing = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing:
            raise RuntimeError(
                f"Kolom source tidak ditemukan: {missing}"
            )

        # Periode.
        df["tahun"] = pd.to_numeric(
            df[config["year_column"]],
            errors="coerce",
        ).astype("Int64")

        tahun = int(tahun)

        available_years = sorted(
            int(year)
            for year in df["tahun"].dropna().unique()
            if int(year) in config["source_years"]
            and int(year) <= tahun
        )

        print("SOURCE YEARS:", available_years)

        if not available_years:
            raise RuntimeError(
                f"Tidak ada data tahun <= {tahun}."
            )

        df = df[
            df["tahun"].isin(available_years)
        ].copy()

        # Ambil total Negeri + Swasta.
        df = df[
            df[config["status_column"]]
            .astype("string")
            .str.strip()
            .eq(config["status_value"])
        ].copy()

        print(
            "ROW SETELAH FILTER NEGERI DAN SWASTA:",
            len(df),
        )

        # Buang SILN karena Kode Kota/Kab kosong.
        df = df[
            df[config["city_code_column"]].notna()
        ].copy()

        print(
            "ROW SETELAH BUANG SILN:",
            len(df),
        )

        df["provinsi"] = (
            df[config["province_column"]]
            .apply(clean_province_display)
        )

        df["kota"] = (
            df[config["city_column"]]
            .apply(clean_city_display)
        )

        df["kode_provinsi"] = df[
            config["province_code_column"]
        ]

        df["kode_kota_kab"] = df[
            config["city_code_column"]
        ]

        df["data_y"] = (
            df[config["value_column"]]
            .apply(parse_numeric)
        )

        result = df[
            [
                "tahun",
                "provinsi",
                "kota",
                "kode_provinsi",
                "kode_kota_kab",
                "data_y",
            ]
        ].copy()

        result["tahun"] = result["tahun"].astype(int)
        result["data_y"] = pd.to_numeric(
            result["data_y"],
            errors="coerce",
        )

        result = filter_not_null(
            result,
            "data_y",
        )

        # Seharusnya 514 kabupaten/kota per tahun.
        print("\nJUMLAH ROW PER TAHUN:")
        print(
            result.groupby("tahun")
            .size()
            .to_string()
        )

        duplicate_rows = find_duplicates(
            result,
            key_columns=["tahun", "kode_kota_kab"],
        )

        if not duplicate_rows.empty:
            print("\nWARNING DUPLIKAT:")
            print(
                duplicate_rows[
                    [
                        "tahun",
                        "provinsi",
                        "kota",
                        "kode_kota_kab",
                    ]
                ].sort_values(
                    ["tahun", "kode_kota_kab"]
                ).to_string(index=False)
            )
        else:
            print("DUPLIKAT: 0")

        print("\nTOTAL SOURCE ROW:", len(result))

        print("\nPREVIEW SOURCE:")
        print(result.head(20).to_string(index=False))

        return result


    def save_raw_parquet(self, df_raw, id_nama_data):
        config = get_data_config(id_nama_data)

        raw_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "kemendikdasmen"
            / config["storage_name"]
        )

        raw_dir.mkdir(parents=True, exist_ok=True)

        raw_paths = []

        print("\n" + "=" * 60)
        print("SAVE RAW PARQUET")
        print("=" * 60)

        for tahun, df_year in df_raw.groupby("tahun"):
            tahun = int(tahun)

            output_path = (
                raw_dir
                / f"{config['file_name']}_{tahun}_raw.parquet"
            )

            save_parquet(
                df_year,
                output_path,
            )

            raw_paths.append(output_path)

            print(
                f"RAW {tahun}: {len(df_year)} row -> {output_path}"
            )

        return raw_paths


# ============================================================
# KEMENDIKDASMEN ETL
# ============================================================

class KemendikdasmenETL:


    def __init__(self, dry_run=False):
        self.session = None
        self.headers = None
        self.minio_client = None

        if dry_run:
            return

        if not KATADATA_API_KEY:
            raise RuntimeError(
                "KATADATA_API_KEY tidak ditemukan di .env"
            )

        self.session = create_session()

        self.headers = {
            "X-Api-Key": KATADATA_API_KEY
        }

        self.minio_client = get_minio_client()

    def get_paths(self, id_nama_data):
        config = get_data_config(id_nama_data)

        raw_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "kemendikdasmen"
            / config["storage_name"]
        )

        final_dir = (
            PROJECT_ROOT
            / "data"
            / "final"
            / "kemendikdasmen"
            / config["storage_name"]
        )

        raw_dir.mkdir(parents=True, exist_ok=True)
        final_dir.mkdir(parents=True, exist_ok=True)

        return {
            "raw_dir": raw_dir,
            "final_dir": final_dir,
            "file_name": config["file_name"],
        }

    def load_raw_parquet(self, raw_paths):
        print("\n" + "=" * 60)
        print("LOAD RAW PARQUET")
        print("=" * 60)

        df_raw = concat_dataframes(
            [
                read_parquet(raw_path)
                for raw_path in raw_paths
            ],
        )

        print("TOTAL RAW ROW:", len(df_raw))
        print("RAW COLUMNS:", df_raw.columns.tolist())

        return df_raw

    def get_xflask_api_data(self, id_nama_data):
        url = (
            f"{XFLASK_API_BASE_URL}"
            f"/getdata/{id_nama_data}"
        )

        print("\n" + "=" * 60)
        print("GET DATA XFLASK API")
        print("=" * 60)
        print("URL:", f"/getdata/{id_nama_data}")

        response = get(
            session=self.session,
            url=url,
            headers=self.headers,
            timeout=60,
        )

        print("STATUS:", response.status_code)

        result = response.json()

        if not isinstance(result, dict):
            raise RuntimeError(
                f"Response XFlask tidak sesuai: {result}"
            )

        if not result.get("success", True):
            raise RuntimeError(
                f"XFlask API gagal: {result}"
            )

        df = pd.DataFrame(
            result.get("data", [])
        )

        print("JUMLAH ROW:", len(df))
        print("KOLOM:", df.columns.tolist())

        return df

    def upload_raw_minio(self, raw_paths, id_nama_data):
        config = get_data_config(id_nama_data)

        print("\n" + "=" * 60)
        print("UPLOAD RAW TO MINIO")
        print("=" * 60)

        for raw_path in raw_paths:
            object_name = (
                f"kemendikdasmen/"
                f"{config['storage_name']}/raw/"
                f"{raw_path.name}"
            )

            upload_local_file(
                self.minio_client,
                MINIO_BUCKET,
                raw_path,
                object_name,
                "application/octet-stream",
            )

            print(
                "MINIO RAW:",
                f"{MINIO_BUCKET}/{object_name}",
            )

    def match_source_to_xflask(self, df_source, df_xflask):
        print("\n" + "=" * 60)
        print("MATCH SOURCE <-> XFLASK")
        print("=" * 60)

        if df_xflask.empty:
            raise RuntimeError("Data existing XFlask kosong.")

        source = df_source.copy()
        existing = df_xflask.copy()

        source["geo_key"] = (
            source["provinsi"].apply(normalize_geo)
            + "|"
            + source["kota"].apply(normalize_geo)
        )

        existing["tahun"] = (
            pd.to_datetime(
                existing["data_x"],
                errors="coerce",
                dayfirst=True,
            )
            .dt.year
        )

        existing["geo_key"] = (
            existing["provinsi"].apply(normalize_geo)
            + "|"
            + existing["kota"].apply(normalize_geo)
        )

        existing_match = existing[
            ["geo_key", "tahun", "data_y"]
        ].copy()

        existing_match = existing_match.drop_duplicates(
            subset=["geo_key", "tahun"],
            keep="first",
        )

        existing_match = existing_match.rename(
            columns={"data_y": "xflask_data_y"}
        )

        merged = source.merge(
            existing_match,
            on=["geo_key", "tahun"],
            how="left",
        )

        merged["status_match"] = "WILAYAH BARU"

        has_existing = merged["xflask_data_y"].notna()

        source_value = pd.to_numeric(
            merged["data_y"],
            errors="coerce",
        )

        existing_value = pd.to_numeric(
            merged["xflask_data_y"],
            errors="coerce",
        )

        same_value = (
            source_value.round(10)
            == existing_value.round(10)
        )

        merged.loc[
            has_existing & same_value,
            "status_match",
        ] = "MATCH"

        merged.loc[
            has_existing & ~same_value,
            "status_match",
        ] = "MISMATCH"

        print(
            "MATCH:",
            (merged["status_match"] == "MATCH").sum(),
        )
        print(
            "MISMATCH:",
            (merged["status_match"] == "MISMATCH").sum(),
        )
        print(
            "WILAYAH BARU:",
            (merged["status_match"] == "WILAYAH BARU").sum(),
        )

        mismatch = merged[
            merged["status_match"] == "MISMATCH"
        ]

        if not mismatch.empty:
            print("\n=== MISMATCH ===")
            print(
                mismatch[
                    [
                        "tahun",
                        "provinsi",
                        "kota",
                        "data_y",
                        "xflask_data_y",
                    ]
                ]
                .sort_values(
                    ["tahun", "provinsi", "kota"]
                )
                .to_string(index=False)
            )

        return merged

    def build_final(
            self,
            df_source,
            df_xflask,
            id_nama_data,
            dry_run=False,
    ):
        config = get_data_config(id_nama_data)

        if dry_run:
            matched = df_source.copy()
        else:
            matched = self.match_source_to_xflask(
                df_source,
                df_xflask,
            )

        final_df = pd.DataFrame({
            "kota": matched["kota"],
            "provinsi": matched["provinsi"],
            "nama_indikator": config["indikator"],
            "nama_item": None,
            "idnamadata": int(id_nama_data),
            "data_x": pd.to_datetime(
                matched["tahun"].astype(str) + "-07-01",
                format="%Y-%m-%d",
                errors="coerce",
            ).dt.strftime("%d-%m-%Y"),
            "data_y": pd.to_numeric(
                matched["data_y"],
                errors="coerce",
            ),
            "satuan": config["default_satuan"],
            "sumber": config["sumber"],
            "note": config["note"],
            "nama_data_import": config["nama_data_import"],
            **(
                {"status_match": matched["status_match"]}
                if not dry_run
                else {}
            ),
        })

        final_df = final_df[
            final_df["data_y"].notna()
        ].copy()

        final_df = final_df[
            final_df["data_y"] >= 0
        ].copy()

        final_df = (
            final_df
            .drop_duplicates(
                subset=["provinsi", "kota", "data_x"]
            )
            .reset_index(drop=True)
        )

        print("\nTOTAL FINAL:", len(final_df))
        print("\nPREVIEW FINAL:")
        print(final_df.head(20).to_string(index=False))

        return final_df

    def save_final(self, df_final, id_nama_data):
        config = get_data_config(id_nama_data)
        paths = self.get_paths(id_nama_data)

        output_path = (
            paths["final_dir"]
            / f"{config['file_name']}.csv"
        )

        save_csv(
            df_final,
            output_path,
        )

        print("\nFINAL CSV:", output_path)

        return output_path

    def upload_final_minio(self, final_path, id_nama_data):
        config = get_data_config(id_nama_data)

        object_name = (
            f"kemendikdasmen/"
            f"{config['storage_name']}/final/"
            f"{final_path.name}"
        )

        upload_local_file(
            self.minio_client,
            MINIO_BUCKET,
            final_path,
            object_name,
            "text/csv",
        )

        print(
            "MINIO FINAL:",
            f"{MINIO_BUCKET}/{object_name}",
        )

    def process(
            self,
            df_source,
            id_nama_data,
            raw_paths,
            dry_run=False,
    ):
        config = get_data_config(id_nama_data)

        print("\n" + "=" * 70)
        print("KEMENDIKDASMEN ETL")
        print("=" * 70)
        print("ID NAMA DATA:", id_nama_data)
        print("INDIKATOR:", config["indikator"])

        if not dry_run:
            self.upload_raw_minio(
                raw_paths,
                id_nama_data,
            )

            df_xflask = self.get_xflask_api_data(
                id_nama_data,
            )
        else:
            print("\nDRY RUN: XFlask dan MinIO dilewati.")
            df_xflask = pd.DataFrame(
                columns=["data_x", "data_y", "provinsi", "kota"]
            )

        final_df = self.build_final(
            df_source,
            df_xflask,
            id_nama_data,
            dry_run=dry_run,
        )

        final_path = self.save_final(
            final_df,
            id_nama_data,
        )

        if not dry_run:
            self.upload_final_minio(
                final_path,
                id_nama_data,
            )

        return final_df


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scraper + ETL Kemendikdasmen."
    )

    parser.add_argument(
        "--idnamadata",
        type=int,
        default=9828,
    )

    parser.add_argument(
        "--tahun",
        type=int,
        default=2025,
        help="Tahun maksimal yang diproses.",
    )

    parser.add_argument(
        "--file",
        required=True,
        help="Path file Excel source Kemendikdasmen.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load + transform + save lokal tanpa XFlask/MinIO.",
    )

    args = parser.parse_args()

    scraper = KemendikdasmenScraper()

    df_source = scraper.load_excel(
        file_path=args.file,
        id_nama_data=args.idnamadata,
        tahun=args.tahun,
    )

    raw_paths = scraper.save_raw_parquet(
        df_source,
        args.idnamadata,
    )

    etl = KemendikdasmenETL(
        dry_run=args.dry_run,
    )

    df_raw = etl.load_raw_parquet(raw_paths)

    final_df = etl.process(
        df_raw,
        args.idnamadata,
        raw_paths=raw_paths,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 70)
    print("PIPELINE KEMENDIKDASMEN SELESAI")
    print("=" * 70)
    print("ID:", args.idnamadata)
    print("FINAL ROW:", len(final_df))


if __name__ == "__main__":
    main()
