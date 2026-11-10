import argparse
import csv
import os
import re
from pathlib import Path
from urllib.parse import unquote

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


load_dotenv(override=True)


# ============================================================
# GLOBAL CONFIG
# ============================================================

KATADATA_API_KEY = os.getenv("KATADATA_API_KEY")

XFLASK_API_BASE_URL = (
    "https://xflask.databoks.id/api"
)

PUPR_API_BASE_URL = (
    "https://data.pu.go.id/api/3/action"
)

PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)

DATA_CONFIG = {

    4941: {
        "package_id": (
            "8325dc01-1b45-45c7-8fc2-"
            "afb241d3e1d4"
        ),

        "indikator": (
            "Jumlah Danau/Situ Menurut Provinsi"
        ),

        "storage_name": "danau",

        "file_name": (
            "jumlah_danau_situ_menurut_provinsi"
        ),

        "default_satuan": "danau/situ",

        "nama_data_import": (
            "s3://maganghub/pupr/danau/final/"
            "jumlah_danau_situ_menurut_provinsi.csv"
        ),

        "leveldata": "provinsi",

        # Nama resource di CKAN/PUPR.
        "resource_name_template": (
            "Danau Tahun {tahun}"
        ),

        # Kolom source PUPR.
        "province_column": "Provinsi",

        "value_column": "Jumlah",

        # Beberapa tahun memakai nama kolom nilai yang berbeda.
        # Kalau diisi, mapping per tahun ini diprioritaskan.
        "value_column_by_year": {
            2023: "Jumlah",
            2024: "Jumlah Danau",
            2025: "Jumlah",
        },

        # Sumber per tahun.
        "sumber_by_year": {
            2023: (
                "Informasi Geospasial Tematik "
                "Kementerian PUPR"
            ),
            2024: (
                "Informasi Geospasial Tematik "
                "Kementerian PUPR"
            ),
            2025: (
                "Direktorat Bina Teknik Sumber "
                "Daya Air Tahun 2025"
            ),
        },

        "note": (
            "Data jumlah danau/situ menurut "
            "provinsi berdasarkan dataset "
            "Open Data Kementerian PUPR."
        ),
    },

    4927: {
        "package_id": (
            "6a8ddbce-cd24-4085-a3be-"
            "640c09cd747b"
        ),

        "indikator": (
            "Jumlah Bendungan Menurut Provinsi"
        ),

        "storage_name": "bendungan",

        "file_name": (
            "jumlah_bendungan_menurut_provinsi"
        ),

        "default_satuan": "Unit",

        "nama_data_import": (
            "s3://maganghub/pupr/bendungan/final/"
            "jumlah_bendungan_menurut_provinsi.csv"
        ),

        "leveldata": "provinsi",

        "resource_name_template": (
            "Bendungan Operasi Tahun {tahun}"
        ),

        "province_column": "Provinsi",

        "value_column": "Jumlah",

        "value_column_by_year": {
            2019: "Jumlah",
            2021: "Jumlah",
            2022: "Jumlah",
            2023: "Jumlah",
            2024: "Jumlah",
            2025: "Jumlah",
        },

        "source_years": [
            2019,
            2021,
            2022,
            2023,
            2024,
            2025,
        ],

        "sumber_by_year": {
            2019: (
                "Direktorat Bendungan dan Danau, "
                "Direktorat Jenderal Sumber Daya Air"
            ),
            2021: (
                "Direktorat Bina Operasi dan Pemeliharaan "
                "Direktorat Jenderal Sumber Daya Air"
            ),
            2022: (
                "Direktorat Bendungan dan Danau, "
                "Direktorat Jenderal Sumber Daya Air"
            ),
            2023: (
                "Direktorat Bendungan dan Danau, "
                "Direktorat Jenderal Sumber Daya Air"
            ),
            2024: (
                "Direktorat Bina Teknik Bendungan dan Danau, "
                "Direktorat Jenderal Sumber Daya Air"
            ),
            2025: (
                "Direktorat Bina Teknik Sumber Daya Air"
            ),
        },

        "note": (
            "Data jumlah bendungan yang beroperasi "
            "menurut provinsi berdasarkan dataset "
            "Open Data Kementerian PUPR."
        ),
    },

    28102: {
        "package_id": (
            "ce78aa0b-4983-4d8d-a393-"
            "bcf9bea8b40c"
        ),

        "indikator": (
            "Panjang Jalan Kabupaten Kota "
            "Kondisi Mantap Menurut Provinsi"
        ),

        "storage_name": "jalan_kondisi_mantap",

        "file_name": (
            "panjang_jalan_kabupaten_kota_"
            "kondisi_mantap_menurut_provinsi"
        ),

        "default_satuan": "km",

        "nama_data_import": (
            "s3://maganghub/pupr/"
            "jalan_kondisi_mantap/final/"
            "panjang_jalan_kabupaten_kota_"
            "kondisi_mantap_menurut_provinsi.csv"
        ),

        "leveldata": "provinsi",

        "resource_name_template": (
            "Kemantapan Jalan Kabupaten dan Kota Tahun {tahun}"
        ),

        "province_column": "Provinsi",

        "value_column": "Mantap_km",

        "value_multiplier_by_year": {
            2014: 1,
            2016: 1000,
            2017: 1000,
            2018: 1000,
            2019: 1000,
            2020: 1000,
            2021: 1000,
            2022: 1000,
            2023: 1,
            2024: 1,
        },

        "source_years": [
            2014,
            2016,
            2017,
            2018,
            2019,
            2020,
            2021,
            2022,
            2023,
            2024,
        ],

        "sumber_by_year": {
            2014: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2016: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2017: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2018: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2019: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2020: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2021: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2022: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2023: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
            2024: (
                "Sistem Pengelolaan Database Jalan Provinsi "
                "dan Kabupaten/Kota (SiPDJD)"
            ),
        },

        "note": (
            "Data panjang jalan kabupaten dan kota dalam kondisi "
            "Mantap menurut provinsi berdasarkan dataset "
            "Open Data Kementerian PUPR. "
            "Mantap merupakan kondisi baik dan sedang."
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
            f"Dataset PUPR ID {id_nama_data} "
            "belum dikonfigurasi."
        )

    return DATA_CONFIG[id_nama_data]

def normalize_text(value):
    if pd.isna(value):
        return None

    value = (
        str(value)
        .strip()
        .upper()
    )

    value = re.sub(
        r"[^A-Z0-9\s]",
        " ",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    ).strip()

    return value

def normalize_province(value):
    value = normalize_text(value)

    if value is None:
        return None

    aliases = {
        "D I YOGYAKARTA":
            "DI YOGYAKARTA",

        "DI YOGYAKARTA":
            "DI YOGYAKARTA",

        "BANGKA BELITUNG":
            "KEPULAUAN BANGKA BELITUNG",

        "KEP BANGKA BELITUNG":
            "KEPULAUAN BANGKA BELITUNG",

        "KEPULAUAN BANGKA BELITUNG":
            "KEPULAUAN BANGKA BELITUNG",

        "KEP RIAU":
            "KEPULAUAN RIAU",

        "KEPULAUAN RIAU":
            "KEPULAUAN RIAU",
    }

    return aliases.get(
        value,
        value,
    )

def parse_pupr_number(value):
    if pd.isna(value):
        return pd.NA

    if isinstance(value, bool):
        return pd.NA

    if isinstance(value, (int, float)):
        return value

    value = str(value).strip()

    if value == "":
        return pd.NA

    # Format Indonesia:
    # 9.275,34 -> 9275.34
    if "," in value:
        value = value.replace(".", "")
        value = value.replace(",", ".")

    # Format resource lama PUPR:
    # 10.86352 -> 10863.52
    # 11.05975 -> 11059.75
    # 10.00641 -> 10006.41
    elif "." in value:
        parts = value.split(".")

        if len(parts) == 2:
            integer_part = parts[0]
            decimal_part = parts[1]

            if (
                len(integer_part) <= 2
                and len(decimal_part) in (3, 4, 5)
            ):
                digits = (
                    integer_part
                    + decimal_part
                )

                if len(decimal_part) == 3:
                    value = digits

                elif len(decimal_part) == 4:
                    value = (
                        digits[:-1]
                        + "."
                        + digits[-1:]
                    )

                elif len(decimal_part) == 5:
                    value = (
                        digits[:-2]
                        + "."
                        + digits[-2:]
                    )

    try:
        number = float(value)

        if number.is_integer():
            return int(number)

        return number

    except (ValueError, TypeError):
        return pd.NA

# ============================================================
# KPU PR SCRAPER
# ============================================================

class KPUPRScraper:

    def __init__(self):

        self.session = create_session()

    # --------------------------------------------------------
    # DATASET CONFIG
    # --------------------------------------------------------

    def get_config(
        self,
        id_nama_data,
    ):

        return get_data_config(
            id_nama_data
        )

    # --------------------------------------------------------
    # GET PUPR DATASET METADATA
    # --------------------------------------------------------

    def get_pupr_metadata(
        self,
        id_nama_data,
    ):

        config = self.get_config(
            id_nama_data
        )

        url = (
            f"{PUPR_API_BASE_URL}"
            "/package_show"
        )

        response = get(
            session=self.session,
            url=url,
            params={
                "id": config["package_id"]
            },
            timeout=60,
        )

        result = response.json()

        if not result.get("success"):
            raise RuntimeError(
                f"API metadata PUPR gagal "
                f"untuk ID {id_nama_data}: "
                f"{result}"
            )

        package_list = result.get(
            "result",
            [],
        )

        if not package_list:
            raise RuntimeError(
                f"Dataset PUPR tidak ditemukan "
                f"untuk ID {id_nama_data}."
            )

        return package_list[0]

    # --------------------------------------------------------
    # NORMALIZE RESOURCE URL
    # --------------------------------------------------------

    def normalize_resource_url(
        self,
        url,
    ):

        if not url:
            return None

        url = str(url).strip()

        url = url.replace(
            "http://host.docker.internal",
            "https://data.pu.go.id",
        )

        url = url.replace(
            "https://host.docker.internal",
            "https://data.pu.go.id",
        )

        for _ in range(3):

            decoded = unquote(url)

            if decoded == url:
                break

            url = decoded

        if url.startswith(
            "http://data.pu.go.id"
        ):
            url = url.replace(
                "http://data.pu.go.id",
                "https://data.pu.go.id",
                1,
            )

        return url

    # --------------------------------------------------------
    # GET RESOURCE
    # --------------------------------------------------------

    def get_resource(
        self,
        id_nama_data,
        tahun,
    ):

        config = self.get_config(
            id_nama_data
        )

        metadata = self.get_pupr_metadata(
            id_nama_data
        )

        resources = metadata.get(
            "resources",
            [],
        )

        template = config.get(
            "resource_name_template"
        )

        target_name = (
            template.format(
                tahun=int(tahun)
            )
            .strip()
            .lower()
        )

        for resource in resources:

            resource_name = str(
                resource.get(
                    "name",
                    ""
                )
            ).strip().lower()

            if resource_name == target_name:

                url = (
                    self.normalize_resource_url(
                        resource.get("url")
                    )
                )

                if not url:
                    continue

                return resource, url

        available = [
            str(
                resource.get(
                    "name",
                    ""
                )
            ).strip()
            for resource in resources
        ]

        raise RuntimeError(
            f"Resource tahun {tahun} "
            f"tidak ditemukan untuk "
            f"ID {id_nama_data}.\n"
            f"Resource tersedia: {available}"
        )

    # --------------------------------------------------------
    # DOWNLOAD RESOURCE
    # --------------------------------------------------------

    def download_year(
        self,
        id_nama_data,
        tahun,
    ):

        resource, url = self.get_resource(
            id_nama_data,
            tahun,
        )

        print("\n" + "=" * 70)
        print(
            f"DOWNLOAD PUPR "
            f"ID {id_nama_data} "
            f"TAHUN {tahun}"
        )
        print("=" * 70)

        print(
            "RESOURCE:",
            resource.get("name")
        )

        print(
            "URL:",
            url
        )

        response = get(
            session=self.session,
            url=url,
            timeout=60,
        )

        print(
            "STATUS:",
            response.status_code
        )

        if not response.content:
            raise RuntimeError(
                f"Resource tahun {tahun} kosong."
            )

        return response.content

    # --------------------------------------------------------
    # PARSE CSV GENERIC
    # --------------------------------------------------------

    def parse_csv(
        self,
        content,
        tahun,
        id_nama_data,
    ):
        config = self.get_config(
            id_nama_data
        )

        raw_text = content.decode(
            "utf-8-sig",
            errors="replace",
        )

        # Deteksi delimiter dari header
        first_line = next(
            (
                line
                for line in raw_text.splitlines()
                if line.strip()
            ),
            "",
        )

        if ";" in first_line:
            delimiter = ";"
        else:
            delimiter = ","

        rows = list(
            csv.reader(
                raw_text.splitlines(),
                delimiter=delimiter,
            )
        )

        # Buang baris kosong
        rows = [
            row
            for row in rows
            if any(
                str(cell).strip()
                for cell in row
            )
        ]

        if not rows:
            raise RuntimeError(
                f"Resource tahun {tahun} kosong."
            )

        # Beberapa file PUPR terbaca sebagai
        # 1 kolom yang masih berisi CSV comma-separated.
        if (
            len(rows[0]) == 1
            and "," in str(rows[0][0])
        ):

            parsed_rows = []

            for row in rows:

                if (
                    len(row) == 1
                    and "," in str(row[0])
                ):
                    row = next(
                        csv.reader(
                            [row[0]]
                        )
                    )

                parsed_rows.append(row)

            rows = parsed_rows

        header = rows[0]

        data_rows = []

        for row in rows[1:]:

            # Kalau jumlah field lebih banyak
            # dari header, ambil field sesuai
            # jumlah kolom header.
            if len(row) > len(header):
                row = row[:len(header)]

            # Kalau kurang, tambahkan None
            # supaya jumlah kolom sama.
            elif len(row) < len(header):
                row = row + (
                    [None]
                    * (
                        len(header)
                        - len(row)
                    )
                )

            data_rows.append(row)

        df = pd.DataFrame(
            data_rows,
            columns=header,
        )

        df.columns = [
            str(column).strip()
            for column in df.columns
        ]

        df = df.loc[
            :,
            [
                column != ""
                for column in df.columns
            ],
        ].copy()

        province_source = (
            config["province_column"]
        )

        value_source = config.get(
            "value_column_by_year",
            {},
        ).get(
            int(tahun),
            config["value_column"],
        )

        # Coba cari kolom secara case-insensitive.
        column_lookup = {
            str(column).strip().lower():
                column
            for column in df.columns
        }

        province_key = (
            province_source.strip().lower()
        )

        value_key = (
            value_source.strip().lower()
        )

        province_actual = (
            column_lookup.get(
                province_key
            )
        )

        value_actual = (
            column_lookup.get(
                value_key
            )
        )

        if province_actual is None:
            raise RuntimeError(
                f"Kolom provinsi "
                f"'{province_source}' "
                f"tidak ditemukan.\n"
                f"Kolom source: "
                f"{df.columns.tolist()}"
            )

        if value_actual is None:
            raise RuntimeError(
                f"Kolom nilai "
                f"'{value_source}' "
                f"tidak ditemukan.\n"
                f"Kolom source: "
                f"{df.columns.tolist()}"
            )

        df = df.rename(
            columns={
                province_actual:
                    "provinsi",
                value_actual:
                    "data_y",
            }
        )

        df["provinsi"] = (
            df["provinsi"]
            .astype("string")
            .str.strip()
        )

        # Untuk dataset numerik umum.
        df["data_y"] = (
            df["data_y"]
            .astype("string")
            .str.strip()
        )

        df["data_y"] = (
            df["data_y"]
            .apply(parse_pupr_number)
        )

        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce",
        )

        multiplier = (
            config
            .get("value_multiplier_by_year", {})
            .get(int(tahun), 1)
        )

        df["data_y"] = (
            df["data_y"]
            * multiplier
        )

        df = df[
            df["provinsi"].notna()
            & df["data_y"].notna()
        ].copy()

        # Buang agregat nasional.
        df = df[
            ~df["provinsi"]
            .str.strip()
            .str.lower()
            .isin({
                "indonesia",
                "nasional",
            })
        ].copy()

        df["tahun"] = int(tahun)

        df["provinsi_normalized"] = (
            df["provinsi"]
            .apply(
                normalize_province
            )
        )

        print(
            f"DATA SOURCE {tahun}:",
            len(df),
            "row"
        )

        return df

    # --------------------------------------------------------
    # SCRAPE RANGE
    # --------------------------------------------------------

    def scrape(
        self,
        id_nama_data,
        tahun=2025,
    ):

        id_nama_data = int(
            id_nama_data
        )

        tahun = int(tahun)

        config = self.get_config(
            id_nama_data
        )

        source_years = config.get(
            "source_years"
        )

        if source_years:
            tahun_list = [
                int(t)
                for t in source_years
                if int(t) <= tahun
            ]
        else:
            tahun_awal = tahun - 2
            tahun_list = list(
                range(
                    tahun_awal,
                    tahun + 1
                )
            )

        all_data = []

        print("\n" + "=" * 70)
        print(
            "KPU PR SCRAPER"
        )
        print("=" * 70)

        print(
            "ID NAMA DATA:",
            id_nama_data
        )

        print(
            "INDIKATOR:",
            config["indikator"]
        )

        print(
            "PERIODE:",
            ", ".join(
                str(t)
                for t in tahun_list
            )
        )

        for tahun_data in tahun_list:

            content = (
                self.download_year(
                    id_nama_data,
                    tahun_data,
                )
            )

            df_year = (
                self.parse_csv(
                    content,
                    tahun_data,
                    id_nama_data,
                )
            )

            all_data.append(
                df_year
            )

        if not all_data:
            raise RuntimeError(
                "Tidak ada data source "
                "yang berhasil diambil."
            )

        df_raw = pd.concat(
            all_data,
            ignore_index=True,
        )

        print(
            "\nTOTAL SOURCE ROW:",
            len(df_raw),
        )

        return df_raw


# ============================================================
# KPU PR ETL
# ============================================================

class KPUPRETL:

    def __init__(self, dry_run=False):
        self.dry_run = dry_run

        if not KATADATA_API_KEY:
            raise RuntimeError(
                "KATADATA_API_KEY tidak ditemukan "
                "di .env"
            )

        self.session = create_session()

        self.headers = {
            "X-Api-Key":
                KATADATA_API_KEY
        }

        self.minio_client = (
            None if self.dry_run else get_minio_client()
        )

    # --------------------------------------------------------
    # DATASET CONFIG
    # --------------------------------------------------------

    def get_config(
        self,
        id_nama_data,
    ):

        return get_data_config(
            id_nama_data
        )

    # --------------------------------------------------------
    # PATH
    # --------------------------------------------------------

    def get_paths(
        self,
        id_nama_data,
    ):

        config = self.get_config(
            id_nama_data
        )

        storage_name = (
            config["storage_name"]
        )

        file_name = (
            config["file_name"]
        )

        raw_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "pupr"
            / storage_name
        )

        final_dir = (
            PROJECT_ROOT
            / "data"
            / "final"
            / "pupr"
            / storage_name
        )

        raw_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        final_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        return {
            "raw_dir":
                raw_dir,

            "final_dir":
                final_dir,

            "file_name":
                file_name,
        }

    # --------------------------------------------------------
    # GET EXISTING XFLASK
    # --------------------------------------------------------

    def get_xflask_api_data(
        self,
        id_nama_data,
    ):

        url = (
            f"{XFLASK_API_BASE_URL}"
            f"/getdata/{id_nama_data}"
        )

        print("\n" + "=" * 60)
        print(
            "GET DATA XFLASK API"
        )
        print("=" * 60)

        print(
            "URL:",
            f"/getdata/{id_nama_data}"
        )

        response = get(
            session=self.session,
            url=url,
            headers=self.headers,
            timeout=60,
        )

        print(
            "STATUS:",
            response.status_code,
        )

        result = response.json()

        if not isinstance(
            result,
            dict,
        ):
            raise RuntimeError(
                f"Response XFlask tidak sesuai: "
                f"{result}"
            )

        if not result.get(
            "success",
            True,
        ):
            raise RuntimeError(
                f"XFlask API gagal: {result}"
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

    # --------------------------------------------------------
    # SAVE RAW PARQUET PER TAHUN
    # --------------------------------------------------------

    def save_raw_parquet(
        self,
        df_raw,
        id_nama_data,
    ):

        paths = self.get_paths(
            id_nama_data
        )

        file_name = (
            paths["file_name"]
        )

        raw_dir = (
            paths["raw_dir"]
        )

        raw_paths = []

        print("\n" + "=" * 60)
        print(
            "SAVE RAW PARQUET"
        )
        print("=" * 60)

        for tahun, df_year in (
            df_raw.groupby("tahun")
        ):

            tahun = int(tahun)

            output_path = (
                raw_dir
                / (
                    f"{file_name}_"
                    f"{tahun}_raw.parquet"
                )
            )

            save_parquet(
                df_year,
                output_path,
            )

            raw_paths.append(
                output_path
            )

            print(
                f"RAW {tahun}:",
                len(df_year),
                "row ->",
                output_path,
            )

            if not self.dry_run:

                object_name = (
                    f"pupr/"
                    f"{DATA_CONFIG[id_nama_data]['storage_name']}/"
                    f"raw/"
                    f"{output_path.name}"
                )

                upload_local_file(
                    self.minio_client,
                    MINIO_BUCKET,
                    output_path,
                    object_name,
                    "application/octet-stream"
                )

                print(
                    "MINIO RAW:",
                    f"{MINIO_BUCKET}/{object_name}"
                )

            else:
                print("DRY RUN: Upload RAW MinIO dilewati.")

        return raw_paths

    # --------------------------------------------------------
    # MATCH SOURCE <-> XFLASK
    # --------------------------------------------------------

    def match_source_to_xflask(
        self,
        df_source,
        df_xflask,
    ):

        print("\n" + "=" * 60)
        print(
            "MATCH SOURCE <-> XFLASK"
        )
        print("=" * 60)

        if df_xflask.empty:
            raise RuntimeError(
                "Data existing XFlask kosong."
            )

        source = df_source.copy()

        existing = df_xflask.copy()

        source[
            "provinsi_normalized"
        ] = (
            source["provinsi"]
            .apply(
                normalize_province
            )
        )

        existing[
            "provinsi_normalized"
        ] = (
            existing["provinsi"]
            .apply(
                normalize_province
            )
        )

        existing["tahun"] = (
            pd.to_datetime(
                existing["data_x"],
                errors="coerce",
                dayfirst=True,
            )
            .dt.year
        )

        existing_match = existing[
            [
                "provinsi_normalized",
                "tahun",
                "data_y",
            ]
        ].rename(
            columns={
                "data_y":
                    "xflask_data_y"
            }
        )

        merged = source.merge(
            existing_match,
            on=[
                "provinsi_normalized",
                "tahun",
            ],
            how="left",
        )

        merged["status_match"] = (
            "PROVINSI BARU"
        )

        has_existing = (
            merged["xflask_data_y"]
            .notna()
        )

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

        match_count = (
            merged["status_match"]
            == "MATCH"
        ).sum()

        mismatch_count = (
            merged["status_match"]
            == "MISMATCH"
        ).sum()

        new_count = (
            merged["status_match"]
            == "PROVINSI BARU"
        ).sum()

        print(
            "MATCH:",
            match_count
        )

        print(
            "MISMATCH:",
            mismatch_count
        )

        print(
            "PROVINSI BARU:",
            new_count
        )

        mismatch = merged[
            merged["status_match"]
            == "MISMATCH"
        ]

        if not mismatch.empty:

            print(
                "\n=== MISMATCH ==="
            )

            print(
                mismatch[
                    [
                        "tahun",
                        "provinsi",
                        "data_y",
                        "xflask_data_y",
                    ]
                ]
                .sort_values(
                    [
                        "tahun",
                        "provinsi",
                    ]
                )
                .to_string(
                    index=False
                )
            )

        return merged

    # --------------------------------------------------------
    # BUILD FINAL
    # --------------------------------------------------------

    def build_final(
        self,
        df_source,
        df_xflask,
        id_nama_data,
    ):

        config = self.get_config(
            id_nama_data
        )

        matched = (
            self.match_source_to_xflask(
                df_source,
                df_xflask,
            )
        )

        tahun_series = (
            matched["tahun"]
            .astype(int)
        )

        final_df = pd.DataFrame({
            "kota": None,

            "provinsi":
                matched["provinsi"],

            "nama_indikator":
                config["indikator"],

            "nama_item": None,

            "idnamadata":
                int(id_nama_data),

            "data_x": (
                pd.to_datetime(
                    tahun_series.astype(str),
                    format="%Y",
                    errors="coerce",
                )
                .apply(
                    lambda x: (
                        x.replace(
                            month=12,
                            day=31,
                        )
                        if pd.notna(x)
                        else pd.NaT
                    )
                )
                .dt.strftime(
                    "%d-%m-%Y"
                )
            ),

            "data_y":
                pd.to_numeric(
                    matched["data_y"],
                    errors="coerce",
                ),

            "satuan":
                config["default_satuan"],

            "sumber": (
                matched["tahun"]
                .map(
                    config.get(
                        "sumber_by_year",
                        {},
                    )
                )
                .fillna(
                    "Kementerian Pekerjaan "
                    "Umum & Perumahan Rakyat"
                )
            ),

            "note":
                config["note"],

            "nama_data_import":
                config["nama_data_import"],
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
                subset=[
                    "provinsi",
                    "data_x",
                ]
            )
            .reset_index(drop=True)
        )

        return final_df

    # --------------------------------------------------------
    # SAVE FINAL CSV
    # --------------------------------------------------------

    def save_final(
        self,
        df_final,
        id_nama_data,
    ):

        paths = self.get_paths(
            id_nama_data
        )

        output_path = (
            paths["final_dir"]
            / (
                f"{paths['file_name']}.csv"
            )
        )

        save_csv(
            df_final,
            output_path,
        )

        print(
            "\nFINAL CSV:",
            output_path
        )

        return output_path

    # --------------------------------------------------------
    # UPLOAD FINAL MINIO
    # --------------------------------------------------------

    def upload_final_minio(
        self,
        final_path,
        id_nama_data,
    ):

        config = self.get_config(
            id_nama_data
        )

        object_name = (
            f"pupr/"
            f"{config['storage_name']}/"
            f"final/"
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
            f"{MINIO_BUCKET}/{object_name}"
        )

    # --------------------------------------------------------
    # PROCESS
    # --------------------------------------------------------

    def process(
        self,
        df_source,
        id_nama_data,
    ):

        id_nama_data = int(
            id_nama_data
        )

        config = self.get_config(
            id_nama_data
        )

        print("\n" + "=" * 70)
        print(
            "KPU PR ETL"
        )
        print("=" * 70)

        print(
            "ID NAMA DATA:",
            id_nama_data
        )

        print(
            "INDIKATOR:",
            config["indikator"]
        )

        # 1. SAVE RAW + UPLOAD RAW
        raw_paths = self.save_raw_parquet(
            df_source,
            id_nama_data,
        )

        # 2. LOAD ULANG RAW PARQUET
        df_raw = pd.concat(
            [
                read_parquet(path)
                for path in raw_paths
            ],
            ignore_index=True,
        )

        # 3. GET EXISTING XFLASK
        df_xflask = (
            self.get_xflask_api_data(
                id_nama_data
            )
        )

        # 4. BUILD FINAL
        df_final = self.build_final(
            df_raw,
            df_xflask,
            id_nama_data,
        )

        print(
            "\nTOTAL FINAL:",
            len(df_final)
        )

        print(
            "\nPREVIEW FINAL:"
        )

        print(
            df_final.head(20)
            .to_string(index=False)
        )

        # 5. SAVE FINAL
        final_path = self.save_final(
            df_final,
            id_nama_data,
        )

        # 5. UPLOAD FINAL
        if not self.dry_run:
            self.upload_final_minio(
                final_path,
                id_nama_data,
            )
        else:
            print("DRY RUN: Upload FINAL MinIO dilewati.")

        return df_final

# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Menjalankan pipeline tanpa upload ke MinIO.",
    )

    parser.add_argument(
        "--idnamadata",
        type=int,
        required=True,
        help="ID dataset PUPR / xFlask",
    )

    parser.add_argument(
        "--tahun",
        type=int,
        default=2025,
        help=(
            "Tahun terbaru. "
            "Akan mengambil tahun-2 "
            "sampai tahun."
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # SOURCE
    # --------------------------------------------------------

    scraper = KPUPRScraper()

    df_source = scraper.scrape(
        id_nama_data=args.idnamadata,
        tahun=args.tahun,
    )

    # --------------------------------------------------------
    # ETL
    # --------------------------------------------------------

    etl = KPUPRETL(dry_run=args.dry_run)

    df_final = etl.process(
        df_source=df_source,
        id_nama_data=args.idnamadata,
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print(
        "PIPELINE PUPR SELESAI"
    )
    print("=" * 70)

    print(
        "ID:",
        args.idnamadata
    )

    print(
        "FINAL ROW:",
        len(df_final)
    )


if __name__ == "__main__":
    main()