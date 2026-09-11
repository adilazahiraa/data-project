import io
import os
import re
from pathlib import Path

import pandas as pd
import requests
import pdfplumber
import pymupdf
from dotenv import load_dotenv

from utils.api import create_session
from utils.storage import (
    get_minio_client,
    upload_bytes,
)


# ============================================================
# ENV
# ============================================================

load_dotenv(override=True)

KATADATA_API_KEY = os.getenv("KATADATA_API_KEY")

DATABASE_BASE_URL = "https://xflask.databoks.id/database"


# ============================================================
# CONFIG
# ============================================================

# ============================================================
# PATH ESDM
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MINIO_BUCKET = "maganghub"

ESDM_RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "esdm"
    / "cadangan_minyak_bumi"
)

ESDM_FINAL_DIR = (
    PROJECT_ROOT
    / "final"
    / "esdm"
    / "cadangan_minyak_bumi"
)

ESDM_RAW_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

ESDM_FINAL_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

ESDM_RAW_MINIO_PREFIX = (
    "esdm/"
    "cadangan_minyak_bumi/"
    "raw/"
)

ESDM_FINAL_FILE = (
    ESDM_FINAL_DIR
    / "cadangan_minyak_bumi.csv"
)

ESDM_MINIO_OBJECT = (
    "esdm/"
    "cadangan_minyak_bumi/"
    "final/"
    "cadangan_minyak_bumi.csv"
)

ESDM_CONFIG = {
    4784: {
        "indikator": "Cadangan Minyak Bumi",
        "sumber": "Kementerian ESDM",
        "tahun_terbaru": 2025,
        "jumlah_tahun_scrape": 3,
        "default_satuan": "MSTB",
        "storage_name": "cadangan_minyak_bumi",
        "file_name": "cadangan_minyak_bumi",

        # Laporan Kinerja 2025 digunakan karena
        # laporan tersebut memuat data tahun 2024.
        "source_url": (
            "https://migas.esdm.go.id/cms/uploads/"
            "informasi-publik/Laporan-Kinerja/"
            "LAKIN-2025.pdf"
        ),
    },
}

ITEM_MASTER_FILE = PROJECT_ROOT / "item.parquet"


# ============================================================
# CLASS 1 - SCRAPER
# ============================================================

class ESDMScraper:

    def __init__(
        self,
        source_url,
        jumlah_tahun=3,
    ):
        self.source_url = source_url
        self.jumlah_tahun = int(jumlah_tahun)

    # --------------------------------------------------------
    # DOWNLOAD PDF
    # --------------------------------------------------------

    def download_pdf(self):
        local_pdf = r"D:\data-project\Laporan Kinerja 2025.pdf"

        print("\n" + "=" * 60)
        print("LOAD PDF ESDM")
        print("=" * 60)

        print("LOCAL PDF:")
        print(local_pdf)

        with open(local_pdf, "rb") as f:
            pdf_bytes = f.read()

        print(
            "PDF BERHASIL DILOAD:",
            f"{len(pdf_bytes):,}",
            "bytes",
        )

        return pdf_bytes

    # --------------------------------------------------------
    # EXTRACT TEXT
    # --------------------------------------------------------

    def extract_data(self):
        pdf_bytes = self.download_pdf()

        pdf = pymupdf.open(
            stream=pdf_bytes,
            filetype="pdf",
        )

        target_page = None

        for page in pdf:
            text = page.get_text("text")

            if (
                "Gambar 10 Cadangan Minyak Bumi" in text
                and "Potensial" in text
                and "Terbukti" in text
            ):
                target_page = page
                break

        if target_page is None:
            raise ValueError(
                "Halaman 'Gambar 10 Cadangan Minyak Bumi' tidak ditemukan."
            )

        text = target_page.get_text("text")

        print("\nTARGET PAGE FOUND")

        # Normalisasi whitespace
        lines = [
            line.strip()
            for line in text.splitlines()
            if line.strip()
        ]

        # Cari posisi label
        try:
            potensial_idx = lines.index("Potensial")
            terbukti_idx = lines.index("Terbukti")
        except ValueError:
            raise ValueError(
                "Label 'Potensial' atau 'Terbukti' tidak ditemukan."
            )

        # Urutan tahun pada chart
        tahun_chart = [
            "2021",
            "2022",
            "2023",
            "2024",
            "2025",
        ]

        # Ambil 5 angka setelah Potensial
        potensial_values = []

        for value in lines[potensial_idx + 1:]:
            if value.isdigit():
                potensial_values.append(int(value))

            if len(potensial_values) == 5:
                break

        # Ambil 5 angka setelah Terbukti
        terbukti_values = []

        for value in lines[terbukti_idx + 1:]:
            if value.isdigit():
                terbukti_values.append(int(value))

            if len(terbukti_values) == 5:
                break

        if len(potensial_values) != 5:
            raise ValueError(
                f"Data Potensial tidak lengkap: {potensial_values}"
            )

        if len(terbukti_values) != 5:
            raise ValueError(
                f"Data Terbukti tidak lengkap: {terbukti_values}"
            )

        # Ambil beberapa tahun terakhir
        tahun_tersedia = [
            int(tahun)
            for tahun in tahun_chart
        ]

        tahun_target = tahun_tersedia[-self.jumlah_tahun:]

        print("\nTAHUN YANG DI-SCRAPE:")
        print(tahun_target)

        data = []

        for tahun in tahun_target:
            tahun_idx = tahun_chart.index(str(tahun))

            potensial = potensial_values[tahun_idx]
            terbukti = terbukti_values[tahun_idx]

            print(f"\nTahun     : {tahun}")
            print(f"Potensial : {potensial} MSTB")
            print(f"Terbukti  : {terbukti} MSTB")

            data.append({
                "tahun": tahun,
                "potential": potensial,
                "proven": terbukti,
                "satuan": "MSTB",
            })

        return data

    # --------------------------------------------------------
    # PARSE DATA
    # --------------------------------------------------------

    def parse_data(
        self,
        extracted_data,
    ):

        print("\n" + "=" * 60)
        print("PARSE DATA")
        print("=" * 60)

        def parse_number(value):

            if value is None:
                return pd.NA

            value = str(value).strip()

            if value in {
                "",
                "...",
                "–",
                "-",
                "NA",
            }:
                return pd.NA

            try:
                return float(
                    value.replace(",", "")
                )
            except ValueError:
                raise ValueError(
                    f"Nilai tidak dapat diparse: {value}"
                )

        rows = []

        for data in extracted_data:

            tahun = int(data["tahun"])

            potential = parse_number(
                data["potential"]
            )

            proven = parse_number(
                data["proven"]
            )

            # XFlask menyimpan nilai dalam skala 1.000.000
            potential = potential * 1_000_000
            proven = proven * 1_000_000

            # Total = Terbukti + Potensial
            total = pd.NA

            if (
                pd.notna(potential)
                and pd.notna(proven)
            ):
                total = proven + potential

            data_x = pd.Timestamp(
                f"{tahun}-12-31"
            )

            rows.extend(
                [
                    {
                        "item": "Total",
                        "data_x": data_x,
                        "data_y": total,
                        "satuan": "MSTB",
                    },
                    {
                        "item": "Terbukti",
                        "data_x": data_x,
                        "data_y": proven,
                        "satuan": "MSTB",
                    },
                    {
                        "item": "Potensial",
                        "data_x": data_x,
                        "data_y": potential,
                        "satuan": "MSTB",
                    },
                ]
            )

        df = pd.DataFrame(rows)

        print("\nHASIL PARSE:")

        print(df.head().to_string(index=False))

        return df

    # --------------------------------------------------------
    # RUN SCRAPER
    # --------------------------------------------------------

    def run(self):

        print("\n" + "=" * 60)
        print("SCRAPE ESDM - CADANGAN MINYAK BUMI")
        print("=" * 60)

        extracted_data = self.extract_data()

        df = (
            self.parse_data(
                extracted_data
            )
        )

        tahun_files = {}

        for tahun, df_tahun in df.groupby(
            df["data_x"].dt.year
        ):

            raw_file = (
                ESDM_RAW_DIR
                / f"cadangan_minyak_bumi_{tahun}.parquet"
            )

            df_tahun.to_parquet(
                raw_file,
                index=False,
            )

            tahun_files[int(tahun)] = raw_file

            print(
                f"\nRAW {tahun} DISIMPAN:"
            )
            print(raw_file)

        print("\n" + "=" * 60)
        print("SCRAPING ESDM SELESAI")
        print("=" * 60)

        print(
            "TOTAL ROW:",
            len(df),
        )

        return df


# ============================================================
# CLASS 2 - ETL
# ============================================================

class ESDMETL:

    def __init__(
        self,
        id_nama_data,
        nama_data_import,
    ):

        self.id_nama_data = int(
            id_nama_data
        )

        self.nama_data_import = nama_data_import

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

    # --------------------------------------------------------
    # GET EXISTING XFLASK
    # --------------------------------------------------------

    def get_existing_xflask_data(self):

        print("\n" + "=" * 60)
        print("GET EXISTING XFLASK")
        print("=" * 60)

        url = (
            f"https://xflask.databoks.id/api/getdata/"
            f"{self.id_nama_data}"
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

        data = response.json()

        print("RESPONSE RECEIVED")

        if isinstance(data, list):
            df = pd.DataFrame(data)

        elif isinstance(data, dict):

            # Coba ambil data dari key yang umum
            possible_keys = [
                "data",
                "result",
                "results",
                "rows",
            ]

            data_rows = None

            for key in possible_keys:

                if (
                    key in data
                    and isinstance(
                        data[key],
                        list,
                    )
                ):
                    data_rows = data[key]
                    break

            if data_rows is None:
                raise RuntimeError(
                    "Response XFlask berupa dict, "
                    "tetapi list data tidak ditemukan. "
                    f"Keys: {list(data.keys())}"
                )

            df = pd.DataFrame(
                data_rows
            )

        else:

            raise RuntimeError(
                "Format response XFlask tidak dikenali."
            )

        print(
            "JUMLAH EXISTING:",
            len(df),
        )

        print(
            "KOLOM:",
            df.columns.tolist(),
        )

        return df

    # --------------------------------------------------------
    # NORMALIZE ITEM
    # --------------------------------------------------------

    @staticmethod
    def normalize_item(
        value,
    ):

        text = str(
            value
        ).strip().lower()

        text = re.sub(
            r"[^a-z0-9]+",
            " ",
            text,
        ).strip()

        aliases = {
            "total": "total",

            "terbukti": "terbukti",
            "proven": "terbukti",
            "proven reserves": "terbukti",

            "potensial": "potensial",
            "potential": "potensial",
            "potential reserves": "potensial",
        }

        return aliases.get(
            text,
            text,
        )

    def map_item_ids(self, df):
        df = df.copy()

        item_master = pd.read_parquet(
            ITEM_MASTER_FILE,
            columns=["id", "nama"],
        )

        item_master["item_match"] = (
            item_master["nama"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df["item_match"] = (
            df["item"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df = df.merge(
            item_master.rename(
                columns={
                    "id": "id_item",
                    "nama": "item_master",
                }
            ),
            on="item_match",
            how="left",
        )

        # Total tidak dikirim ke final
        missing = df[
            (df["item"] != "Total")
            & (df["id_item"].isna())
        ]

        if not missing.empty:
            raise RuntimeError(
                "Item tidak ditemukan di item.parquet:\n"
                + missing["item"]
                .drop_duplicates()
                .to_string(index=False)
            )

        df["id_item"] = (
            pd.to_numeric(
                df["id_item"],
                errors="coerce",
            )
            .astype("Int64")
        )

        print("\nMAPPING ITEM:")
        print(
            df[
                ["item", "id_item"]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

        return df

    # --------------------------------------------------------
    # LOAD MASTER ITEM
    # --------------------------------------------------------

    def load_item_master(self):
        print("\n" + "=" * 60)
        print("LOAD MASTER ITEM")
        print("=" * 60)

        if not ITEM_MASTER_FILE.exists():
            raise FileNotFoundError(
                f"File item master tidak ditemukan: "
                f"{ITEM_MASTER_FILE}"
            )

        df_item = pd.read_parquet(
            ITEM_MASTER_FILE
        )

        print("MASTER ITEM BERHASIL DIBACA:", len(df_item))

        required_columns = {
            "id",
            "nama",
        }

        missing_columns = (
            required_columns
            - set(df_item.columns)
        )

        if missing_columns:
            raise RuntimeError(
                "Kolom item.parquet tidak lengkap. "
                f"Kolom yang hilang: {missing_columns}"
            )

        print("KOLOM MASTER:", df_item.columns.tolist())

        df_item = df_item[
            ["id", "nama"]
        ].copy()

        print("FILTER KOLOM MASTER BERHASIL:", len(df_item))

        df_item["item_match"] = df_item["nama"].apply(self.normalize_item)

        print("NORMALIZE MASTER BERHASIL")

        print("DUPLIKAT MASTER:", df_item["item_match"].duplicated().sum())

        print("LOAD MASTER SELESAI")

        df_item["item_match"] = (
            df_item["nama"]
            .apply(self.normalize_item)
        )

        # Buang item yang tidak punya nama
        df_item = df_item[
            df_item["item_match"].notna()
        ].copy()

        print(
            "\nTOTAL MASTER ITEM:",
            len(df_item),
        )

        return df_item

    # --------------------------------------------------------
    # MATCH ITEM SCRAPING VS MASTER ITEM
    # --------------------------------------------------------

    def map_item_ids(self, df_scraping):
        print("\n" + "=" * 60)
        print("MATCH ITEM SCRAPING VS MASTER ITEM")
        print("=" * 60)

        df = df_scraping.copy()

        df["item_match"] = (
            df["item"]
            .apply(self.normalize_item)
        )

        df_item = self.load_item_master()

        print("RETURN DARI LOAD MASTER")
        print("SCRAPING ROW:", len(df))
        print("MASTER ROW:", len(df_item))

        mapping = df_item[
            [
                "item_match",
                "id",
                "nama",
            ]
        ].rename(
            columns={
                "id": "id_item",
                "nama": "item_master",
            }
        )

        # Total tidak perlu dimapping karena
        # tidak akan masuk ke data final
        df_non_total = df[
            df["item"] != "Total"
        ].copy()

        df_total = df[
            df["item"] == "Total"
        ].copy()

        # Mapping hanya untuk item selain Total
        df_non_total = df_non_total.merge(
            mapping,
            on="item_match",
            how="left",
        )

        # Total tetap dipertahankan untuk proses validasi,
        # tetapi tidak memiliki id_item
        df_total["id_item"] = pd.NA
        df_total["item_master"] = "(total)"

        df = pd.concat(
            [
                df_total,
                df_non_total,
            ],
            ignore_index=True,
        )

        # Total tidak wajib punya id_item
        missing_item_id = df[
            (df["item"] != "Total")
            & (df["id_item"].isna())
        ].copy()

        if not missing_item_id.empty:
            raise RuntimeError(
                "Ada item hasil scraping yang "
                "tidak ditemukan di item.parquet:\n"
                + missing_item_id[
                    ["item"]
                ]
                .drop_duplicates()
                .to_string(index=False)
            )

        # Pastikan id_item integer
        df["id_item"] = (
            pd.to_numeric(
                df["id_item"],
                errors="coerce",
            )
            .astype("Int64")
        )

        print("\nHASIL MAPPING ITEM:")
        print(
            df[
                [
                    "item",
                    "item_master",
                    "id_item",
                ]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

        return df

    # --------------------------------------------------------
    # MATCH SCRAPING VS XFLASK
    # --------------------------------------------------------

    def match_data(
        self,
        df_scraping,
        df_existing,
    ):
        print("\n" + "=" * 60)
        print("MATCH SCRAPING VS XFLASK")
        print("=" * 60)

        scraping = df_scraping.copy()
        existing = df_existing.copy()

        # --------------------------------------------------------
        # Normalize item
        # --------------------------------------------------------

        scraping["item_match"] = (
            scraping["item"]
            .apply(self.normalize_item)
        )

        existing["item_match"] = (
            existing["item"]
            .apply(self.normalize_item)
        )

        # --------------------------------------------------------
        # Convert date
        # --------------------------------------------------------

        scraping["data_x"] = pd.to_datetime(
            scraping["data_x"],
            errors="coerce",
        )

        existing["date"] = pd.to_datetime(
            existing["date"],
            errors="coerce",
        )

        # Tahun untuk matching
        scraping["tahun"] = (
            scraping["data_x"].dt.year
        )

        existing["tahun"] = (
            existing["date"].dt.year
        )

        # --------------------------------------------------------
        # Mapping existing XFlask
        # berdasarkan ITEM + TAHUN
        # --------------------------------------------------------

        mapping = existing[
            [
                "item_match",
                "item",
                "tahun",
                "data_y",
                "satuan",
            ]
        ].drop_duplicates(
            subset=[
                "item_match",
                "tahun",
            ]
        )

        mapping = mapping.rename(
            columns={
                "item": "item_xflask",
                "data_y": "data_y_xflask",
                "satuan": "satuan_xflask",
            }
        )

        # --------------------------------------------------------
        # Merge scraping vs XFlask
        # --------------------------------------------------------

        result = scraping.merge(
            mapping,
            on=[
                "item_match",
                "tahun",
            ],
            how="left",
        )

        # --------------------------------------------------------
        # Status match
        # --------------------------------------------------------

        result["status_match"] = (
            result["data_y_xflask"].notna()
            & result["satuan_xflask"].notna()
        )

        result["status_match"] = result[
            "status_match"
        ].map(
            {
                True: "MATCH",
                False: "BELUM ADA",
            }
        )

        print(
            "\nSTATUS MATCH:",
            result["status_match"]
            .value_counts()
            .to_dict()
        )

        print(
            "TOTAL HASIL MATCH:",
            len(result)
        )

        return result
    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    def validate_data(
        self,
        df_match,
    ):

        print("\n" + "=" * 60)
        print("VALIDASI DATA")
        print("=" * 60)

        # ----------------------------------------------------
        # VALIDASI DATA BARU
        # ----------------------------------------------------

        df_new = df_match[
            (df_match["status_match"] != "MATCH")
            & (df_match["item"] != "Total")
        ].copy()

        print("FILTER DATA BARU BERHASIL:", len(df_new))

        if not df_new.empty:
            print("\nℹ️ DATA BARU / BELUM ADA DI XFLASK:")
            print("JUMLAH DATA BARU:", len(df_new))
        else:
            print("✅ SEMUA DATA SUDAH ADA DI XFLASK")

        # ----------------------------------------------------
        # VALIDASI NILAI
        # ----------------------------------------------------

        df_valid = df_match[
            df_match["status_match"] == "MATCH"
        ].copy()

        print("FILTER DATA MATCH BERHASIL:", len(df_valid))
        print("CHECKPOINT 1")

        if not df_new.empty:
            print("\nℹ️ DATA BARU / BELUM ADA DI XFLASK:")
            print("JUMLAH DATA BARU:", len(df_new))
        else:
            print("✅ SEMUA DATA SUDAH ADA DI XFLASK")

        df_valid = df_match[
            df_match["status_match"] == "MATCH"
        ].copy()

        print("CHECKPOINT 2")
        print("FILTER DATA MATCH BERHASIL:", len(df_valid))
        print("KOLOM DATA MATCH:", df_valid.columns.tolist())

        nilai_scraping = (
            pd.to_numeric(
                df_valid["data_y"],
                errors="coerce",
            )
        )

        nilai_xflask = (
            pd.to_numeric(
                df_valid["data_y_xflask"],
                errors="coerce",
            )
        )

        mismatch_nilai = (
            nilai_scraping != nilai_xflask
        )

        if mismatch_nilai.any():

            print(
                "\n❌ NILAI TIDAK SAMA:"
            )

            print(
                df_valid.loc[
                    mismatch_nilai,
                    [
                        "item",
                        "tahun",
                        "data_y",
                        "data_y_xflask",
                    ],
                ].to_string(
                    index=False
                )
            )

            raise RuntimeError(
                "Ada nilai scraping yang "
                "berbeda dengan XFlask."
            )

        print(
            "✅ NILAI DATA SESUAI"
        )

        # ----------------------------------------------------
        # VALIDASI SATUAN
        # ----------------------------------------------------

        satuan_scraping = (
            df_valid["satuan"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        satuan_xflask = (
            df_valid["satuan_xflask"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        mismatch_satuan = (
            satuan_scraping
            != satuan_xflask
        )

        if mismatch_satuan.any():

            print(
                "\n❌ SATUAN TIDAK SAMA:"
            )

            print(
                df_valid.loc[
                    mismatch_satuan,
                    [
                        "item",
                        "tahun",
                        "satuan",
                        "satuan_xflask",
                    ],
                ].to_string(
                    index=False
                )
            )

            raise RuntimeError(
                "Ada satuan scraping yang "
                "berbeda dengan XFlask."
            )

        print(
            "✅ SATUAN SESUAI"
        )

        print(
            "\n✅ VALIDASI SELESAI"
        )

        return True

    # --------------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------------

    def transform(
        self,
        df_match,
    ):

        print("\n" + "=" * 60)
        print("TRANSFORM DATA")
        print("=" * 60)

        # ----------------------------------------------------
        # COPY DATA HASIL MATCH
        # ----------------------------------------------------

        df_final = df_match.copy()

        # ----------------------------------------------------
        # FORMAT DATA_X
        # ----------------------------------------------------

        df_final["data_x"] = pd.to_datetime(
            df_final["data_x"],
            errors="coerce",
        )

        df_final = df_final[
            df_final["data_x"].notna()
        ].copy()

        # ----------------------------------------------------
        # AMBIL 3 PERIODE TERAKHIR
        # ----------------------------------------------------

        periode_terakhir = (
            df_final["data_x"]
            .dt.year
            .dropna()
            .drop_duplicates()
            .sort_values()
            .tail(3)
            .tolist()
        )

        print("\nPERIODE FINAL:")
        print(periode_terakhir)

        df_final = df_final[
            df_final["data_x"]
            .dt.year
            .isin(periode_terakhir)
        ].copy()

        # ----------------------------------------------------
        # BUANG ITEM TOTAL
        # ----------------------------------------------------

        df_final = df_final[
            df_final["item"] != "Total"
        ].copy()

        # ----------------------------------------------------
        # VALIDASI NILAI
        # ----------------------------------------------------

        df_final = df_final[
            df_final["data_y"].notna()
        ].copy()

        # ----------------------------------------------------
        # TAMBAHKAN METADATA
        # ----------------------------------------------------

        df_final["idnamadata"] = (
            self.id_nama_data
        )

        df_final["nama_indikator"] = (
            "Cadangan Minyak Bumi"
        )

        df_final["sumber"] = (
            "Kementerian ESDM"
        )

        df_final["nama_data_import"] = (
            self.nama_data_import
        )

        df_final["negara"] = "Indonesia"

        # ----------------------------------------------------
        # FORMAT TANGGAL FINAL
        # ----------------------------------------------------

        df_final["data_x"] = (
            df_final["data_x"]
            .dt.strftime("%d-%m-%Y")
        )

        # ----------------------------------------------------
        # KOLOM FINAL
        # ----------------------------------------------------

        df_final = df_final[
            [
                "idnamadata",
                "nama_indikator",
                "item",
                "id_item",
                "data_x",
                "data_y",
                "satuan",
                "sumber",
                "nama_data_import",
                "negara",
            ]
        ]

        # ----------------------------------------------------
        # SORT
        # ----------------------------------------------------

        df_final["_sort_date"] = pd.to_datetime(
            df_final["data_x"],
            format="%d-%m-%Y",
            errors="coerce",
        )

        df_final = (
            df_final
            .sort_values(
                [
                    "_sort_date",
                    "item",
                ]
            )
            .drop(
                columns="_sort_date"
            )
            .reset_index(drop=True)
        )

        # ----------------------------------------------------
        # CHECK FINAL
        # ----------------------------------------------------

        print("\nFINAL DATA:")
        print(
            df_final.to_string(index=False)
        )

        print(
            "\nPERIODE FINAL:",
            sorted(
                pd.to_datetime(
                    df_final["data_x"],
                    format="%d-%m-%Y",
                )
                .dt.year
                .unique()
                .tolist()
            )
        )

        print(
            "JUMLAH FINAL ROW:",
            len(df_final),
        )

        return df_final

    def save_final(
        self,
        df_final,
    ):
        print(
            "\n" + "=" * 60
        )
        print(
            "SAVE FINAL CSV"
        )
        print(
            "=" * 60
        )

        df_final.to_csv(
            ESDM_FINAL_FILE,
            index=False,
            encoding="utf-8-sig",
        )

        print(
            "\nFINAL FILE:"
        )

        print(
            ESDM_FINAL_FILE
        )

        print(
            "TOTAL ROW:",
            len(df_final),
        )

        return ESDM_FINAL_FILE

    def upload_minio(
        self,
    ):

        print("\n" + "=" * 60)
        print("UPLOAD MINIO")
        print("=" * 60)

        # ========================================================
        # UPLOAD RAW PARQUET PER TAHUN
        # ========================================================

        raw_files = sorted(
            ESDM_RAW_DIR.glob(
                "cadangan_minyak_bumi_*.parquet"
            )
        )

        if not raw_files:
            raise FileNotFoundError(
                "File RAW Parquet tidak ditemukan."
            )

        raw_minio_paths = []

        for raw_file in raw_files:

            object_name = (
                f"{ESDM_RAW_MINIO_PREFIX}"
                f"{raw_file.name}"
            )

            with open(
                raw_file,
                "rb",
            ) as file:
                raw_bytes = file.read()

            upload_bytes(
                self.minio_client,
                MINIO_BUCKET,
                raw_bytes,
                object_name,
                "application/octet-stream",
            )

            raw_minio_path = (
                f"s3://{MINIO_BUCKET}/"
                f"{object_name}"
            )

            raw_minio_paths.append(
                raw_minio_path
            )

            print(
                "\nRAW PARQUET MINIO UPLOAD BERHASIL:"
            )
            print(
                f"{MINIO_BUCKET}/"
                f"{object_name}"
            )

        # ========================================================
        # UPLOAD FINAL CSV
        # ========================================================

        with open(
            ESDM_FINAL_FILE,
            "rb",
        ) as file:
            final_bytes = file.read()

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            final_bytes,
            ESDM_MINIO_OBJECT,
            "text/csv",
        )

        print(
            "\nFINAL CSV MINIO UPLOAD BERHASIL:"
        )

        print(
            f"{MINIO_BUCKET}/"
            f"{ESDM_MINIO_OBJECT}"
        )

        return {
            "raw": raw_minio_paths,
            "final": (
                f"s3://{MINIO_BUCKET}/"
                f"{ESDM_MINIO_OBJECT}"
            ),
        }

    # --------------------------------------------------------
    # RUN ETL
    # --------------------------------------------------------

    def run(
        self,
        scraper,
    ):

        print("\n" + "=" * 60)
        print("ETL CADANGAN MINYAK BUMI")
        print("=" * 60)

        # ====================================================
        # EXTRACT
        # ====================================================

        df_scraping = (
            scraper.run()
        )

        df_scraping = self.map_item_ids(
            df_scraping
        )

        # ====================================================
        # LOAD EXISTING XFLASK
        # ====================================================

        df_existing = (
            self.get_existing_xflask_data()
        )

        # ====================================================
        # MATCH
        # ====================================================

        df_match = (
            self.match_data(
                df_scraping,
                df_existing,
            )
        )

        # ====================================================
        # VALIDATE
        # ====================================================

        self.validate_data(
            df_match
        )

        # ====================================================
        # TRANSFORM
        # ====================================================

        df_final = (
            self.transform(
                df_match
            )
        )

        final_file = self.save_final(
            df_final
        )

        minio_path = self.upload_minio()

        print(
            "\nFINAL LOCAL:"
        )

        print(
            final_file
        )

        print(
            "\nFINAL MINIO:"
        )

        print(
            minio_path
        )

        print("\n" + "=" * 60)
        print("PIPELINE ESDM SELESAI")
        print("=" * 60)

        return df_final

def fetch(params=None):

    params = params or {}

    idnamadata = params.get("idnamadata")

    if idnamadata is not None:
        idnamadata = int(idnamadata)

    # CADANGAN MINYAK BUMI
    if idnamadata == 4784:

        config = ESDM_CONFIG[4784]

        scraper = ESDMScraper(
            source_url=config["source_url"],
            jumlah_tahun=config["jumlah_tahun_scrape"],
        )

        etl = ESDMETL(
            id_nama_data=4784,
            nama_data_import=config["source_url"],
        )

        return etl.run(scraper)

    raise ValueError(
        f"idnamadata ESDM tidak dikenali: {idnamadata}"
    )

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    config = ESDM_CONFIG[4784]

    scraper = ESDMScraper(
        source_url=config["source_url"],
        jumlah_tahun=config["jumlah_tahun_scrape"],
    )

    etl = ESDMETL(
        id_nama_data=4784,
        nama_data_import=config["source_url"],
    )

    df_final = etl.run(
        scraper
    )

    print(
        "\nTOTAL FINAL:",
        len(df_final),
    )