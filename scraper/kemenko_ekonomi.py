import os
import re
import requests
import pdfplumber
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.storage import get_minio_client, upload_bytes


load_dotenv(override=True)


# ============================================================
# CONFIG
# ============================================================

KATADATA_API_KEY = os.getenv("KATADATA_API_KEY")

DATABASE_BASE_URL = "https://xflask.databoks.id/database"
API_BASE_URL = "https://xflask.databoks.id/api"

MINIO_BUCKET = "maganghub"

PDF_FILE_2019 = "data_kemenko/BUKU STATISTIK PERKEBUNAN UNGGULAN 2019-2021 OK.pdf"
PDF_FILE_2020 = "data_kemenko/STATISTIK-UNGGULAN-2020-2022.pdf"
PDF_FILE_2021 = "data_kemenko/Buku Statistik Perkebunan Unggulan 2021-2023.pdf"
PDF_FILE_2022 = "data_kemenko/STATISTIK PERKEBUNAN JILID I 2022-2024.pdf"
PDF_FILE_2023 = "data_kemenko/BUKU Statistik perkebunan 2023-2025 JILID 1.pdf"
PDF_FILE_2024_2026 = "data_kemenko/BUKU STATISTIK 2024-2026 JILID 1.pdf"

# Folder raw
RAW_BASE_DIR = Path("data/raw/kemenko")

DATA_CONFIG = {
    32679: {
        "nama_data": "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi",
        "frekuensi": "tahun",
        "leveldata": "provinsi",
        "satuan": "Ton",
        "raw_folder": "produksi_kelapa_sawit_bumn_provinsi",
        "filename": "produksi_kelapa_sawit_bumn_provinsi",
        "jenis": "Perkebunan Besar Negara (PBN)",
        "pdf_file": {
            2019: PDF_FILE_2019,
            2020: PDF_FILE_2020,
            2021: PDF_FILE_2021,
            2022: PDF_FILE_2022,
            2023: PDF_FILE_2023,
            2024: PDF_FILE_2024_2026,
            2025: PDF_FILE_2024_2026,
            2026: PDF_FILE_2024_2026,
        },

        "pdf_pages": {
            2019: [60, 61],
            2020: [102, 103],
            2021: [100, 101],
            2022: [106, 107],
            2023: [108, 109],
            2024: [108, 109],
            2025: [110, 111],
            2026: [112, 113],
        },
    },

    32677: {
        "nama_data": "Produksi Kelapa Sawit Milik Swasta Menurut Provinsi",
        "frekuensi": "tahun",
        "leveldata": "provinsi",
        "satuan": "Ton",
        "raw_folder": "produksi_kelapa_sawit_swasta_provinsi",
        "filename": "produksi_kelapa_sawit_swasta_provinsi",
        "jenis": "Perkebunan Besar Swasta (PBS)",

        "pdf_file": {
            2019: PDF_FILE_2019,
            2020: PDF_FILE_2020,
            2021: PDF_FILE_2021,
            2022: PDF_FILE_2022,
            2023: PDF_FILE_2023,
            2024: PDF_FILE_2024_2026,
            2025: PDF_FILE_2024_2026,
            2026: PDF_FILE_2024_2026,
        },

        "pdf_pages": {
            2019: [66, 67],
            2020: [108, 109],
            2021: [106, 107],
            2022: [112, 113],
            2023: [114, 115],
            2024: [114, 115],
            2025: [116, 117],
            2026: [118, 119],
        },
    },

    32678: {
        "nama_data": "Produksi Kelapa Sawit Milik Rakyat Menurut Provinsi",
        "frekuensi": "tahun",
        "leveldata": "provinsi",
        "satuan": "Ton",
        "raw_folder": "produksi_kelapa_sawit_rakyat_provinsi",
        "filename": "produksi_kelapa_sawit_rakyat_provinsi",
        "jenis": "Perkebunan Rakyat (PR)",

        "pdf_file": {
            2019: PDF_FILE_2019,
            2020: PDF_FILE_2020,
            2021: PDF_FILE_2021,
            2022: PDF_FILE_2022,
            2023: PDF_FILE_2023,
            2024: PDF_FILE_2024_2026,
            2025: PDF_FILE_2024_2026,
            2026: PDF_FILE_2024_2026,
        },

        "pdf_pages": {
            2019: [41, 42],
            2020: [83, 84],
            2021: [81, 82],
            2022: [87, 88],
            2023: [102, 103],
            2024: [89, 90],
            2025: [104, 105],
            2026: [106, 107],
        },
    },

    2883: {
        "nama_data": "Produksi Kelapa Sawit Milik Rakyat Menurut Kabupaten/Kota",
        "frekuensi": "tahun",
        "leveldata": "kabupaten/kota",
        "satuan": "Ton",

        "raw_folder": "produksi_kelapa_sawit_rakyat_kabupaten_kota",
        "filename": "produksi_kelapa_sawit_rakyat_kabupaten_kota",

        "jenis": "Perkebunan Rakyat (PR)",

        "pdf_file": {
            2019: PDF_FILE_2019,
            2020: PDF_FILE_2020,
            2021: PDF_FILE_2021,
            2022: PDF_FILE_2022,
            2023: PDF_FILE_2023,
            2024: PDF_FILE_2024_2026,
        },

        "pdf_pages": {
            2019: [43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55],
            2020: [85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97],
            2021: [83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95],
            2022: [89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101],
            2023: [89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101],
            2024: [91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103],
        },
    },
}

PROVINCE_METADATA_32677 = {
    "SUMATERA UTARA": {
        "id_provinsi": 2,
        "kode_bps": "1200",
    },
    "KALIMANTAN UTARA": {
        "id_provinsi": 34,
        "kode_bps": "6500",
    },
    "GORONTALO": {
        "id_provinsi": 28,
        "kode_bps": "7500",
    },
    "MALUKU": {
        "id_provinsi": 29,
        "kode_bps": "8100",
    },
    "MALUKU UTARA": {
        "id_provinsi": 30,
        "kode_bps": "8200",
    },
}

PROVINCE_METADATA_32678 = {
    "SUMATERA UTARA": {
        "id_provinsi": 2,
        "kode_bps": "1200",
    },
    "KEPULAUAN RIAU": {
        "id_provinsi": 10,
        "kode_bps": "2100",
    },
    "KALIMANTAN UTARA": {
        "id_provinsi": 34,
        "kode_bps": "6500",
    },
    "GORONTALO": {
        "id_provinsi": 28,
        "kode_bps": "7500",
    },
    "SULAWESI TENGGARA": {
        "id_provinsi": 27,
        "kode_bps": "7400",
    },
    "MALUKU": {
        "id_provinsi": 29,
        "kode_bps": "8100",
    },
}


# ============================================================
# KEMENKO EKONOMI SCRAPER
# ============================================================

class KemenkoEkonomiScraper:

    def __init__(self):

        self.headers = {
            "X-Api-Key": KATADATA_API_KEY
        }

        self.raw_dir = (
            RAW_BASE_DIR
            / DATA_CONFIG[32679]["raw_folder"]
        )

        self.raw_dir.mkdir(
            parents=True,
            exist_ok=True
        )

    # --------------------------------------------------------
    # Get existing data from xFlask
    # --------------------------------------------------------

    def get_existing_xflask_data(self, id_nama_data):

        url = f"{DATABASE_BASE_URL}/data"

        params = {
            "id_nama_data": id_nama_data
        }

        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=60
        )

        response.raise_for_status()

        return pd.DataFrame(
            response.json()["data"]
        )

    # --------------------------------------------------------
    # Get API data from xFlask
    # --------------------------------------------------------

    def get_xflask_api_data(self, id_nama_data):

        url = f"{API_BASE_URL}/getdata/{id_nama_data}"

        response = requests.get(
            url,
            headers=self.headers,
            timeout=60
        )

        response.raise_for_status()

        result = response.json()

        return pd.DataFrame(
            result["data"]
        )

    # --------------------------------------------------------
    # Build final dataframe sesuai schema xFlask
    # --------------------------------------------------------

    def build_final_32679(self, df_pdf, df_existing):

        print("\n========================================")
        print("BUILD FINAL DATA ID 32679")
        print("========================================")

        if df_existing.empty:
            raise ValueError(
                "Data existing xFlask kosong. "
                "Metadata provinsi tidak bisa digunakan."
            )

        # ----------------------------------------------------
        # Normalize existing province
        # ----------------------------------------------------

        df_existing = df_existing.copy()

        df_existing["provinsi_normalized"] = (
            df_existing["provinsi"]
            .apply(self.normalize_province)
        )

        # ----------------------------------------------------
        # Kolom yang memang berasal dari xFlask
        # ----------------------------------------------------

        xflask_columns = [
            "data_x",
            "data_y",
            "date",
            "frekuensi",
            "id",
            "id_kota",
            "id_nama_data",
            "id_provinsi",
            "indikator",
            "institusi",
            "item",
            "ket_tambahan",
            "kode_bps",
            "kota",
            "leveldata",
            "nama",
            "nama_data_import",
            "nama_alias",
            "negara",
            "provinsi",
            "satuan",
            "sumber",
        ]

        final_rows = []

        # ----------------------------------------------------
        # Set metadata template per provinsi
        # ----------------------------------------------------

        for _, row in df_pdf.iterrows():

            provinsi = row["provinsi"]
            tahun = row["tahun"]

            # Cari metadata provinsi yang sama
            metadata = df_existing[
                df_existing["provinsi_normalized"] == provinsi
            ]

            if not metadata.empty:

                # Ambil metadata existing sebagai template
                template = metadata.iloc[0].copy()

            elif provinsi == "SUMATERA UTARA":

                # Metadata Sumatera Utara belum tersedia
                # di data existing xFlask
                template = pd.Series({
                    "data_x": None,
                    "data_y": None,
                    "date": None,
                    "frekuensi": "tahun",
                    "id": None,
                    "id_kota": 0,
                    "id_nama_data": 32679,
                    "id_provinsi": 2,
                    "indikator": "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi",
                    "institusi": "Kementerian Koordinator Bidang Perekonomian",
                    "item": None,
                    "ket_tambahan": None,
                    "kode_bps": "1200",
                    "kota": None,
                    "leveldata": "provinsi",
                    "nama": "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi",
                    "nama_alias": "Sumatera Utara",
                    "negara": "Indonesia",
                    "provinsi": "SUMATERA UTARA",
                    "satuan": "Ton",
                    "sumber": None,
                })

            else:

                print(
                    f"⚠️ Metadata provinsi tidak ditemukan: "
                    f"{provinsi}"
                )
                continue

            # ------------------------------------------------
            # Buat row baru
            # ------------------------------------------------

            new_row = {}

            for column in xflask_columns:

                if column in template.index:
                    new_row[column] = template[column]
                else:
                    new_row[column] = None

            # ------------------------------------------------
            # Override nilai yang berasal dari PDF
            # ------------------------------------------------

            new_row["data_x"] = row["data_x"]
            new_row["data_y"] = row["data_y"]

            new_row["date"] = pd.to_datetime(
                row["data_x"],
                format="%d-%m-%Y"
            )

            new_row["id_nama_data"] = 32679

            new_row["nama"] = (
                "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi"
            )

            new_row["nama_data_import"] = (
                "s3://maganghub/"
                "kemenko/"
                "produksi_kelapa_sawit_bumn_provinsi/"
                "final/"
                "produksi_kelapa_sawit_bumn_provinsi.csv"
            )

            # ID row baru belum ditentukan
            new_row["id"] = None

            new_row["provinsi"] = provinsi
            new_row["leveldata"] = "provinsi"
            new_row["satuan"] = "Ton"
            new_row["frekuensi"] = "tahun"

            # ------------------------------------------------
            # Update metadata sumber
            # ------------------------------------------------

            if tahun == 2019:
                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan Nasional 2019-2021"
                )

            elif tahun in [2020, 2021, 2022]:
                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2020-2022"
                )

            elif tahun in [2023, 2024, 2025]:
                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan 2023-2025"
                )

            new_row["ket_tambahan"] = (
                "Data produksi kelapa sawit Perkebunan Besar Negara "
                "(PBN/BUMN) menurut provinsi berdasarkan publikasi "
                "Statistik Perkebunan Direktorat Jenderal Perkebunan."
            )

            final_rows.append(new_row)

        df_final = pd.DataFrame(
            final_rows,
            columns=xflask_columns
        )

        return df_final

    # --------------------------------------------------------
    # Normalize nama kabupaten/kota
    # --------------------------------------------------------

    def normalize_kabupaten_kota(self, value):

        if pd.isna(value):
            return None

        value = str(value).upper().strip()

        # Hilangkan titik
        value = re.sub(r"\.", "", value)

        # Rapikan spasi
        value = re.sub(r"\s+", " ", value)

        kabupaten_kota_mapping = {

            # ====================================================
            # ACEH
            # ====================================================

            "KAB SIMEULUE":
                "KABUPATEN SIMEULUE",

            "KAB ACEH SINGKIL":
                "KABUPATEN ACEH SINGKIL",

            "KAB ACEH SELATAN":
                "KABUPATEN ACEH SELATAN",

            "KAB ACEH TENGGARA":
                "KABUPATEN ACEH TENGGARA",

            "KAB ACEH TIMUR":
                "KABUPATEN ACEH TIMUR",

            "KAB ACEH TENGAH":
                "KABUPATEN ACEH TENGAH",

            "KAB ACEH BARAT":
                "KABUPATEN ACEH BARAT",

            "KAB ACEH BESAR":
                "KABUPATEN ACEH BESAR",

            "KAB PIDIE":
                "KABUPATEN PIDIE",

            "KAB BIREUN":
                "KABUPATEN BIREUEN",

            "KAB ACEH UTARA":
                "KABUPATEN ACEH UTARA",

            "KAB ACEH BARAT DAYA":
                "KABUPATEN ACEH BARAT DAYA",

            "KAB GAYO LUES":
                "KABUPATEN GAYO LUES",

            "KAB ACEH TAMIANG":
                "KABUPATEN ACEH TAMIANG",

            "KAB NAGAN RAYA":
                "KABUPATEN NAGAN RAYA",

            "KAB ACEH JAYA":
                "KABUPATEN ACEH JAYA",

            "KAB BENER MERIAH":
                "KABUPATEN BENER MERIAH",

            "KAB PIDIE JAYA":
                "KABUPATEN PIDIE JAYA",

            "KOTA BANDA ACEH":
                "KOTA BANDA ACEH",

            "KOTA SABANG":
                "KOTA SABANG",

            "KOTA LANGSA":
                "KOTA LANGSA",

            "KOTA LHOKSEUMAWE":
                "KOTA LHOKSEUMAWE",

            "KOTA SUBULUSSALAM":
                "KOTA SUBULUSSALAM",


            # ====================================================
            # SUMATERA UTARA
            # ====================================================

            "KAB NIAS":
                "KABUPATEN NIAS",

            "KAB MANDAILING NATAL":
                "KABUPATEN MANDAILING NATAL",

            "KAB TAPANULI SELATAN":
                "KABUPATEN TAPANULI SELATAN",

            "KAB TAPANULI TENGAH":
                "KABUPATEN TAPANULI TENGAH",

            "KAB TAPANULI UTARA":
                "KABUPATEN TAPANULI UTARA",

            "KAB TOBA SAMOSIR":
                "KABUPATEN TOBA",

            "KAB LABUHAN BATU":
                "KABUPATEN LABUHANBATU",

            "KAB ASAHAN":
                "KABUPATEN ASAHAN",

            "KAB SIMALUNGUN":
                "KABUPATEN SIMALUNGUN",

            "KAB DAIRI":
                "KABUPATEN DAIRI",

            "KAB KARO":
                "KABUPATEN KARO",

            "KAB DELI SERDANG":
                "KABUPATEN DELI SERDANG",

            "KAB LANGKAT":
                "KABUPATEN LANGKAT",

            "KAB NIAS SELATAN":
                "KABUPATEN NIAS SELATAN",

            "KAB HUMBANG HASUNDUTAN":
                "KABUPATEN HUMBANG HASUNDUTAN",

            "KAB PAKPAK BHARAT":
                "KABUPATEN PAKPAK BHARAT",

            "KAB SAMOSIR":
                "KABUPATEN SAMOSIR",

            "KAB SERDANG BEDAGAI":
                "KABUPATEN SERDANG BEDAGAI",

            "KAB BATU BARA":
                "KABUPATEN BATU BARA",

            "KAB PADANG LAWAS":
                "KABUPATEN PADANG LAWAS",

            "KAB PADANG LAWAS UTARA":
                "KABUPATEN PADANG LAWAS UTARA",

            "KAB LABUHANBATU SELATAN":
                "KABUPATEN LABUHANBATU SELATAN",

            "KAB LABUHANBATU UTARA":
                "KABUPATEN LABUHANBATU UTARA",

            "KAB NIAS UTARA":
                "KABUPATEN NIAS UTARA",

            "KAB NIAS BARAT":
                "KABUPATEN NIAS BARAT",

            "KOTA SIBOLGA":
                "KOTA SIBOLGA",

            "KOTA TANJUNG BALAI":
                "KOTA TANJUNG BALAI",

            "KOTA PEMATANG SIANTAR":
                "KOTA PEMATANG SIANTAR",

            "KOTA TEBING TINGGI":
                "KOTA TEBING TINGGI",

            "KOTA MEDAN":
                "KOTA MEDAN",

            "KOTA BINJAI":
                "KOTA BINJAI",

            "KOTA PADANGSIDIMPUAN":
                "KOTA PADANGSIDIMPUAN",


            # ====================================================
            # SUMATERA BARAT
            # ====================================================

            "KAB KEPULAUAN MENTAWAI":
                "KABUPATEN KEPULAUAN MENTAWAI",

            "KAB PESISIR SELATAN":
                "KABUPATEN PESISIR SELATAN",

            "KAB SOLOK":
                "KABUPATEN SOLOK",

            "KAB SIJUNJUNG":
                "KABUPATEN SIJUNJUNG",

            "KAB TANAH DATAR":
                "KABUPATEN TANAH DATAR",

            "KAB PADANG PARIAMAN":
                "KABUPATEN PADANG PARIAMAN",

            "KAB AGAM":
                "KABUPATEN AGAM",

            "KAB LIMA PULUH KOTA":
                "KABUPATEN LIMA PULUH KOTA",

            "KAB PASAMAN":
                "KABUPATEN PASAMAN",

            "KAB SOLOK SELATAN":
                "KABUPATEN SOLOK SELATAN",

            "KAB DHARMASRAYA":
                "KABUPATEN DHARMASRAYA",

            "KAB PASAMAN BARAT":
                "KABUPATEN PASAMAN BARAT",

            "KOTA PADANG":
                "KOTA PADANG",

            "KOTA SOLOK":
                "KOTA SOLOK",

            "KOTA SAWAHLUNTO":
                "KOTA SAWAHLUNTO",

            "KOTA PADANG PANJANG":
                "KOTA PADANG PANJANG",

            "KOTA BUKITTINGGI":
                "KOTA BUKITTINGGI",

            "KOTA PAYAKUMBUH":
                "KOTA PAYAKUMBUH",

            "KOTA PARIAMAN":
                "KOTA PARIAMAN",


            # ====================================================
            # RIAU
            # ====================================================

            "KAB KUANTAN SINGINGI":
                "KABUPATEN KUANTAN SINGINGI",

            "KAB INDRAGIRI HULU":
                "KABUPATEN INDRAGIRI HULU",

            "KAB INDRAGIRI HILIR":
                "KABUPATEN INDRAGIRI HILIR",

            "KAB PELALAWAN":
                "KABUPATEN PELALAWAN",

            "KAB SIAK":
                "KABUPATEN SIAK",

            "KAB KAMPAR":
                "KABUPATEN KAMPAR",

            "KAB ROKAN HULU":
                "KABUPATEN ROKAN HULU",

            "KAB BENGKALIS":
                "KABUPATEN BENGKALIS",

            "KAB ROKAN HILIR":
                "KABUPATEN ROKAN HILIR",

            "KAB KEPULAUAN MERANTI":
                "KABUPATEN KEPULAUAN MERANTI",

            "KOTA PEKANBARU":
                "KOTA PEKANBARU",

            "KOTA DUMAI":
                "KOTA DUMAI",


            # ====================================================
            # JAMBI
            # ====================================================

            "KAB KERINCI":
                "KABUPATEN KERINCI",

            "KAB MERANGIN":
                "KABUPATEN MERANGIN",

            "KAB SAROLANGUN":
                "KABUPATEN SAROLANGUN",

            "KAB BATANG HARI":
                "KABUPATEN BATANG HARI",

            "KAB MUARO JAMBI":
                "KABUPATEN MUARO JAMBI",

            "KAB TANJUNG JABUNG BARAT":
                "KABUPATEN TANJUNG JABUNG BARAT",

            "KAB TANJUNG JABUNG TIMUR":
                "KABUPATEN TANJUNG JABUNG TIMUR",

            "KAB BUNGO":
                "KABUPATEN BUNGO",

            "KAB TEBO":
                "KABUPATEN TEBO",

            "KOTA JAMBI":
                "KOTA JAMBI",

            "KOTA SUNGAI PENUH":
                "KOTA SUNGAI PENUH",


            # ====================================================
            # SUMATERA SELATAN
            # ====================================================

            "KAB OGAN KOMERING ULU":
                "KABUPATEN OGAN KOMERING ULU",

            "KAB OGAN KOMERING ILIR":
                "KABUPATEN OGAN KOMERING ILIR",

            "KAB MUARA ENIM":
                "KABUPATEN MUARA ENIM",

            "KAB LAHAT":
                "KABUPATEN LAHAT",

            "KAB MUSI RAWAS":
                "KABUPATEN MUSI RAWAS",

            "KAB MUSI BANYUASIN":
                "KABUPATEN MUSI BANYUASIN",

            "KAB BANYUASIN":
                "KABUPATEN BANYUASIN",

            "KAB OGAN KOMERING ULU SELATAN":
                "KABUPATEN OGAN KOMERING ULU SELATAN",

            "KAB OGAN ILIR":
                "KABUPATEN OGAN ILIR",

            "KAB EMPAT LAWANG":
                "KABUPATEN EMPAT LAWANG",

            "KAB PENUKAL ABAB LEMATANG ILIR":
                "KABUPATEN PENUKAL ABAB LEMATANG ILIR",

            "KAB MUSI RAWAS UTARA":
                "KABUPATEN MUSI RAWAS UTARA",

            "KOTA PALEMBANG":
                "KOTA PALEMBANG",

            "KOTA PAGAR ALAM":
                "KOTA PAGAR ALAM",

            "KOTA LUBUKLINGGAU":
                "KOTA LUBUKLINGGAU",

            "KOTA PRABUMULIH":
                "KOTA PRABUMULIH",


            # ====================================================
            # BENGKULU
            # ====================================================

            "KAB BENGKULU SELATAN":
                "KABUPATEN BENGKULU SELATAN",

            "KAB REJANG LEBONG":
                "KABUPATEN REJANG LEBONG",

            "KAB BENGKULU UTARA":
                "KABUPATEN BENGKULU UTARA",

            "KAB KAUR":
                "KABUPATEN KAUR",

            "KAB SELUMA":
                "KABUPATEN SELUMA",

            "KAB MUKOMUKO":
                "KABUPATEN MUKOMUKO",

            "KAB LEBONG":
                "KABUPATEN LEBONG",

            "KAB KEPAHIANG":
                "KABUPATEN KEPAHIANG",

            "KAB BENGKULU TENGAH":
                "KABUPATEN BENGKULU TENGAH",

            "KOTA BENGKULU":
                "KOTA BENGKULU",


            # ====================================================
            # LAMPUNG
            # ====================================================

            "KAB LAMPUNG BARAT":
                "KABUPATEN LAMPUNG BARAT",

            "KAB TANGGAMUS":
                "KABUPATEN TANGGAMUS",

            "KAB LAMPUNG SELATAN":
                "KABUPATEN LAMPUNG SELATAN",

            "KAB LAMPUNG TIMUR":
                "KABUPATEN LAMPUNG TIMUR",

            "KAB LAMPUNG TENGAH":
                "KABUPATEN LAMPUNG TENGAH",

            "KAB LAMPUNG UTARA":
                "KABUPATEN LAMPUNG UTARA",

            "KAB WAY KANAN":
                "KABUPATEN WAY KANAN",

            "KAB TULANG BAWANG":
                "KABUPATEN TULANG BAWANG",

            "KAB PESAWARAN":
                "KABUPATEN PESAWARAN",

            "KAB PRINGSEWU":
                "KABUPATEN PRINGSEWU",

            "KAB MESUJI":
                "KABUPATEN MESUJI",

            "KAB TULANG BAWANG BARAT":
                "KABUPATEN TULANG BAWANG BARAT",

            "KAB PESISIR BARAT":
                "KABUPATEN PESISIR BARAT",

            "KOTA BANDAR LAMPUNG":
                "KOTA BANDAR LAMPUNG",

            "KOTA METRO":
                "KOTA METRO",


            # ====================================================
            # KEPULAUAN BANGKA BELITUNG
            # ====================================================

            "KAB BANGKA":
                "KABUPATEN BANGKA",

            "KAB BELITUNG":
                "KABUPATEN BELITUNG",

            "KAB BANGKA BARAT":
                "KABUPATEN BANGKA BARAT",

            "KAB BANGKA TENGAH":
                "KABUPATEN BANGKA TENGAH",

            "KAB BANGKA SELATAN":
                "KABUPATEN BANGKA SELATAN",

            "KAB BELITUNG TIMUR":
                "KABUPATEN BELITUNG TIMUR",

            "KOTA PANGKALPINANG":
                "KOTA PANGKALPINANG",


            # ====================================================
            # KEPULAUAN RIAU
            # ====================================================

            "KAB BINTAN":
                "KABUPATEN BINTAN",

            "KAB KARIMUN":
                "KABUPATEN KARIMUN",

            "KAB NATUNA":
                "KABUPATEN NATUNA",

            "KAB LINGGA":
                "KABUPATEN LINGGA",

            "KAB KEPULAUAN ANAMBAS":
                "KABUPATEN KEPULAUAN ANAMBAS",

            "KOTA BATAM":
                "KOTA BATAM",

            "KOTA TANJUNGPINANG":
                "KOTA TANJUNGPINANG",


            # ====================================================
            # JAWA BARAT
            # ====================================================

            "KAB BOGOR":
                "KABUPATEN BOGOR",

            "KAB SUKABUMI":
                "KABUPATEN SUKABUMI",

            "KAB CIANJUR":
                "KABUPATEN CIANJUR",

            "KAB BANDUNG":
                "KABUPATEN BANDUNG",

            "KAB GARUT":
                "KABUPATEN GARUT",

            "KAB TASIKMALAYA":
                "KABUPATEN TASIKMALAYA",

            "KAB CIAMIS":
                "KABUPATEN CIAMIS",

            "KAB KUNINGAN":
                "KABUPATEN KUNINGAN",

            "KAB CIREBON":
                "KABUPATEN CIREBON",

            "KAB MAJALENGKA":
                "KABUPATEN MAJALENGKA",

            "KAB SUMEDANG":
                "KABUPATEN SUMEDANG",

            "KAB INDRAMAYU":
                "KABUPATEN INDRAMAYU",

            "KAB SUBANG":
                "KABUPATEN SUBANG",

            "KAB PURWAKARTA":
                "KABUPATEN PURWAKARTA",

            "KAB KARAWANG":
                "KABUPATEN KARAWANG",

            "KAB BEKASI":
                "KABUPATEN BEKASI",

            "KAB BANDUNG BARAT":
                "KABUPATEN BANDUNG BARAT",

            "KAB PANGANDARAN":
                "KABUPATEN PANGANDARAN",

            "KOTA BOGOR":
                "KOTA BOGOR",

            "KOTA SUKABUMI":
                "KOTA SUKABUMI",

            "KOTA BANDUNG":
                "KOTA BANDUNG",

            "KOTA CIREBON":
                "KOTA CIREBON",

            "KOTA BEKASI":
                "KOTA BEKASI",

            "KOTA DEPOK":
                "KOTA DEPOK",

            "KOTA CIMAHI":
                "KOTA CIMAHI",

            "KOTA TASIKMALAYA":
                "KOTA TASIKMALAYA",

            "KOTA BANJAR":
                "KOTA BANJAR",


            # ====================================================
            # BANTEN
            # ====================================================

            "KAB PANDEGLANG":
                "KABUPATEN PANDEGLANG",

            "KAB LEBAK":
                "KABUPATEN LEBAK",

            "KAB TANGERANG":
                "KABUPATEN TANGERANG",

            "KAB SERANG":
                "KABUPATEN SERANG",

            "KOTA TANGERANG":
                "KOTA TANGERANG",

            "KOTA CILEGON":
                "KOTA CILEGON",

            "KOTA SERANG":
                "KOTA SERANG",

            "KOTA TANGERANG SELATAN":
                "KOTA TANGERANG SELATAN",


            # ====================================================
            # JAWA TENGAH
            # ====================================================

            "KAB CILACAP":
                "KABUPATEN CILACAP",

            "KAB BANYUMAS":
                "KABUPATEN BANYUMAS",

            "KAB PURBALINGGA":
                "KABUPATEN PURBALINGGA",

            "KAB BANJARNEGARA":
                "KABUPATEN BANJARNEGARA",

            "KAB KEBUMEN":
                "KABUPATEN KEBUMEN",

            "KAB PURWOREJO":
                "KABUPATEN PURWOREJO",

            "KAB WONOSOBO":
                "KABUPATEN WONOSOBO",

            "KAB MAGELANG":
                "KABUPATEN MAGELANG",

            "KAB BOYOLALI":
                "KABUPATEN BOYOLALI",

            "KAB KLATEN":
                "KABUPATEN KLATEN",

            "KAB SUKOHARJO":
                "KABUPATEN SUKOHARJO",

            "KAB WONOGIRI":
                "KABUPATEN WONOGIRI",

            "KAB KARANGANYAR":
                "KABUPATEN KARANGANYAR",

            "KAB SRAGEN":
                "KABUPATEN SRAGEN",

            "KAB GROBOGAN":
                "KABUPATEN GROBOGAN",

            "KAB BLORA":
                "KABUPATEN BLORA",

            "KAB REMBANG":
                "KABUPATEN REMBANG",

            "KAB PATI":
                "KABUPATEN PATI",

            "KAB KUDUS":
                "KABUPATEN KUDUS",

            "KAB JEPARA":
                "KABUPATEN JEPARA",

            "KAB DEMAK":
                "KABUPATEN DEMAK",

            "KAB SEMARANG":
                "KABUPATEN SEMARANG",

            "KAB TEMANGGUNG":
                "KABUPATEN TEMANGGUNG",

            "KAB KENDAL":
                "KABUPATEN KENDAL",

            "KAB BATANG":
                "KABUPATEN BATANG",

            "KAB PEKALONGAN":
                "KABUPATEN PEKALONGAN",

            "KAB PEMALANG":
                "KABUPATEN PEMALANG",

            "KAB TEGAL":
                "KABUPATEN TEGAL",

            "KAB BREBES":
                "KABUPATEN BREBES",

            "KOTA MAGELANG":
                "KOTA MAGELANG",

            "KOTA SURAKARTA":
                "KOTA SURAKARTA",

            "KOTA SALATIGA":
                "KOTA SALATIGA",

            "KOTA SEMARANG":
                "KOTA SEMARANG",

            "KOTA PEKALONGAN":
                "KOTA PEKALONGAN",

            "KOTA TEGAL":
                "KOTA TEGAL",


            # ====================================================
            # JAWA TIMUR
            # ====================================================

            "KAB PACITAN":
                "KABUPATEN PACITAN",

            "KAB PONOROGO":
                "KABUPATEN PONOROGO",

            "KAB TRENGGALEK":
                "KABUPATEN TRENGGALEK",

            "KAB TULUNGAGUNG":
                "KABUPATEN TULUNGAGUNG",

            "KAB BLITAR":
                "KABUPATEN BLITAR",

            "KAB KEDIRI":
                "KABUPATEN KEDIRI",

            "KAB MALANG":
                "KABUPATEN MALANG",

            "KAB LUMAJANG":
                "KABUPATEN LUMAJANG",

            "KAB JEMBER":
                "KABUPATEN JEMBER",

            "KAB BANYUWANGI":
                "KABUPATEN BANYUWANGI",

            "KAB BONDOWOSO":
                "KABUPATEN BONDOWOSO",

            "KAB SITUBONDO":
                "KABUPATEN SITUBONDO",

            "KAB PROBOLINGGO":
                "KABUPATEN PROBOLINGGO",

            "KAB PASURUAN":
                "KABUPATEN PASURUAN",

            "KAB SIDOARJO":
                "KABUPATEN SIDOARJO",

            "KAB MOJOKERTO":
                "KABUPATEN MOJOKERTO",

            "KAB JOMBANG":
                "KABUPATEN JOMBANG",

            "KAB NGANJUK":
                "KABUPATEN NGANJUK",

            "KAB MADIUN":
                "KABUPATEN MADIUN",

            "KAB MAGETAN":
                "KABUPATEN MAGETAN",

            "KAB NGAWI":
                "KABUPATEN NGAWI",

            "KAB BOJONEGORO":
                "KABUPATEN BOJONEGORO",

            "KAB TUBAN":
                "KABUPATEN TUBAN",

            "KAB LAMONGAN":
                "KABUPATEN LAMONGAN",

            "KAB GRESIK":
                "KABUPATEN GRESIK",

            "KAB BANGKALAN":
                "KABUPATEN BANGKALAN",

            "KAB SAMPANG":
                "KABUPATEN SAMPANG",

            "KAB PAMEKASAN":
                "KABUPATEN PAMEKASAN",

            "KAB SUMENEP":
                "KABUPATEN SUMENEP",

            "KOTA KEDIRI":
                "KOTA KEDIRI",

            "KOTA BLITAR":
                "KOTA BLITAR",

            "KOTA MALANG":
                "KOTA MALANG",

            "KOTA PROBOLINGGO":
                "KOTA PROBOLINGGO",

            "KOTA PASURUAN":
                "KOTA PASURUAN",

            "KOTA MOJOKERTO":
                "KOTA MOJOKERTO",

            "KOTA MADIUN":
                "KOTA MADIUN",

            "KOTA SURABAYA":
                "KOTA SURABAYA",

            "KOTA BATU":
                "KOTA BATU",


            # ====================================================
            # BALI
            # ====================================================

            "KAB JEMBRANA":
                "KABUPATEN JEMBRANA",

            "KAB TABANAN":
                "KABUPATEN TABANAN",

            "KAB BADUNG":
                "KABUPATEN BADUNG",

            "KAB GIANYAR":
                "KABUPATEN GIANYAR",

            "KAB KLUNGKUNG":
                "KABUPATEN KLUNGKUNG",

            "KAB BANGLI":
                "KABUPATEN BANGLI",

            "KAB KARANGASEM":
                "KABUPATEN KARANGASEM",

            "KAB BULELENG":
                "KABUPATEN BULELENG",

            "KOTA DENPASAR":
                "KOTA DENPASAR",


            # ====================================================
            # NUSA TENGGARA BARAT
            # ====================================================

            "KAB LOMBOK BARAT":
                "KABUPATEN LOMBOK BARAT",

            "KAB LOMBOK TENGAH":
                "KABUPATEN LOMBOK TENGAH",

            "KAB LOMBOK TIMUR":
                "KABUPATEN LOMBOK TIMUR",

            "KAB SUMBAWA":
                "KABUPATEN SUMBAWA",

            "KAB DOMPU":
                "KABUPATEN DOMPU",

            "KAB BIMA":
                "KABUPATEN BIMA",

            "KAB SUMBAWA BARAT":
                "KABUPATEN SUMBAWA BARAT",

            "KAB LOMBOK UTARA":
                "KABUPATEN LOMBOK UTARA",

            "KOTA MATARAM":
                "KOTA MATARAM",

            "KOTA BIMA":
                "KOTA BIMA",


            # ====================================================
            # NUSA TENGGARA TIMUR
            # ====================================================

            "KAB SUMBA BARAT":
                "KABUPATEN SUMBA BARAT",

            "KAB SUMBA TIMUR":
                "KABUPATEN SUMBA TIMUR",

            "KAB KUPANG":
                "KABUPATEN KUPANG",

            "KAB TIMOR TENGAH SELATAN":
                "KABUPATEN TIMOR TENGAH SELATAN",

            "KAB TIMOR TENGAH UTARA":
                "KABUPATEN TIMOR TENGAH UTARA",

            "KAB BELU":
                "KABUPATEN BELU",

            "KAB ALOR":
                "KABUPATEN ALOR",

            "KAB LEMBATA":
                "KABUPATEN LEMBATA",

            "KAB FLORES TIMUR":
                "KABUPATEN FLORES TIMUR",

            "KAB SIKKA":
                "KABUPATEN SIKKA",

            "KAB ENDE":
                "KABUPATEN ENDE",

            "KAB NGADA":
                "KABUPATEN NGADA",

            "KAB MANGGARAI":
                "KABUPATEN MANGGARAI",

            "KAB ROTE NDAO":
                "KABUPATEN ROTE NDAO",

            "KAB MANGGARAI BARAT":
                "KABUPATEN MANGGARAI BARAT",

            "KAB SUMBA TENGAH":
                "KABUPATEN SUMBA TENGAH",

            "KAB SUMBA BARAT DAYA":
                "KABUPATEN SUMBA BARAT DAYA",

            "KAB NAGEKEO":
                "KABUPATEN NAGEKEO",

            "KAB MANGGARAI TIMUR":
                "KABUPATEN MANGGARAI TIMUR",

            "KAB SABU RAIJUA":
                "KABUPATEN SABU RAIJUA",

            "KAB MALAKA":
                "KABUPATEN MALAKA",

            "KOTA KUPANG":
                "KOTA KUPANG",


            # ====================================================
            # KALIMANTAN BARAT
            # ====================================================

            "KAB SAMBAS":
                "KABUPATEN SAMBAS",

            "KAB BENGKAYANG":
                "KABUPATEN BENGKAYANG",

            "KAB LANDAK":
                "KABUPATEN LANDAK",

            "KAB MEMPAWAH":
                "KABUPATEN MEMPAWAH",

            "KAB SANGGAU":
                "KABUPATEN SANGGAU",

            "KAB KETAPANG":
                "KABUPATEN KETAPANG",

            "KAB SINTANG":
                "KABUPATEN SINTANG",

            "KAB KAPUAS HULU":
                "KABUPATEN KAPUAS HULU",

            "KAB SEKADAU":
                "KABUPATEN SEKADAU",

            "KAB MELAWI":
                "KABUPATEN MELAWI",

            "KAB KAYONG UTARA":
                "KABUPATEN KAYONG UTARA",

            "KAB KUBU RAYA":
                "KABUPATEN KUBU RAYA",

            "KOTA PONTIANAK":
                "KOTA PONTIANAK",

            "KOTA SINGKAWANG":
                "KOTA SINGKAWANG",


            # ====================================================
            # KALIMANTAN TENGAH
            # ====================================================

            "KAB KOTAWARINGIN BARAT":
                "KABUPATEN KOTA WARINGIN BARAT",

            "KAB KOTAWARINGIN TIMUR":
                "KABUPATEN KOTA WARINGIN TIMUR",

            "KAB KAPUAS":
                "KABUPATEN KAPUAS",

            "KAB BARITO SELATAN":
                "KABUPATEN BARITO SELATAN",

            "KAB BARITO UTARA":
                "KABUPATEN BARITO UTARA",

            "KAB SUKAMARA":
                "KABUPATEN SUKAMARA",

            "KAB LAMANDAU":
                "KABUPATEN LAMANDAU",

            "KAB SERUYAN":
                "KABUPATEN SERUYAN",

            "KAB KATINGAN":
                "KABUPATEN KATINGAN",

            "KAB PULANG PISAU":
                "KABUPATEN PULANG PISAU",

            "KAB GUNUNG MAS":
                "KABUPATEN GUNUNG MAS",

            "KAB BARITO TIMUR":
                "KABUPATEN BARITO TIMUR",

            "KAB MURUNG RAYA":
                "KABUPATEN MURUNG RAYA",

            "KOTA PALANGKA RAYA":
                "KOTA PALANGKA RAYA",


            # ====================================================
            # KALIMANTAN SELATAN
            # ====================================================

            "KAB TANAH LAUT":
                "KABUPATEN TANAH LAUT",

            "KAB KOTABARU":
                "KABUPATEN KOTABARU",

            "KAB BANJAR":
                "KABUPATEN BANJAR",

            "KAB BARITO KUALA":
                "KABUPATEN BARITO KUALA",

            "KAB TAPIN":
                "KABUPATEN TAPIN",

            "KAB HULU SUNGAI SELATAN":
                "KABUPATEN HULU SUNGAI SELATAN",

            "KAB HULU SUNGAI TENGAH":
                "KABUPATEN HULU SUNGAI TENGAH",

            "KAB HULU SUNGAI UTARA":
                "KABUPATEN HULU SUNGAI UTARA",

            "KAB TABALONG":
                "KABUPATEN TABALONG",

            "KAB TANAH BUMBU":
                "KABUPATEN TANAH BUMBU",

            "KAB BALANGAN":
                "KABUPATEN BALANGAN",

            "KOTA BANJARMASIN":
                "KOTA BANJARMASIN",

            "KOTA BANJARBARU":
                "KOTA BANJARBARU",


            # ====================================================
            # KALIMANTAN TIMUR
            # ====================================================

            "KAB PASER":
                "KABUPATEN PASER",

            "KAB KUTAI BARAT":
                "KABUPATEN KUTAI BARAT",

            "KAB KUTAI KARTANEGARA":
                "KABUPATEN KUTAI KARTANEGARA",

            "KAB KUTAI TIMUR":
                "KABUPATEN KUTAI TIMUR",

            "KAB BERAU":
                "KABUPATEN BERAU",

            "KAB PENAJAM PASER UTARA":
                "KABUPATEN PENAJAM PASER UTARA",

            "KAB MAHAKAM ULU":
                "KABUPATEN MAHAKAM ULU",

            "KOTA BALIKPAPAN":
                "KOTA BALIKPAPAN",

            "KOTA SAMARINDA":
                "KOTA SAMARINDA",

            "KOTA BONTANG":
                "KOTA BONTANG",


            # ====================================================
            # KALIMANTAN UTARA
            # ====================================================

            "KAB BULUNGAN":
                "KABUPATEN BULUNGAN",

            "KAB MALINAU":
                "KABUPATEN MALINAU",

            "KAB NUNUKAN":
                "KABUPATEN NUNUKAN",

            "KAB TANA TIDUNG":
                "KABUPATEN TANA TIDUNG",

            "KOTA TARAKAN":
                "KOTA TARAKAN",


            # ====================================================
            # SULAWESI UTARA
            # ====================================================

            "KAB BOLAANG MONGONDOW":
                "KABUPATEN BOLAANG MONGONDOW",

            "KAB MINAHASA":
                "KABUPATEN MINAHASA",

            "KAB KEPULAUAN SANGIHE":
                "KABUPATEN KEPULAUAN SANGIHE",

            "KAB KEPULAUAN TALAUD":
                "KABUPATEN KEPULAUAN TALAUD",

            "KAB MINAHASA SELATAN":
                "KABUPATEN MINAHASA SELATAN",

            "KAB MINAHASA UTARA":
                "KABUPATEN MINAHASA UTARA",

            "KAB BOLAANG MONGONDOW UTARA":
                "KABUPATEN BOLAANG MONGONDOW UTARA",

            "KAB SIAU TAGULANDANG BIARO":
                "KABUPATEN SIAU TAGULANDANG BIARO",

            "KAB MINAHASA TENGGARA":
                "KABUPATEN MINAHASA TENGGARA",

            "KAB BOLAANG MONGONDOW SELATAN":
                "KABUPATEN BOLAANG MONGONDOW SELATAN",

            "KAB BOLAANG MONGONDOW TIMUR":
                "KABUPATEN BOLAANG MONGONDOW TIMUR",

            "KOTA MANADO":
                "KOTA MANADO",

            "KOTA BITUNG":
                "KOTA BITUNG",

            "KOTA TOMOHON":
                "KOTA TOMOHON",

            "KOTA KOTAMOBAGU":
                "KOTA KOTAMOBAGU",


            # ====================================================
            # SULAWESI TENGAH
            # ====================================================

            "KAB BANGGAI":
                "KABUPATEN BANGGAI",

            "KAB POSO":
                "KABUPATEN POSO",

            "KAB DONGGALA":
                "KABUPATEN DONGGALA",

            "KAB TOLI TOLI":
                "KABUPATEN TOLI-TOLI",

            "KAB BUOL":
                "KABUPATEN BUOL",

            "KAB MOROWALI":
                "KABUPATEN MOROWALI",

            "KAB BANGGAI KEPULAUAN":
                "KABUPATEN BANGGAI KEPULAUAN",

            "KAB PARIGI MOUTONG":
                "KABUPATEN PARIGI MOUTONG",

            "KAB TOJO UNA UNA":
                "KABUPATEN TOJO UNA-UNA",

            "KAB SIGI":
                "KABUPATEN SIGI",

            "KAB BANGGAI LAUT":
                "KABUPATEN BANGGAI LAUT",

            "KAB MOROWALI UTARA":
                "KABUPATEN MOROWALI UTARA",

            "KOTA PALU":
                "KOTA PALU",


            # ====================================================
            # SULAWESI SELATAN
            # ====================================================

            "KAB KEPULAUAN SELAYAR":
                "KABUPATEN KEPULAUAN SELAYAR",

            "KAB BULUKUMBA":
                "KABUPATEN BULUKUMBA",

            "KAB BANTAENG":
                "KABUPATEN BANTAENG",

            "KAB JENEPONTO":
                "KABUPATEN JENEPONTO",

            "KAB TAKALAR":
                "KABUPATEN TAKALAR",

            "KAB GOWA":
                "KABUPATEN GOWA",

            "KAB SINJAI":
                "KABUPATEN SINJAI",

            "KAB MAROS":
                "KABUPATEN MAROS",

            "KAB PANGKAJENE DAN KEPULAUAN":
                "KABUPATEN PANGKAJENE DAN KEPULAUAN",

            "KAB BARRU":
                "KABUPATEN BARRU",

            "KAB BONE":
                "KABUPATEN BONE",

            "KAB SOPPENG":
                "KABUPATEN SOPPENG",

            "KAB WAJO":
                "KABUPATEN WAJO",

            "KAB SIDENRENG RAPPANG":
                "KABUPATEN SIDENRENG RAPPANG",

            "KAB PINRANG":
                "KABUPATEN PINRANG",

            "KAB ENREKANG":
                "KABUPATEN ENREKANG",

            "KAB LUWU":
                "KABUPATEN LUWU",

            "KAB TANA TORAJA":
                "KABUPATEN TANA TORAJA",

            "KAB LUWU UTARA":
                "KABUPATEN LUWU UTARA",

            "KAB LUWU TIMUR":
                "KABUPATEN LUWU TIMUR",

            "KAB TORAJA UTARA":
                "KABUPATEN TORAJA UTARA",

            "KOTA MAKASSAR":
                "KOTA MAKASSAR",

            "KOTA PAREPARE":
                "KOTA PAREPARE",

            "KOTA PALOPO":
                "KOTA PALOPO",


            # ====================================================
            # SULAWESI TENGGARA
            # ====================================================

            "KAB BUTON":
                "KABUPATEN BUTON",

            "KAB MUNA":
                "KABUPATEN MUNA",

            "KAB KONAWE":
                "KABUPATEN KONAWE",

            "KAB KOLAKA":
                "KABUPATEN KOLAKA",

            "KAB KONAWE SELATAN":
                "KABUPATEN KONAWE SELATAN",

            "KAB BOMBANA":
                "KABUPATEN BOMBANA",

            "KAB WAKATOBI":
                "KABUPATEN WAKATOBI",

            "KAB KOLAKA UTARA":
                "KABUPATEN KOLAKA UTARA",

            "KAB BUTON UTARA":
                "KABUPATEN BUTON UTARA",

            "KAB KONAWE UTARA":
                "KABUPATEN KONAWE UTARA",

            "KAB KOLAKA TIMUR":
                "KABUPATEN KOLAKA TIMUR",

            "KAB KONAWE KEPULAUAN":
                "KABUPATEN KONAWE KEPULAUAN",

            "KAB MUNA BARAT":
                "KABUPATEN MUNA BARAT",

            "KAB BUTON TENGAH":
                "KABUPATEN BUTON TENGAH",

            "KAB BUTON SELATAN":
                "KABUPATEN BUTON SELATAN",

            "KOTA KENDARI":
                "KOTA KENDARI",

            "KOTA BAUBAU":
                "KOTA BAUBAU",


            # ====================================================
            # GORONTALO
            # ====================================================

            "KAB BOALEMO":
                "KABUPATEN BOALEMO",

            "KAB GORONTALO":
                "KABUPATEN GORONTALO",

            "KAB POHUWATO":
                "KABUPATEN POHUWATO",

            "KAB BONE BOLANGO":
                "KABUPATEN BONE BOLANGO",

            "KAB GORONTALO UTARA":
                "KABUPATEN GORONTALO UTARA",

            "KOTA GORONTALO":
                "KOTA GORONTALO",


            # ====================================================
            # SULAWESI BARAT
            # ====================================================

            "KAB MAJENE":
                "KABUPATEN MAJENE",

            "KAB POLEWALI MANDAR":
                "KABUPATEN POLEWALI MANDAR",

            "KAB MAMASA":
                "KABUPATEN MAMASA",

            "KAB MAMUJU":
                "KABUPATEN MAMUJU",

            "KAB PASANGKAYU":
                "KABUPATEN PASANGKAYU",

            "KAB MAMUJU TENGAH":
                "KABUPATEN MAMUJU TENGAH",


            # ====================================================
            # MALUKU
            # ====================================================

            "KAB MALUKU TENGAH":
                "KABUPATEN MALUKU TENGAH",

            "KAB MALUKU TENGGARA":
                "KABUPATEN MALUKU TENGGARA",

            "KAB KEPULAUAN TANIMBAR":
                "KABUPATEN KEPULAUAN TANIMBAR",

            "KAB BURU":
                "KABUPATEN BURU",

            "KAB SERAM BAGIAN BARAT":
                "KABUPATEN SERAM BAGIAN BARAT",

            "KAB SERAM BAGIAN TIMUR":
                "KABUPATEN SERAM BAGIAN TIMUR",

            "KAB ARU":
                "KABUPATEN KEPULAUAN ARU",

            "KAB MALUKU BARAT DAYA":
                "KABUPATEN MALUKU BARAT DAYA",

            "KAB BURU SELATAN":
                "KABUPATEN BURU SELATAN",

            "KOTA AMBON":
                "KOTA AMBON",

            "KOTA TUAL":
                "KOTA TUAL",


            # ====================================================
            # MALUKU UTARA
            # ====================================================

            "KAB HALMAHERA BARAT":
                "KABUPATEN HALMAHERA BARAT",

            "KAB HALMAHERA TENGAH":
                "KABUPATEN HALMAHERA TENGAH",

            "KAB KEPULAUAN SULA":
                "KABUPATEN KEPULAUAN SULA",

            "KAB HALMAHERA SELATAN":
                "KABUPATEN HALMAHERA SELATAN",

            "KAB HALMAHERA UTARA":
                "KABUPATEN HALMAHERA UTARA",

            "KAB HALMAHERA TIMUR":
                "KABUPATEN HALMAHERA TIMUR",

            "KAB PULAU MOROTAI":
                "KABUPATEN PULAU MOROTAI",

            "KAB PULAU TALIABU":
                "KABUPATEN PULAU TALIABU",

            "KOTA TERNATE":
                "KOTA TERNATE",

            "KOTA TIDORE KEPULAUAN":
                "KOTA TIDORE KEPULAUAN",


            # ====================================================
            # PAPUA BARAT
            # ====================================================

            "KAB FAKFAK":
                "KABUPATEN FAKFAK",

            "KAB KAIMANA":
                "KABUPATEN KAIMANA",

            "KAB TELUK WONDAMA":
                "KABUPATEN TELUK WONDAMA",

            "KAB TELUK BINTUNI":
                "KABUPATEN TELUK BINTUNI",

            "KAB MANOKWARI":
                "KABUPATEN MANOKWARI",

            "KAB SORONG SELATAN":
                "KABUPATEN SORONG SELATAN",

            "KAB SORONG":
                "KABUPATEN SORONG",

            "KAB RAJA AMPAT":
                "KABUPATEN RAJA AMPAT",

            "KAB TAMRAUW":
                "KABUPATEN TAMRAUW",

            "KAB MAYBRAT":
                "KABUPATEN MAYBRAT",

            "KAB MANOKWARI SELATAN":
                "KABUPATEN MANOKWARI SELATAN",

            "KAB PEGUNUNGAN ARFAK":
                "KABUPATEN PEGUNUNGAN ARFAK",

            "KOTA SORONG":
                "KOTA SORONG",


            # ====================================================
            # PAPUA
            # ====================================================

            "KAB JAYAPURA":
                "KABUPATEN JAYAPURA",

            "KAB KEPULAUAN YAPEN":
                "KABUPATEN KEPULAUAN YAPEN",

            "KAB BIAK NUMFOR":
                "KABUPATEN BIAK NUMFOR",

            "KAB PANIAI":
                "KABUPATEN PANIAI",

            "KAB PUNCAK JAYA":
                "KABUPATEN PUNCAK JAYA",

            "KAB NABIRE":
                "KABUPATEN NABIRE",

            "KAB MIMIKA":
                "KABUPATEN MIMIKA",

            "KAB BOVEN DIGOEL":
                "KABUPATEN BOVEN DIGOEL",

            "KAB MERAUKE":
                "KABUPATEN MERAUKE",

            "KAB ASMAT":
                "KABUPATEN ASMAT",

            "KAB YAHUKIMO":
                "KABUPATEN YAHUKIMO",

            "KAB PEGUNUNGAN BINTANG":
                "KABUPATEN PEGUNUNGAN BINTANG",

            "KAB TOLIKARA":
                "KABUPATEN TOLIKARA",

            "KAB WAROPEN":
                "KABUPATEN WAROPEN",

            "KAB SUPIORI":
                "KABUPATEN SUPIORI",

            "KAB MAMBERAMO RAYA":
                "KABUPATEN MAMBERAMO RAYA",

            "KAB NDUGA":
                "KABUPATEN NDUGA",

            "KAB LANNY JAYA":
                "KABUPATEN LANNY JAYA",

            "KAB MAMBERAMO TENGAH":
                "KABUPATEN MAMBERAMO TENGAH",

            "KAB YALIMO":
                "KABUPATEN YALIMO",

            "KAB DOGIYAI":
                "KABUPATEN DOGIYAI",

            "KAB DEIYAI":
                "KABUPATEN DEIYAI",

            "KAB INTAN JAYA":
                "KABUPATEN INTAN JAYA",

            "KOTA JAYAPURA":
                "KOTA JAYAPURA",


            # ====================================================
            # PAPUA SELATAN
            # ====================================================

            "KAB MERAUKE":
                "KABUPATEN MERAUKE",

            "KAB BOVEN DIGOEL":
                "KABUPATEN BOVEN DIGOEL",

            "KAB MAPPI":
                "KABUPATEN MAPPI",

            "KAB ASMAT":
                "KABUPATEN ASMAT",


            # ====================================================
            # PAPUA TENGAH
            # ====================================================

            "KAB NABIRE":
                "KABUPATEN NABIRE",

            "KAB PUNCAK":
                "KABUPATEN PUNCAK",

            "KAB PANIAI":
                "KABUPATEN PANIAI",

            "KAB MIMIKA":
                "KABUPATEN MIMIKA",

            "KAB PUNCAK JAYA":
                "KABUPATEN PUNCAK JAYA",

            "KAB DOGIYAI":
                "KABUPATEN DOGIYAI",

            "KAB INTAN JAYA":
                "KABUPATEN INTAN JAYA",

            "KAB DEIYAI":
                "KABUPATEN DEIYAI",


            # ====================================================
            # PAPUA PEGUNUNGAN
            # ====================================================

            "KAB JAYAWIJAYA":
                "KABUPATEN JAYAWIJAYA",

            "KAB PEGUNUNGAN BINTANG":
                "KABUPATEN PEGUNUNGAN BINTANG",

            "KAB YAHUKIMO":
                "KABUPATEN YAHUKIMO",

            "KAB TOLIKARA":
                "KABUPATEN TOLIKARA",

            "KAB MAMBERAMO TENGAH":
                "KABUPATEN MAMBERAMO TENGAH",

            "KAB YALIMO":
                "KABUPATEN YALIMO",

            "KAB LANNY JAYA":
                "KABUPATEN LANNY JAYA",

            "KAB NDUGA":
                "KABUPATEN NDUGA",


            # ====================================================
            # PAPUA BARAT DAYA
            # ====================================================

            "KAB SORONG":
                "KABUPATEN SORONG",

            "KAB SORONG SELATAN":
                "KABUPATEN SORONG SELATAN",

            "KAB RAJA AMPAT":
                "KABUPATEN RAJA AMPAT",

            "KAB TAMRAUW":
                "KABUPATEN TAMRAUW",

            "KAB MAYBRAT":
                "KABUPATEN MAYBRAT",

            "KAB MAJELIA":
                "KABUPATEN MAJELIA",

            "KOTA SORONG":
                "KOTA SORONG",
        }

        return kabupaten_kota_mapping.get(
            value,
            value
        )


    # --------------------------------------------------------
    # Normalize nama provinsi
    # --------------------------------------------------------

    def normalize_province(self, value):

        if pd.isna(value):
            return None

        value = str(value).upper().strip()

        value = re.sub(r"\.", "", value)
        value = re.sub(r"\s+", " ", value)

        province_mapping = {
            "ACEH": "ACEH",
            "SUMATERA UTARA": "SUMATERA UTARA",
            "SUMATERA BARAT": "SUMATERA BARAT",
            "RIAU": "RIAU",
            "JAMBI": "JAMBI",
            "SUMATERA SELATAN": "SUMATERA SELATAN",
            "BENGKULU": "BENGKULU",
            "LAMPUNG": "LAMPUNG",
            "JAWA BARAT": "JAWA BARAT",
            "BANTEN": "BANTEN",
            "KALIMANTAN BARAT": "KALIMANTAN BARAT",
            "KALIMANTAN SELATAN": "KALIMANTAN SELATAN",
            "KALIMANTAN TIMUR": "KALIMANTAN TIMUR",
            "SULAWESI SELATAN": "SULAWESI SELATAN",
            "SULAWESI TENGGARA": "SULAWESI TENGGARA",
        }

        return province_mapping.get(
            value,
            value
        )

    # --------------------------------------------------------
    # Extract PDF 32679
    # --------------------------------------------------------
    def extract_pdf_32679(self):

        print("\n========================================")
        print("EXTRACT PDF ID 32679")
        print("========================================")

        config = DATA_CONFIG[32679]

        pdf_files = config["pdf_file"]
        pages_config = config["pdf_pages"]

        province_pattern = (
            r"ACEH|"
            r"SUMATERA UTARA|"
            r"SUMATERA BARAT|"
            r"RIAU|"
            r"KEP\. BANGKA BELITUNG|"
            r"KEPULAUAN RIAU|"
            r"JAMBI|"
            r"SUMATERA SELATAN|"
            r"BENGKULU|"
            r"LAMPUNG|"
            r"DKI\. JAKARTA|"
            r"D\.K\.I\. JAKARTA|"
            r"JAWA BARAT|"
            r"BANTEN|"
            r"JAWA TENGAH|"
            r"DI\. YOGYAKARTA|"
            r"JAWA TIMUR|"
            r"BALI|"
            r"NUSA TENGGARA BARAT|"
            r"NUSA TENGGARA TIMUR|"
            r"KALIMANTAN BARAT|"
            r"KALIMANTAN TENGAH|"
            r"KALIMANTAN SELATAN|"
            r"KALIMANTAN TIMUR|"
            r"KALIMANTAN UTARA|"
            r"SULAWESI UTARA|"
            r"GORONTALO|"
            r"SULAWESI TENGAH|"
            r"SULAWESI SELATAN|"
            r"SULAWESI BARAT|"
            r"SULAWESI TENGGARA|"
            r"MALUKU UTARA|"
            r"MALUKU|"
            r"PAPUA BARAT DAYA|"
            r"PAPUA BARAT|"
            r"PAPUA SELATAN|"
            r"PAPUA TENGAH|"
            r"PAPUA PEGUNUNGAN|"
            r"PAPUA"
        )

        row_pattern = re.compile(
            rf"^\s*\d+\.\s+({province_pattern})\s+(.*)$"
        )

        def parse_number(value):
            value = value.strip()

            if value == "-":
                return None

            value = value.replace(".", "")
            value = value.replace("(", "")
            value = value.replace(")", "")

            return int(value)

        all_data = []

        for tahun, pages in pages_config.items():

            pdf_file = pdf_files[tahun]

            print(f"\nSource: {pdf_file}")
            print(f"Extract tahun {tahun}...")

            with pdfplumber.open(pdf_file) as pdf:

                for page_index in pages:

                    print(
                        f"  PDF page index: {page_index} "
                        f"(PDF page {page_index + 1})"
                    )

                    page = pdf.pages[page_index]
                    text = page.extract_text()

                    if not text:
                        print(
                            f"  ⚠️ Tidak ada text pada page {page_index}"
                        )
                        continue

                    for line in text.splitlines():

                        match = row_pattern.match(line)

                        if not match:
                            continue

                        provinsi = match.group(1)
                        values = match.group(2).split()

                        # Struktur tabel:
                        # 2019-2023 = 7 values
                        # 2024-2026 = 6 values
                        expected_len = 7 if tahun <= 2023 else 6

                        if len(values) != expected_len:
                            print(
                                "⚠️ Struktur tidak sesuai:",
                                tahun,
                                provinsi,
                                values
                            )
                            continue

                        # Kolom produksi PBN / BUMN
                        pbn_production = parse_number(values[4])

                        # "-" tidak dimasukkan
                        if pbn_production is None:
                            continue

                        provinsi = self.normalize_province(provinsi)

                        all_data.append({
                            "provinsi": provinsi,
                            "data_x": f"31-12-{tahun}",
                            "data_y": pbn_production,
                            "tahun": tahun,
                        })

        return pd.DataFrame(all_data)

    # --------------------------------------------------------
    # Extract PDF 32677
    # --------------------------------------------------------

    def extract_pdf_32677(self):

        print("\n========================================")
        print("EXTRACT PDF ID 32677")
        print("========================================")

        config = DATA_CONFIG[32677]

        pdf_files = config["pdf_file"]
        pages_config = config["pdf_pages"]

        province_pattern = (
            r"ACEH|"
            r"SUMATERA UTARA|"
            r"SUMATERA BARAT|"
            r"RIAU|"
            r"KEP\. BANGKA BELITUNG|"
            r"KEPULAUAN RIAU|"
            r"JAMBI|"
            r"SUMATERA SELATAN|"
            r"BENGKULU|"
            r"LAMPUNG|"
            r"DKI\. JAKARTA|"
            r"D\.K\.I\. JAKARTA|"
            r"JAWA BARAT|"
            r"BANTEN|"
            r"JAWA TENGAH|"
            r"DI\. YOGYAKARTA|"
            r"JAWA TIMUR|"
            r"BALI|"
            r"NUSA TENGGARA BARAT|"
            r"NUSA TENGGARA TIMUR|"
            r"KALIMANTAN BARAT|"
            r"KALIMANTAN TENGAH|"
            r"KALIMANTAN SELATAN|"
            r"KALIMANTAN TIMUR|"
            r"KALIMANTAN UTARA|"
            r"SULAWESI UTARA|"
            r"GORONTALO|"
            r"SULAWESI TENGAH|"
            r"SULAWESI SELATAN|"
            r"SULAWESI BARAT|"
            r"SULAWESI TENGGARA|"
            r"MALUKU UTARA|"
            r"MALUKU|"
            r"PAPUA BARAT DAYA|"
            r"PAPUA BARAT|"
            r"PAPUA SELATAN|"
            r"PAPUA TENGAH|"
            r"PAPUA PEGUNUNGAN|"
            r"PAPUA"
        )

        row_pattern = re.compile(
            rf"^\s*\d+\.\s+({province_pattern})\s+(.*)$"
        )

        def parse_number(value):
            value = value.strip()

            if value == "-":
                return None

            value = value.replace(".", "")
            value = value.replace("(", "")
            value = value.replace(")", "")

            return int(value)

        all_data = []

        # ========================================================
        # LOOP BERDASARKAN TAHUN
        # ========================================================

        for tahun, pages in pages_config.items():

            # Ambil PDF sesuai tahun dari DATA_CONFIG
            pdf_file = pdf_files[tahun]

            print(f"\nSource: {pdf_file}")
            print(f"Extract tahun {tahun}...")

            with pdfplumber.open(pdf_file) as pdf:

                for page_index in pages:

                    print(
                        f"  PDF page index: {page_index} "
                        f"(PDF page {page_index + 1})"
                    )

                    page = pdf.pages[page_index]
                    text = page.extract_text()

                    if not text:
                        print(
                            f"  ⚠️ Tidak ada text pada page {page_index}"
                        )
                        continue

                    for line in text.splitlines():

                        match = row_pattern.match(line)

                        if not match:
                            continue

                        provinsi = match.group(1)
                        values = match.group(2).split()

                        # Struktur tabel:
                        # 2019-2023 = 7 values
                        # 2024-2026 = 6 values
                        expected_len = 7 if tahun <= 2023 else 6

                        if len(values) != expected_len:
                            print(
                                "⚠️ Struktur tidak sesuai:",
                                tahun,
                                provinsi,
                                values
                            )
                            continue

                        # ====================================================
                        # Kolom Produksi PBS / Swasta
                        # ====================================================

                        pbs_production = parse_number(values[4])

                        # "-" tidak dimasukkan
                        if pbs_production is None:
                            continue

                        provinsi = self.normalize_province(
                            provinsi
                        )

                        all_data.append({
                            "provinsi": provinsi,
                            "data_x": f"31-12-{tahun}",
                            "data_y": pbs_production,
                            "tahun": tahun,
                        })

        # ========================================================
        # DATAFRAME
        # ========================================================

        df = pd.DataFrame(all_data)

        print("\n=== HASIL EXTRACT 32677 ===")
        print(f"Total rows: {len(df)}")

        if not df.empty:

            print("\nJumlah per tahun:")
            print(
                df.groupby("tahun").size()
            )

            print("\nTotal produksi per tahun:")
            print(
                df.groupby("tahun")["data_y"].sum()
            )

        return df

    # --------------------------------------------------------
    # Extract PDF 32678
    # --------------------------------------------------------

    def extract_pdf_32678(self):

        print("\n========================================")
        print("EXTRACT PDF ID 32678")
        print("========================================")

        config = DATA_CONFIG[32678]

        pdf_files = config["pdf_file"]
        pages_config = config["pdf_pages"]

        province_pattern = (
            r"ACEH|"
            r"SUMATERA UTARA|"
            r"SUMATERA BARAT|"
            r"RIAU|"
            r"KEP\. BANGKA BELITUNG|"
            r"KEPULAUAN RIAU|"
            r"JAMBI|"
            r"SUMATERA SELATAN|"
            r"BENGKULU|"
            r"LAMPUNG|"
            r"DKI\. JAKARTA|"
            r"D\.K\.I\. JAKARTA|"
            r"JAWA BARAT|"
            r"BANTEN|"
            r"JAWA TENGAH|"
            r"DI\. YOGYAKARTA|"
            r"D\.I\. YOGYAKARTA|"
            r"JAWA TIMUR|"
            r"BALI|"
            r"NUSA TENGGARA BARAT|"
            r"NUSA TENGGARA TIMUR|"
            r"KALIMANTAN BARAT|"
            r"KALIMANTAN TENGAH|"
            r"KALIMANTAN SELATAN|"
            r"KALIMANTAN TIMUR|"
            r"KALIMANTAN UTARA|"
            r"SULAWESI UTARA|"
            r"SULAWESI TENGAH|"
            r"SULAWESI SELATAN|"
            r"SULAWESI TENGGARA|"
            r"SULAWESI BARAT|"
            r"GORONTALO|"
            r"MALUKU|"
            r"MALUKU UTARA|"
            r"PAPUA BARAT DAYA|"
            r"PAPUA BARAT|"
            r"PAPUA SELATAN|"
            r"PAPUA TENGAH|"
            r"PAPUA PEGUNUNGAN|"
            r"PAPUA"
        )

        row_pattern = re.compile(
            rf"^\s*\d+\.\s+({province_pattern})\s+(.*)$"
        )

        def parse_number(value):

            value = value.strip()

            if value == "-":
                return None

            value = value.replace(".", "")
            value = value.replace("(", "")
            value = value.replace(")", "")

            return int(value)

        all_data = []

        for tahun, pages in pages_config.items():

            pdf_file = pdf_files[tahun]

            print(f"\nSource: {pdf_file}")
            print(f"Extract tahun {tahun}...")

            with pdfplumber.open(pdf_file) as pdf:

                for page_index in pages:

                    print(
                        f"  PDF page index: {page_index} "
                        f"(PDF page {page_index + 1})"
                    )

                    page = pdf.pages[page_index]
                    text = page.extract_text()

                    if not text:
                        continue

                    for line in text.splitlines():

                        match = row_pattern.match(line)

                        if not match:
                            continue

                        provinsi = match.group(1)
                        values = match.group(2).split()
                        expected_len = 7

                        if len(values) != expected_len:
                            print(
                                "⚠️ Struktur tidak sesuai:",
                                tahun,
                                provinsi,
                                values
                            )
                            continue

                        # Kolom Produksi Perkebunan Rakyat
                        pr_production = parse_number(
                            values[4]
                        )

                        if pr_production is None:
                            continue

                        provinsi = self.normalize_province(
                            provinsi
                        )

                        all_data.append({
                            "provinsi": provinsi,
                            "data_x": f"31-12-{tahun}",
                            "data_y": pr_production,
                            "tahun": tahun,
                        })

        df = pd.DataFrame(all_data)

        print("\n=== HASIL EXTRACT 32678 ===")
        print(f"Total rows: {len(df)}")

        if not df.empty:
            print("\nJumlah per tahun:")
            print(df.groupby("tahun").size())

            print("\nTotal produksi per tahun:")
            print(df.groupby("tahun")["data_y"].sum())

        return df

    # --------------------------------------------------------
    # Extract PDF 2883
    # --------------------------------------------------------

    def extract_pdf_2883(self):

        print("\n========================================")
        print("EXTRACT PDF ID 2883")
        print("========================================")

        config = DATA_CONFIG[2883]

        pdf_files = config["pdf_file"]
        pages_config = config["pdf_pages"]

        all_data = []

        for tahun, pages in pages_config.items():

            pdf_file = pdf_files[tahun]

            print(f"\nSource: {pdf_file}")
            print(f"Extract tahun {tahun}...")

            with pdfplumber.open(pdf_file) as pdf:

                for page_index in pages:

                    print(
                        f"  PDF page index: {page_index} "
                        f"(PDF page {page_index + 1})"
                    )

                    page = pdf.pages[page_index]
                    text = page.extract_text()

                    if not text:
                        continue

                    lines = text.splitlines()

                    i = 0

                    while i < len(lines):

                        line = lines[i].strip()

                        # ------------------------------------------------
                        # Lewati header / baris bukan data
                        # ------------------------------------------------

                        if (
                            not line
                            or line.startswith("DAFTAR TABEL")
                            or line.startswith("No. Provinsi")
                            or line.startswith("Kabupaten")
                            or line.startswith("Luas Areal")
                            or line.startswith("T B M")
                            or line.startswith("TM")
                            or line.startswith("TTM")
                            or line.startswith("(1)")
                            or line.startswith("PROPINSI")
                            or line.startswith("Lanjutan")
                            or line.startswith("Continued")
                            or line.startswith("statistik")
                            or line.startswith("statistics")
                        ):
                            i += 1
                            continue

                        # ------------------------------------------------
                        # Baris kabupaten / kota
                        # ------------------------------------------------

                        if re.match(
                            r"^(Kab\.|Kota)\s+",
                            line,
                            re.IGNORECASE
                        ):

                            nama_kab_kota = line

                            # --------------------------------------------
                            # Cek apakah nama kab/kota terpotong
                            # --------------------------------------------

                            # Kalau belum ada angka di baris,
                            # berarti kemungkinan nama lanjut ke baris berikutnya.
                            if not re.search(
                                r"\d|-",
                                line
                            ):
                                if i + 1 < len(lines):

                                    next_line = lines[i + 1].strip()

                                    # Gabungkan nama yang terpotong
                                    # selama baris berikutnya bukan
                                    # kab/kota baru atau provinsi
                                    if (
                                        not re.match(
                                            r"^(Kab\.|Kota)\s+",
                                            next_line,
                                            re.IGNORECASE
                                        )
                                        and not next_line.startswith(
                                            "PROPINSI"
                                        )
                                    ):

                                        nama_kab_kota = (
                                            nama_kab_kota
                                            + " "
                                            + next_line
                                        )

                                        i += 1

                            # --------------------------------------------
                            # Pisahkan nama dan angka
                            # --------------------------------------------

                            match = re.match(
                                r"^(Kab\.|Kota)\s+(.+?)\s+"
                                r"((?:[\d.-]+\s*)+)$",
                                nama_kab_kota
                            )

                            if not match:
                                i += 1
                                continue

                            tipe = match.group(1)
                            nama = match.group(2).strip()
                            values = match.group(3).split()

                            # ------------------------------------------------
                            # Struktur tabel:
                            #
                            # TBM
                            # TM
                            # TTM/TR
                            # Jumlah
                            # Produksi
                            # Produktivitas
                            # Petani
                            #
                            # Total = 7 values
                            # ------------------------------------------------

                            if len(values) != 7:

                                print(
                                    "⚠️ Struktur tidak sesuai:",
                                    tahun,
                                    nama_kab_kota,
                                    values
                                )

                                i += 1
                                continue

                            def parse_number(value):

                                value = value.strip()

                                if value == "-":
                                    return None

                                value = value.replace(".", "")
                                value = value.replace(",", "")
                                value = value.replace("(", "")
                                value = value.replace(")", "")

                                return int(value)

                            # ------------------------------------------------
                            # Produksi = values[4]
                            # ------------------------------------------------

                            production = parse_number(
                                values[4]
                            )

                            # "-" tidak dimasukkan
                            if production is None:
                                i += 1
                                continue

                            # ------------------------------------------------
                            # Normalisasi nama kabupaten/kota
                            # ------------------------------------------------

                            kabupaten_kota = (
                                f"{tipe} {nama}"
                            )

                            kabupaten_kota = (
                                self.normalize_kabupaten_kota(
                                    kabupaten_kota
                                )
                            )

                            all_data.append({

                                "kabupaten_kota":
                                    kabupaten_kota,

                                "data_x":
                                    f"31-12-{tahun}",

                                "data_y":
                                    production,

                                "tahun":
                                    tahun,
                            })

                        i += 1

        df = pd.DataFrame(all_data)

        print("\n=== HASIL EXTRACT 2883 ===")
        print(f"Total rows: {len(df)}")

        if not df.empty:

            print("\nJumlah per tahun:")
            print(
                df.groupby("tahun").size()
            )

            print("\nPreview:")
            print(
                df.head(20).to_string(
                    index=False
                )
            )

        return df

    # --------------------------------------------------------
    # Save raw parquet per tahun
    # --------------------------------------------------------

    def save_raw_parquet(
        self,
        df,
        id_nama_data
    ):

        print("\n========================================")
        print("SAVE RAW PARQUET")
        print("========================================")

        raw_folder = (
            RAW_BASE_DIR
            / DATA_CONFIG[id_nama_data]["raw_folder"]
        )

        raw_folder.mkdir(
            parents=True,
            exist_ok=True
        )

        for tahun, df_year in df.groupby("tahun"):

            filename = (
                f"{DATA_CONFIG[id_nama_data]['raw_folder']}"
                f"_{tahun}.parquet"
            )

            output_path = raw_folder / filename

            df_year = df_year.copy()

            # tahun hanya dipakai untuk nama file,
            # data_x sudah menyimpan tanggal
            df_year.to_parquet(
                output_path,
                index=False
            )

            print(
                f"Saved {tahun}: "
                f"{len(df_year)} rows -> {output_path}"
            )

    # --------------------------------------------------------
    # Update 32679
    # --------------------------------------------------------

    def update_32679(self):

        id_nama_data = 32679

        print("\n========================================")
        print("PROCESSING ID 32679")
        print("Produksi Kelapa Sawit Milik BUMN")
        print("========================================")

        # ----------------------------------------------------
        # 1. Extract PDF
        # ----------------------------------------------------

        df_pdf = self.extract_pdf_32679()

        if df_pdf.empty:
            print("❌ Tidak ada data hasil extract.")
            return pd.DataFrame()

        # ----------------------------------------------------
        # 2. Cek hasil extract
        # ----------------------------------------------------

        print("\n=== JUMLAH DATA PDF ===")
        print(len(df_pdf))

        print("\n=== JUMLAH PER TAHUN ===")
        print(
            df_pdf.groupby("tahun").size()
        )

        print("\n=== TOTAL PRODUKSI ===")
        print(
            df_pdf.groupby("tahun")["data_y"]
            .sum()
        )

        # ----------------------------------------------------
        # 3. Save RAW
        # ----------------------------------------------------

        self.save_raw_parquet(
            df_pdf,
            id_nama_data
        )

        # ----------------------------------------------------
        # 4. Get existing xFlask
        # ----------------------------------------------------

        print("\n========================================")
        print("GET EXISTING DATA FROM xFLASK")
        print("========================================")

        df_existing = self.get_xflask_api_data(
            id_nama_data
        )

        print("\n=== PROVINSI EXISTING xFLASK ===")
        print(
            df_existing[
                ["provinsi", "id_provinsi", "kode_bps"]
            ]
            .drop_duplicates()
            .sort_values("id_provinsi")
            .to_string(index=False)
        )
        print(
            f"Existing rows: {len(df_existing)}"
        )

        # ----------------------------------------------------
        # 5. Normalize existing data
        # ----------------------------------------------------

        if not df_existing.empty:

            df_existing["provinsi_normalized"] = (
                df_existing["provinsi"]
                .apply(
                    self.normalize_province
                )
            )

            df_existing["data_x"] = (
                df_existing["data_x"]
                .astype(str)
                .str.strip()
            )

        # ----------------------------------------------------
        # 6. Normalize PDF data
        # ----------------------------------------------------

        df_pdf["data_x"] = (
            df_pdf["data_x"]
            .astype(str)
            .str.strip()
        )

        # ----------------------------------------------------
        # 7. Matching
        # ----------------------------------------------------

        if df_existing.empty:

            df_new = df_pdf.copy()

        else:

            existing_keys = set(
                zip(
                    df_existing["data_x"],
                    df_existing[
                        "provinsi_normalized"
                    ]
                )
            )

            df_new = df_pdf[
                ~df_pdf.apply(
                    lambda row: (
                        row["data_x"],
                        row["provinsi"]
                    ) in existing_keys,
                    axis=1
                )
            ].copy()

        # ----------------------------------------------------
        # 8. Build final dataframe
        # ----------------------------------------------------

        df_final = self.build_final_32679(
            df_new,
            df_existing
        )

        # ----------------------------------------------------
        # 9. Check final schema
        # ----------------------------------------------------

        print("\n========================================")
        print("FINAL SCHEMA")
        print("========================================")

        print(df_final.columns.tolist())

        print("\nJumlah final rows:")
        print(len(df_final))

        print("\nPreview:")
        print(
            df_final.head(10).to_string(index=False)
        )

        # ----------------------------------------------------
        # 10. Save final sementara
        # ----------------------------------------------------

        final_dir = (
            Path("data/final/kemenko")
            / DATA_CONFIG[32679]["raw_folder"]
        )

        final_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        final_path = (
            final_dir
            / "produksi_kelapa_sawit_bumn_provinsi_2019_2026.parquet"
        )

        df_final.to_parquet(
            final_path,
            index=False
        )

        print(
            f"\nFinal parquet saved: {final_path}"
        )

        return df_final

    # --------------------------------------------------------
    # Current update
    # --------------------------------------------------------

    def update_curr_32679(self):

        return self.update_32679()

# ============================================================
# KEMENKO EKONOMI ETL
# ============================================================

class KemenkoEkonomiETL:

    def __init__(self):

        self.headers = {
            "X-Api-Key": KATADATA_API_KEY
        }

        self.raw_dir = (
            RAW_BASE_DIR
            / DATA_CONFIG[32679]["raw_folder"]
        )

        self.raw_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        self.minio_client = get_minio_client()


    # ========================================================
    # GET EXISTING DATA FROM xFLASK
    # ========================================================

    def get_xflask_api_data(self, id_nama_data):

        url = f"{API_BASE_URL}/getdata/{id_nama_data}"

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        session = requests.Session()

        adapter = HTTPAdapter(
            max_retries=retry
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        print(
            f"Request xFlask ID {id_nama_data}: {url}"
        )

        response = session.get(
            url,
            headers=self.headers,
            timeout=(10, 120)
        )

        response.raise_for_status()

        result = response.json()

        df = pd.DataFrame(
            result["data"]
        )

        print(
            f"✅ xFlask berhasil: {len(df)} rows"
        )

        return df


    # ========================================================
    # NORMALIZE PROVINCE
    # ========================================================

    def normalize_province(self, value):

        if pd.isna(value):
            return None

        value = str(value).upper().strip()

        # Hilangkan titik
        value = re.sub(r"\.", "", value)

        # Rapikan spasi
        value = re.sub(r"\s+", " ", value)

        province_mapping = {
            "ACEH": "ACEH",
            "SUMATERA UTARA": "SUMATERA UTARA",
            "SUMATERA BARAT": "SUMATERA BARAT",
            "RIAU": "RIAU",
            "JAMBI": "JAMBI",
            "SUMATERA SELATAN": "SUMATERA SELATAN",
            "BENGKULU": "BENGKULU",
            "LAMPUNG": "LAMPUNG",

            "KEP BANGKA BELITUNG": "KEP BANGKA BELITUNG",
            "KEPULAUAN BANGKA BELITUNG": "KEP BANGKA BELITUNG",

            # Samakan PDF dengan xFlask
            "KEPULAUAN RIAU": "KEP RIAU",
            "KEP RIAU": "KEP RIAU",

            "JAWA BARAT": "JAWA BARAT",
            "BANTEN": "BANTEN",

            "KALIMANTAN BARAT": "KALIMANTAN BARAT",
            "KALIMANTAN TENGAH": "KALIMANTAN TENGAH",
            "KALIMANTAN SELATAN": "KALIMANTAN SELATAN",
            "KALIMANTAN TIMUR": "KALIMANTAN TIMUR",
            "KALIMANTAN UTARA": "KALIMANTAN UTARA",

            "SULAWESI UTARA": "SULAWESI UTARA",
            "SULAWESI TENGAH": "SULAWESI TENGAH",
            "SULAWESI SELATAN": "SULAWESI SELATAN",
            "SULAWESI BARAT": "SULAWESI BARAT",
            "SULAWESI TENGGARA": "SULAWESI TENGGARA",

            "GORONTALO": "GORONTALO",

            "MALUKU": "MALUKU",
            "MALUKU UTARA": "MALUKU UTARA",

            "PAPUA BARAT": "PAPUA BARAT",
            "PAPUA BARAT DAYA": "PAPUA BARAT DAYA",
            "PAPUA SELATAN": "PAPUA SELATAN",
            "PAPUA TENGAH": "PAPUA TENGAH",
            "PAPUA PEGUNUNGAN": "PAPUA PEGUNUNGAN",
            "PAPUA": "PAPUA",
        }

        return province_mapping.get(value, value)

    # --------------------------------------------------------
    # Normalize nama kabupaten/kota
    # --------------------------------------------------------

    def normalize_kabupaten_kota(self, value):

        if pd.isna(value):
            return None

        value = str(value).upper().strip()

        # Hilangkan titik
        value = re.sub(r"\.", "", value)

        # Rapikan spasi
        value = re.sub(r"\s+", " ", value)

        kabupaten_kota_mapping = {

            # ====================================================
            # ACEH
            # ====================================================

            "KAB SIMEULUE":
                "KABUPATEN SIMEULUE",

            "KAB ACEH SINGKIL":
                "KABUPATEN ACEH SINGKIL",

            "KAB ACEH SELATAN":
                "KABUPATEN ACEH SELATAN",

            "KAB ACEH TENGGARA":
                "KABUPATEN ACEH TENGGARA",

            "KAB ACEH TIMUR":
                "KABUPATEN ACEH TIMUR",

            "KAB ACEH TENGAH":
                "KABUPATEN ACEH TENGAH",

            "KAB ACEH BARAT":
                "KABUPATEN ACEH BARAT",

            "KAB ACEH BESAR":
                "KABUPATEN ACEH BESAR",

            "KAB PIDIE":
                "KABUPATEN PIDIE",

            "KAB BIREUN":
                "KABUPATEN BIREUEN",

            "KAB ACEH UTARA":
                "KABUPATEN ACEH UTARA",

            "KAB ACEH BARAT DAYA":
                "KABUPATEN ACEH BARAT DAYA",

            "KAB GAYO LUES":
                "KABUPATEN GAYO LUES",

            "KAB ACEH TAMIANG":
                "KABUPATEN ACEH TAMIANG",

            "KAB NAGAN RAYA":
                "KABUPATEN NAGAN RAYA",

            "KAB ACEH JAYA":
                "KABUPATEN ACEH JAYA",

            "KAB BENER MERIAH":
                "KABUPATEN BENER MERIAH",

            "KAB PIDIE JAYA":
                "KABUPATEN PIDIE JAYA",

            "KOTA BANDA ACEH":
                "KOTA BANDA ACEH",

            "KOTA SABANG":
                "KOTA SABANG",

            "KOTA LANGSA":
                "KOTA LANGSA",

            "KOTA LHOKSEUMAWE":
                "KOTA LHOKSEUMAWE",

            "KOTA SUBULUSSALAM":
                "KOTA SUBULUSSALAM",


            # ====================================================
            # SUMATERA UTARA
            # ====================================================

            "KAB NIAS":
                "KABUPATEN NIAS",

            "KAB MANDAILING NATAL":
                "KABUPATEN MANDAILING NATAL",

            "KAB TAPANULI SELATAN":
                "KABUPATEN TAPANULI SELATAN",

            "KAB TAPANULI TENGAH":
                "KABUPATEN TAPANULI TENGAH",

            "KAB TAPANULI UTARA":
                "KABUPATEN TAPANULI UTARA",

            "KAB TOBA SAMOSIR":
                "KABUPATEN TOBA",

            "KAB LABUHAN BATU":
                "KABUPATEN LABUHANBATU",

            "KAB ASAHAN":
                "KABUPATEN ASAHAN",

            "KAB SIMALUNGUN":
                "KABUPATEN SIMALUNGUN",

            "KAB DAIRI":
                "KABUPATEN DAIRI",

            "KAB KARO":
                "KABUPATEN KARO",

            "KAB DELI SERDANG":
                "KABUPATEN DELI SERDANG",

            "KAB LANGKAT":
                "KABUPATEN LANGKAT",

            "KAB NIAS SELATAN":
                "KABUPATEN NIAS SELATAN",

            "KAB HUMBANG HASUNDUTAN":
                "KABUPATEN HUMBANG HASUNDUTAN",

            "KAB PAKPAK BHARAT":
                "KABUPATEN PAKPAK BHARAT",

            "KAB SAMOSIR":
                "KABUPATEN SAMOSIR",

            "KAB SERDANG BEDAGAI":
                "KABUPATEN SERDANG BEDAGAI",

            "KAB BATU BARA":
                "KABUPATEN BATU BARA",

            "KAB PADANG LAWAS":
                "KABUPATEN PADANG LAWAS",

            "KAB PADANG LAWAS UTARA":
                "KABUPATEN PADANG LAWAS UTARA",

            "KAB LABUHANBATU SELATAN":
                "KABUPATEN LABUHANBATU SELATAN",

            "KAB LABUHANBATU UTARA":
                "KABUPATEN LABUHANBATU UTARA",

            "KAB NIAS UTARA":
                "KABUPATEN NIAS UTARA",

            "KAB NIAS BARAT":
                "KABUPATEN NIAS BARAT",

            "KOTA SIBOLGA":
                "KOTA SIBOLGA",

            "KOTA TANJUNG BALAI":
                "KOTA TANJUNG BALAI",

            "KOTA PEMATANG SIANTAR":
                "KOTA PEMATANG SIANTAR",

            "KOTA TEBING TINGGI":
                "KOTA TEBING TINGGI",

            "KOTA MEDAN":
                "KOTA MEDAN",

            "KOTA BINJAI":
                "KOTA BINJAI",

            "KOTA PADANGSIDIMPUAN":
                "KOTA PADANGSIDIMPUAN",


            # ====================================================
            # SUMATERA BARAT
            # ====================================================

            "KAB KEPULAUAN MENTAWAI":
                "KABUPATEN KEPULAUAN MENTAWAI",

            "KAB PESISIR SELATAN":
                "KABUPATEN PESISIR SELATAN",

            "KAB SOLOK":
                "KABUPATEN SOLOK",

            "KAB SIJUNJUNG":
                "KABUPATEN SIJUNJUNG",

            "KAB TANAH DATAR":
                "KABUPATEN TANAH DATAR",

            "KAB PADANG PARIAMAN":
                "KABUPATEN PADANG PARIAMAN",

            "KAB AGAM":
                "KABUPATEN AGAM",

            "KAB LIMA PULUH KOTA":
                "KABUPATEN LIMA PULUH KOTA",

            "KAB PASAMAN":
                "KABUPATEN PASAMAN",

            "KAB SOLOK SELATAN":
                "KABUPATEN SOLOK SELATAN",

            "KAB DHARMASRAYA":
                "KABUPATEN DHARMASRAYA",

            "KAB PASAMAN BARAT":
                "KABUPATEN PASAMAN BARAT",

            "KOTA PADANG":
                "KOTA PADANG",

            "KOTA SOLOK":
                "KOTA SOLOK",

            "KOTA SAWAHLUNTO":
                "KOTA SAWAHLUNTO",

            "KOTA PADANG PANJANG":
                "KOTA PADANG PANJANG",

            "KOTA BUKITTINGGI":
                "KOTA BUKITTINGGI",

            "KOTA PAYAKUMBUH":
                "KOTA PAYAKUMBUH",

            "KOTA PARIAMAN":
                "KOTA PARIAMAN",


            # ====================================================
            # RIAU
            # ====================================================

            "KAB KUANTAN SINGINGI":
                "KABUPATEN KUANTAN SINGINGI",

            "KAB INDRAGIRI HULU":
                "KABUPATEN INDRAGIRI HULU",

            "KAB INDRAGIRI HILIR":
                "KABUPATEN INDRAGIRI HILIR",

            "KAB PELALAWAN":
                "KABUPATEN PELALAWAN",

            "KAB SIAK":
                "KABUPATEN SIAK",

            "KAB KAMPAR":
                "KABUPATEN KAMPAR",

            "KAB ROKAN HULU":
                "KABUPATEN ROKAN HULU",

            "KAB BENGKALIS":
                "KABUPATEN BENGKALIS",

            "KAB ROKAN HILIR":
                "KABUPATEN ROKAN HILIR",

            "KAB KEPULAUAN MERANTI":
                "KABUPATEN KEPULAUAN MERANTI",

            "KOTA PEKANBARU":
                "KOTA PEKANBARU",

            "KOTA DUMAI":
                "KOTA DUMAI",


            # ====================================================
            # JAMBI
            # ====================================================

            "KAB KERINCI":
                "KABUPATEN KERINCI",

            "KAB MERANGIN":
                "KABUPATEN MERANGIN",

            "KAB SAROLANGUN":
                "KABUPATEN SAROLANGUN",

            "KAB BATANG HARI":
                "KABUPATEN BATANG HARI",

            "KAB MUARO JAMBI":
                "KABUPATEN MUARO JAMBI",

            "KAB TANJUNG JABUNG BARAT":
                "KABUPATEN TANJUNG JABUNG BARAT",

            "KAB TANJUNG JABUNG TIMUR":
                "KABUPATEN TANJUNG JABUNG TIMUR",

            "KAB BUNGO":
                "KABUPATEN BUNGO",

            "KAB TEBO":
                "KABUPATEN TEBO",

            "KOTA JAMBI":
                "KOTA JAMBI",

            "KOTA SUNGAI PENUH":
                "KOTA SUNGAI PENUH",


            # ====================================================
            # SUMATERA SELATAN
            # ====================================================

            "KAB OGAN KOMERING ULU":
                "KABUPATEN OGAN KOMERING ULU",

            "KAB OGAN KOMERING ILIR":
                "KABUPATEN OGAN KOMERING ILIR",

            "KAB MUARA ENIM":
                "KABUPATEN MUARA ENIM",

            "KAB LAHAT":
                "KABUPATEN LAHAT",

            "KAB MUSI RAWAS":
                "KABUPATEN MUSI RAWAS",

            "KAB MUSI BANYUASIN":
                "KABUPATEN MUSI BANYUASIN",

            "KAB BANYUASIN":
                "KABUPATEN BANYUASIN",

            "KAB OGAN KOMERING ULU SELATAN":
                "KABUPATEN OGAN KOMERING ULU SELATAN",

            "KAB OGAN ILIR":
                "KABUPATEN OGAN ILIR",

            "KAB EMPAT LAWANG":
                "KABUPATEN EMPAT LAWANG",

            "KAB PENUKAL ABAB LEMATANG ILIR":
                "KABUPATEN PENUKAL ABAB LEMATANG ILIR",

            "KAB MUSI RAWAS UTARA":
                "KABUPATEN MUSI RAWAS UTARA",

            "KOTA PALEMBANG":
                "KOTA PALEMBANG",

            "KOTA PAGAR ALAM":
                "KOTA PAGAR ALAM",

            "KOTA LUBUKLINGGAU":
                "KOTA LUBUKLINGGAU",

            "KOTA PRABUMULIH":
                "KOTA PRABUMULIH",


            # ====================================================
            # BENGKULU
            # ====================================================

            "KAB BENGKULU SELATAN":
                "KABUPATEN BENGKULU SELATAN",

            "KAB REJANG LEBONG":
                "KABUPATEN REJANG LEBONG",

            "KAB BENGKULU UTARA":
                "KABUPATEN BENGKULU UTARA",

            "KAB KAUR":
                "KABUPATEN KAUR",

            "KAB SELUMA":
                "KABUPATEN SELUMA",

            "KAB MUKOMUKO":
                "KABUPATEN MUKOMUKO",

            "KAB LEBONG":
                "KABUPATEN LEBONG",

            "KAB KEPAHIANG":
                "KABUPATEN KEPAHIANG",

            "KAB BENGKULU TENGAH":
                "KABUPATEN BENGKULU TENGAH",

            "KOTA BENGKULU":
                "KOTA BENGKULU",


            # ====================================================
            # LAMPUNG
            # ====================================================

            "KAB LAMPUNG BARAT":
                "KABUPATEN LAMPUNG BARAT",

            "KAB TANGGAMUS":
                "KABUPATEN TANGGAMUS",

            "KAB LAMPUNG SELATAN":
                "KABUPATEN LAMPUNG SELATAN",

            "KAB LAMPUNG TIMUR":
                "KABUPATEN LAMPUNG TIMUR",

            "KAB LAMPUNG TENGAH":
                "KABUPATEN LAMPUNG TENGAH",

            "KAB LAMPUNG UTARA":
                "KABUPATEN LAMPUNG UTARA",

            "KAB WAY KANAN":
                "KABUPATEN WAY KANAN",

            "KAB TULANG BAWANG":
                "KABUPATEN TULANG BAWANG",

            "KAB PESAWARAN":
                "KABUPATEN PESAWARAN",

            "KAB PRINGSEWU":
                "KABUPATEN PRINGSEWU",

            "KAB MESUJI":
                "KABUPATEN MESUJI",

            "KAB TULANG BAWANG BARAT":
                "KABUPATEN TULANG BAWANG BARAT",

            "KAB PESISIR BARAT":
                "KABUPATEN PESISIR BARAT",

            "KOTA BANDAR LAMPUNG":
                "KOTA BANDAR LAMPUNG",

            "KOTA METRO":
                "KOTA METRO",


            # ====================================================
            # KEPULAUAN BANGKA BELITUNG
            # ====================================================

            "KAB BANGKA":
                "KABUPATEN BANGKA",

            "KAB BELITUNG":
                "KABUPATEN BELITUNG",

            "KAB BANGKA BARAT":
                "KABUPATEN BANGKA BARAT",

            "KAB BANGKA TENGAH":
                "KABUPATEN BANGKA TENGAH",

            "KAB BANGKA SELATAN":
                "KABUPATEN BANGKA SELATAN",

            "KAB BELITUNG TIMUR":
                "KABUPATEN BELITUNG TIMUR",

            "KOTA PANGKALPINANG":
                "KOTA PANGKALPINANG",


            # ====================================================
            # KEPULAUAN RIAU
            # ====================================================

            "KAB BINTAN":
                "KABUPATEN BINTAN",

            "KAB KARIMUN":
                "KABUPATEN KARIMUN",

            "KAB NATUNA":
                "KABUPATEN NATUNA",

            "KAB LINGGA":
                "KABUPATEN LINGGA",

            "KAB KEPULAUAN ANAMBAS":
                "KABUPATEN KEPULAUAN ANAMBAS",

            "KOTA BATAM":
                "KOTA BATAM",

            "KOTA TANJUNGPINANG":
                "KOTA TANJUNGPINANG",


            # ====================================================
            # JAWA BARAT
            # ====================================================

            "KAB BOGOR":
                "KABUPATEN BOGOR",

            "KAB SUKABUMI":
                "KABUPATEN SUKABUMI",

            "KAB CIANJUR":
                "KABUPATEN CIANJUR",

            "KAB BANDUNG":
                "KABUPATEN BANDUNG",

            "KAB GARUT":
                "KABUPATEN GARUT",

            "KAB TASIKMALAYA":
                "KABUPATEN TASIKMALAYA",

            "KAB CIAMIS":
                "KABUPATEN CIAMIS",

            "KAB KUNINGAN":
                "KABUPATEN KUNINGAN",

            "KAB CIREBON":
                "KABUPATEN CIREBON",

            "KAB MAJALENGKA":
                "KABUPATEN MAJALENGKA",

            "KAB SUMEDANG":
                "KABUPATEN SUMEDANG",

            "KAB INDRAMAYU":
                "KABUPATEN INDRAMAYU",

            "KAB SUBANG":
                "KABUPATEN SUBANG",

            "KAB PURWAKARTA":
                "KABUPATEN PURWAKARTA",

            "KAB KARAWANG":
                "KABUPATEN KARAWANG",

            "KAB BEKASI":
                "KABUPATEN BEKASI",

            "KAB BANDUNG BARAT":
                "KABUPATEN BANDUNG BARAT",

            "KAB PANGANDARAN":
                "KABUPATEN PANGANDARAN",

            "KOTA BOGOR":
                "KOTA BOGOR",

            "KOTA SUKABUMI":
                "KOTA SUKABUMI",

            "KOTA BANDUNG":
                "KOTA BANDUNG",

            "KOTA CIREBON":
                "KOTA CIREBON",

            "KOTA BEKASI":
                "KOTA BEKASI",

            "KOTA DEPOK":
                "KOTA DEPOK",

            "KOTA CIMAHI":
                "KOTA CIMAHI",

            "KOTA TASIKMALAYA":
                "KOTA TASIKMALAYA",

            "KOTA BANJAR":
                "KOTA BANJAR",


            # ====================================================
            # BANTEN
            # ====================================================

            "KAB PANDEGLANG":
                "KABUPATEN PANDEGLANG",

            "KAB LEBAK":
                "KABUPATEN LEBAK",

            "KAB TANGERANG":
                "KABUPATEN TANGERANG",

            "KAB SERANG":
                "KABUPATEN SERANG",

            "KOTA TANGERANG":
                "KOTA TANGERANG",

            "KOTA CILEGON":
                "KOTA CILEGON",

            "KOTA SERANG":
                "KOTA SERANG",

            "KOTA TANGERANG SELATAN":
                "KOTA TANGERANG SELATAN",


            # ====================================================
            # JAWA TENGAH
            # ====================================================

            "KAB CILACAP":
                "KABUPATEN CILACAP",

            "KAB BANYUMAS":
                "KABUPATEN BANYUMAS",

            "KAB PURBALINGGA":
                "KABUPATEN PURBALINGGA",

            "KAB BANJARNEGARA":
                "KABUPATEN BANJARNEGARA",

            "KAB KEBUMEN":
                "KABUPATEN KEBUMEN",

            "KAB PURWOREJO":
                "KABUPATEN PURWOREJO",

            "KAB WONOSOBO":
                "KABUPATEN WONOSOBO",

            "KAB MAGELANG":
                "KABUPATEN MAGELANG",

            "KAB BOYOLALI":
                "KABUPATEN BOYOLALI",

            "KAB KLATEN":
                "KABUPATEN KLATEN",

            "KAB SUKOHARJO":
                "KABUPATEN SUKOHARJO",

            "KAB WONOGIRI":
                "KABUPATEN WONOGIRI",

            "KAB KARANGANYAR":
                "KABUPATEN KARANGANYAR",

            "KAB SRAGEN":
                "KABUPATEN SRAGEN",

            "KAB GROBOGAN":
                "KABUPATEN GROBOGAN",

            "KAB BLORA":
                "KABUPATEN BLORA",

            "KAB REMBANG":
                "KABUPATEN REMBANG",

            "KAB PATI":
                "KABUPATEN PATI",

            "KAB KUDUS":
                "KABUPATEN KUDUS",

            "KAB JEPARA":
                "KABUPATEN JEPARA",

            "KAB DEMAK":
                "KABUPATEN DEMAK",

            "KAB SEMARANG":
                "KABUPATEN SEMARANG",

            "KAB TEMANGGUNG":
                "KABUPATEN TEMANGGUNG",

            "KAB KENDAL":
                "KABUPATEN KENDAL",

            "KAB BATANG":
                "KABUPATEN BATANG",

            "KAB PEKALONGAN":
                "KABUPATEN PEKALONGAN",

            "KAB PEMALANG":
                "KABUPATEN PEMALANG",

            "KAB TEGAL":
                "KABUPATEN TEGAL",

            "KAB BREBES":
                "KABUPATEN BREBES",

            "KOTA MAGELANG":
                "KOTA MAGELANG",

            "KOTA SURAKARTA":
                "KOTA SURAKARTA",

            "KOTA SALATIGA":
                "KOTA SALATIGA",

            "KOTA SEMARANG":
                "KOTA SEMARANG",

            "KOTA PEKALONGAN":
                "KOTA PEKALONGAN",

            "KOTA TEGAL":
                "KOTA TEGAL",


            # ====================================================
            # JAWA TIMUR
            # ====================================================

            "KAB PACITAN":
                "KABUPATEN PACITAN",

            "KAB PONOROGO":
                "KABUPATEN PONOROGO",

            "KAB TRENGGALEK":
                "KABUPATEN TRENGGALEK",

            "KAB TULUNGAGUNG":
                "KABUPATEN TULUNGAGUNG",

            "KAB BLITAR":
                "KABUPATEN BLITAR",

            "KAB KEDIRI":
                "KABUPATEN KEDIRI",

            "KAB MALANG":
                "KABUPATEN MALANG",

            "KAB LUMAJANG":
                "KABUPATEN LUMAJANG",

            "KAB JEMBER":
                "KABUPATEN JEMBER",

            "KAB BANYUWANGI":
                "KABUPATEN BANYUWANGI",

            "KAB BONDOWOSO":
                "KABUPATEN BONDOWOSO",

            "KAB SITUBONDO":
                "KABUPATEN SITUBONDO",

            "KAB PROBOLINGGO":
                "KABUPATEN PROBOLINGGO",

            "KAB PASURUAN":
                "KABUPATEN PASURUAN",

            "KAB SIDOARJO":
                "KABUPATEN SIDOARJO",

            "KAB MOJOKERTO":
                "KABUPATEN MOJOKERTO",

            "KAB JOMBANG":
                "KABUPATEN JOMBANG",

            "KAB NGANJUK":
                "KABUPATEN NGANJUK",

            "KAB MADIUN":
                "KABUPATEN MADIUN",

            "KAB MAGETAN":
                "KABUPATEN MAGETAN",

            "KAB NGAWI":
                "KABUPATEN NGAWI",

            "KAB BOJONEGORO":
                "KABUPATEN BOJONEGORO",

            "KAB TUBAN":
                "KABUPATEN TUBAN",

            "KAB LAMONGAN":
                "KABUPATEN LAMONGAN",

            "KAB GRESIK":
                "KABUPATEN GRESIK",

            "KAB BANGKALAN":
                "KABUPATEN BANGKALAN",

            "KAB SAMPANG":
                "KABUPATEN SAMPANG",

            "KAB PAMEKASAN":
                "KABUPATEN PAMEKASAN",

            "KAB SUMENEP":
                "KABUPATEN SUMENEP",

            "KOTA KEDIRI":
                "KOTA KEDIRI",

            "KOTA BLITAR":
                "KOTA BLITAR",

            "KOTA MALANG":
                "KOTA MALANG",

            "KOTA PROBOLINGGO":
                "KOTA PROBOLINGGO",

            "KOTA PASURUAN":
                "KOTA PASURUAN",

            "KOTA MOJOKERTO":
                "KOTA MOJOKERTO",

            "KOTA MADIUN":
                "KOTA MADIUN",

            "KOTA SURABAYA":
                "KOTA SURABAYA",

            "KOTA BATU":
                "KOTA BATU",


            # ====================================================
            # BALI
            # ====================================================

            "KAB JEMBRANA":
                "KABUPATEN JEMBRANA",

            "KAB TABANAN":
                "KABUPATEN TABANAN",

            "KAB BADUNG":
                "KABUPATEN BADUNG",

            "KAB GIANYAR":
                "KABUPATEN GIANYAR",

            "KAB KLUNGKUNG":
                "KABUPATEN KLUNGKUNG",

            "KAB BANGLI":
                "KABUPATEN BANGLI",

            "KAB KARANGASEM":
                "KABUPATEN KARANGASEM",

            "KAB BULELENG":
                "KABUPATEN BULELENG",

            "KOTA DENPASAR":
                "KOTA DENPASAR",


            # ====================================================
            # NUSA TENGGARA BARAT
            # ====================================================

            "KAB LOMBOK BARAT":
                "KABUPATEN LOMBOK BARAT",

            "KAB LOMBOK TENGAH":
                "KABUPATEN LOMBOK TENGAH",

            "KAB LOMBOK TIMUR":
                "KABUPATEN LOMBOK TIMUR",

            "KAB SUMBAWA":
                "KABUPATEN SUMBAWA",

            "KAB DOMPU":
                "KABUPATEN DOMPU",

            "KAB BIMA":
                "KABUPATEN BIMA",

            "KAB SUMBAWA BARAT":
                "KABUPATEN SUMBAWA BARAT",

            "KAB LOMBOK UTARA":
                "KABUPATEN LOMBOK UTARA",

            "KOTA MATARAM":
                "KOTA MATARAM",

            "KOTA BIMA":
                "KOTA BIMA",


            # ====================================================
            # NUSA TENGGARA TIMUR
            # ====================================================

            "KAB SUMBA BARAT":
                "KABUPATEN SUMBA BARAT",

            "KAB SUMBA TIMUR":
                "KABUPATEN SUMBA TIMUR",

            "KAB KUPANG":
                "KABUPATEN KUPANG",

            "KAB TIMOR TENGAH SELATAN":
                "KABUPATEN TIMOR TENGAH SELATAN",

            "KAB TIMOR TENGAH UTARA":
                "KABUPATEN TIMOR TENGAH UTARA",

            "KAB BELU":
                "KABUPATEN BELU",

            "KAB ALOR":
                "KABUPATEN ALOR",

            "KAB LEMBATA":
                "KABUPATEN LEMBATA",

            "KAB FLORES TIMUR":
                "KABUPATEN FLORES TIMUR",

            "KAB SIKKA":
                "KABUPATEN SIKKA",

            "KAB ENDE":
                "KABUPATEN ENDE",

            "KAB NGADA":
                "KABUPATEN NGADA",

            "KAB MANGGARAI":
                "KABUPATEN MANGGARAI",

            "KAB ROTE NDAO":
                "KABUPATEN ROTE NDAO",

            "KAB MANGGARAI BARAT":
                "KABUPATEN MANGGARAI BARAT",

            "KAB SUMBA TENGAH":
                "KABUPATEN SUMBA TENGAH",

            "KAB SUMBA BARAT DAYA":
                "KABUPATEN SUMBA BARAT DAYA",

            "KAB NAGEKEO":
                "KABUPATEN NAGEKEO",

            "KAB MANGGARAI TIMUR":
                "KABUPATEN MANGGARAI TIMUR",

            "KAB SABU RAIJUA":
                "KABUPATEN SABU RAIJUA",

            "KAB MALAKA":
                "KABUPATEN MALAKA",

            "KOTA KUPANG":
                "KOTA KUPANG",


            # ====================================================
            # KALIMANTAN BARAT
            # ====================================================

            "KAB SAMBAS":
                "KABUPATEN SAMBAS",

            "KAB BENGKAYANG":
                "KABUPATEN BENGKAYANG",

            "KAB LANDAK":
                "KABUPATEN LANDAK",

            "KAB MEMPAWAH":
                "KABUPATEN MEMPAWAH",

            "KAB SANGGAU":
                "KABUPATEN SANGGAU",

            "KAB KETAPANG":
                "KABUPATEN KETAPANG",

            "KAB SINTANG":
                "KABUPATEN SINTANG",

            "KAB KAPUAS HULU":
                "KABUPATEN KAPUAS HULU",

            "KAB SEKADAU":
                "KABUPATEN SEKADAU",

            "KAB MELAWI":
                "KABUPATEN MELAWI",

            "KAB KAYONG UTARA":
                "KABUPATEN KAYONG UTARA",

            "KAB KUBU RAYA":
                "KABUPATEN KUBU RAYA",

            "KOTA PONTIANAK":
                "KOTA PONTIANAK",

            "KOTA SINGKAWANG":
                "KOTA SINGKAWANG",


            # ====================================================
            # KALIMANTAN TENGAH
            # ====================================================

            "KAB KOTAWARINGIN BARAT":
                "KABUPATEN KOTAWARINGIN BARAT",

            "KAB KOTAWARINGIN TIMUR":
                "KABUPATEN KOTAWARINGIN TIMUR",

            "KAB KAPUAS":
                "KABUPATEN KAPUAS",

            "KAB BARITO SELATAN":
                "KABUPATEN BARITO SELATAN",

            "KAB BARITO UTARA":
                "KABUPATEN BARITO UTARA",

            "KAB SUKAMARA":
                "KABUPATEN SUKAMARA",

            "KAB LAMANDAU":
                "KABUPATEN LAMANDAU",

            "KAB SERUYAN":
                "KABUPATEN SERUYAN",

            "KAB KATINGAN":
                "KABUPATEN KATINGAN",

            "KAB PULANG PISAU":
                "KABUPATEN PULANG PISAU",

            "KAB GUNUNG MAS":
                "KABUPATEN GUNUNG MAS",

            "KAB BARITO TIMUR":
                "KABUPATEN BARITO TIMUR",

            "KAB MURUNG RAYA":
                "KABUPATEN MURUNG RAYA",

            "KOTA PALANGKA RAYA":
                "KOTA PALANGKA RAYA",


            # ====================================================
            # KALIMANTAN SELATAN
            # ====================================================

            "KAB TANAH LAUT":
                "KABUPATEN TANAH LAUT",

            "KAB KOTABARU":
                "KABUPATEN KOTABARU",

            "KAB BANJAR":
                "KABUPATEN BANJAR",

            "KAB BARITO KUALA":
                "KABUPATEN BARITO KUALA",

            "KAB TAPIN":
                "KABUPATEN TAPIN",

            "KAB HULU SUNGAI SELATAN":
                "KABUPATEN HULU SUNGAI SELATAN",

            "KAB HULU SUNGAI TENGAH":
                "KABUPATEN HULU SUNGAI TENGAH",

            "KAB HULU SUNGAI UTARA":
                "KABUPATEN HULU SUNGAI UTARA",

            "KAB TABALONG":
                "KABUPATEN TABALONG",

            "KAB TANAH BUMBU":
                "KABUPATEN TANAH BUMBU",

            "KAB BALANGAN":
                "KABUPATEN BALANGAN",

            "KOTA BANJARMASIN":
                "KOTA BANJARMASIN",

            "KOTA BANJARBARU":
                "KOTA BANJARBARU",


            # ====================================================
            # KALIMANTAN TIMUR
            # ====================================================

            "KAB PASER":
                "KABUPATEN PASER",

            "KAB KUTAI BARAT":
                "KABUPATEN KUTAI BARAT",

            "KAB KUTAI KARTANEGARA":
                "KABUPATEN KUTAI KARTANEGARA",

            "KAB KUTAI TIMUR":
                "KABUPATEN KUTAI TIMUR",

            "KAB BERAU":
                "KABUPATEN BERAU",

            "KAB PENAJAM PASER UTARA":
                "KABUPATEN PENAJAM PASER UTARA",

            "KAB MAHAKAM ULU":
                "KABUPATEN MAHAKAM ULU",

            "KOTA BALIKPAPAN":
                "KOTA BALIKPAPAN",

            "KOTA SAMARINDA":
                "KOTA SAMARINDA",

            "KOTA BONTANG":
                "KOTA BONTANG",


            # ====================================================
            # KALIMANTAN UTARA
            # ====================================================

            "KAB BULUNGAN":
                "KABUPATEN BULUNGAN",

            "KAB MALINAU":
                "KABUPATEN MALINAU",

            "KAB NUNUKAN":
                "KABUPATEN NUNUKAN",

            "KAB TANA TIDUNG":
                "KABUPATEN TANA TIDUNG",

            "KOTA TARAKAN":
                "KOTA TARAKAN",


            # ====================================================
            # SULAWESI UTARA
            # ====================================================

            "KAB BOLAANG MONGONDOW":
                "KABUPATEN BOLAANG MONGONDOW",

            "KAB MINAHASA":
                "KABUPATEN MINAHASA",

            "KAB KEPULAUAN SANGIHE":
                "KABUPATEN KEPULAUAN SANGIHE",

            "KAB KEPULAUAN TALAUD":
                "KABUPATEN KEPULAUAN TALAUD",

            "KAB MINAHASA SELATAN":
                "KABUPATEN MINAHASA SELATAN",

            "KAB MINAHASA UTARA":
                "KABUPATEN MINAHASA UTARA",

            "KAB BOLAANG MONGONDOW UTARA":
                "KABUPATEN BOLAANG MONGONDOW UTARA",

            "KAB SIAU TAGULANDANG BIARO":
                "KABUPATEN SIAU TAGULANDANG BIARO",

            "KAB MINAHASA TENGGARA":
                "KABUPATEN MINAHASA TENGGARA",

            "KAB BOLAANG MONGONDOW SELATAN":
                "KABUPATEN BOLAANG MONGONDOW SELATAN",

            "KAB BOLAANG MONGONDOW TIMUR":
                "KABUPATEN BOLAANG MONGONDOW TIMUR",

            "KOTA MANADO":
                "KOTA MANADO",

            "KOTA BITUNG":
                "KOTA BITUNG",

            "KOTA TOMOHON":
                "KOTA TOMOHON",

            "KOTA KOTAMOBAGU":
                "KOTA KOTAMOBAGU",


            # ====================================================
            # SULAWESI TENGAH
            # ====================================================

            "KAB BANGGAI":
                "KABUPATEN BANGGAI",

            "KAB POSO":
                "KABUPATEN POSO",

            "KAB DONGGALA":
                "KABUPATEN DONGGALA",

            "KAB TOLI TOLI":
                "KABUPATEN TOLI-TOLI",

            "KAB BUOL":
                "KABUPATEN BUOL",

            "KAB MOROWALI":
                "KABUPATEN MOROWALI",

            "KAB BANGGAI KEPULAUAN":
                "KABUPATEN BANGGAI KEPULAUAN",

            "KAB PARIGI MOUTONG":
                "KABUPATEN PARIGI MOUTONG",

            "KAB TOJO UNA UNA":
                "KABUPATEN TOJO UNA-UNA",

            "KAB SIGI":
                "KABUPATEN SIGI",

            "KAB BANGGAI LAUT":
                "KABUPATEN BANGGAI LAUT",

            "KAB MOROWALI UTARA":
                "KABUPATEN MOROWALI UTARA",

            "KOTA PALU":
                "KOTA PALU",


            # ====================================================
            # SULAWESI SELATAN
            # ====================================================

            "KAB KEPULAUAN SELAYAR":
                "KABUPATEN KEPULAUAN SELAYAR",

            "KAB BULUKUMBA":
                "KABUPATEN BULUKUMBA",

            "KAB BANTAENG":
                "KABUPATEN BANTAENG",

            "KAB JENEPONTO":
                "KABUPATEN JENEPONTO",

            "KAB TAKALAR":
                "KABUPATEN TAKALAR",

            "KAB GOWA":
                "KABUPATEN GOWA",

            "KAB SINJAI":
                "KABUPATEN SINJAI",

            "KAB MAROS":
                "KABUPATEN MAROS",

            "KAB PANGKAJENE DAN KEPULAUAN":
                "KABUPATEN PANGKAJENE DAN KEPULAUAN",

            "KAB BARRU":
                "KABUPATEN BARRU",

            "KAB BONE":
                "KABUPATEN BONE",

            "KAB SOPPENG":
                "KABUPATEN SOPPENG",

            "KAB WAJO":
                "KABUPATEN WAJO",

            "KAB SIDENRENG RAPPANG":
                "KABUPATEN SIDENRENG RAPPANG",

            "KAB PINRANG":
                "KABUPATEN PINRANG",

            "KAB ENREKANG":
                "KABUPATEN ENREKANG",

            "KAB LUWU":
                "KABUPATEN LUWU",

            "KAB TANA TORAJA":
                "KABUPATEN TANA TORAJA",

            "KAB LUWU UTARA":
                "KABUPATEN LUWU UTARA",

            "KAB LUWU TIMUR":
                "KABUPATEN LUWU TIMUR",

            "KAB TORAJA UTARA":
                "KABUPATEN TORAJA UTARA",

            "KOTA MAKASSAR":
                "KOTA MAKASSAR",

            "KOTA PAREPARE":
                "KOTA PAREPARE",

            "KOTA PALOPO":
                "KOTA PALOPO",


            # ====================================================
            # SULAWESI TENGGARA
            # ====================================================

            "KAB BUTON":
                "KABUPATEN BUTON",

            "KAB MUNA":
                "KABUPATEN MUNA",

            "KAB KONAWE":
                "KABUPATEN KONAWE",

            "KAB KOLAKA":
                "KABUPATEN KOLAKA",

            "KAB KONAWE SELATAN":
                "KABUPATEN KONAWE SELATAN",

            "KAB BOMBANA":
                "KABUPATEN BOMBANA",

            "KAB WAKATOBI":
                "KABUPATEN WAKATOBI",

            "KAB KOLAKA UTARA":
                "KABUPATEN KOLAKA UTARA",

            "KAB BUTON UTARA":
                "KABUPATEN BUTON UTARA",

            "KAB KONAWE UTARA":
                "KABUPATEN KONAWE UTARA",

            "KAB KOLAKA TIMUR":
                "KABUPATEN KOLAKA TIMUR",

            "KAB KONAWE KEPULAUAN":
                "KABUPATEN KONAWE KEPULAUAN",

            "KAB MUNA BARAT":
                "KABUPATEN MUNA BARAT",

            "KAB BUTON TENGAH":
                "KABUPATEN BUTON TENGAH",

            "KAB BUTON SELATAN":
                "KABUPATEN BUTON SELATAN",

            "KOTA KENDARI":
                "KOTA KENDARI",

            "KOTA BAUBAU":
                "KOTA BAUBAU",


            # ====================================================
            # GORONTALO
            # ====================================================

            "KAB BOALEMO":
                "KABUPATEN BOALEMO",

            "KAB GORONTALO":
                "KABUPATEN GORONTALO",

            "KAB POHUWATO":
                "KABUPATEN POHUWATO",

            "KAB BONE BOLANGO":
                "KABUPATEN BONE BOLANGO",

            "KAB GORONTALO UTARA":
                "KABUPATEN GORONTALO UTARA",

            "KOTA GORONTALO":
                "KOTA GORONTALO",


            # ====================================================
            # SULAWESI BARAT
            # ====================================================

            "KAB MAJENE":
                "KABUPATEN MAJENE",

            "KAB POLEWALI MANDAR":
                "KABUPATEN POLEWALI MANDAR",

            "KAB MAMASA":
                "KABUPATEN MAMASA",

            "KAB MAMUJU":
                "KABUPATEN MAMUJU",

            "KAB PASANGKAYU":
                "KABUPATEN PASANGKAYU",

            "KAB MAMUJU TENGAH":
                "KABUPATEN MAMUJU TENGAH",


            # ====================================================
            # MALUKU
            # ====================================================

            "KAB MALUKU TENGAH":
                "KABUPATEN MALUKU TENGAH",

            "KAB MALUKU TENGGARA":
                "KABUPATEN MALUKU TENGGARA",

            "KAB KEPULAUAN TANIMBAR":
                "KABUPATEN KEPULAUAN TANIMBAR",

            "KAB BURU":
                "KABUPATEN BURU",

            "KAB SERAM BAGIAN BARAT":
                "KABUPATEN SERAM BAGIAN BARAT",

            "KAB SERAM BAGIAN TIMUR":
                "KABUPATEN SERAM BAGIAN TIMUR",

            "KAB ARU":
                "KABUPATEN KEPULAUAN ARU",

            "KAB MALUKU BARAT DAYA":
                "KABUPATEN MALUKU BARAT DAYA",

            "KAB BURU SELATAN":
                "KABUPATEN BURU SELATAN",

            "KOTA AMBON":
                "KOTA AMBON",

            "KOTA TUAL":
                "KOTA TUAL",


            # ====================================================
            # MALUKU UTARA
            # ====================================================

            "KAB HALMAHERA BARAT":
                "KABUPATEN HALMAHERA BARAT",

            "KAB HALMAHERA TENGAH":
                "KABUPATEN HALMAHERA TENGAH",

            "KAB KEPULAUAN SULA":
                "KABUPATEN KEPULAUAN SULA",

            "KAB HALMAHERA SELATAN":
                "KABUPATEN HALMAHERA SELATAN",

            "KAB HALMAHERA UTARA":
                "KABUPATEN HALMAHERA UTARA",

            "KAB HALMAHERA TIMUR":
                "KABUPATEN HALMAHERA TIMUR",

            "KAB PULAU MOROTAI":
                "KABUPATEN PULAU MOROTAI",

            "KAB PULAU TALIABU":
                "KABUPATEN PULAU TALIABU",

            "KOTA TERNATE":
                "KOTA TERNATE",

            "KOTA TIDORE KEPULAUAN":
                "KOTA TIDORE KEPULAUAN",


            # ====================================================
            # PAPUA BARAT
            # ====================================================

            "KAB FAKFAK":
                "KABUPATEN FAKFAK",

            "KAB KAIMANA":
                "KABUPATEN KAIMANA",

            "KAB TELUK WONDAMA":
                "KABUPATEN TELUK WONDAMA",

            "KAB TELUK BINTUNI":
                "KABUPATEN TELUK BINTUNI",

            "KAB MANOKWARI":
                "KABUPATEN MANOKWARI",

            "KAB SORONG SELATAN":
                "KABUPATEN SORONG SELATAN",

            "KAB SORONG":
                "KABUPATEN SORONG",

            "KAB RAJA AMPAT":
                "KABUPATEN RAJA AMPAT",

            "KAB TAMRAUW":
                "KABUPATEN TAMRAUW",

            "KAB MAYBRAT":
                "KABUPATEN MAYBRAT",

            "KAB MANOKWARI SELATAN":
                "KABUPATEN MANOKWARI SELATAN",

            "KAB PEGUNUNGAN ARFAK":
                "KABUPATEN PEGUNUNGAN ARFAK",

            "KOTA SORONG":
                "KOTA SORONG",


            # ====================================================
            # PAPUA
            # ====================================================

            "KAB JAYAPURA":
                "KABUPATEN JAYAPURA",

            "KAB KEPULAUAN YAPEN":
                "KABUPATEN KEPULAUAN YAPEN",

            "KAB BIAK NUMFOR":
                "KABUPATEN BIAK NUMFOR",

            "KAB PANIAI":
                "KABUPATEN PANIAI",

            "KAB PUNCAK JAYA":
                "KABUPATEN PUNCAK JAYA",

            "KAB NABIRE":
                "KABUPATEN NABIRE",

            "KAB MIMIKA":
                "KABUPATEN MIMIKA",

            "KAB BOVEN DIGOEL":
                "KABUPATEN BOVEN DIGOEL",

            "KAB MERAUKE":
                "KABUPATEN MERAUKE",

            "KAB ASMAT":
                "KABUPATEN ASMAT",

            "KAB YAHUKIMO":
                "KABUPATEN YAHUKIMO",

            "KAB PEGUNUNGAN BINTANG":
                "KABUPATEN PEGUNUNGAN BINTANG",

            "KAB TOLIKARA":
                "KABUPATEN TOLIKARA",

            "KAB WAROPEN":
                "KABUPATEN WAROPEN",

            "KAB SUPIORI":
                "KABUPATEN SUPIORI",

            "KAB MAMBERAMO RAYA":
                "KABUPATEN MAMBERAMO RAYA",

            "KAB NDUGA":
                "KABUPATEN NDUGA",

            "KAB LANNY JAYA":
                "KABUPATEN LANNY JAYA",

            "KAB MAMBERAMO TENGAH":
                "KABUPATEN MAMBERAMO TENGAH",

            "KAB YALIMO":
                "KABUPATEN YALIMO",

            "KAB DOGIYAI":
                "KABUPATEN DOGIYAI",

            "KAB DEIYAI":
                "KABUPATEN DEIYAI",

            "KAB INTAN JAYA":
                "KABUPATEN INTAN JAYA",

            "KOTA JAYAPURA":
                "KOTA JAYAPURA",


            # ====================================================
            # PAPUA SELATAN
            # ====================================================

            "KAB MERAUKE":
                "KABUPATEN MERAUKE",

            "KAB BOVEN DIGOEL":
                "KABUPATEN BOVEN DIGOEL",

            "KAB MAPPI":
                "KABUPATEN MAPPI",

            "KAB ASMAT":
                "KABUPATEN ASMAT",


            # ====================================================
            # PAPUA TENGAH
            # ====================================================

            "KAB NABIRE":
                "KABUPATEN NABIRE",

            "KAB PUNCAK":
                "KABUPATEN PUNCAK",

            "KAB PANIAI":
                "KABUPATEN PANIAI",

            "KAB MIMIKA":
                "KABUPATEN MIMIKA",

            "KAB PUNCAK JAYA":
                "KABUPATEN PUNCAK JAYA",

            "KAB DOGIYAI":
                "KABUPATEN DOGIYAI",

            "KAB INTAN JAYA":
                "KABUPATEN INTAN JAYA",

            "KAB DEIYAI":
                "KABUPATEN DEIYAI",


            # ====================================================
            # PAPUA PEGUNUNGAN
            # ====================================================

            "KAB JAYAWIJAYA":
                "KABUPATEN JAYAWIJAYA",

            "KAB PEGUNUNGAN BINTANG":
                "KABUPATEN PEGUNUNGAN BINTANG",

            "KAB YAHUKIMO":
                "KABUPATEN YAHUKIMO",

            "KAB TOLIKARA":
                "KABUPATEN TOLIKARA",

            "KAB MAMBERAMO TENGAH":
                "KABUPATEN MAMBERAMO TENGAH",

            "KAB YALIMO":
                "KABUPATEN YALIMO",

            "KAB LANNY JAYA":
                "KABUPATEN LANNY JAYA",

            "KAB NDUGA":
                "KABUPATEN NDUGA",


            # ====================================================
            # PAPUA BARAT DAYA
            # ====================================================

            "KAB SORONG":
                "KABUPATEN SORONG",

            "KAB SORONG SELATAN":
                "KABUPATEN SORONG SELATAN",

            "KAB RAJA AMPAT":
                "KABUPATEN RAJA AMPAT",

            "KAB TAMRAUW":
                "KABUPATEN TAMRAUW",

            "KAB MAYBRAT":
                "KABUPATEN MAYBRAT",

            "KAB MAJELIA":
                "KABUPATEN MAJELIA",

            "KOTA SORONG":
                "KOTA SORONG",
        }

        return kabupaten_kota_mapping.get(
            value,
            value
        )


    # ========================================================
    # MATCH DATA 32679
    # ========================================================

    def match_32679(
        self,
        df_pdf,
        df_xflask
    ):

        print("\n========================================")
        print("MATCH DATA ID 32679")
        print("========================================")

        pdf = df_pdf.copy()

        db = df_xflask.copy()

        # ----------------------------------------------------
        # VALIDASI
        # ----------------------------------------------------

        required_pdf = [
            "provinsi",
            "data_x",
            "data_y",
        ]

        required_db = [
            "provinsi",
            "id_provinsi",
            "kode_bps",
        ]

        for column in required_pdf:

            if column not in pdf.columns:

                raise RuntimeError(
                    f"Kolom PDF tidak ditemukan: {column}"
                )

        for column in required_db:

            if column not in db.columns:

                raise RuntimeError(
                    f"Kolom XFlask tidak ditemukan: {column}"
                )

        # ----------------------------------------------------
        # NORMALIZE PROVINSI
        # ----------------------------------------------------

        pdf["nama_match"] = (
            pdf["provinsi"]
            .apply(self.normalize_province)
        )

        db["nama_match"] = (
            db["provinsi"]
            .apply(self.normalize_province)
        )

        # ----------------------------------------------------
        # MAPPING XFLASK
        # ----------------------------------------------------

        mapping = (
            db[
                [
                    "nama_match",
                    "provinsi",
                    "id_provinsi",
                    "kode_bps",
                ]
            ]
            .drop_duplicates(
                subset=["nama_match"]
            )
        )

        # ----------------------------------------------------
        # MERGE
        # ----------------------------------------------------

        result = pdf.merge(
            mapping,
            on="nama_match",
            how="left",
            suffixes=(
                "_pdf",
                "_xflask"
            )
        )

        # ----------------------------------------------------
        # STATUS MATCH
        # ----------------------------------------------------

        result["status_match"] = "PROVINSI BARU"

        result.loc[
            result["id_provinsi"].notna(),
            "status_match"
        ] = "MATCH"

        # ----------------------------------------------------
        # REPORT
        # ----------------------------------------------------

        new_province = result[
            result["status_match"]
            == "PROVINSI BARU"
        ].copy()

        print(
            "TOTAL DATA PDF:",
            len(pdf)
        )

        print(
            "TOTAL DATA MATCH:",
            len(
                result[
                    result["status_match"] == "MATCH"
                ]
            )
        )

        print(
            "TOTAL PROVINSI BARU:",
            len(new_province)
        )

        if not new_province.empty:

            print("\nPROVINSI BELUM MATCH:")

            print(
                new_province[
                    [
                        "provinsi_pdf",
                        "tahun"
                    ]
                ]
                .drop_duplicates()
                .to_string(index=False)
            )

        return result

    # ========================================================
    # MATCH DATA 32677
    # ========================================================

    def match_32677(
        self,
        df_pdf,
        df_xflask
    ):

        print("\n========================================")
        print("MATCH DATA ID 32677")
        print("========================================")

        pdf = df_pdf.copy()
        db = df_xflask.copy()

        required_pdf = [
            "provinsi",
            "data_x",
            "data_y",
        ]

        required_db = [
            "provinsi",
            "id_provinsi",
            "kode_bps",
        ]

        for column in required_pdf:
            if column not in pdf.columns:
                raise RuntimeError(
                    f"Kolom PDF tidak ditemukan: {column}"
                )

        for column in required_db:
            if column not in db.columns:
                raise RuntimeError(
                    f"Kolom XFlask tidak ditemukan: {column}"
                )

        # Normalize provinsi
        pdf["nama_match"] = (
            pdf["provinsi"]
            .apply(self.normalize_province)
        )

        db["nama_match"] = (
            db["provinsi"]
            .apply(self.normalize_province)
        )

        # Mapping metadata dari xFlask
        mapping = (
            db[
                [
                    "nama_match",
                    "provinsi",
                    "id_provinsi",
                    "kode_bps",
                ]
            ]
            .drop_duplicates(
                subset=["nama_match"]
            )
        )

        # Merge
        result = pdf.merge(
            mapping,
            on="nama_match",
            how="left",
            suffixes=(
                "_pdf",
                "_xflask"
            )
        )

        # Status match
        result["status_match"] = "PROVINSI BARU"

        result.loc[
            result["id_provinsi"].notna(),
            "status_match"
        ] = "MATCH"

        # Report
        new_province = result[
            result["status_match"] == "PROVINSI BARU"
        ].copy()

        print("TOTAL DATA PDF:", len(pdf))

        print(
            "TOTAL DATA MATCH:",
            len(
                result[
                    result["status_match"] == "MATCH"
                ]
            )
        )

        print(
            "TOTAL PROVINSI BARU:",
            len(new_province)
        )

        if not new_province.empty:

            print("\n⚠️ PROVINSI BELUM MATCH:")

            print(
                new_province[
                    [
                        "provinsi_pdf",
                        "tahun"
                    ]
                ]
                .drop_duplicates()
                .to_string(index=False)
            )

        return result

    # --------------------------------------------------------
    # Match 32678
    # --------------------------------------------------------

    def match_32678(
        self,
        df_pdf,
        df_existing
    ):

        print("\n========================================")
        print("MATCH DATA ID 32678")
        print("========================================")

        df_pdf = df_pdf.copy()
        df_existing = df_existing.copy()

        df_pdf["provinsi_normalized"] = (
            df_pdf["provinsi"]
            .apply(self.normalize_province)
        )

        df_existing["provinsi_normalized"] = (
            df_existing["provinsi"]
            .apply(self.normalize_province)
        )

        df_existing["data_x"] = (
            df_existing["data_x"]
            .astype(str)
            .str.strip()
        )

        existing_keys = set(
            zip(
                df_existing["data_x"],
                df_existing["provinsi_normalized"]
            )
        )

        df_pdf["match"] = df_pdf.apply(
            lambda row: (
                row["data_x"],
                row["provinsi_normalized"]
            ) in existing_keys,
            axis=1
        )

        df_match = df_pdf[
            df_pdf["match"]
        ].copy()

        df_unmatch = df_pdf[
            ~df_pdf["match"]
        ].copy()

        print(
            f"TOTAL DATA PDF: {len(df_pdf)}"
        )

        print(
            f"TOTAL DATA MATCH: {len(df_match)}"
        )

        print(
            f"TOTAL PROVINSI BARU: {len(df_unmatch)}"
        )

        if not df_unmatch.empty:

            print(
                "\n⚠️ PROVINSI BELUM MATCH:"
            )

            print(
                df_unmatch[
                    ["provinsi", "tahun"]
                ]
                .to_string(index=False)
            )

        return df_match

    # --------------------------------------------------------
    # Match 2883
    # --------------------------------------------------------

    def match_2883(self, df_pdf, df_existing):

        print("\n=== MATCHING 2883 ===")

        pdf = df_pdf.copy()
        db = df_existing.copy()

        # ==========================================
        # NORMALISASI NAMA KABUPATEN/KOTA
        # ==========================================

        pdf["kota_normalized"] = (
            pdf["kabupaten_kota"]
            .apply(self.normalize_kabupaten_kota)
        )

        db["kota_normalized"] = (
            db["kota"]
            .apply(self.normalize_kabupaten_kota)
        )

        # ==========================================
        # CEK MATCH
        # ==========================================

        existing_keys = set(
            zip(
                db["data_x"].astype(str).str.strip(),
                db["kota_normalized"]
            )
        )

        pdf["is_match"] = pdf.apply(
            lambda row: (
                str(row["data_x"]).strip(),
                row["kota_normalized"]
            ) in existing_keys,
            axis=1
        )

        # ==========================================
        # HASIL
        # ==========================================

        total = len(pdf)
        matched = pdf["is_match"].sum()
        not_matched = total - matched

        print(f"Total PDF       : {total}")
        print(f"Matched         : {matched}")
        print(f"Not matched     : {not_matched}")

        print("\n=== CONTOH MATCH ===")

        print(
            pdf[pdf["is_match"]][
                ["kabupaten_kota", "kota_normalized", "data_x"]
            ]
            .drop_duplicates()
            .head(20)
            .to_string(index=False)
        )

        print("\n=== CONTOH TIDAK MATCH ===")

        print(
            pdf[~pdf["is_match"]][
                ["kabupaten_kota", "kota_normalized", "data_x"]
            ]
            .drop_duplicates()
            .head(20)
            .to_string(index=False)
        )

        return pdf

    # ========================================================
    # TRANSFORM
    # ========================================================

    def transform_32679(self, df_pdf):

        print("\n========================================")
        print("TRANSFORM DATA ID 32679")
        print("========================================")

        df = df_pdf.copy()

        # Normalize provinsi

        df["provinsi"] = (
            df["provinsi"]
            .apply(self.normalize_province)
        )

        # Normalize tanggal

        df["data_x"] = (
            df["data_x"]
            .astype(str)
            .str.strip()
        )

        # Pastikan produksi numeric

        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce"
        )

        # Buang data invalid

        df = df.dropna(
            subset=[
                "provinsi",
                "data_x",
                "data_y"
            ]
        )

        # Hapus duplicate
        # satu provinsi satu tahun

        df = (
            df
            .drop_duplicates(
                subset=[
                    "provinsi",
                    "tahun"
                ]
            )
            .reset_index(drop=True)
        )

        print(
            f"Total data setelah transform: "
            f"{len(df)}"
        )

        return df

    # ========================================================
    # TRANSFORM 32677
    # ========================================================

    def transform_32677(self, df_pdf):

        print("\n========================================")
        print("TRANSFORM DATA ID 32677")
        print("========================================")

        df = df_pdf.copy()

        # Normalize provinsi
        df["provinsi"] = (
            df["provinsi"]
            .apply(self.normalize_province)
        )

        # Normalize tanggal
        df["data_x"] = (
            df["data_x"]
            .astype(str)
            .str.strip()
        )

        # Pastikan produksi numeric
        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce"
        )

        # Buang data invalid
        df = df.dropna(
            subset=[
                "provinsi",
                "data_x",
                "data_y"
            ]
        )

        # Hapus duplicate
        df = (
            df
            .drop_duplicates(
                subset=[
                    "provinsi",
                    "tahun"
                ]
            )
            .reset_index(drop=True)
        )

        print(
            f"Total data setelah transform: {len(df)}"
        )

        return df

    # --------------------------------------------------------
    # Transform 32678
    # --------------------------------------------------------

    def transform_32678(self, df):

        print("\n========================================")
        print("TRANSFORM DATA ID 32678")
        print("========================================")

        df = df.copy()

        df["data_x"] = (
            pd.to_datetime(
                df["data_x"],
                format="%d-%m-%Y"
            )
            .dt.strftime("%d-%m-%Y")
        )

        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["data_y"]
        )

        print(
            f"Total data setelah transform: {len(df)}"
        )

        return df

    # --------------------------------------------------------
    # Transform 2883
    # --------------------------------------------------------

    def transform_2883(self, df_pdf):

        print("\n========================================")
        print("TRANSFORM DATA ID 2883")
        print("========================================")

        df = df_pdf.copy()

        if df.empty:
            return df

        # Rapikan nama kabupaten/kota
        df["kabupaten_kota"] = (
            df["kabupaten_kota"]
            .astype(str)
            .str.strip()
        )

        # Rapikan tanggal
        df["data_x"] = (
            pd.to_datetime(
                df["data_x"],
                format="%d-%m-%Y"
            )
            .dt.strftime("%d-%m-%Y")
        )

        # Pastikan produksi numeric
        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce"
        )

        # Buang data invalid
        df = df.dropna(
            subset=[
                "kabupaten_kota",
                "data_x",
                "data_y"
            ]
        )

        # Satu kabupaten/kota satu tahun
        df = (
            df
            .drop_duplicates(
                subset=[
                    "kabupaten_kota",
                    "tahun"
                ]
            )
            .reset_index(drop=True)
        )

        print(
            f"Total data setelah transform: "
            f"{len(df)}"
        )

        return df

    # ========================================================
    # SAVE RAW PARQUET
    # ========================================================

    def save_raw_parquet(self, df, id_nama_data):

        print("\n========================================")
        print("SAVE RAW PARQUET")
        print("========================================")

        raw_folder = (
            RAW_BASE_DIR
            / DATA_CONFIG[id_nama_data]["raw_folder"]
        )

        raw_folder.mkdir(
            parents=True,
            exist_ok=True
        )

        for tahun, df_year in df.groupby("tahun"):

            filename = (
                f"{DATA_CONFIG[id_nama_data]['raw_folder']}"
                f"_{tahun}_raw.parquet"
            )

            output_path = raw_folder / filename

            df_year.to_parquet(
                output_path,
                index=False
            )

            print(
                f"Saved RAW {tahun}: "
                f"{len(df_year)} rows -> {output_path}"
            )

            # UPLOAD RAW KE MINIO
            self.upload_raw_minio(
                raw_file=output_path,
                tahun=int(tahun),
                id_nama_data=id_nama_data
            )

    # ========================================================
    # UPLOAD RAW MINIO
    # ========================================================

    def upload_raw_minio(
        self,
        raw_file,
        tahun,
        id_nama_data
    ):

        storage_name = DATA_CONFIG[id_nama_data]["raw_folder"]

        filename = (
            f"{storage_name}_{tahun}_raw.parquet"
        )

        with open(raw_file, "rb") as file:
            raw_bytes = file.read()

        object_name = (
            f"kemenko/"
            f"{storage_name}/raw/"
            f"{filename}"
        )

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            raw_bytes,
            object_name,
            "application/octet-stream",
        )

        print("RAW MinIO upload berhasil:")
        print(
            f"{MINIO_BUCKET}/"
            f"{object_name}"
        )

    def save_final_32679(self, df):
        output_dir = Path(
            "data/final/kemenko/produksi_kelapa_sawit_bumn_provinsi"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = (
            output_dir /
            "produksi_kelapa_sawit_bumn_provinsi.csv"
        )

        df.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig"
        )

        print(f"Final CSV saved: {output_path}")

        return output_path

    # ========================================================
    # BUILD FINAL
    # ========================================================

    def build_final_32679(
        self,
        df_pdf,
        df_existing
    ):

        print("\n========================================")
        print("BUILD FINAL DATA ID 32679")
        print("========================================")

        if df_existing.empty:

            raise ValueError(
                "Data existing xFlask kosong."
            )

        df_existing = df_existing.copy()

        df_existing["provinsi_normalized"] = (
            df_existing["provinsi"]
            .apply(self.normalize_province)
        )

        xflask_columns = [

            "data_x",
            "data_y",
            "date",
            "frekuensi",
            "id",
            "id_kota",
            "id_nama_data",
            "id_provinsi",
            "indikator",
            "institusi",
            "item",
            "ket_tambahan",
            "kode_bps",
            "kota",
            "leveldata",
            "nama",
            "nama_data_import",
            "nama_alias",
            "negara",
            "provinsi",
            "satuan",
            "sumber",

        ]

        final_rows = []

        for _, row in df_pdf.iterrows():

            provinsi = row["provinsi"]

            tahun = row["tahun"]

            metadata = df_existing[
                df_existing["provinsi_normalized"]
                == provinsi
            ]

            if not metadata.empty:

                template = metadata.iloc[0].copy()

            elif provinsi == "SUMATERA UTARA":

                template = pd.Series({

                    "data_x": None,
                    "data_y": None,
                    "date": None,
                    "frekuensi": "tahun",
                    "id": None,
                    "id_kota": 0,
                    "id_nama_data": 32679,
                    "id_provinsi": 2,

                    "indikator":
                        "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi",

                    "institusi":
                        "Kementerian Koordinator Bidang Perekonomian",

                    "item": None,
                    "ket_tambahan": None,
                    "kode_bps": "1200",
                    "kota": None,
                    "leveldata": "provinsi",

                    "nama":
                        "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi",

                    "nama_alias": "Sumatera Utara",
                    "negara": "Indonesia",
                    "provinsi": "SUMATERA UTARA",
                    "satuan": "Ton",
                    "sumber": None,

                })

            else:

                print(
                    f"⚠️ Metadata provinsi tidak ditemukan: "
                    f"{provinsi}"
                )

                continue

            new_row = {}

            for column in xflask_columns:

                if column in template.index:

                    new_row[column] = template[column]

                else:

                    new_row[column] = None

            # Data dari PDF

            new_row["data_x"] = row["data_x"]

            new_row["data_y"] = row["data_y"]

            new_row["date"] = pd.to_datetime(
                row["data_x"],
                format="%d-%m-%Y"
            )

            new_row["id_nama_data"] = 32679

            new_row["nama"] = (
                "Produksi Kelapa Sawit Milik BUMN Menurut Provinsi"
            )

            new_row["nama_data_import"] = (
                "s3://maganghub/"
                "kemenko/"
                "produksi_kelapa_sawit_bumn_provinsi/"
                "final/"
                "produksi_kelapa_sawit_bumn_provinsi.csv"
            )

            new_row["id"] = None

            new_row["provinsi"] = provinsi

            new_row["leveldata"] = "provinsi"

            new_row["satuan"] = "Ton"

            new_row["frekuensi"] = "tahun"

            # ----------------------------------------------------
            # Source berdasarkan PDF final yang digunakan per tahun
            # ----------------------------------------------------

            if tahun == 2019:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan Nasional 2019-2021"
                )

            elif tahun == 2020:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2020-2022"
                )

            elif tahun == 2021:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2021-2023"
                )

            elif tahun == 2022:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2022-2024"
                )

            elif tahun == 2023:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2023-2025"
                )

            elif tahun in [2024, 2025, 2026]:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2024-2026"
                )

            new_row["ket_tambahan"] = (
                "Data produksi kelapa sawit Perkebunan Besar Negara "
                "(PBN/BUMN) menurut provinsi berdasarkan publikasi "
                "Statistik Perkebunan Direktorat Jenderal Perkebunan."
            )

            final_rows.append(new_row)

        return pd.DataFrame(
            final_rows,
            columns=xflask_columns
        )

    # ========================================================
    # BUILD FINAL 32677
    # ========================================================

    def build_final_32677(
        self,
        df_pdf,
        df_existing
    ):

        print("\n========================================")
        print("BUILD FINAL DATA ID 32677")
        print("========================================")

        if df_existing.empty:
            raise ValueError(
                "Data existing xFlask kosong. "
                "Metadata provinsi tidak bisa digunakan."
            )

        df_existing = df_existing.copy()

        # ----------------------------------------------------
        # Normalize existing province
        # ----------------------------------------------------

        df_existing["provinsi_normalized"] = (
            df_existing["provinsi"]
            .apply(self.normalize_province)
        )

        # ----------------------------------------------------
        # Schema xFlask
        # ----------------------------------------------------

        xflask_columns = [
            "data_x",
            "data_y",
            "date",
            "frekuensi",
            "id",
            "id_kota",
            "id_nama_data",
            "id_provinsi",
            "indikator",
            "institusi",
            "item",
            "ket_tambahan",
            "kode_bps",
            "kota",
            "leveldata",
            "nama",
            "nama_data_import",
            "nama_alias",
            "negara",
            "provinsi",
            "satuan",
            "sumber",
        ]

        final_rows = []

        # ----------------------------------------------------
        # Metadata lookup
        # ----------------------------------------------------

        metadata_lookup = (
            df_existing
            .drop_duplicates(
                subset=["provinsi_normalized"]
            )
            .set_index("provinsi_normalized")
        )

        # ----------------------------------------------------
        # Build final
        # ----------------------------------------------------

        for _, row in df_pdf.iterrows():

            provinsi = row["provinsi"]
            tahun = row["tahun"]

            # Cari metadata provinsi
            # ------------------------------------------------

            if provinsi in metadata_lookup.index:

                # Metadata tersedia di xFlask
                template = metadata_lookup.loc[provinsi].copy()

            elif provinsi in PROVINCE_METADATA_32677:

                # Metadata belum ada di xFlask,
                # gunakan mapping provinsi yang sudah tersedia
                metadata = PROVINCE_METADATA_32677[provinsi]

                template = pd.Series({
                    "data_x": None,
                    "data_y": None,
                    "date": None,
                    "frekuensi": "tahun",
                    "id": None,
                    "id_kota": 0,
                    "id_nama_data": 32677,
                    "id_provinsi": metadata["id_provinsi"],
                    "indikator": (
                        "Produksi Kelapa Sawit Milik Swasta Menurut Provinsi"
                    ),
                    "institusi": (
                        "Kementerian Koordinator Bidang Perekonomian"
                    ),
                    "item": None,
                    "ket_tambahan": None,
                    "kode_bps": metadata["kode_bps"],
                    "kota": None,
                    "leveldata": "provinsi",
                    "nama": (
                        "Produksi Kelapa Sawit Milik Swasta Menurut Provinsi"
                    ),
                    "nama_data_import": None,
                    "nama_alias": provinsi.title(),
                    "negara": "Indonesia",
                    "provinsi": provinsi,
                    "satuan": "Ton",
                    "sumber": None,
                })

                print(
                    f"ℹ️ Gunakan metadata mapping: {provinsi}"
                )

            else:

                # Belum ada metadata xFlask maupun mapping
                print(
                    f"⚠️ Metadata provinsi belum tersedia: "
                    f"{provinsi}"
                )

                continue

            new_row = {}

            # ------------------------------------------------
            # Ambil metadata dari xFlask
            # ------------------------------------------------

            for column in xflask_columns:

                if column in template.index:
                    new_row[column] = template[column]
                else:
                    new_row[column] = None

            # ------------------------------------------------
            # Override data dari PDF
            # ------------------------------------------------

            new_row["data_x"] = row["data_x"]

            new_row["data_y"] = row["data_y"]

            new_row["date"] = pd.to_datetime(
                row["data_x"],
                format="%d-%m-%Y"
            )

            # ------------------------------------------------
            # Metadata dataset 32677
            # ------------------------------------------------

            new_row["id_nama_data"] = 32677

            new_row["nama"] = (
                "Produksi Kelapa Sawit Milik Swasta Menurut Provinsi"
            )

            new_row["indikator"] = (
                "Produksi Kelapa Sawit Milik Swasta Menurut Provinsi"
            )

            new_row["nama_data_import"] = (
                "s3://maganghub/"
                "kemenko/"
                "produksi_kelapa_sawit_swasta_provinsi/"
                "final/"
                "produksi_kelapa_sawit_swasta_provinsi.csv"
            )

            # ID row baru belum ditentukan
            new_row["id"] = None

            # Dataset level
            new_row["provinsi"] = provinsi

            new_row["id_kota"] = 0

            new_row["kota"] = None

            new_row["leveldata"] = "provinsi"

            new_row["satuan"] = "Ton"

            new_row["frekuensi"] = "tahun"

            new_row["item"] = None

            new_row["negara"] = "Indonesia"

            # ----------------------------------------------------
            # Source
            # ----------------------------------------------------

            if tahun == 2019:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan Nasional 2019-2021"
                )

            elif tahun == 2020:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2020-2022"
                )

            elif tahun == 2021:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2021-2023"
                )

            elif tahun == 2022:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2022-2024"
                )

            elif tahun == 2023:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2023-2025"
                )

            elif tahun in [2024, 2025, 2026]:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2024-2026"
                )

            # ------------------------------------------------
            # Keterangan
            # ------------------------------------------------

            new_row["ket_tambahan"] = (
                "Data produksi kelapa sawit "
                "Perkebunan Besar Swasta (PBS) "
                "menurut provinsi berdasarkan publikasi "
                "Statistik Perkebunan Direktorat Jenderal "
                "Perkebunan."
            )

            final_rows.append(new_row)

        # ----------------------------------------------------
        # DataFrame final
        # ----------------------------------------------------

        df_final = pd.DataFrame(
            final_rows,
            columns=xflask_columns
        )

        print(
            f"Total final rows: {len(df_final)}"
        )

        return df_final

    def build_final_32678(
        self,
        df_pdf,
        df_existing
    ):

        print("\n========================================")
        print("BUILD FINAL DATA ID 32678")
        print("========================================")

        if df_existing.empty:
            raise ValueError(
                "Data existing xFlask kosong. "
                "Metadata provinsi tidak bisa digunakan."
            )

        df_existing = df_existing.copy()

        df_existing["provinsi_normalized"] = (
            df_existing["provinsi"]
            .apply(self.normalize_province)
        )

        xflask_columns = [
            "data_x",
            "data_y",
            "date",
            "frekuensi",
            "id",
            "id_kota",
            "id_nama_data",
            "id_provinsi",
            "indikator",
            "institusi",
            "item",
            "ket_tambahan",
            "kode_bps",
            "kota",
            "leveldata",
            "nama",
            "nama_data_import",
            "nama_alias",
            "negara",
            "provinsi",
            "satuan",
            "sumber",
        ]

        final_rows = []

        metadata_lookup = (
            df_existing
            .drop_duplicates(
                subset=["provinsi_normalized"]
            )
            .set_index("provinsi_normalized")
        )

        for _, row in df_pdf.iterrows():

            provinsi = row["provinsi"]
            tahun = row["tahun"]

            if provinsi in metadata_lookup.index:

                template = (
                    metadata_lookup
                    .loc[provinsi]
                    .copy()
                )

            elif provinsi in PROVINCE_METADATA_32678:

                metadata = (
                    PROVINCE_METADATA_32678[
                        provinsi
                    ]
                )

                template = pd.Series({
                    "data_x": None,
                    "data_y": None,
                    "date": None,
                    "frekuensi": "tahun",
                    "id": None,
                    "id_kota": 0,
                    "id_nama_data": 32678,
                    "id_provinsi": metadata["id_provinsi"],
                    "indikator": (
                        "Produksi Kelapa Sawit Milik Rakyat "
                        "Menurut Provinsi"
                    ),
                    "institusi": (
                        "Kementerian Koordinator Bidang "
                        "Perekonomian"
                    ),
                    "item": None,
                    "ket_tambahan": None,
                    "kode_bps": metadata["kode_bps"],
                    "kota": None,
                    "leveldata": "provinsi",
                    "nama": (
                        "Produksi Kelapa Sawit Milik Rakyat "
                        "Menurut Provinsi"
                    ),
                    "nama_data_import": None,
                    "nama_alias": provinsi.title(),
                    "negara": "Indonesia",
                    "provinsi": provinsi,
                    "satuan": "Ton",
                    "sumber": None,
                })

                print(
                    f"ℹ️ Gunakan metadata mapping: "
                    f"{provinsi}"
                )

            else:

                print(
                    f"⚠️ Metadata provinsi belum tersedia: "
                    f"{provinsi}"
                )

                continue

            new_row = {}

            for column in xflask_columns:

                if column in template.index:
                    new_row[column] = template[column]
                else:
                    new_row[column] = None

            new_row["data_x"] = row["data_x"]
            new_row["data_y"] = row["data_y"]

            new_row["date"] = pd.to_datetime(
                row["data_x"],
                format="%d-%m-%Y"
            )

            new_row["id_nama_data"] = 32678

            new_row["nama"] = (
                "Produksi Kelapa Sawit Milik Rakyat "
                "Menurut Provinsi"
            )

            new_row["indikator"] = (
                "Produksi Kelapa Sawit Milik Rakyat "
                "Menurut Provinsi"
            )

            new_row["nama_data_import"] = (
                "s3://maganghub/"
                "kemenko/"
                "produksi_kelapa_sawit_rakyat_provinsi/"
                "final/"
                "produksi_kelapa_sawit_rakyat_provinsi.csv"
            )

            new_row["id"] = None
            new_row["provinsi"] = provinsi
            new_row["id_kota"] = 0
            new_row["kota"] = None
            new_row["leveldata"] = "provinsi"
            new_row["satuan"] = "Ton"
            new_row["frekuensi"] = "tahun"
            new_row["item"] = None
            new_row["negara"] = "Indonesia"

            if tahun == 2019:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan Nasional 2019-2021"
                )

            elif tahun == 2020:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2020-2022"
                )

            elif tahun == 2021:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Unggulan 2021-2023"
                )

            elif tahun == 2022:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2022-2024"
                )

            elif tahun == 2023:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2023-2025"
                )

            elif tahun in [2024, 2025, 2026]:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan, "
                    "Statistik Perkebunan Jilid 1 2024-2026"
                )

            new_row["ket_tambahan"] = (
                "Data produksi kelapa sawit "
                "Perkebunan Rakyat (PR) "
                "menurut provinsi berdasarkan publikasi "
                "Statistik Perkebunan Direktorat Jenderal "
                "Perkebunan."
            )

            final_rows.append(new_row)

        df_final = pd.DataFrame(
            final_rows,
            columns=xflask_columns
        )

        print(
            f"Total final rows: {len(df_final)}"
        )

        return df_final

    # --------------------------------------------------------
    # Build Final 2883
    # --------------------------------------------------------

    def build_final_2883(self, df_pdf, df_existing):

        print("\n=== BUILD FINAL 2883 ===")

        if df_existing.empty:
            raise ValueError(
                "Data existing xFlask kosong. "
                "Metadata kabupaten/kota tidak tersedia."
            )

        df_existing = df_existing.copy()

        # ==========================================
        # NORMALISASI NAMA KOTA XFLASK
        # ==========================================

        df_existing["kota_normalized"] = (
            df_existing["kota"]
            .apply(self.normalize_kabupaten_kota)
        )

        # ==========================================
        # KOLOM FINAL
        # ==========================================

        xflask_columns = [
            "data_x",
            "data_y",
            "date",
            "frekuensi",
            "id",
            "id_kota",
            "id_nama_data",
            "id_provinsi",
            "indikator",
            "institusi",
            "item",
            "ket_tambahan",
            "kode_bps",
            "kota",
            "leveldata",
            "nama",
            "nama_data_import",
            "nama_alias",
            "negara",
            "provinsi",
            "satuan",
            "sumber",
        ]

        # ==========================================
        # METADATA LOOKUP
        # ==========================================

        metadata_lookup = (
            df_existing
            .drop_duplicates(
                subset=["kota_normalized"]
            )
            .set_index("kota_normalized")
        )

        final_rows = []
        missing_metadata = []

        # ==========================================
        # LOOP DATA PDF
        # ==========================================

        for _, row in df_pdf.iterrows():

            kota = row["kabupaten_kota"]
            tahun = row["tahun"]

            kota_normalized = (
                self.normalize_kabupaten_kota(kota)
            )

            # ======================================
            # CARI METADATA
            # ======================================

            if kota_normalized in metadata_lookup.index:

                template = (
                    metadata_lookup
                    .loc[kota_normalized]
                    .copy()
                )

            else:

                missing_metadata.append(
                    kota_normalized
                )

                # JANGAN BUAT ROW DENGAN ID 0
                continue

            # ======================================
            # BUAT ROW
            # ======================================

            new_row = {}

            for column in xflask_columns:

                if column in template.index:
                    new_row[column] = template[column]

                else:
                    new_row[column] = None

            # ======================================
            # DATA DARI PDF
            # ======================================

            new_row["data_x"] = row["data_x"]

            new_row["data_y"] = row["data_y"]

            new_row["date"] = pd.to_datetime(
                row["data_x"],
                format="%d-%m-%Y"
            )

            # ======================================
            # ID DATA
            # ======================================

            new_row["id"] = None

            new_row["id_nama_data"] = 2883

            # ======================================
            # IDENTITAS DATA
            # ======================================

            new_row["nama"] = (
                "Produksi Kelapa Sawit Milik Rakyat "
                "Menurut Kabupaten/Kota"
            )

            new_row["indikator"] = (
                "Produksi Kelapa Sawit Milik Rakyat "
                "Menurut Kabupaten/Kota"
            )

            new_row["leveldata"] = "kabupaten/kota"

            new_row["satuan"] = "Ton"

            new_row["frekuensi"] = "tahun"

            # ======================================
            # NEGARA
            # ======================================

            new_row["negara"] = "Indonesia"

            # ======================================
            # IMPORT PATH
            # ======================================

            new_row["nama_data_import"] = (
                "s3://maganghub/kemenko/"
                "produksi_kelapa_sawit_rakyat_kabupaten_kota/"
                "final/"
                "produksi_kelapa_sawit_rakyat_kabupaten_kota.csv"
            )

            # ======================================
            # KETERANGAN
            # ======================================

            new_row["ket_tambahan"] = (
                "Data produksi kelapa sawit "
                "Perkebunan Rakyat (PR) menurut "
                "kabupaten/kota berdasarkan publikasi "
                "Statistik Perkebunan Direktorat "
                "Jenderal Perkebunan."
            )

            # ======================================
            # SOURCE
            # ======================================

            if tahun <= 2021:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan"
                )

            elif tahun <= 2023:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan"
                )

            else:

                new_row["sumber"] = (
                    "Direktorat Jenderal Perkebunan"
                )

            # ======================================
            # APPEND
            # ======================================

            final_rows.append(new_row)

        # ==========================================
        # CEK MISSING METADATA
        # ==========================================

        missing_metadata = sorted(
            set(missing_metadata)
        )

        print(
            f"\nMetadata ditemukan : "
            f"{len(final_rows)} rows"
        )

        print(
            f"Metadata tidak ditemukan : "
            f"{len(missing_metadata)} kota"
        )

        if missing_metadata:

            print("\n=== KOTA TANPA METADATA ===")

            for kota in missing_metadata:
                print("-", kota)

        # ==========================================
        # DATAFRAME FINAL
        # ==========================================

        df_final = pd.DataFrame(
            final_rows,
            columns=xflask_columns
        )

        print(
            f"\nTotal final rows: "
            f"{len(df_final)}"
        )

        return df_final

    def save_final_32678(self, df):

        config = DATA_CONFIG[32678]

        output_dir = (
            Path("data/final/kemenko")
            / config["raw_folder"]
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        output_path = (
            output_dir
            / f"{config['filename']}.csv"
        )

        df.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"Final CSV saved: {output_path}"
        )

        return output_path

    def save_final_32679(self, df):
        output_dir = Path(
            "data/final/kemenko/produksi_kelapa_sawit_bumn_provinsi"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = (
            output_dir /
            "produksi_kelapa_sawit_bumn_provinsi.csv"
        )

        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        print(f"Final CSV saved: {output_path}")

        return output_path

    # --------------------------------------------------------
    # Save Final 2883
    # --------------------------------------------------------

    def save_final_2883(self, df):

        config = DATA_CONFIG[2883]

        output_dir = (
            Path("data/final/kemenko")
            / config["raw_folder"]
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        output_path = (
            output_dir
            / f"{config['filename']}.csv"
        )

        df.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"Final CSV saved: {output_path}"
        )

        return output_path

    def upload_final_minio(
        self,
        final_file,
        id_nama_data
    ):

        config = DATA_CONFIG[id_nama_data]

        storage_name = config["raw_folder"]
        filename = f"{config['filename']}.csv"

        with open(final_file, "rb") as file:
            final_bytes = file.read()

        object_name = (
            f"kemenko/"
            f"{storage_name}/final/"
            f"{filename}"
        )

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            final_bytes,
            object_name,
            "text/csv",
        )

        print("FINAL CSV MinIO upload berhasil:")

        print(
            f"{MINIO_BUCKET}/"
            f"{object_name}"
        )

    def save_final_32677(self, df):

        config = DATA_CONFIG[32677]

        output_dir = (
            Path("data/final/kemenko")
            / config["raw_folder"]
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        output_path = (
            output_dir
            / f"{config['filename']}.csv"
        )

        df.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"Final CSV saved: {output_path}"
        )

        return output_path

    def process_32679(self, df_pdf):
        df_xflask = self.get_xflask_api_data(32679)

        df_match = self.match_32679(
            df_pdf,
            df_xflask
        )

        df_final = self.build_final_32679(
            df_pdf,
            df_xflask
        )

        self.save_final_32679(df_final)

        return df_final

    # ========================================================
    # ETL PROCESS
    # ========================================================

    def process_32679(self, df_pdf):

        print("\n========================================")
        print("ETL PROCESS ID 32679")
        print("========================================")

        # ----------------------------------------------------
        # 1. TRANSFORM
        # ----------------------------------------------------

        df_transform = self.transform_32679(
            df_pdf
        )

        # ----------------------------------------------------
        # 2. SAVE RAW LOCAL
        # ----------------------------------------------------

        self.save_raw_parquet(
            df_transform,
            32679
        )

        # ----------------------------------------------------
        # 3. GET EXISTING xFLASK
        # ----------------------------------------------------

        df_existing = self.get_xflask_api_data(
            32679
        )

        # ----------------------------------------------------
        # 4. MATCH
        # ----------------------------------------------------

        df_match = self.match_32679(
            df_transform,
            df_existing
        )

        # ----------------------------------------------------
        # 5. CEK DATA YANG SUDAH ADA
        # ----------------------------------------------------

        if df_existing.empty:

            df_new = df_transform.copy()

        else:

            df_existing["provinsi_normalized"] = (
                df_existing["provinsi"]
                .apply(self.normalize_province)
            )

            df_existing["data_x"] = (
                df_existing["data_x"]
                .astype(str)
                .str.strip()
            )

            existing_keys = set(
                zip(
                    df_existing["data_x"],
                    df_existing["provinsi_normalized"]
                )
            )

            df_new = df_transform[
                ~df_transform.apply(
                    lambda row: (
                        row["data_x"],
                        row["provinsi"]
                    ) in existing_keys,
                    axis=1
                )
            ].copy()

        # ----------------------------------------------------
        # 6. BUILD FINAL
        # ----------------------------------------------------

        df_final = self.build_final_32679(
            df_transform,
            df_existing
        )

        # ----------------------------------------------------
        # 7. SAVE FINAL LOCAL
        # ----------------------------------------------------

        final_path = self.save_final_32679(df_final)

        # ----------------------------------------------------
        # 8. LOAD MINIO
        # ----------------------------------------------------

        self.upload_final_minio(final_path, 32679)

        return df_final

    # ========================================================
    # PROCESS 32677
    # ========================================================

    def process_32677(self, df_pdf):

        id_nama_data = 32677

        print("\n========================================")
        print("ETL PROCESS ID 32677")
        print("Produksi Kelapa Sawit Milik Swasta")
        print("========================================")

        # ----------------------------------------------------
        # 1. TRANSFORM PDF
        # ----------------------------------------------------

        df_transform = self.transform_32677(
            df_pdf
        )

        # ----------------------------------------------------
        # 2. GET EXISTING xFLASK
        # ----------------------------------------------------

        print("\n========================================")
        print("GET EXISTING DATA FROM xFLASK")
        print("========================================")

        df_existing = self.get_xflask_api_data(
            id_nama_data
        )

        print("\n=== PROVINSI EXISTING xFLASK ===")

        if df_existing.empty:

            print("⚠️ Data xFlask kosong.")

        else:

            print(
                df_existing[
                    [
                        "provinsi",
                        "id_provinsi",
                        "kode_bps"
                    ]
                ]
                .drop_duplicates()
                .sort_values("id_provinsi")
                .to_string(index=False)
            )

            print(
                f"\nExisting rows: "
                f"{len(df_existing)}"
            )

        # ----------------------------------------------------
        # 3. MATCH
        # ----------------------------------------------------

        df_match = self.match_32677(
            df_transform,
            df_existing
        )

        # ----------------------------------------------------
        # 4. CEK DATA YANG SUDAH ADA
        # ----------------------------------------------------

        if df_existing.empty:

            df_new = df_transform.copy()

        else:

            df_existing["provinsi_normalized"] = (
                df_existing["provinsi"]
                .apply(self.normalize_province)
            )

            df_existing["data_x"] = (
                df_existing["data_x"]
                .astype(str)
                .str.strip()
            )

            existing_keys = set(
                zip(
                    df_existing["data_x"],
                    df_existing["provinsi_normalized"]
                )
            )

            df_new = df_transform[
                ~df_transform.apply(
                    lambda row: (
                        row["data_x"],
                        row["provinsi"]
                    ) in existing_keys,
                    axis=1
                )
            ].copy()

        print(
            f"\nData PDF: {len(df_transform)}"
        )

        print(
            f"Data baru: {len(df_new)}"
        )

        # ----------------------------------------------------
        # 5. SAVE RAW
        # ----------------------------------------------------

        self.save_raw_parquet(
            df_transform,
            id_nama_data
        )

        # ----------------------------------------------------
        # 6. BUILD FINAL
        # ----------------------------------------------------

        df_final = self.build_final_32677(
            df_transform,
            df_existing
        )

        # ----------------------------------------------------
        # 7. SAVE FINAL
        # ----------------------------------------------------

        final_path = self.save_final_32677(
            df_final
        )

        # ----------------------------------------------------
        # 8. UPLOAD MINIO
        # ----------------------------------------------------

        self.upload_final_minio(
            final_path,
            id_nama_data
        )

        return df_final

    def process_32678(self, df_pdf):

        id_nama_data = 32678

        print("\n========================================")
        print("ETL PROCESS ID 32678")
        print("Produksi Kelapa Sawit Milik Rakyat")
        print("========================================")

        # ----------------------------------------------------
        # 1. TRANSFORM PDF
        # ----------------------------------------------------

        df_transform = self.transform_32678(
            df_pdf
        )

        # ----------------------------------------------------
        # 2. SAVE RAW
        # ----------------------------------------------------

        self.save_raw_parquet(
            df_transform,
            id_nama_data
        )

        # ----------------------------------------------------
        # 3. GET EXISTING xFLASK
        # ----------------------------------------------------

        print("\n========================================")
        print("GET EXISTING DATA FROM xFLASK")
        print("========================================")

        df_existing = self.get_xflask_api_data(
            id_nama_data
        )

        print("\n=== PROVINSI EXISTING xFLASK ===")

        if df_existing.empty:

            print("⚠️ Data xFlask kosong.")

        else:

            print(
                df_existing[
                    [
                        "provinsi",
                        "id_provinsi",
                        "kode_bps"
                    ]
                ].drop_duplicates()
            )

        # ----------------------------------------------------
        # 4. MATCH
        # ----------------------------------------------------

        df_match = self.match_32678(
            df_transform,
            df_existing
        )

        print("\n=== HASIL MATCH 32678 ===")
        print(df_match)

        # ----------------------------------------------------
        # 5. CEK DATA YANG SUDAH ADA
        # ----------------------------------------------------

        if df_existing.empty:

            df_new = df_transform.copy()

        else:

            df_existing["provinsi_normalized"] = (
                df_existing["provinsi"]
                .apply(self.normalize_province)
            )

            df_existing["data_x"] = (
                df_existing["data_x"]
                .astype(str)
                .str.strip()
            )

            existing_keys = set(
                zip(
                    df_existing["data_x"],
                    df_existing["provinsi_normalized"]
                )
            )

            df_new = df_transform[
                ~df_transform.apply(
                    lambda row: (
                        row["data_x"],
                        self.normalize_province(
                            row["provinsi"]
                        )
                    ) in existing_keys,
                    axis=1
                )
            ].copy()

        print(
            f"\nData baru yang akan masuk: {len(df_new)} rows"
        )

        # ----------------------------------------------------
        # 6. BUILD FINAL
        # ----------------------------------------------------

        df_final = self.build_final_32678(
            df_transform,
            df_existing
        )

        # ----------------------------------------------------
        # 7. SAVE FINAL LOCAL
        # ----------------------------------------------------

        final_path = self.save_final_32678(
            df_final
        )

        # ----------------------------------------------------
        # 8. UPLOAD FINAL MINIO
        # ----------------------------------------------------

        self.upload_final_minio(
            final_path,
            id_nama_data
        )

        return df_final

    # --------------------------------------------------------
    # Process 2883
    # --------------------------------------------------------

    def process_2883(self, df_pdf):

        id_nama_data = 2883

        print("\n" + "=" * 70)
        print("PROCESSING ID 2883")
        print("=" * 70)

        # ==========================================
        # TRANSFORM PDF
        # ==========================================

        df_transform = self.transform_2883(df_pdf)

        self.save_raw_parquet(
            df_transform,
            id_nama_data
        )

        print(
            f"Raw data saved: {len(df_transform)} rows"
        )

        # ==========================================
        # AMBIL DATA EXISTING DARI XFLASK
        # ==========================================

        df_existing = self.get_xflask_api_data(
            id_nama_data
        )

        print("\n=== METADATA 2883 ===")

        print(
            df_existing[
                [
                    "id_kota",
                    "id_provinsi",
                    "kode_bps",
                    "kota",
                    "provinsi"
                ]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

        print(
            "TOTAL UNIQUE KOTA:",
            df_existing["kota"].nunique()
        )

        print(
            f"Existing rows: {len(df_existing)}"
        )

        # ==========================================
        # MATCHING
        # ==========================================

        df_match = self.match_2883(
            df_transform,
            df_existing
        )

        # ==========================================
        # CEK DATA BARU
        # ==========================================

        if df_existing.empty:

            df_new = df_transform.copy()

        else:

            df_existing = df_existing.copy()

            # NORMALISASI XFLASK
            df_existing["kota_normalized"] = (
                df_existing["kota"]
                .apply(self.normalize_kabupaten_kota)
            )

            df_existing["data_x"] = (
                df_existing["data_x"]
                .astype(str)
                .str.strip()
            )

            existing_keys = set(
                zip(
                    df_existing["data_x"],
                    df_existing["kota_normalized"]
                )
            )

            # NORMALISASI PDF
            df_new = df_transform[
                ~df_transform.apply(
                    lambda row: (
                        str(row["data_x"]).strip(),
                        self.normalize_kabupaten_kota(
                            row["kabupaten_kota"]
                        )
                    ) in existing_keys,
                    axis=1
                )
            ].copy()

        print(
            f"\nData PDF : {len(df_transform)}"
        )

        print(
            f"Data baru: {len(df_new)}"
        )

        # ==========================================
        # BUILD FINAL
        # ==========================================

        df_final = self.build_final_2883(
            df_transform,
            df_existing
        )

        # ==========================================
        # SAVE FINAL
        # ==========================================

        final_path = self.save_final_2883(
            df_final
        )

        # ==========================================
        # UPLOAD
        # ==========================================

        self.upload_final_minio(
            final_path,
            id_nama_data
        )

        return df_final


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    ID_NAMA_DATA = 2883

    scraper = KemenkoEkonomiScraper()

    # --------------------------------------------------------
    # PILIH DATASET BERDASARKAN ID
    # --------------------------------------------------------

    if ID_NAMA_DATA == 32677:

        df_pdf = scraper.extract_pdf_32677()

    elif ID_NAMA_DATA == 32678:

        df_pdf = scraper.extract_pdf_32678()

    elif ID_NAMA_DATA == 32679:

        df_pdf = scraper.extract_pdf_32679()

    elif ID_NAMA_DATA == 2883:

        df_pdf = scraper.extract_pdf_2883()

    else:

        raise ValueError(
            f"ID {ID_NAMA_DATA} belum tersedia."
        )

    # --------------------------------------------------------
    # CEK HASIL EXTRACT
    # --------------------------------------------------------

    if df_pdf.empty:

        print(
            f"❌ Tidak ada data hasil extract "
            f"untuk ID {ID_NAMA_DATA}."
        )

    else:

        etl = KemenkoEkonomiETL()

        if ID_NAMA_DATA == 32677:

            df = etl.process_32677(df_pdf)

        elif ID_NAMA_DATA == 32678:

            df = etl.process_32678(df_pdf)

        elif ID_NAMA_DATA == 32679:

            df = etl.process_32679(df_pdf)

        elif ID_NAMA_DATA == 2883:

            df = etl.process_2883(df_pdf)

        print("\n========================================")
        print(f"FINAL RESULT ID {ID_NAMA_DATA}")
        print("========================================")

        print(df)