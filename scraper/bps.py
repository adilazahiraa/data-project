import os
import argparse
import requests
import re
import io
import pymupdf
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


from utils.api import (
    create_session,
)

from utils.file import (
    read_parquet,
    save_parquet,
    save_csv,
)

from utils.data import (
    normalize_dataframe,
)

from utils.storage import (
    get_minio_client,
    upload_bytes,
)


load_dotenv(override=True)


PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)


MINIO_BUCKET = "maganghub"


BPS_API_KEY = os.getenv(
    "BPS_API_KEY"
)

KATADATA_API_KEY = os.getenv(
    "KATADATA_API_KEY"
)


BPS_BASE_URL = (
    "https://webapi.bps.go.id/v1/api"
)

DATABASE_BASE_URL = (
    "https://xflask.databoks.id/database"
)


BPS_CONFIG = {
    "gini_ratio": {
        "indikator": "Gini Ratio Menurut Kabupaten Kota",
        "configs": [
            {
                "domain_id": "1200",
                "provinsi": "Sumatera Utara",
                "var_id": 467,
            },
            {
                "domain_id": "1300",
                "provinsi": "Sumatera Barat",
                "var_id": 83,
            },
            {
                "domain_id": "1700",
                "provinsi": "Bengkulu",
                "var_id": 268,
            },
            {
                "domain_id": "1800",
                "provinsi": "Lampung",
                "var_id": 632,
            },
            {
                "domain_id": "3400",
                "provinsi": "D.I. Yogyakarta",
                "var_id": 333,
            },
            {
                "domain_id": "3600",
                "provinsi": "Banten",
                "var_id": 425,
            },
            {
                "domain_id": "6200",
                "provinsi": "Kalimantan Tengah",
                "var_id": 371,
            },
            {
                "domain_id": "6400",
                "provinsi": "Kalimantan Timur",
                "var_id": 549,
            },
            {
                "domain_id": "7100",
                "provinsi": "Sulawesi Utara",
                "var_id": 280,
            },
            {
                "domain_id": "7300",
                "provinsi": "Sulawesi Selatan",
                "var_id": 1743,
            },
            {
                "domain_id": "1400",
                "provinsi": "Riau",
                "var_id": 387,
            },
            {
                "domain_id": "1500",
                "provinsi": "Jambi",
                "var_id": 51,
            },
            {
                "domain_id": "1600",
                "provinsi": "Sumatera Selatan",
                "var_id": 623,
            },
            {
                "domain_id": "1900",
                "provinsi": "Kepulauan Bangka Belitung",
                "var_id": 1174,
            },
            {
                "domain_id": "3100",
                "provinsi": "DKI Jakarta",
                "var_id": 884,
            },
            {
                "domain_id": "3500",
                "provinsi": "Jawa Timur",
                "var_id": 488,
            },
            {
                "domain_id": "5100",
                "provinsi": "Bali",
                "var_id": 41,
            },
            {
                "domain_id": "5200",
                "provinsi": "Nusa Tenggara Barat",
                "var_id": 426,
            },
            {
                "domain_id": "6100",
                "provinsi": "Kalimantan Barat",
                "var_id": 41,
            },
            {
                "domain_id": "6500",
                "provinsi": "Kalimantan Utara",
                "var_id": 495,
            },
            {
                "domain_id": "7200",
                "provinsi": "Sulawesi Tengah",
                "var_id": 52,
            },
            {
                "domain_id": "7400",
                "provinsi": "Sulawesi Tenggara",
                "var_id": 467,
            },
            {
                "domain_id": "8200",
                "provinsi": "Maluku Utara",
                "var_id": 142,
            },
            {
                "domain_id": "9100",
                "provinsi": "Papua Barat",
                "var_id": 171,
            },
            {
                "domain_id": "9400",
                "provinsi": "Papua",
                "var_id": 50,
            },
            {
                "domain_id": "7600",
                "provinsi": "Sulawesi Barat",
                "var_id": 166,
            },
        ],
        "default_satuan": "Poin Indeks",
        "sumber": "Badan Pusat Statistik (BPS)",
        "note": "Data Gini Ratio Kabupaten/Kota berdasarkan BPS",
        "storage_name": "gini_kabupaten_kota",
        "file_name": "bps_gini_kabupaten_kota",
        "nama_data_import": (
            "s3://maganghub/bps/final/"
            "bps_gini_kabupaten_kota.csv"
        ),
        
    },

    # NANTI DATA BPS LAIN TINGGAL DITAMBAHKAN
    "apm_sd": {
        "idnamadata": 9824,
        "indikator": "Angka Partisipasi Murni (APM) SD Menurut Kabupaten Kota",
        "default_satuan": "Persen",
        "sumber": "Badan Pusat Statistik (BPS)",
        "storage_name": "apm",
        "file_name": "angka_partisipasi_murni_sd_kabupaten_kota.csv",
        "note": "Angka Partisipasi Murni (APM) SD Menurut Kabupaten/Kota berdasarkan BPS",
        "nama_data_import": (
            "s3://maganghub/bps/apm/final/"
            "angka_partisipasi_murni_sd_kabupaten_kota.csv"
        ),
        "configs": [
            {
                "domain_id": "1200",
                "provinsi": "Sumatera Utara",
                "var_id": 140,
                "turvar_id": 98,
                "turvar_label": "SD",
            },
            {
                "domain_id": "1900",
                "provinsi": "Kepulauan Bangka Belitung",
                "var_id": 649,
            },
            {
                "domain_id": "3100",
                "provinsi": "DKI Jakarta",
                "var_id": 1073,
            },
            {
                "domain_id": "3200",
                "provinsi": "Jawa Barat",
                "var_id": 97,
                "turvar_id": 97,
            },
            {
                "domain_id": "3300",
                "provinsi": "Jawa Tengah",
                "var_id": 3614,
                "turvar_id": 1287,
            },
            {
                "domain_id": "3400",
                "provinsi": "D.I. Yogyakarta",
                "var_id": 481,
            },
            {
                "domain_id": "5300",
                "provinsi": "Nusa Tenggara Timur",
                "var_id": 860,
            },
            {
                "domain_id": "6200",
                "provinsi": "Kalimantan Tengah",
                "var_id": 352,
            },
            {
                "domain_id": "6300",
                "provinsi": "Kalimantan Selatan",
                "var_id": 79,
                "turvar_id": 149,
                "turvar_label": "Laki-laki+Perempuan",
            },
            {
                "domain_id": "6400",
                "provinsi": "Kalimantan Timur",
                "var_id": 912,
                "turvar_id": 138,
                "turvar_label": "Jumlah",
            },
            {
                "domain_id": "7100",
                "provinsi": "Sulawesi Utara",
                "var_id": 93,
                "turvar_id": 1273,
            },
                        {
                "domain_id": "7400",
                "provinsi": "Sulawesi Tenggara",
                "var_id": 414,
            },
        ],
    },

    # APM SMP
    "apm_smp": {
        "idnamadata": 9826,
        "indikator": "Angka Partisipasi Murni (APM) SMP Menurut Kabupaten Kota",
        "default_satuan": "Persen",
        "sumber": "Badan Pusat Statistik (BPS)",
        "storage_name": "apm",
        "file_name": "angka_partisipasi_murni_smp_kabupaten_kota.csv",
        "note": "Angka Partisipasi Murni (APM) SMP menurut Kabupaten/Kota berdasarkan BPS",
        "nama_data_import": (
            "s3://maganghub/bps/apm/final/"
            "angka_partisipasi_murni_smp_kabupaten_kota.csv"
        ),
        "configs": [
            {
                "domain_id": "1200",
                "provinsi": "Sumatera Utara",
                "var_id": 140,
                "turvar_id": 97,
                "turvar_label": "SMP",
            },
            {
                "domain_id": "1900",
                "provinsi": "Kepulauan Bangka Belitung",
                "var_id": 650,
            },
            {
                "domain_id": "3100",
                "provinsi": "DKI Jakarta",
                "var_id": 1074,
            },
            {
                "domain_id": "3400",
                "provinsi": "D.I. Yogyakarta",
                "var_id": 482,
            },
            {
                "domain_id": "5300",
                "provinsi": "Nusa Tenggara Timur",
                "var_id": 861,
            },
            {
                "domain_id": "6300",
                "provinsi": "Kalimantan Selatan",
                "var_id": 81,
                "turvar_id": 149,
                "turvar_label": "Laki-laki+Perempuan",
            },
            {
                "domain_id": "6400",
                "provinsi": "Kalimantan Timur",
                "var_id": 913,
                "turvar_id": 138,
                "turvar_label": "Jumlah",
            },
        ],
    },

    # APM SMA
    "apm_sma": {
        "idnamadata": 9825,
        "indikator": "Angka Partisipasi Murni (APM) SMA Menurut Kabupaten Kota",
        "default_satuan": "Persen",
        "sumber": "Badan Pusat Statistik (BPS)",
        "storage_name": "apm",
        "file_name": "angka_partisipasi_murni_sma_kabupaten_kota.csv",
        "note": "Angka Partisipasi Murni (APM) SMA menurut Kabupaten/Kota berdasarkan BPS",
        "nama_data_import": (
            "s3://maganghub/bps/apm/final/"
            "angka_partisipasi_murni_sma_kabupaten_kota.csv"
        ),
        "configs": [
            {
                "domain_id": "1200",
                "provinsi": "Sumatera Utara",
                "var_id": 140,
                "turvar_id": 96,
                "turvar_label": "SMTA",
            },
            {
                "domain_id": "1900",
                "provinsi": "Kepulauan Bangka Belitung",
                "var_id": 652,
            },
            {
                "domain_id": "3100",
                "provinsi": "DKI Jakarta",
                "var_id": 1075,
            },
            {
                "domain_id": "3400",
                "provinsi": "D.I. Yogyakarta",
                "var_id": 483,
            },
            {
                "domain_id": "5300",
                "provinsi": "Nusa Tenggara Timur",
                "var_id": 862,
            },
            {
                "domain_id": "6300",
                "provinsi": "Kalimantan Selatan",
                "var_id": 82,
                "turvar_id": 149,
                "turvar_label": "Laki-laki+Perempuan",
            },
            {
                "domain_id": "6400",
                "provinsi": "Kalimantan Timur",
                "var_id": 914,
                "turvar_id": 138,
                "turvar_label": "Jumlah",
            },
        ],
    },
      # HOTEL AKOMODASI
    "hotel_akomodasi": {
        "idnamadata": 9869,
        "indikator": "Jumlah Akomodasi Hotel Berbintang Menurut Provinsi",
        "default_satuan": "Unit",
        "sumber": "Badan Pusat Statistik (BPS)",
        "storage_name": "hotel_akomodasi",
        "file_name": "jumlah_akomodasi_hotel_berbintang_menurut_provinsi",
        "note": (
            "Jumlah Akomodasi Hotel Berbintang Menurut Provinsi "
            "berdasarkan BPS"
        ),
        "nama_data_import": (
            "s3://maganghub/bps/hotel_akomodasi/final/"
            "jumlah_akomodasi_hotel_berbintang_menurut_provinsi.csv"
        ),

        # SIMDASI
        "simdasi_id": 25,
        "simdasi_table_id": (
            "N0VJWlZIYVpWSTJqYlU3RExiSksrQT09"
        ),
        "simdasi_variable_id": "fg10tydh4f",
        "simdasi_wilayah": "0000000",
        "tahun_terbaru": 2025,
        "publikasi_pdf": (
            "statistik-hotel-dan-akomodasi-lainnya-di-indonesia-2024.pdf"
        ),
    },
}

def get_bps_config(indikator):
    if indikator not in BPS_CONFIG:
        raise ValueError(
            f"Indikator BPS tidak ditemukan: {indikator}"
        )

    return BPS_CONFIG[indikator]

def get_storage_paths(config):
    storage_name = config["storage_name"]
    file_name = config["file_name"]

    raw_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "bps"
        / storage_name
    )

    final_dir = (
        PROJECT_ROOT
        / "data"
        / "final"
        / "bps"
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

class BPSScraper:

    def __init__(
        self,
        domain_id,
        var_id,
        provinsi,
    ):

        self.domain_id = str(
            domain_id
        )

        self.var_id = int(
            var_id
        )

        self.provinsi = provinsi

        self.session = (
            create_session()
        )


    def get_available_years(self):

        url = (
            f"{BPS_BASE_URL}/list/model/th/"
            f"domain/{self.domain_id}/"
            f"var/{self.var_id}/"
            f"key/{BPS_API_KEY}"
        )


        response = self.session.get(
            url,
            timeout=60,
        )

        response.raise_for_status()


        metadata = response.json()


        if metadata.get("status") != "OK":

            raise RuntimeError(
                metadata.get(
                    "message",
                    metadata,
                )
            )


        return metadata["data"][1]


    def get_data(self, th_id):

        url = (
            f"{BPS_BASE_URL}/list/model/data/"
            f"lang/ind/"
            f"domain/{self.domain_id}/"
            f"var/{self.var_id}/"
            f"th/{th_id}/"
            f"key/{BPS_API_KEY}"
        )


        response = self.session.get(
            url,
            timeout=60,
        )

        response.raise_for_status()


        result = response.json()


        if result.get("status") != "OK":

            print(
                "⚠️ API tidak OK:",
                result.get("message"),
            )

            return None


        return result


    def parse_data(
        self,
        result,
        tahun,
        indikator=None,
        turvar_id=None,
        turvar_label=None,
    ):
        vervar = result.get("vervar", [])
        var = result.get("var", [])
        turvar = result.get("turvar", [])
        turtahun = result.get("turtahun", [])
        tahun_meta = result.get("tahun", [])
        datacontent = result.get("datacontent", {})

        if not datacontent:
            return pd.DataFrame()

        if not isinstance(datacontent, dict):
            print(
                "⚠️ Format datacontent:",
                type(datacontent),
            )
            return pd.DataFrame()

        # ==============================
        # METADATA
        # ==============================
        nama_indikator = (
            var[0].get(
                "label",
                indikator,
            )
            if var
            else indikator
        )

        satuan = (
            var[0].get(
                "unit",
                pd.NA,
            )
            if var
            else pd.NA
        )

        vervar_map = {
            str(item["val"]): item["label"]
            for item in vervar
        }

        turvar_map = {
            str(item["val"]): item["label"]
            for item in turvar
        }

        var_id = (
            str(var[0]["val"])
            if var
            else str(self.var_id)
        )

        tahun_id = (
            str(tahun_meta[0]["val"])
            if tahun_meta
            else None
        )

        turtahun_id = (
            str(turtahun[0]["val"])
            if turtahun
            else "0"
        )

        # ==============================
        # TURVAR YANG DIPILIH
        # ==============================
        if turvar_id is not None:
            selected_turvar = [
                item
                for item in turvar
                if str(item["val"]) == str(turvar_id)
            ]

            if not selected_turvar:
                raise ValueError(
                    f"turvar_id {turvar_id} "
                    f"tidak ditemukan untuk "
                    f"{self.provinsi} - var {self.var_id}"
                )
        else:
            selected_turvar = turvar

        # Dataset yang tidak punya turvar
        has_turvar = bool(turvar)

        if not selected_turvar:
            selected_turvar = [
                {
                    "val": 0,
                    "label": "",
                }
            ]

        # ==============================
        # BUAT MAPPING KEY BPS
        # ==============================
        key_map = {}

        for vervar_item in vervar:
            vervar_id = str(vervar_item["val"])

            for turvar_item in selected_turvar:
                current_turvar_id = str(
                    turvar_item["val"]
                )

                parts = [
                    vervar_id,
                    var_id,
                ]

                if has_turvar:
                    parts.append(current_turvar_id)

                if tahun_id is not None:
                    parts.append(tahun_id)

                parts.append(turtahun_id)

                expected_key = "".join(parts)

                key_map[expected_key] = {
                    "kode_wilayah": vervar_id,
                    "turvar_id": current_turvar_id,
                    "turvar": turvar_map.get(
                        current_turvar_id,
                        turvar_label or "",
                    ),
                }

        # ==============================
        # PARSE DATA
        # ==============================
        rows = []

        for key, value in datacontent.items():
            key = str(key)

            metadata = key_map.get(key)

            if metadata is None:
                continue

            kode_wilayah = metadata[
                "kode_wilayah"
            ]

            current_turvar_id = metadata[
                "turvar_id"
            ]

            current_turvar = metadata[
                "turvar"
            ]

            rows.append({
                "kode_wilayah": kode_wilayah,
                "wilayah": vervar_map.get(
                    kode_wilayah,
                    kode_wilayah,
                ),
                "provinsi": self.provinsi,
                "indikator": nama_indikator,
                "turvar_id": current_turvar_id,
                "turvar": current_turvar,
                "data_x": tahun,
                "data_y": pd.to_numeric(
                    value,
                    errors="coerce",
                ),
                "satuan": satuan,
            })

        return pd.DataFrame(rows)


    def run(
        self,
        tahun_awal=None,
        tahun_akhir=None,
        indikator=None,
        turvar_id=None,
        turvar_label=None,
    ):


        print(
            "DOMAIN:",
            self.domain_id,
        )

        print(
            "PROVINSI:",
            self.provinsi,
        )

        print(
            "VAR ID:",
            self.var_id,
        )


        years = (
            self.get_available_years()
        )

        # Jika batas tahun diberikan, batasi data yang diambil.
        # Ini dipakai untuk skenario current update agar tidak
        # selalu menarik seluruh histori.
        if tahun_awal is not None:
            years = [
                year
                for year in years
                if int(year["th"]) >= int(tahun_awal)
            ]

        if tahun_akhir is not None:
            years = [
                year
                for year in years
                if int(year["th"]) <= int(tahun_akhir)
            ]

        print(
            "\nTAHUN TERSEDIA / DIPILIH:"
        )

        print(years)

        all_data = []

        for year in years:

            th_id = year["th_id"]
            tahun = year["th"]


            print(
                f"\nSCRAPE "
                f"{self.provinsi} - {tahun}"
            )


            print(
                "TH ID:",
                th_id,
            )


            result = self.get_data(
                th_id
            )


            if result is None:
                continue


            df = self.parse_data(
                result,
                tahun,
                indikator=indikator,
                turvar_id=turvar_id,
                turvar_label=turvar_label,
            )


            print(
                "JUMLAH DATA:",
                len(df),
            )


            if not df.empty:
                all_data.append(df)


        if not all_data:
            return pd.DataFrame()


        return pd.concat(
            all_data,
            ignore_index=True,
        )


def scrape_all(
    configs,
    indikator,
    save_raw=True,
    tahun_awal=None,
    tahun_akhir=None,
    raw_file=None,
):

    all_data = []


    for config in configs:

        print(
            "\n--------------------------------"
        )

        print(
            config["provinsi"]
        )

        print(
            "--------------------------------"
        )


        scraper = BPSScraper(
            domain_id=config["domain_id"],
            var_id=config["var_id"],
            provinsi=config["provinsi"],
        )


        try:

            df = scraper.run(
                tahun_awal=tahun_awal,
                tahun_akhir=tahun_akhir,
                indikator=indikator,
                turvar_id=config.get(
                    "turvar_id"
                ),
                turvar_label=config.get(
                    "turvar_label"
                )
            )


            if df.empty:

                print(
                    "⚠️ Data kosong:",
                    config["provinsi"],
                )

                continue


            all_data.append(df)


            print(
                "✅ Berhasil:",
                len(df),
                "row",
            )


        except Exception as e:

            print(
                "❌ Gagal:",
                config["provinsi"],
            )

            print(
                "   ",
                str(e),
            )

            continue


    if not all_data:

        raise RuntimeError(
            "Tidak ada data yang berhasil "
            "di-scrape."
        )


    df_raw = pd.concat(
        all_data,
        ignore_index=True,
    )

    if save_raw and raw_file is not None:

        save_parquet(
            df_raw,
            raw_file,
        )

    print(
        "JUMLAH ROW:",
        len(df_raw),
    )

    return df_raw


def get_existing_xflask_data(id_nama_data):
    """
    Mengambil data existing dari XFlask sebagai referensi
    Tidak melakukan insert/update
    """
    if not KATADATA_API_KEY:
        raise RuntimeError(
            "KATADATA_API_KEY tidak ditemukan di .env"
        )
    
    session = create_session()

    session.headers.update({
        "X-API-Key": KATADATA_API_KEY
    })

    response = session.get(
        f"{DATABASE_BASE_URL}/data",
        params={
            "id_nama_data": id_nama_data
        },
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError(
            f"Response XFlask tidak berupa list: {data}"
        )

    return pd.DataFrame(data)

def get_xflask_api_data(id_nama_data):
    """
    Mengambil data existing dari endpoint API XFlask:

        GET /api/getdata/{id_nama_data}

    Dipakai untuk proses matching/update dataset.
    """

    if not KATADATA_API_KEY:
        raise RuntimeError(
            "KATADATA_API_KEY tidak ditemukan di .env"
        )

    session = create_session()

    session.headers.update({
        "X-Api-Key": KATADATA_API_KEY
    })

    response = session.get(
        f"https://xflask.databoks.id/api/getdata/{id_nama_data}",
        timeout=60,
    )

    print("\n==============================")
    print("GET DATA XFLASK API")
    print("==============================")

    print(
        "URL:",
        f"/api/getdata/{id_nama_data}",
    )

    print(
        "STATUS:",
        response.status_code,
    )

    response.raise_for_status()

    result = response.json()

    if not isinstance(result, dict):
        raise RuntimeError(
            f"Response XFlask tidak sesuai: {result}"
        )

    if not result.get("success"):
        raise RuntimeError(
            f"XFlask API gagal: {result}"
        )

    data = result.get("data", [])

    df = pd.DataFrame(data)

    print(
        "JUMLAH ROW:",
        len(df),
    )

    print(
        "KOLOM:",
        df.columns.tolist(),
    )

    return df


def normalize_province_name(value):
    """
    Normalisasi nama provinsi untuk kebutuhan matching
    BPS <-> XFlask.

    Tidak mengubah nama asli pada dataframe.
    """

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
        value = value.replace(old, new)

    value = " ".join(
        value.split()
    )

    aliases = {
        "d i yogyakarta": "di yogyakarta",
        "di yogyakarta": "di yogyakarta",
        "kepulauan bangka belitung": "kepulauan bangka belitung",
        "bangka belitung": "kepulauan bangka belitung",
        "kepulauan riau": "kepulauan riau",
        "jakarta": "dki jakarta",
        "dki jakarta": "dki jakarta",
    }

    return aliases.get(
        value,
        value,
    )

def get_hotel_simdasi_table_id(
    tahun,
    config,
):
    """
    Mencari ID tabel SIMDASI hotel berdasarkan tahun.

    Endpoint:
        SIMDASI ID 23 - List of Tables by Region

    Dipakai agar ID tabel yang berubah antar tahun
    tidak perlu di-hardcode.
    """

    simdasi_id = config["simdasi_id"]

    wilayah = config.get(
        "simdasi_wilayah",
        "0000000",
    )

    url = (
        f"{BPS_BASE_URL}/interoperabilitas/"
        f"datasource/simdasi/"
        f"id/23/"
        f"wilayah/{wilayah}/"
        f"key/{BPS_API_KEY}"
    )

    session = create_session()

    print("\n========================================")
    print("CARI TABLE ID SIMDASI")
    print("========================================")
    print("TAHUN:", tahun)
    print("WILAYAH:", wilayah)

    response = session.get(
        url,
        timeout=60,
    )

    print(
        "STATUS LIST TABLE:",
        response.status_code,
    )

    response.raise_for_status()

    result = response.json()

    if not isinstance(result, dict):
        raise RuntimeError(
            "Response list tabel SIMDASI bukan dictionary."
        )

    data = result.get(
        "data",
        [],
    )

    if not data:
        raise RuntimeError(
            f"Response SIMDASI kosong: {result}"
        )

    print(
        "\nSTRUKTUR DATA SIMDASI:"
    )
    print(
        repr(data)[:10000]
    )

    # Endpoint SIMDASI biasanya mengembalikan:
    # data[0] = pagination
    # data[1] = daftar tabel

    if len(data) < 2:
        raise RuntimeError(
            "Response SIMDASI tidak memiliki "
            "bagian daftar tabel."
        )

    table_data = data[1]

    print(
        "\nTIPE TABLE DATA:",
        type(table_data),
    )

    print(
        "TABLE DATA:"
    )
    print(
        repr(table_data)[:10000]
    )

    if isinstance(
        table_data,
        dict,
    ):
        # Beberapa response membungkus
        # daftar tabel di salah satu key.
        for key in [
            "data",
            "table",
            "tables",
            "result",
            "items",
        ]:
            if key in table_data:
                table_data = table_data[key]
                break

    if not isinstance(
        table_data,
        list,
    ):
        raise RuntimeError(
            "Daftar tabel SIMDASI bukan list. "
            f"Format: {type(table_data)}"
        )

    data = table_data

    print(
        "\nJUMLAH TABLE SEBENARNYA:",
        len(data),
    )

    if not data:
        raise RuntimeError(
            f"Tidak ada tabel SIMDASI "
            f"untuk wilayah {wilayah}."
        )

    # =====================================
    # CARI TABEL YANG SESUAI
    # =====================================

    target_title = (
        config["indikator"]
        .strip()
        .lower()
    )

    def clean_text(value):
        if value is None:
            return ""

        return (
            str(value)
            .replace("<b>", "")
            .replace("</b>", "")
            .replace("<br>", " ")
            .replace("</br>", " ")
            .strip()
            .lower()
        )

    # Debug:
    # tampilkan struktur item pertama
    print(
        "\nCONTOH ITEM TABLE:"
    )
    print(
        repr(data[0])[:3000]
    )

    candidates = []

    for item in data:

        if not isinstance(
            item,
            dict,
        ):
            continue

        # Ambil kemungkinan field ID tabel
        table_id = (
            item.get("id_tabel")
            or item.get("id")
            or item.get("table_id")
        )

        # Ambil kemungkinan field judul
        title = (
            item.get("judul")
            or item.get("title")
            or item.get("nama")
            or item.get("nama_tabel")
        )

        # Ambil tahun jika tersedia
        years = (
            item.get("tahun")
            or item.get("years")
            or item.get("available_year")
            or item.get("available_years")
        )

        title_clean = clean_text(
            title
        )

        if (
            "akomodasi" in title_clean
            and "hotel" in title_clean
        ):
            candidates.append({
                "id_tabel": item.get("id_tabel"),
                "title": item.get("judul") or item.get("title"),
                "ketersediaan_tahun": item.get("ketersediaan_tahun", []),
            })

    print(
        "\nCANDIDATE TABLE HOTEL:"
    )

    for candidate in candidates:
        print(
            candidate
        )

    if not candidates:
        raise RuntimeError(
            "Tabel hotel tidak ditemukan "
            "pada daftar SIMDASI."
        )

    # =====================================
    # PILIH TABLE ID
    # =====================================

    # Prioritas:
    # 1. Kandidat yang secara eksplisit
    #    mencantumkan tahun yang dicari.
    # 2. Kalau API tidak memberikan tahun
    #    pada item tabel, gunakan kandidat
    #    pertama.
    for candidate in candidates:

        years = candidate.get(
            "ketersediaan_tahun",
            [],
        )

        if isinstance(years, list):

            year_values = {
                str(x)
                for x in years
            }

            if str(tahun) in year_values:

                print(
                    "\nTABLE ID TERPILIH:",
                    candidate["id_tabel"],
                )

                print(
                    "TAHUN TERSEDIA:",
                    years,
                )

                return candidate[
                    "id_tabel"
                ]

    # =====================================
    # TAHUN TIDAK TERSEDIA
    # =====================================

    raise RuntimeError(
        f"Tabel hotel ditemukan, "
        f"tetapi tahun {tahun} tidak tersedia "
        f"pada ketersediaan_tahun."
    )
                
    # =====================================
    # FALLBACK
    # =====================================

    table_id = candidates[0]["id_tabel"]

    if not table_id:
        raise RuntimeError(
            f"Candidate tabel hotel ditemukan "
            f"tetapi id_tabel kosong: "
            f"{candidates[0]}"
        )

    print(
        "\nTABLE ID TERPILIH:",
        table_id,
    )

    return table_id

def scrape_hotel_akomodasi(
    tahun=2025,
    config=None,
):
    """
    Scraping khusus dataset:

    9869 - Jumlah Akomodasi Hotel Berbintang
    Menurut Provinsi

    Sumber:
    BPS SIMDASI.

    Tidak menggunakan BPSScraper biasa karena
    dataset ini levelnya provinsi.
    """

    if config is None:
        config = get_bps_config(
            "hotel_akomodasi"
        )

    if not BPS_API_KEY:
        raise RuntimeError(
            "BPS_API_KEY tidak ditemukan di .env"
        )

    simdasi_id = config[
        "simdasi_id"
    ]

    table_id = get_hotel_simdasi_table_id(
        tahun=tahun,
        config=config,
    )

    variable_id = config[
        "simdasi_variable_id"
    ]

    wilayah = config.get(
        "simdasi_wilayah",
        "0000000",
    )

    url = (
        f"{BPS_BASE_URL}/interoperabilitas/"
        f"datasource/simdasi/"
        f"id/{simdasi_id}/"
        f"tahun/{tahun}/"
        f"id_tabel/{table_id}/"
        f"wilayah/{wilayah}/"
        f"key/{BPS_API_KEY}"
    )

    session = create_session()

    print("\n========================================")
    print("SCRAPE BPS HOTEL AKOMODASI")
    print("========================================")

    print(
        "TAHUN:",
        tahun,
    )

    print(
        "URL:",
        url.replace(
            BPS_API_KEY,
            "***",
        ),
    )

    response = session.get(
        url,
        timeout=60,
    )

    print(
        "STATUS:",
        response.status_code,
    )

    response.raise_for_status()

    result = response.json()

    if not isinstance(result, dict):
        raise RuntimeError(
            "Response BPS bukan dictionary."
        )

    data = result.get(
        "data",
        [],
    )

    if len(data) < 2:
        raise RuntimeError(
            f"Struktur response BPS tidak sesuai: {result}"
        )

    metadata = data[0]
    table = data[1]

    if isinstance(table, dict):
        status = table.get(
            "status"
        )

        if status == 404:
            raise RuntimeError(
                f"Tabel SIMDASI tidak ditemukan "
                f"untuk tahun {tahun}."
            )

    print(
        "TABLE:",
        table.get(
            "judul",
            table.get(
                "title",
                "-"
            ),
        ),
    )

    rows = table.get(
        "data",
        [],
    )

    if not rows:
        raise RuntimeError(
            f"Tidak ada data BPS untuk tahun {tahun}."
        )

    df = pd.DataFrame(rows)

    print(
        "JUMLAH ROW RAW:",
        len(df),
    )

    print(
        "KOLOM RAW:",
        df.columns.tolist(),
    )

    print("\nCONTOH VARIABLES:")
    print(repr(df["variables"].iloc[0])[:3000])

    def extract_variable_value(variables):
        if not isinstance(variables, dict):
            return pd.NA

        variable = variables.get(variable_id)

        if not isinstance(variable, dict):
            return pd.NA

        return variable.get("value")


    df["nilai"] = df["variables"].apply(
        extract_variable_value
    )

    print("\nCONTOH HASIL PARSING:")
    print(
        df[
            [
                "kode_wilayah",
                "label",
                "nilai",
            ]
        ].head().to_string(index=False)
)

    wilayah_candidates = [
        "label",
        "nama_wilayah",
        "wilayah",
        "nama",
    ]

    wilayah_column = None

    for column in wilayah_candidates:
        if column in df.columns:
            wilayah_column = column
            break

    if wilayah_column is None:
        raise RuntimeError(
            "Kolom nama wilayah tidak ditemukan "
            f"pada response BPS: {df.columns.tolist()}"
        )

    kode_candidates = [
        "kode_wilayah",
        "kode",
        "id_wilayah",
    ]

    kode_column = None

    for column in kode_candidates:
        if column in df.columns:
            kode_column = column
            break

    if kode_column is None:
        raise RuntimeError(
            "Kolom kode wilayah tidak ditemukan "
            f"pada response BPS: {df.columns.tolist()}"
        )

    df_result = pd.DataFrame({
        "kode_wilayah":
            df[kode_column]
            .astype("string")
            .str.strip(),

        "provinsi":
            df[wilayah_column]
            .astype("string")
            .str.strip(),

        "data_x":
            pd.Timestamp(
                year=tahun,
                month=12,
                day=31,
            ),

        "data_y":
            df["nilai"],

    })

    def parse_bps_number(value):
        if pd.isna(value):
            return pd.NA

        value = str(value).strip()

        if value in {
            "",
            "...",
            "…",
            "–",
            "-",
            "NA",
            "N/A",
        }:
            return pd.NA

        # Contoh BPS:
        # "1.234" = 1234
        value = value.replace(
            ".",
            "",
        )

        value = value.replace(
            ",",
            ".",
        )

        try:
            number = float(value)

            if number.is_integer():
                return int(number)

            return number

        except (ValueError, TypeError):
            return pd.NA

    df_result["data_y"] = (
        df_result["data_y"]
        .apply(parse_bps_number)
    )

    df_result["data_y"] = pd.to_numeric(
        df_result["data_y"],
        errors="coerce",
    )

    print(
        "\nJUMLAH DATA HASIL SCRAPING:",
        len(df_result),
    )

    print(
        "JUMLAH NILAI KOSONG:",
        df_result["data_y"].isna().sum(),
    )

    print(
        "\nDATA BPS:"
    )

    print(
        df_result[
            [
                "kode_wilayah",
                "provinsi",
                "data_x",
                "data_y",
            ]
        ].to_string(
            index=False
        )
    )

    return df_result

def scrape_hotel_akomodasi_publikasi(
    tahun,
    config=None,
):
    """
    Scraping dataset 9869 dari publikasi resmi BPS.

    Sumber:
    Statistik Hotel dan Akomodasi Lainnya di Indonesia 2024
    Lampiran 5:
    Banyaknya Usaha Hotel Bintang Menurut Provinsi, 2020–2024

    Digunakan untuk tahun 2020–2024.

    RAW hanya:
        kode_wilayah
        provinsi
        data_x
        data_y
    """

    if config is None:
        config = get_bps_config(
            "hotel_akomodasi"
        )

    tahun = int(tahun)

    if tahun < 2020 or tahun > 2024:
        raise ValueError(
            "scrape_hotel_akomodasi_publikasi "
            "hanya untuk tahun 2020–2024."
        )

    pdf_path = Path(
        config["publikasi_pdf"]
    )

    if not pdf_path.exists():
        raise FileNotFoundError(
            f"PDF BPS tidak ditemukan: {pdf_path}"
        )

    print("\n========================================")
    print("SCRAPE HOTEL AKOMODASI - PUBLIKASI BPS")
    print("========================================")
    print("TAHUN:", tahun)
    print("PDF:", pdf_path)

    # ==============================
    # NAMA PROVINSI SESUAI TABEL BPS
    # ==============================

    province_names = {
        "01": "Aceh",
        "02": "Sumatera Utara",
        "03": "Sumatera Barat",
        "04": "Riau",
        "05": "Jambi",
        "06": "Sumatera Selatan",
        "07": "Bengkulu",
        "08": "Lampung",
        "09": "Kep. Bangka Belitung",
        "10": "Kepulauan Riau",
        "11": "DKI Jakarta",
        "12": "Jawa Barat",
        "13": "Jawa Tengah",
        "14": "D.I. Yogyakarta",
        "15": "Jawa Timur",
        "16": "Banten",
        "17": "Bali",
        "18": "Nusa Tenggara Barat",
        "19": "Nusa Tenggara Timur",
        "20": "Kalimantan Barat",
        "21": "Kalimantan Tengah",
        "22": "Kalimantan Selatan",
        "23": "Kalimantan Timur",
        "24": "Kalimantan Utara",
        "25": "Sulawesi Utara",
        "26": "Sulawesi Tengah",
        "27": "Sulawesi Selatan",
        "28": "Sulawesi Tenggara",
        "29": "Gorontalo",
        "30": "Sulawesi Barat",
        "31": "Maluku",
        "32": "Maluku Utara",
        "33": "Papua Barat",
        "34": "Papua Barat Daya",
        "35": "Papua",
        "36": "Papua Selatan",
        "37": "Papua Tengah",
        "38": "Papua Pegunungan",
    }

    year_index = {
        2020: 0,
        2021: 1,
        2022: 2,
        2023: 3,
        2024: 4,
    }

    target_index = year_index[tahun]

    # ==============================
    # BUKA PDF
    # ==============================

    document = pymupdf.open(
        str(pdf_path)
    )

    target_page = None

    for page in document:
        text = page.get_text(
            "text"
        )

        if (
            "75\n5\t\nBanyaknya Usaha Hotel Bintang Menurut Provinsi"
            in text
        ):
            target_page = page
            break

    if target_page is None:
        document.close()

        raise RuntimeError(
            "Tabel Lampiran 5 "
            "tidak ditemukan di PDF BPS."
        )

    print(
        "HALAMAN TABEL:",
        target_page.number + 1,
    )

    # ==============================
    # AMBIL WORD
    # ==============================

    words = target_page.get_text(
        "words"
    )

    # ==============================
    # POSISI KOLOM TAHUN
    # ==============================

    year_words = {}

    for word in words:
        text = word[4]

        if text in {
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
        }:
            # Header tabel berada di bagian atas
            if word[1] < 180:
                year_words[int(text)] = word

    if len(year_words) != 5:
        document.close()

        raise RuntimeError(
            "Kolom tahun 2020–2024 "
            "tidak berhasil ditemukan."
        )

    column_centers = [
        (
            year_words[year][0]
            + year_words[year][2]
        ) / 2
        for year in [
            2020,
            2021,
            2022,
            2023,
            2024,
        ]
    ]

    # ==============================
    # CARI KODE PROVINSI
    # ==============================

    province_code_words = {}

    for word in words:
        text = word[4]

        if re.fullmatch(
            r"\d{2}\.",
            text,
        ):
            code = text[:2]

            if code in province_names:
                province_code_words[
                    code
                ] = word

    if len(province_code_words) != 38:
        document.close()

        raise RuntimeError(
            "Tidak semua 38 provinsi "
            "berhasil ditemukan di tabel. "
            f"Ditemukan: "
            f"{len(province_code_words)}"
        )

    # ==============================
    # PARSE NILAI PROVINSI
    # ==============================

    rows = []

    for code in [
        f"{i:02d}"
        for i in range(1, 39)
    ]:

        code_word = province_code_words[
            code
        ]

        row_y = code_word[1]

        values = []

        for center_x in column_centers:

            candidates = []

            for word in words:

                word_text = word[4]

                # Harus berada pada baris
                if abs(
                    word[1] - row_y
                ) >= 3:
                    continue

                # Harus berada di area
                # kolom angka
                if word[0] < 200:
                    continue

                word_center = (
                    word[0] + word[2]
                ) / 2

                if abs(
                    word_center - center_x
                ) >= 12:
                    continue

                # Angka atau tanda kosong BPS
                if not (
                    re.fullmatch(
                        r"\d+",
                        word_text,
                    )
                    or word_text == "─"
                ):
                    continue

                # Hindari watermark
                if word[3] - word[1] > 12:
                    continue

                candidates.append(word)

            if not candidates:
                values.append(None)
            else:
                candidates.sort(
                    key=lambda x: abs(
                        (
                            x[0] + x[2]
                        ) / 2
                        - center_x
                    )
                )

                values.append(
                    candidates[0][4]
                )

        if len(values) != 5:
            document.close()

            raise RuntimeError(
                f"Gagal membaca baris "
                f"provinsi {code}."
            )

        raw_value = values[
            target_index
        ]

        if raw_value == "─":
            data_y = pd.NA
        else:
            data_y = int(raw_value)

        rows.append({
            "kode_wilayah": code,
            "provinsi": province_names[
                code
            ],
            "data_x": pd.Timestamp(
                year=tahun,
                month=12,
                day=31,
            ),
            "data_y": data_y,
        })

    # ==============================
    # PARSE NASIONAL
    # ==============================

    # Cari baris Indonesia dari teks
    # PDF. Nilai nasional berada
    # setelah baris provinsi 38.

    indonesia_candidates = []

    for word in words:

        text = word[4]

        if not re.fullmatch(
            r"\d[\d.]*",
            text,
        ):
            continue

        # Indonesia berada di bawah
        # baris provinsi 38.
        if word[1] <= (
            province_code_words["38"][1]
        ):
            continue

        if word[0] < 200:
            continue

        word_center = (
            word[0] + word[2]
        ) / 2

        if not any(
            abs(
                word_center - center
            ) < 15
            for center in column_centers
        ):
            continue

        indonesia_candidates.append(
            word
        )

    national_values = []

    for center_x in column_centers:

        candidates = [
            word
            for word in indonesia_candidates
            if abs(
                (
                    word[0] + word[2]
                ) / 2
                - center_x
            ) < 15
        ]

        if not candidates:
            national_values.append(
                None
            )
            continue

        candidates.sort(
            key=lambda x: abs(
                (
                    x[0] + x[2]
                ) / 2
                - center_x
            )
        )

        national_values.append(
            candidates[0][4]
        )

    national_raw = national_values[
        target_index
    ]

    if national_raw is None:
        document.close()

        raise RuntimeError(
            "Nilai nasional Indonesia "
            f"tahun {tahun} tidak ditemukan."
        )

    national_value = int(
        national_raw.replace(
            ".",
            "",
        )
    )

    rows.append({
        "kode_wilayah": "0000000",
        "provinsi": "Indonesia",
        "data_x": pd.Timestamp(
            year=tahun,
            month=12,
            day=31,
        ),
        "data_y": national_value,
    })

    document.close()

    df_result = pd.DataFrame(
        rows
    )

    # ==============================
    # RAW COLUMN VALIDATION
    # ==============================

    raw_columns = [
        "kode_wilayah",
        "provinsi",
        "data_x",
        "data_y",
    ]

    df_result = df_result[
        raw_columns
    ].copy()

    print(
        "\nJUMLAH DATA:",
        len(df_result),
    )

    print(
        "NILAI KOSONG:",
        df_result["data_y"].isna().sum(),
    )

    print(
        "\nDATA HASIL PDF BPS:"
    )

    print(
        df_result.to_string(
            index=False
        )
    )

    return df_result

def scrape_hotel_akomodasi_range(
    tahun_awal,
    tahun_akhir,
    config=None,
    raw_file=None,
):
    """
    Scrape dataset 9869 untuk range tahun.

    Sumber:
        2020–2024 -> Publikasi BPS 2024
        2025      -> BPS SIMDASI

    RAW disimpan PER TAHUN.

    Format file:
        {file_name}_{tahun}_raw.parquet

    Isi RAW hanya:
        kode_wilayah
        provinsi
        data_x
        data_y

    Data tahun yang tidak sedang di-refresh
    tetap dipertahankan.

    Return:
        Gabungan seluruh raw tahun untuk
        kebutuhan proses ETL / matching.
    """

    if config is None:
        config = get_bps_config(
            "hotel_akomodasi"
        )

    tahun_awal = int(tahun_awal)
    tahun_akhir = int(tahun_akhir)

    print("\n========================================")
    print("SCRAPE HOTEL AKOMODASI - RANGE")
    print("========================================")
    print(
        f"TAHUN: {tahun_awal} - {tahun_akhir}"
    )

    # ==================================================
    # RAW COLUMNS
    # ==================================================

    raw_columns = [
        "kode_wilayah",
        "provinsi",
        "data_x",
        "data_y",
    ]

    # ==================================================
    # RAW DIRECTORY
    # ==================================================

    if raw_file is not None:
        raw_file = Path(raw_file)
        raw_dir = raw_file.parent
    else:
        raw_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "bps"
            / config["storage_name"]
        )

    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_name = config["file_name"]

    # ==================================================
    # DATA HASIL SCRAPING
    # ==================================================

    scraped_data = {}

    # ==================================================
    # SCRAPE PER TAHUN
    # ==================================================

    for tahun in range(
        tahun_awal,
        tahun_akhir + 1,
    ):

        print("\n----------------------------------------")
        print(f"SCRAPE TAHUN {tahun}")
        print("----------------------------------------")

        try:

            # ------------------------------------------
            # SOURCE
            # ------------------------------------------

            if 2020 <= tahun <= 2024:

                print(
                    "SOURCE: PUBLIKASI BPS 2024"
                )

                df = scrape_hotel_akomodasi_publikasi(
                    tahun=tahun,
                    config=config,
                )

            else:

                print(
                    "SOURCE: BPS SIMDASI"
                )

                df = scrape_hotel_akomodasi(
                    tahun=tahun,
                    config=config,
                )

            # ------------------------------------------
            # VALIDASI DATA
            # ------------------------------------------

            if df.empty:

                print(
                    f"⚠️ Data kosong tahun {tahun}"
                )

                continue

            # ------------------------------------------
            # RAW ONLY
            # ------------------------------------------

            df = df[
                raw_columns
            ].copy()

            df["data_x"] = pd.to_datetime(
                df["data_x"],
                errors="coerce",
            )

            # ------------------------------------------
            # DEDUP
            # ------------------------------------------

            df = (
                df
                .drop_duplicates(
                    subset=[
                        "kode_wilayah",
                        "data_x",
                    ],
                    keep="last",
                )
                .sort_values(
                    [
                        "data_x",
                        "kode_wilayah",
                    ]
                )
                .reset_index(
                    drop=True
                )
            )

            # ------------------------------------------
            # SIMPAN DI MEMORY
            # ------------------------------------------

            scraped_data[tahun] = df

            # ------------------------------------------
            # SAVE RAW PER TAHUN
            # ------------------------------------------

            raw_path_tahun = (
                raw_dir
                / f"{file_name}_{tahun}_raw.parquet"
            )

            df.to_parquet(
                raw_path_tahun,
                index=False,
            )

            print(
                f"✅ RAW {tahun} BERHASIL"
            )

            print(
                f"   ROW  : {len(df)}"
            )

            print(
                f"   FILE : {raw_path_tahun}"
            )

        except Exception as e:

            print(
                f"❌ Gagal tahun {tahun}: {e}"
            )

    # ==================================================
    # LOAD SEMUA RAW TAHUN YANG SUDAH ADA
    # ==================================================

    print("\n========================================")
    print("LOAD RAW PER TAHUN")
    print("========================================")

    all_data = []

    # Tahun yang sedang di-refresh:
    # gunakan hasil scraping terbaru.
    #
    # Tahun di luar range:
    # baca file raw tahunannya.
    #
    # Dengan cara ini 2020/2021 tidak hilang
    # ketika normal update hanya 2022-2025.

    for raw_year_file in sorted(
        raw_dir.glob(
            f"{file_name}_*_raw.parquet"
        )
    ):

        filename = raw_year_file.name

        # ------------------------------------------
        # Ambil tahun dari nama file
        # ------------------------------------------

        match = re.search(
            r"_(\d{4})_raw\.parquet$",
            filename,
        )

        if not match:
            continue

        tahun_file = int(
            match.group(1)
        )

        # ------------------------------------------
        # Kalau tahun sedang di-refresh,
        # pakai hasil scraping yang baru.
        # ------------------------------------------

        if (
            tahun_awal
            <= tahun_file
            <= tahun_akhir
        ):

            if tahun_file in scraped_data:

                df = scraped_data[
                    tahun_file
                ]

                print(
                    f"✓ {tahun_file}: "
                    f"hasil scrape terbaru "
                    f"({len(df)} row)"
                )

                all_data.append(df)

            else:

                print(
                    f"⚠️ {tahun_file}: "
                    "gagal scrape, file lama "
                    "tidak digunakan."
                )

            continue

        # ------------------------------------------
        # Tahun di luar range:
        # tetap gunakan raw lama.
        # ------------------------------------------

        try:

            df = pd.read_parquet(
                raw_year_file
            )

            # Pastikan hanya raw columns
            df = df[
                [
                    column
                    for column in raw_columns
                    if column in df.columns
                ]
            ].copy()

            if "data_x" not in df.columns:
                continue

            df["data_x"] = pd.to_datetime(
                df["data_x"],
                errors="coerce",
            )

            # Dedup
            df = (
                df
                .drop_duplicates(
                    subset=[
                        "kode_wilayah",
                        "data_x",
                    ],
                    keep="last",
                )
                .sort_values(
                    [
                        "data_x",
                        "kode_wilayah",
                    ]
                )
                .reset_index(
                    drop=True
                )
            )

            print(
                f"✓ {tahun_file}: "
                f"raw lama "
                f"({len(df)} row)"
            )

            all_data.append(df)

        except Exception as e:

            print(
                f"⚠️ Gagal membaca raw "
                f"{tahun_file}: {e}"
            )

    # ==================================================
    # MIGRASI RAW LAMA SATU FILE
    # ==================================================
    #
    # Ini penting untuk kondisi kamu sekarang.
    #
    # Sebelumnya:
    #
    # jumlah_..._raw.parquet
    #
    # masih berisi gabungan 2020-2025.
    #
    # Kalau file per tahun belum ada,
    # kita pecah otomatis berdasarkan data_x.
    #
    # ==================================================

    old_raw_file = (
        raw_dir
        / f"{file_name}_raw.parquet"
    )

    if old_raw_file.exists():

        print("\n========================================")
        print("CEK RAW LAMA GABUNGAN")
        print("========================================")
        print(old_raw_file)

        try:

            old_df = pd.read_parquet(
                old_raw_file
            )

            old_df = old_df[
                [
                    column
                    for column in raw_columns
                    if column in old_df.columns
                ]
            ].copy()

            old_df["data_x"] = pd.to_datetime(
                old_df["data_x"],
                errors="coerce",
            )

            old_df["tahun"] = (
                old_df["data_x"].dt.year
            )

            # ------------------------------------------
            # Pecah raw lama berdasarkan tahun
            # ------------------------------------------

            for tahun_file in sorted(
                old_df["tahun"]
                .dropna()
                .astype(int)
                .unique()
            ):

                yearly_file = (
                    raw_dir
                    / f"{file_name}_{tahun_file}_raw.parquet"
                )

                # Jangan overwrite hasil scrape
                # terbaru untuk tahun refresh.
                if (
                    tahun_awal
                    <= tahun_file
                    <= tahun_akhir
                ):

                    if tahun_file in scraped_data:
                        continue

                # Kalau file tahunan belum ada,
                # buat dari raw lama.

                if yearly_file.exists():
                    continue

                df_year = (
                    old_df[
                        old_df["tahun"]
                        == tahun_file
                    ]
                    .drop(
                        columns=["tahun"]
                    )
                    .copy()
                )

                df_year = (
                    df_year
                    .drop_duplicates(
                        subset=[
                            "kode_wilayah",
                            "data_x",
                        ],
                        keep="last",
                    )
                    .sort_values(
                        [
                            "data_x",
                            "kode_wilayah",
                        ]
                    )
                    .reset_index(
                        drop=True
                    )
                )

                df_year.to_parquet(
                    yearly_file,
                    index=False,
                )

                print(
                    f"✓ MIGRASI RAW {tahun_file}: "
                    f"{len(df_year)} row"
                )

                # Tahun lama harus ikut
                # ke df_raw untuk ETL.
                if not (
                    tahun_awal
                    <= tahun_file
                    <= tahun_akhir
                ):
                    all_data.append(
                        df_year
                    )

        except Exception as e:

            print(
                "⚠️ Gagal migrasi raw lama:",
                e,
            )

    # ==================================================
    # VALIDASI
    # ==================================================

    if not all_data:

        raise RuntimeError(
            "Tidak ada data hotel yang "
            "berhasil dimuat."
        )

    # ==================================================
    # GABUNG UNTUK KEPERLUAN ETL
    # ==================================================

    df_raw = pd.concat(
        all_data,
        ignore_index=True,
    )

    # Pastikan hanya raw columns
    df_raw = df_raw[
        raw_columns
    ].copy()

    # ==================================================
    # DEDUP FINAL IN-MEMORY
    # ==================================================

    df_raw["data_x"] = pd.to_datetime(
        df_raw["data_x"],
        errors="coerce",
    )

    df_raw = (
        df_raw
        .drop_duplicates(
            subset=[
                "kode_wilayah",
                "data_x",
            ],
            keep="last",
        )
        .sort_values(
            [
                "data_x",
                "kode_wilayah",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    # ==================================================
    # HAPUS RAW GABUNGAN LAMA
    # ==================================================
    #
    # Setelah raw sudah benar-benar tersedia
    # per tahun, file lama tidak diperlukan lagi.
    #
    # ==================================================

    if old_raw_file.exists():

        try:

            old_raw_file.unlink()

            print(
                "\n✓ RAW GABUNGAN LAMA DIHAPUS:"
            )

            print(
                old_raw_file
            )

        except Exception as e:

            print(
                "\n⚠️ Gagal menghapus "
                "raw gabungan lama:",
                e,
            )

    # ==================================================
    # SUMMARY
    # ==================================================

    print("\n========================================")
    print("RAW PER TAHUN SELESAI")
    print("========================================")

    yearly_files = sorted(
        raw_dir.glob(
            f"{file_name}_*_raw.parquet"
        )
    )

    for yearly_file in yearly_files:

        print(
            f"  {yearly_file.name}"
        )

    print(
        "\nTOTAL RAW UNTUK ETL:",
        len(df_raw),
    )

    print(
        "KOLOM RAW:",
        df_raw.columns.tolist(),
    )

    return df_raw

def clean_hotel_province_name(value):
    if value is None:
        return value

    value = str(value)

    # Hapus tag HTML
    value = re.sub(r"<[^>]+>", "", value)

    # Hapus angka footnote "2" di akhir nama provinsi
    value = re.sub(r"\s*2\s*$", "", value)

    # Rapikan whitespace
    value = re.sub(r"\s+", " ", value).strip()

    return value

def match_hotel_bps_xflask(
    df_bps,
    df_xflask,
):

    df_bps = df_bps.copy()

    df_bps["provinsi"] = (
        df_bps["provinsi"]
        .apply(clean_hotel_province_name)
    )

    """
    Match BPS vs XFlask berdasarkan:

        provinsi + tahun

    Nasional dipisahkan.

    Data BPS tetap dipertahankan meskipun
    belum ada di XFlask.
    """

    print("\n========================================")
    print("MATCH BPS VS XFLASK")
    print("========================================")

    bps = df_bps.copy()
    db = df_xflask.copy()

    # ==============================
    # VALIDASI
    # ==============================

    required_bps = [
        "kode_wilayah",
        "provinsi",
        "data_x",
        "data_y",
    ]

    required_db = [
        "provinsi",
        "id_provinsi",
        "date",
        "data_y",
    ]

    for column in required_bps:
        if column not in bps.columns:
            raise RuntimeError(
                f"Kolom BPS tidak ditemukan: {column}"
            )

    for column in required_db:
        if column not in db.columns:
            raise RuntimeError(
                f"Kolom XFlask tidak ditemukan: {column}"
            )

    # ==============================
    # DATE
    # ==============================

    bps["data_x"] = pd.to_datetime(
        bps["data_x"],
        errors="coerce",
    )

    db["date"] = pd.to_datetime(
        db["date"],
        errors="coerce",
    )

    bps["tahun_match"] = (
        bps["data_x"].dt.year
    )

    db["tahun_match"] = (
        db["date"].dt.year
    )

    # ==============================
    # NORMALISASI NAMA
    # ==============================

    bps["nama_match"] = (
        bps["provinsi"]
        .apply(normalize_province_name)
    )

    db["nama_match"] = (
        db["provinsi"]
        .apply(normalize_province_name)
    )

    # ==============================
    # PISAH NASIONAL
    # ==============================

    db_provinsi = db[
        db["id_provinsi"] != 0
    ].copy()

    # ==============================
    # MAPPING PROVINSI + TAHUN
    # ==============================

    mapping = (
        db_provinsi[
            [
                "nama_match",
                "tahun_match",
                "provinsi",
                "id_provinsi",
            ]
        ]
        .drop_duplicates(
            subset=[
                "nama_match",
                "tahun_match",
            ]
        )
    )

    result = bps.merge(
        mapping,
        on=[
            "nama_match",
            "tahun_match",
        ],
        how="left",
        suffixes=(
            "_bps",
            "_xflask",
        ),
    )

    # ==============================
    # STATUS
    # ==============================

    result["status_match"] = (
        "PROVINSI BARU"
    )

    result.loc[
        result["id_provinsi"].notna(),
        "status_match",
    ] = "UPDATE EXISTING"

    result.loc[
        result["data_y"].isna(),
        "status_match",
    ] = "SKIP NILAI KOSONG"

    result["is_nasional"] = (
        result["kode_wilayah"]
        .astype("string")
        .eq("0000000")
    )

    result.loc[
        result["is_nasional"],
        "status_match",
    ] = "NASIONAL"

    # ==============================
    # SUMMARY
    # ==============================

    print(
        "\nTOTAL DATA BPS:",
        len(result),
    )

    print(
        "UPDATE EXISTING:",
        (
            result["status_match"]
            == "UPDATE EXISTING"
        ).sum(),
    )

    print(
        "PROVINSI BARU:",
        (
            result["status_match"]
            == "PROVINSI BARU"
        ).sum(),
    )

    print(
        "NILAI KOSONG:",
        (
            result["status_match"]
            == "SKIP NILAI KOSONG"
        ).sum(),
    )

    print(
        "NASIONAL:",
        (
            result["status_match"]
            == "NASIONAL"
        ).sum(),
    )

    return result


def validate_hotel_id_provinsi(
    df_match,
):
    """
    Validasi mapping id_provinsi XFlask.

    Yang dicek:

    1. Semua provinsi existing punya ID.
    2. Tidak ada dua provinsi yang mendapat
       id_provinsi yang sama.
    3. Provinsi baru dipisahkan.
    """

    print("\n========================================")
    print("VALIDASI ID PROVINSI XFLASK")
    print("========================================")

    existing = df_match[
        df_match["status_match"]
        == "UPDATE EXISTING"
    ].copy()

    new_province = df_match[
        df_match["status_match"]
        == "PROVINSI BARU"
    ].copy()

    # ==============================
    # JUMLAH EXISTING
    # ==============================

    print(
        "TOTAL PROVINSI EXISTING:",
        len(existing),
    )

    print(
        "TOTAL PROVINSI BARU:",
        len(new_province),
    )

    # ==============================
    # CEK ID NULL
    # ==============================

    missing_id = existing[
        existing["id_provinsi"].isna()
    ]

    if not missing_id.empty:
        print(
            "\n❌ ADA ID PROVINSI YANG NULL:"
        )

        print(
            missing_id[
                ["provinsi_bps"]
            ].to_string(
                index=False
            )
        )

        raise RuntimeError(
            "Mapping id_provinsi XFlask tidak lengkap."
        )

    # ==============================
    # CEK DUPLIKAT ID
    # ==============================

    print("TOTAL ROW EXISTING:", len(existing))
    print("TOTAL PROVINSI EXISTING:", existing["provinsi_bps"].nunique())    

    mapping_id = (
        existing[
            ["provinsi_bps", "id_provinsi"]
        ]
        .drop_duplicates()
    )

    duplicate_mask = (
        mapping_id["id_provinsi"]
        .duplicated(keep=False)
    )

    duplicate_id = mapping_id[
        duplicate_mask
    ].copy()

    print(
        "DUPLIKAT ID:",
        len(duplicate_id),
    )

    if not duplicate_id.empty:
        print(
            "\n❌ DUPLIKAT ID PROVINSI:"
        )

        print(
            duplicate_id[
                [
                    "provinsi_bps",
                    "id_provinsi",
                ]
            ].sort_values(
                "id_provinsi"
            ).to_string(
                index=False
            )
        )

        raise RuntimeError(
            "Ada id_provinsi XFlask "
            "yang dipakai lebih dari satu provinsi."
        )

    print(
        "\n✅ ID PROVINSI EXISTING UNIK."
    )

    # ==============================
    # PROVINSI BARU
    # ==============================

    if not new_province.empty:
        print(
            "\nPROVINSI BARU:"
        )

        print(
            new_province[
                [
                    "provinsi_bps",
                    "data_y",
                ]
            ].to_string(
                index=False
            )
        )

        print(
            "\n⚠️ Provinsi baru belum diberi "
            "id_provinsi XFlask."
        )

    return existing, new_province

def inspect_xflask_gini(id_nama_data=14288):
    """
    Mengecek struktur data Gini Ration yang sudah ada di XFlask.
    """

    df = get_existing_xflask_data(id_nama_data)

    print("\n" + "=" * 60)
    print("EXISTING XFLASK")
    print("=" * 60)

    print("ID NAMA DATA:", id_nama_data)
    print("RAW RECORD:", len(df))

    print("\nKOLOM:")
    print(df.columns.tolist())

    if "provinsi" in df.columns:
        print("\nPROVINISI:")
        print(
            df.groupby("provinsi")["kota"]
            .unique()
            .sort_values(ascending=False)
            .to_string()
        )

    if "item" in df.columns:
        print("\nITEM:")
        print(
            df["item"]
            .value_counts(dropna=False)
            .to_string()
        )

    return df

class BPSETL:

    def __init__(self):

        config = get_bps_config("gini_ratio")
        paths = get_storage_paths(config)

        self.raw_file = paths["raw_file"]
        self.final_file = paths["final_file"]

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


    def load_raw(self):

        print(
            "\n=============================="
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


    def get_nama_data(self):

        print(
            "\n=============================="
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


    def match_nama_data(
        self,
        df_nama,
        indikator,
    ):

        print(
            "\n=============================="
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

            print(
                "TARGET:",
                indikator,
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
            row["nama"]
        )

        print(
            "ID:",
            id_nama_data
        )


        return id_nama_data


    def transform(
        self,
        df_raw,
        id_nama_data,
        config,
    ):

        indikator = config["indikator"]
        default_satuan = config.get (
            "default_satuan",
            pd.NA,
        )

        sumber = config.get (
            "sumber",
            "Badan Pusat Statistik",
        )

        note = config.get(
            "note",
            pd.NA,
        )

        nama_data_import = config.get(
            "nama_data_import",
            pd.NA,
        )

        print(
            "\nWILAYAH RAW:"
        )

        print(
            df_raw["wilayah"]
            .drop_duplicates()
            .to_string(index=False)
        )


        print(
            "\nJUMLAH WILAYAH:",
            df_raw["wilayah"].nunique()
        )


        print(
            "\n=============================="
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
                "wilayah",
                "provinsi",
            ],
            date_columns=[
                "data_x",
            ],
        )


        # BUSINESS LOGIC BPS
        # BUANG BARIS PROVINSI

        df = df[
            ~df["kode_wilayah"]
            .str.endswith("00")
        ].copy()

        # Buang wilayah provinsi yang ikut terbaca sebagai kabupaten/kota
        df = df[
            ~df["wilayah"].isin([
                "Provinsi Jawa Tengah",
                "Nusa Tenggara Timur",
                "Kalimantan Timur",
            ])
        ].copy()


        print(
            "JUMLAH ROW SETELAH "
            "BUANG PROVINSI:",
            len(df),
        )

        print("\nDEBUG NTT WILAYAH:")
        print(
            df[
                df["wilayah"].astype("string").str.contains(
                    "Kupang",
                    case=False,
                    na=False,
                )
            ][
                ["kode_wilayah", "wilayah", "provinsi", "turvar", "data_x", "data_y"]
            ].to_string(index=False)
        )

        # BUSINESS LOGIC BPS
        # TENTUKAN KABUPATEN / KOTA

        kode_belakang = pd.to_numeric(
            df["kode_wilayah"]
            .astype("string")
            .str[-2:],
            errors="coerce",
        )

        # Sebagian domain BPS menggunakan kode vervar pendek,
        # sehingga penentuan kota tidak bisa hanya berdasarkan angka kode.
        # Contoh NTT:
        # 3  = Kupang
        # 22 = Kota Kupang
        is_kota = (
            (kode_belakang >= 71)
            | df["wilayah"]
                .astype("string")
                .str.match(
                    r"^(?:\d+\s+)?Kota\s+",
                    case=False,
                    na=False,
                )
        )

        # Bersihkan nama wilayah dari:
        # - kode wilayah di depan, contoh: "3310 Kabupaten Klaten"
        # - prefix "Kabupaten"
        # - prefix "Kab."
        # - prefix "Kota"
        df["kota"] = (
            df["wilayah"]
            .astype("string")
            .str.strip()
            .str.replace(r"^\d+\s+", "", regex=True)
            .str.replace(r"^Kabupaten\s+", "", regex=True, case=False)
            .str.replace(r"^Kab\.\s*", "", regex=True, case=False)
            .str.replace(r"^Kota\s+", "", regex=True, case=False)
            .str.strip()
        )

        # Kabupaten
        df.loc[~is_kota, "kota"] = (
            "Kab. " + df.loc[~is_kota, "kota"]
        )

        # Kota
        df.loc[is_kota, "kota"] = (
            "Kota " + df.loc[is_kota, "kota"]
        )

        # BUANG WILAYAH YANG BUKAN KABUPATEN/KOTA
        df = df[
            ~df["kota"].isin([
                "Kab. Provinsi Jawa Tengah",
                "Kab. Nusa Tenggara Timur",
                "Kota Kalimantan Timur",
            ])
        ].copy()
        # SPECIAL CASE BPS

        df.loc[
            df["wilayah"]
            == "Labuanbatu Utara",
            "kota",
        ] = (
            "Kab. Labuhanbatu Utara"
        )


        df.loc[
            df["wilayah"]
            == "Toba",
            "kota",
        ] = (
            "Kab. Toba Samosir"
        )

        # SPECIAL CASE BPS
        df.loc[
            df["kota"] == "Kab. Surakarta",
            "kota"
        ] = "Kota Surakarta"

        df.loc[
            df["kota"] == "Kab. Salatiga",
            "kota"
        ] = "Kota Salatiga"

        df.loc[
            df["kota"] == "Kab. Mahakam Ulu",
            "kota"
        ] = "Kab. Mahakam Hulu"

        df.loc[
            df["kota"] == "Kab. Kepulauan Sitaro",
            "kota"
        ] = "Kab. Kep. Siau Tagulandang Biaro"


        # FINAL DATA

        df_final = pd.DataFrame({

            "kota":
                df["kota"],

            "provinsi":
                df["provinsi"],

            "nama_indikator":
                indikator,

            "nama_item": (
                df["turvar"]
                if "turvar" in df.columns
                else pd.NA
            ),

            "idnamadata":
                id_nama_data,

            "data_x": (
                pd.to_datetime(
                    df["data_x"],
                    errors="coerce",
                )
                .apply(
                    lambda x: (
                        x.replace(month=12, day=31)
                        if pd.notna(x)
                        else pd.NaT
                    )
                )
                .dt.strftime("%d-%m-%Y")
            ),

            "data_y":
                pd.to_numeric(
                    df["data_y"],
                    errors="coerce",
                ),

            "satuan": (
                df["satuan"]
                .fillna(default_satuan)
                .astype("string")
                .str.strip()
                .replace(
                    "",
                    default_satuan,
                )
            ),

            "sumber": sumber, 

            "note": note,

            "nama_data_import": nama_data_import,
        })


        df_final = df_final[
            df_final["data_y"].notna()
        ].copy()

        if "nama_item" in df_final.columns:
            df_final["nama_item"] = (
                df_final["nama_item"]
                .astype("string")
                .str.strip()
                .replace({
                    "SMTA": "SMA",
                    "Laki-Laki": "Laki-laki",
                    "Laki-laki+Perempuan": "Jumlah",
                })
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
            "\n=============================="
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
            "\nFINAL FILE:",
            self.final_file,
        )


        return self.final_file


    def upload_minio(self):

        print(
            "\n=============================="
        )

        print(
            "UPLOAD MINIO"
        )

        print(
            "=============================="
        )


        with open(
            self.final_file,
            "rb",
        ) as file:

            csv_bytes = file.read()


        object_name = (
            "bps/final/"
            "bps_gini_kabupaten_kota.csv"
        )


        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            csv_bytes,
            object_name,
            "text/csv",
        )


        print(
            "MinIO upload berhasil:"
        )

        print(
            f"{MINIO_BUCKET}/{object_name}"
        )

    def run(
        self,
        df_raw=None,
        save_outputs=True,
        config=None,
    ):
        print(
            "\n================================"
        )
        print(
            "BPS ETL"
        )
        print(
            "================================"
        )

        if config is None:
            config = get_bps_config(
                "gini_ratio"
            )

        if df_raw is None:
            df_raw = self.load_raw()
        else:
            print(
                "\nMENGGUNAKAN RAW DATA "
                "DARI MEMORY"
            )
            print(
                "JUMLAH RAW DATA:",
                len(df_raw),
            )

        indikator = config["indikator"]

        df_nama = self.get_nama_data()

        id_nama_data = self.match_nama_data(
            df_nama,
            indikator,
        )

        if id_nama_data is None:
            raise RuntimeError(
                f"Gagal mendapatkan "
                f"idnamadata untuk {indikator}."
            )

        df_final = self.transform(
            df_raw,
            id_nama_data,
            config,
        )

        if save_outputs:
            self.save_final(
                df_final
            )

            self.upload_minio()

        else:
            print(
                "\n================================"
            )
            print(
                "DRY RUN - OUTPUT "
                "TIDAK DISIMPAN"
            )
            print(
                "================================"
            )

        return df_final

def _resolve_gini_idnamadata(
    idnamadata=None,
):
    """Ambil idnamadata Gini Ratio jika tidak diberikan."""

    if idnamadata is not None:
        return int(idnamadata)

    config = get_bps_config(
        "gini_ratio"
    )

    etl = BPSETL()

    df_nama = etl.get_nama_data()

    id_nama_data = etl.match_nama_data(
        df_nama,
        config["indikator"],
    )

    if id_nama_data is None:
        raise RuntimeError(
            "Gagal mendapatkan "
            "idnamadata Gini Ratio."
        )

    return id_nama_data

def get_latest_bps_year(config):
    """
    Mengambil tahun terbaru yang tersedia dari seluruh konfigurasi BPS.
    """

    tahun_terbaru = None

    for scraper_config in config["configs"]:
        scraper = BPSScraper(
            domain_id=scraper_config["domain_id"],
            var_id=scraper_config["var_id"],
            provinsi=scraper_config["provinsi"],
        )

        years = scraper.get_available_years()

        tahun_valid = [
            int(year["th"])
            for year in years
            if str(year.get("th", "")).isdigit()
        ]

        if tahun_valid:
            kandidat = max(tahun_valid)

            if (
                tahun_terbaru is None
                or kandidat > tahun_terbaru
            ):
                tahun_terbaru = kandidat

    if tahun_terbaru is None:
        raise RuntimeError(
            "Tidak ditemukan tahun data BPS."
        )

    return tahun_terbaru

def get_existing_latest_year(id_nama_data):
    """
    Mengambil data existing dari XFlask dan mencari
    data_x terbaru yang sudah tersimpan.
    """

    df_existing = get_existing_xflask_data(
        id_nama_data
    )

    if df_existing.empty:
        return None

    if "data_x" not in df_existing.columns:
        raise RuntimeError(
            "Kolom data_x tidak ditemukan "
            "di data existing XFlask."
        )

    data_x = pd.to_datetime(
        df_existing["data_x"],
        errors="coerce",
        dayfirst=True,
    )

    data_x = data_x.dropna()

    if data_x.empty:
        return None

    return data_x.dt.year.max()

def map_turvar_to_idnamadata(
    df,
    turvar_column,
    mapping,
):
    """
    Mapping nilai turvar ke id_nama_data.

    Contoh:
        turvar = ["indikator 1", "indikator 2", "indikator 3"]

        mapping = {
            "indikator 1": 1,
            "indikator 2": 2,
            "indikator 3": 3,
        }
    """

    if turvar_column not in df.columns:
        raise KeyError(
            f"Kolom turvar tidak ditemukan: {turvar_column}"
        )

    df = df.copy()

    df["idnamadata"] = (
        df[turvar_column]
        .map(mapping)
    )

    unmapped = (
        df.loc[
            df["idnamadata"].isna(),
            turvar_column,
        ]
        .dropna()
        .unique()
        .tolist()
    )

    if unmapped:
        raise ValueError(
            "Ada nilai turvar yang belum memiliki mapping: "
            f"{unmapped}"
        )

    return df

def update_bps_dataset(
    config,
    idnamadata=None,
    iditem=None,
    backfill=True,
):
    """
    Generic update untuk dataset BPS.

    backfill=True:
        Ambil seluruh histori.

    backfill=False:
        Ambil data terbaru + maksimal dua periode sebelumnya.

    Output:
        - raw parquet
        - final csv
        - upload raw ke MinIO
        - upload final ke MinIO
    """

    # ==============================
    # RESOLVE ID NAMA DATA
    # ==============================
    if idnamadata is None:
        etl = BPSETL()

        df_nama = etl.get_nama_data()

        idnamadata = etl.match_nama_data(
            df_nama,
            config["indikator"],
        )

        if idnamadata is None:
            raise RuntimeError(
                f"Gagal mendapatkan idnamadata "
                f"untuk {config['indikator']}."
            )

    # ==============================
    # STORAGE PATH
    # ==============================
    paths = get_storage_paths(config)

    raw_file = paths["raw_file"]
    final_file = paths["final_file"]

    print("\n==============================")
    print("STORAGE")
    print("==============================")

    print("RAW:")
    print(raw_file)

    print("FINAL:")
    print(final_file)

    # ==============================
    # TENTUKAN RENTANG TAHUN
    # ==============================
    if backfill:
        tahun_awal = None
        tahun_akhir = None

        print("\n========================================")
        print("BPS DATASET - BACKFILL")
        print("========================================")

    else:
        tahun_existing = get_existing_latest_year(
            idnamadata
        )

        tahun_terbaru = get_latest_bps_year(
            config
        )

        if tahun_existing is None:
            tahun_awal = tahun_terbaru - 2
        else:
            tahun_awal = tahun_existing - 2

        tahun_akhir = tahun_terbaru

        print(
            "\nTAHUN TERAKHIR EXISTING:",
            tahun_existing,
        )

        print(
            "TAHUN TERBARU BPS:",
            tahun_terbaru,
        )

        print(
            "RENTANG UPDATE:",
            f"{tahun_awal}-{tahun_akhir}",
        )

    # ==============================
    # SCRAPING
    # ==============================
    df_raw = scrape_all(
        configs=config["configs"],
        indikator=config["indikator"],
        save_raw=True,
        raw_file=raw_file,
        tahun_awal=tahun_awal,
        tahun_akhir=tahun_akhir,
    )

    if df_raw.empty:
        raise RuntimeError(
            f"Tidak ada data yang berhasil diambil "
            f"untuk {config['indikator']}."
        )

    # ==============================
    # TRANSFORM
    # ==============================
    etl = BPSETL()

    df_final = etl.transform(
        df_raw,
        idnamadata,
        config,
    )

    # ==============================
    # SAVE FINAL
    # ==============================
    print("\n==============================")
    print("SAVE FINAL CSV")
    print("==============================")

    save_csv(
        df_final,
        final_file,
    )

    print(
        "\nFINAL FILE:",
        final_file,
    )

    # ==============================
    # UPLOAD MINIO
    # ==============================
    print("\n==============================")
    print("UPLOAD MINIO")
    print("==============================")

    storage_name = config["storage_name"]
    file_name = config["file_name"]

    # RAW
    with open(raw_file, "rb") as file:
        raw_bytes = file.read()

    raw_object_name = (
        f"bps/{storage_name}/raw/"
        f"{file_name}_raw.parquet"
    )

    upload_bytes(
        etl.minio_client,
        MINIO_BUCKET,
        raw_bytes,
        raw_object_name,
        "application/octet-stream",
    )

    print(
        "RAW MinIO upload berhasil:"
    )
    print(
        f"{MINIO_BUCKET}/{raw_object_name}"
    )

    # FINAL
    with open(final_file, "rb") as file:
        csv_bytes = file.read()

    final_object_name = (
        f"bps/{storage_name}/final/"
        f"{file_name}.csv"
    )

    upload_bytes(
        etl.minio_client,
        MINIO_BUCKET,
        csv_bytes,
        final_object_name,
        "text/csv",
    )

    print(
        "FINAL MinIO upload berhasil:"
    )
    print(
        f"{MINIO_BUCKET}/{final_object_name}"
    )

    return df_final

def update_idnamadata_hotel(
    idnamadata=9869,
    tahun=None,
    backfill=False,
):
    """
    Pipeline dataset 9869.

    backfill=True:
        scrape seluruh histori 2000-2019.

    backfill=False:
        scrape 3 periode:
        tahun terbaru + 2 periode sebelumnya.
    """

    print("\n")
    print("=" * 60)
    print("HOTEL AKOMODASI - 9869")
    print("=" * 60)

    config = get_bps_config(
        "hotel_akomodasi"
    )

    paths = get_storage_paths(config)

    raw_file = paths["raw_file"]
    final_file = paths["final_file"]

    # ==============================
    # EXISTING XFLASK
    # ==============================

    print("\n")
    print("=" * 60)
    print("STEP 1 - EXISTING XFLASK")
    print("=" * 60)

    df_xflask = get_xflask_api_data(
        idnamadata
    )

    if df_xflask.empty:
        raise RuntimeError(
            "Data existing XFlask kosong."
        )
    
    # ==============================
    # TENTUKAN RANGE REFRESH
    # ==============================

    tahun_terbaru_bps = int(
        config["tahun_terbaru"]
    )

    if tahun is not None:
        tahun_terbaru_bps = int(tahun)

    # Dataset 9869:
    # 2022-2024 -> publikasi BPS
    # 2025       -> SIMDASI
    #
    # RAW lama (mis. 2018/2019)
    # tetap dipertahankan oleh
    # scrape_hotel_akomodasi_range().

    tahun_awal = 2022
    tahun_akhir = tahun_terbaru_bps

    if tahun_akhir < tahun_awal:
        raise ValueError(
            f"Tahun {tahun_akhir} tidak valid. "
            "Untuk refresh dataset hotel, "
            "minimal tahun 2022."
        )

    print(
        "\nMODE REFRESH HOTEL"
    )

    print(
        "SOURCE 2022-2024: "
        "Publikasi BPS"
    )

    print(
        "SOURCE 2025: "
        "BPS SIMDASI"
    )

    print(
        "RANGE REFRESH:",
        f"{tahun_awal}-{tahun_akhir}",
    )

    print(
        "CATATAN: RAW lama di luar "
        "range refresh tetap dipertahankan."
    )

    # ==============================
    # SCRAPE
    # ==============================

    print("\n")
    print("=" * 60)
    print("STEP 2 - SCRAP BPS")
    print("=" * 60)

    df_bps = scrape_hotel_akomodasi_range(
        tahun_awal=tahun_awal,
        tahun_akhir=tahun_akhir,
        config=config,
        raw_file=raw_file,
    )

    if df_bps.empty:
        raise RuntimeError(
            "Data BPS kosong."
        )

    # ==============================
    # MATCH
    # ==============================

    print("\n")
    print("=" * 60)
    print("STEP 3 - MATCH BPS VS XFLASK")
    print("=" * 60)

    df_match = match_hotel_bps_xflask(
        df_bps,
        df_xflask,
    )

    # ==============================
    # VALIDASI ID
    # ==============================

    print("\n")
    print("=" * 60)
    print("STEP 4 - VALIDASI ID PROVINSI")
    print("=" * 60)

    validate_hotel_id_provinsi(
        df_match
    )

    # ==============================
    # FINAL
    # ==============================

    print("\n")
    print("=" * 60)
    print("STEP 5 - FINAL DATA")
    print("=" * 60)

    df_final = df_match.copy()

    df_final["idnamadata"] = (
        idnamadata
    )

    df_final["nama_indikator"] = (
        config["indikator"]
    )

    df_final["satuan"] = (
        config["default_satuan"]
    )

    df_final["sumber"] = (
        config["sumber"]
    )

    df_final["note"] = (
        config["note"]
    )

    df_final["nama_data_import"] = (
        config["nama_data_import"]
    )

    # ==============================
    # FINAL COLUMNS
    # ==============================

    if "provinsi_bps" in df_final.columns:
        df_final["provinsi"] = df_final["provinsi_bps"]

    final_columns = [
        "idnamadata",
        "nama_indikator",
        "kode_wilayah",
        "provinsi",
        "id_provinsi",
        "data_x",
        "data_y",
        "satuan",
        "sumber",
        "note",
        "nama_data_import",
        "status_match",
    ]

    final_columns = [
        column
        for column in final_columns
        if column in df_final.columns
    ]

    df_final = df_final[
        final_columns
    ].copy()

    # ==============================
    # SAVE FINAL
    # ==============================

    # Format data_x sesuai schema XFlask: DD-MM-YYYY
    if "data_x" in df_final.columns:
        df_final["data_x"] = pd.to_datetime(
            df_final["data_x"],
            errors="coerce",
        ).dt.strftime("%d-%m-%Y")

    save_csv(
        df_final,
        final_file,
    )

    print(
        "\nFINAL FILE:",
        final_file,
    )

    # ==============================
    # MINIO
    # ==============================

    etl = BPSETL()

    storage_name = config[
        "storage_name"
    ]

    file_name = config[
        "file_name"
    ]

    # ============================================================
    # UPLOAD RAW PARQUET PER TAHUN
    # ============================================================

    raw_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "bps"
        / config["storage_name"]
    )

    raw_files = sorted(
        raw_dir.glob(
            f"{config['file_name']}_*_raw.parquet"
        )
    )

    if not raw_files:
        raise FileNotFoundError(
            f"Tidak ditemukan raw parquet per tahun di: {raw_dir}"
        )

    print("\n========================================")
    print("UPLOAD RAW PARQUET PER TAHUN")
    print("========================================")

    for raw_path in raw_files:

        # Ambil tahun dari nama file
        match = re.search(
            r"_(\d{4})_raw\.parquet$",
            raw_path.name,
        )

        if not match:
            continue

        tahun = match.group(1)

        print(
            f"UPLOAD RAW {tahun}: {raw_path.name}"
        )

        with open(
            raw_path,
            "rb",
        ) as f:
            raw_bytes = f.read()

        raw_object_name = (
            f"bps/{config['storage_name']}/raw/"
            f"{raw_path.name}"
        )

        upload_bytes(
            etl.minio_client,
            MINIO_BUCKET,
            raw_bytes,
            raw_object_name,
            "application/octet-stream",
        )

        print(
            f"✅ RAW {tahun} berhasil di-upload"
        )

    # FINAL
    with open(
        final_file,
        "rb",
    ) as file:
        csv_bytes = file.read()

    final_object_name = (
        f"bps/{storage_name}/final/"
        f"{file_name}.csv"
    )

    upload_bytes(
        etl.minio_client,
        MINIO_BUCKET,
        csv_bytes,
        final_object_name,
        "text/csv",
    )

    print(
        "\nFINAL MinIO upload berhasil:"
    )
    print(
        f"{MINIO_BUCKET}/{final_object_name}"
    )

    print("\n")
    print("=" * 60)
    print("PIPELINE 9869 SELESAI")
    print("=" * 60)

    print(
        "TOTAL RAW:",
        len(df_bps),
    )

    print(
        "TOTAL FINAL:",
        len(df_final),
    )

    return df_final

def update_idnamadata_ginirasio(
    idnamadata=None,
    iditem=None,
    backfill=True,
):
    """
    Update dataframe Gini Ratio.
    """

    config = get_bps_config(
        "gini_ratio"
    )

    return update_bps_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def update_curr_ginirasio():
    """Ambil data terbaru + dua tahun sebelumnya untuk update rutin."""

    return update_idnamadata_ginirasio(
        backfill=False,
    )

def update_idnamadata_apm(
    config_name,
    idnamadata=None,
    iditem=None,
    backfill=True,
):
    """
    Generic update untuk dataset APM.
    """

    config = get_bps_config(config_name)

    if idnamadata is None:
        idnamadata = config.get("idnamadata")

    return update_bps_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def update_idnamadata_apm_sd(
    idnamadata=9824,
    iditem=None,
    backfill=True,
):
    return update_idnamadata_apm(
        config_name="apm_sd",
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )


def update_idnamadata_apm_smp(
    idnamadata=9826,
    iditem=None,
    backfill=True,
):
    return update_idnamadata_apm(
        config_name="apm_smp",
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )


def update_idnamadata_apm_sma(
    idnamadata=9825,
    iditem=None,
    backfill=True,
):
    return update_idnamadata_apm(
        config_name="apm_sma",
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def fetch(params=None):
    params = params or {}

    idnamadata = params.get("idnamadata")
    iditem = params.get("iditem")
    backfill = params.get(
        "backfill",
        False,
    )

    if idnamadata is not None:
        idnamadata = int(idnamadata)

    if isinstance(backfill, str):
        backfill = (
            backfill.strip().lower()
            in {"true", "1", "yes", "y"}
        )

    config_map = {
        9824: "apm_sd",
        9826: "apm_smp",
        9825: "apm_sma",
    }

    # HOTEL AKOMODASI
    if idnamadata == 9869:
        return update_idnamadata_hotel(
            idnamadata=9869,
            tahun=params.get(
                "tahun",
                2025,
            ),
        )

    # APM
    if idnamadata in config_map:
        return update_idnamadata_apm(
            config_name=config_map[idnamadata],
            idnamadata=idnamadata,
            iditem=iditem,
            backfill=backfill,
        )

    # GINI RASIO
    return update_idnamadata_ginirasio(
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--idnamadata",
        type=int,
        default=None,
        help="ID nama data yang ingin di-update",
    )

    parser.add_argument(
        "--tahun",
        type=int,
        default=2025,
        help="Tahun data BPS",
    )

    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Ambil seluruh histori data BPS",
    )

    args = parser.parse_args()

    print("\n========================================")
    print("BPS GINI RATIO PIPELINE")
    print("========================================")

    if args.idnamadata == 9869:

        df_final = update_idnamadata_hotel(
            idnamadata=9869,
            tahun=args.tahun,
            backfill=args.backfill,
        )

    elif args.idnamadata in {
        9824,
        9825,
        9826,
    }:

        df_final = fetch({
            "idnamadata": args.idnamadata,
            "backfill": args.backfill,
        })

    else:

        df_final = update_idnamadata_ginirasio(
            idnamadata=args.idnamadata,
            backfill=args.backfill,
        )

    print("\n========================================")
    print("PIPELINE SELESAI")
    print("========================================")
    print(
        "FINAL ROW:",
        len(df_final),
    )


if __name__ == "__main__":
    main()