import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent

MINIO_BUCKET = "maganghub"

BPS_API_KEY = os.getenv("BPS_API_KEY")
KATADATA_API_KEY = os.getenv("KATADATA_API_KEY")

BPS_BASE_URL = "https://webapi.bps.go.id/v1/api"
DATABASE_BASE_URL = "https://xflask.databoks.id/database"

DOMAIN = 1173
VARIABLE = 115

INDIKATOR = "Gini Ratio Menurut Kabupaten Kota"

SOURCE_URL = (
    "https://langsakota.bps.go.id/id/"
    "statistics-table/2/MTE1IzI=/"
    "rasio-gini-provinsi-aceh-menurut-kabupaten-kota.html"
)

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "bps"
    / "gini_kabupaten_kota"
)

RAW_FILE = (
    RAW_DIR
    / "bps_gini_kabupaten_kota_raw.parquet"
)

class BPSScraper:

    def __init__(self):

        if not BPS_API_KEY:
            raise RuntimeError(
                "BPS_API_KEY tidak ditemukan di .env"
            )

        self.session = requests.Session()

    def scrape(self):

        print("\n==============================")
        print("SCRAPING BPS WEB API")
        print("==============================")

        url = (
            f"{BPS_BASE_URL}/list/model/data/"
            f"lang/ind/"
            f"domain/{DOMAIN}/"
            f"var/{VARIABLE}/"
            f"th/122/"
            f"key/{BPS_API_KEY}"
        )

        print("URL:", url.replace(
            BPS_API_KEY,
            "***"
        ))

        response = self.session.get(
            url,
            timeout=60
        )

        print("STATUS:", response.status_code)

        response.raise_for_status()
        result = response.json()

        print("\n==============================")
        print("RAW BPS DATA")
        print("==============================")
        print(result)

        print("\nSTATUS API:", result.get("status"))
        print(
            "DATA AVAILABILITY:",
            result.get("data-availability")
        )

        if result.get("status") != "OK":
            raise RuntimeError(
                result.get("message", result)
            )

        return result

    def prepare_dataframe(self, result):

        print("\n==============================")
        print("PREPARE DATA BPS")
        print("==============================")

        vervar = result.get("vervar", [])
        tahun = result.get("tahun", [])
        turvar = result.get("turvar", [])
        turtahun = result.get("turtahun", [])
        datacontent = result.get("datacontent", {})

        if not datacontent:
            raise RuntimeError(
                "Response BPS tidak memiliki datacontent"
            )

        var_info = result.get("var", [])

        if not var_info:
            raise RuntimeError(
                "Metadata indikator BPS tidak ditemukan"
            )

        indikator = var_info[0].get("label", "")
        id_var = var_info[0].get("val")

        id_turvar = str(turvar[0]["val"]) if turvar else "0"
        id_turtahun = str(turtahun[0]["val"]) if turtahun else "0"

        rows = []

        for wilayah in vervar:

            id_wilayah = str(wilayah["val"])
            nama_wilayah = wilayah["label"].strip()

            for periode in tahun:

                id_tahun = str(periode["val"])
                tahun_label = periode["label"]

                # Key datacontent BPS:
                # vervar + var + tahun + turvar
                key = (
                    f"{id_wilayah}"
                    f"{id_var}"
                    f"{id_turvar}"
                    f"{id_tahun}"
                    f"{id_turtahun}"
                )

                if key not in datacontent:
                    print(
                        f"KEY TIDAK DITEMUKAN: {key}"
                    )

                if id_wilayah == "1100":
                    print("DEBUG KEY:", key)
                    print("DEBUG VALUE:", datacontent.get(key))
                    print("DEBUG KEYS:", list(datacontent.keys())[:5])

                value = datacontent.get(key)

                rows.append({
                    "id_bps": wilayah["val"],
                    "wilayah": nama_wilayah,
                    "indikator": indikator,
                    "id_var_bps": id_var,
                    "tahun": tahun_label,
                    "data_x": f"31-12-{tahun_label}",
                    "data_y": value,
                    "satuan": "Poin Indeks",
                })

        df = pd.DataFrame(rows)

        print("KOLOM BPS:")
        print(df.columns.tolist())

        print("\nJUMLAH ROW:", len(df))

        print("\nDATA:")
        print(df.to_string(index=False))

        return df

    def normalize(self, df):

        print("\n==============================")
        print("NORMALIZE RAW DATA")
        print("==============================")

        df = df.copy()

        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce"
        )

        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce"
        )

        df["sumber"] = "BPS"

        df["nama_data_import"] = SOURCE_URL

        df["id_nama_data"] = pd.NA

        df = df[
            [
                "id_bps",
                "indikator",
                "wilayah",
                "data_x",
                "data_y",
                "satuan",
                "sumber",
                "nama_data_import",
                "id_nama_data"
            ]
        ]

        print("\nDATA RAW SIAP:")

        print(
            df.to_string(index=False)
        )

        print(
            "\nJUMLAH DATA:",
            len(df)
        )

        print(
            "DATA NILAI TERISI:",
            df["data_y"].notna().sum()
        )

        return df

    def save_parquet(self, df):

        RAW_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        df.to_parquet(
            RAW_FILE,
            index=False
        )

        print("\n==============================")
        print("SAVE RAW PARQUET")
        print("==============================")

        print("FILE:", RAW_FILE)

    def upload_minio(self):

        from utils.minio import (
            create_client,
            write_file
        )

        client = create_client()

        with open(
            RAW_FILE,
            "rb"
        ) as file:

            parquet_bytes = file.read()

        object_name = (
            "bps/raw/"
            "bps_gini_kabupaten_kota_raw.parquet"
        )

        write_file(
            client,
            MINIO_BUCKET,
            parquet_bytes,
            object_name,
            "application/octet-stream"
        )

        print("\n==============================")
        print("UPLOAD MINIO")
        print("==============================")

        print(
            f"{MINIO_BUCKET}/{object_name}"
        )

    def run(self):

        result = self.scrape()

        df = self.prepare_dataframe(
            result
        )

        df = self.normalize(
            df
        )

        self.save_parquet(
            df
        )

        return df


class BPSETL:

    def __init__(self):

        if not KATADATA_API_KEY:
            raise RuntimeError(
                "KATADATA_API_KEY tidak ditemukan "
                "di .env"
            )

        self.session = requests.Session()

        self.session.headers.update({
            "X-API-Key": KATADATA_API_KEY
        })

    def load_raw(self):

        print("\n==============================")
        print("LOAD RAW PARQUET")
        print("==============================")

        df = pd.read_parquet(
            RAW_FILE
        )

        print(
            "JUMLAH RAW DATA:",
            len(df)
        )

        return df

    def get_nama_data(self):

        print("\n==============================")
        print("GET NAMA DATA")
        print("==============================")

        response = self.session.get(
            f"{DATABASE_BASE_URL}/nama_data",
            timeout=30
        )

        print(
            "STATUS:",
            response.status_code
        )

        response.raise_for_status()

        df = pd.DataFrame(
            response.json()
        )

        print(
            "JUMLAH DATASET:",
            len(df)
        )

        return df

    def match_indikator(self, df_nama, indikator):

        print("\n==============================")
        print("MATCH INDIKATOR")
        print("==============================")

        print("INDIKATOR BPS:", indikator)

        indicator_mapping = {
            "Rasio Gini Menurut Kabupaten/Kota di Provinsi Aceh":
                "Gini Ratio Menurut Kabupaten Kota"
        }

        target_nama = indicator_mapping.get(indikator)

        if not target_nama:
            print("❌ MAPPING INDIKATOR TIDAK DITEMUKAN")
            return None

        df_nama = df_nama.copy()

        df_nama["nama_clean"] = (
            df_nama["nama"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        target_clean = (
            target_nama
            .strip()
            .lower()
        )

        matched = df_nama[
            df_nama["nama_clean"] == target_clean
        ]

        if matched.empty:

            print("❌ NAMA DATA DATABASE TIDAK DITEMUKAN")
            print("TARGET:", target_nama)

            return None

        row = matched.iloc[0]

        id_nama_data = int(row["id"])

        print("✅ MATCH")
        print("NAMA DATA:", row["nama"])
        print("ID NAMA DATA:", id_nama_data)

        return id_nama_data

    def get_existing_data(
        self,
        id_nama_data
    ):

        print("\n==============================")
        print("GET DATA EXISTING")
        print("==============================")

        response = self.session.get(
            f"{DATABASE_BASE_URL}/data",
            params={
                "id_nama_data": id_nama_data
            },
            timeout=60
        )

        print(
            "STATUS:",
            response.status_code
        )

        response.raise_for_status()

        rows = response.json()

        records = []

        for row in rows:

            xs = (
                row.get("data_x")
                or ""
            ).split(",")

            ys = (
                row.get("data_y")
                or ""
            ).split(",")

            for x, y in zip(
                xs,
                ys
            ):

                records.append({

                    "id": row["id"],

                    "id_kota": row.get(
                        "id_kota"
                    ),

                    "id_provinsi": row.get(
                        "id_provinsi"
                    ),

                    "kota": row.get(
                        "kota"
                    ),

                    "provinsi": row.get(
                        "provinsi"
                    ),

                    "item": row.get(
                        "item"
                    ),

                    "satuan": row.get(
                        "satuan"
                    ),

                    "data_x": x.strip(),

                    "data_y": (
                        float(y)
                        if y.strip()
                        else None
                    ),

                    "sumber": row.get(
                        "sumber"
                    )
                })

        df = pd.DataFrame(
            records
        )

        print(
            "JUMLAH DATA EXISTING:",
            len(df)
        )

        return df

    @staticmethod
    def normalize_location(value):

        value = (
            str(value)
            .strip()
            .lower()
        )

        prefixes = [
            "kabupaten ",
            "kab. ",
            "kota "
        ]

        for prefix in prefixes:

            if value.startswith(prefix):

                value = value[
                    len(prefix):
                ]

                break

        return value.strip()


    def match_data(
        self,
        df_scraping,
        df_db
    ):

        print("\n==============================")
        print("MATCH SCRAPING VS DATABASE")
        print("==============================")

        scraping = df_scraping.copy()

        database = df_db.copy()

        print("\n==============================")
        print("CEK ROW ACEH DI DATABASE")
        print("==============================")

        aceh = database[
            database["provinsi"]
            .astype(str)
            .str.strip()
            .str.lower()
            == "aceh"
        ]

        print("JUMLAH ROW ACEH:", len(aceh))

        if not aceh.empty:
            print(
                aceh[
                    [
                        "id",
                        "id_kota",
                        "provinsi",
                        "kota",
                        "data_x",
                        "data_y",
                        "satuan",
                    ]
                ]
                .head(50)
                .to_string(index=False)
            )

        print("\n==============================")
        print("CEK STRUKTUR LOKASI DATABASE")
        print("==============================")

        print("JUMLAH KOTA UNIK:", database["kota"].nunique())

        print("\nCONTOH KOTA:")
        print(
            database["kota"]
            .drop_duplicates()
            .head(50)
            .to_string(index=False)
        )

        print("\nID PROVINSI UNIK:")
        print(
            database["id_provinsi"]
            .drop_duplicates()
            .tolist()
        )

        print("\nID KOTA UNIK:")
        print(
            database["id_kota"]
            .drop_duplicates()
            .tolist()
        )

        scraping["lokasi_key"] = (
            scraping["wilayah"]
            .apply(
                self.normalize_location
            )
        )

        database["lokasi_key"] = (
            database["kota"]
            .apply(
                self.normalize_location
            )
        )

        scraping["date_key"] = (
            pd.to_datetime(
                scraping["data_x"],
                errors="coerce"
            ).dt.date
        )

        database["date_key"] = (
            pd.to_datetime(
                database["data_x"],
                errors="coerce"
            ).dt.date
        )

        result = scraping.merge(
            database[
                [
                    "lokasi_key",
                    "date_key",
                    "kota",
                    "provinsi",
                    "item",
                    "satuan",
                    "data_y"
                ]
            ],
            on=[
                "lokasi_key",
                "date_key"
            ],
            how="left",
            suffixes=(
                "_scraping",
                "_db"
            )
        )

        result["selisih"] = (
            result["data_y_scraping"]
            - result["data_y_db"]
        ).abs()

        result["status_match"] = (
            "TIDAK MATCH"
        )

        result.loc[
            result["data_y_db"].notna(),
            "status_match"
        ] = "LOKASI/PERIODE MATCH"

        result.loc[
            result["selisih"].fillna(
                999999
            ) == 0,
            "status_match"
        ] = "MATCH"

        result["cek_satuan"] = (
            result["satuan_db"]
            .fillna("")
            .apply(
                lambda x:
                "SESUAI"
                if x.strip().lower()
                == "poin indeks"
                else "PERLU CEK"
            )
        )

        return result
    
    def run(self):

        print("\n================================")
        print("BPS ETL")
        print("================================")

        df_raw = self.load_raw()

        indikator_list = (
            df_raw["indikator"]
            .dropna()
            .unique()
        )

        print(
            "\nINDIKATOR YANG DIPROSES:"
        )

        for indikator in indikator_list:

            print(
                "-",
                indikator
            )

        df_nama = self.get_nama_data()

        for indikator in indikator_list:

            print(
                "\n\n################################"
            )

            print(
                "PROCESS:",
                indikator
            )

            print(
                "################################"
            )

            # 4. MATCH INDIKATOR
            id_nama_data = (
                self.match_indikator(
                    df_nama,
                    indikator
                )
            )

            if id_nama_data is None:

                print(
                    "SKIP INDIKATOR"
                )

                continue

            df_db = (
                self.get_existing_data(
                    id_nama_data
                )
            )

            hasil = (
                self.match_data(
                    df_raw,
                    df_db
                )
            )

            print(
                "\n=============================="
            )

            print(
                "HASIL MATCH"
            )

            print(
                "=============================="
            )

            columns = [
                "wilayah",
                "data_x",
                "data_y_scraping",
                "kota",
                "data_y_db",
                "satuan_db",
                "selisih",
                "status_match",
                "cek_satuan"
            ]

            print(
                hasil[
                    columns
                ].to_string(
                    index=False
                )
            )

            print(
                "\nSUMMARY:"
            )

            print(
                hasil[
                    "status_match"
                ].value_counts()
                .to_string()
            )

            print(
                "\nCEK SATUAN:"
            )

            print(
                hasil[
                    "cek_satuan"
                ].value_counts()
                .to_string()
            )

            return hasil

def main():

    print(
        "========================================"
    )

    print(
        "BPS GINI RATIO PIPELINE"
    )

    print(
        "========================================"
    )

    scraper = BPSScraper()

    df_raw = scraper.run()

    etl = BPSETL()

    hasil = etl.run()

    print(
        "\n========================================"
    )

    print(
        "SELESAI"
    )

    print(
        "========================================"
    )


if __name__ == "__main__":
    main()