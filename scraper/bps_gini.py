import os
import argparse
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


load_dotenv()


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


INDIKATOR = (
    "Gini Ratio Menurut Kabupaten Kota"
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


FINAL_DIR = (
    PROJECT_ROOT
    / "data"
    / "final"
    / "bps"
    / "gini_kabupaten_kota"
)


FINAL_FILE = (
    FINAL_DIR
    / "bps_gini_kabupaten_kota.csv"
)


GINI_CONFIG = [
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
]


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
    ):

        vervar = result.get(
            "vervar",
            [],
        )

        var = result.get(
            "var",
            [],
        )

        datacontent = result.get(
            "datacontent",
            {},
        )


        if not datacontent:
            return pd.DataFrame()


        if not isinstance(
            datacontent,
            dict,
        ):

            print(
                "⚠️ Format datacontent:",
                type(datacontent),
            )

            return pd.DataFrame()


        nama_indikator = (
            var[0].get(
                "label",
                INDIKATOR,
            )
            if var
            else INDIKATOR
        )


        satuan = (
            var[0].get(
                "unit",
                pd.NA,
            )
            if var
            else pd.NA
        )


        wilayah_map = {
            str(item["val"]): item["label"]
            for item in vervar
        }


        rows = []


        for key, value in (
            datacontent.items()
        ):

            key = str(key)


            if len(key) < 13:
                continue


            wilayah_id = key[:4]
            var_id = key[4:7]


            if var_id != str(
                self.var_id
            ):
                continue


            wilayah = (
                wilayah_map.get(
                    wilayah_id,
                    wilayah_id,
                )
            )


            rows.append({

                "kode_wilayah":
                    wilayah_id,

                "wilayah":
                    wilayah,

                "provinsi":
                    self.provinsi,

                "indikator":
                    nama_indikator,

                "data_x":
                    tahun,

                "data_y":
                    pd.to_numeric(
                        value,
                        errors="coerce",
                    ),

                "satuan":
                    satuan,
            })


        return pd.DataFrame(rows)


    def run(self):

        print(
            "\n================================"
        )

        print(
            "BPS GINI RATIO SCRAPER"
        )

        print(
            "================================"
        )


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


        print(
            "\nTAHUN TERSEDIA:"
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


def scrape_all(save_raw=True):

    print(
        "\n========================================"
    )

    print(
        "SCRAPE GINI RATIO KABUPATEN/KOTA"
    )

    print(
        "========================================"
    )


    all_data = []


    for config in GINI_CONFIG:

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

            df = scraper.run()


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


    if save_raw:

        save_parquet(
            df_raw,
            RAW_FILE,
        )

        print(
            "\n================================"
        )

        print(
            "RAW PARQUET SELESAI"
        )

        print(
            "================================"
        )

        print(
            "FILE:",
            RAW_FILE,
        )


    else:

        print(
            "\n================================"
        )

        print(
            "DRY RUN - RAW TIDAK DISIMPAN"
        )

        print(
            "================================"
        )


    print(
        "JUMLAH ROW:",
        len(df_raw),
    )


    print(
        "\nJUMLAH ROW PER PROVINSI:"
    )


    print(
        df_raw
        .groupby("provinsi")
        .size()
        .sort_values(
            ascending=False
        )
    )


    return df_raw


class BPSETL:

    def __init__(self):

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
            RAW_FILE
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
            INDIKATOR,
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
            INDIKATOR
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
                INDIKATOR,
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
    ):

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


        print(
            "JUMLAH ROW SETELAH "
            "BUANG PROVINSI:",
            len(df),
        )


        # BUSINESS LOGIC BPS
        # TENTUKAN KABUPATEN / KOTA

        kode_belakang = (
            pd.to_numeric(
                df["kode_wilayah"]
                .str[-2:],
                errors="coerce",
            )
        )


        is_kota = (
            kode_belakang >= 71
        )


        df["kota"] = (
            df["wilayah"]
        )


        df.loc[
            ~is_kota,
            "kota"
        ] = (
            "Kab. "
            + df.loc[
                ~is_kota,
                "wilayah"
            ]
        )


        df.loc[
            is_kota,
            "kota"
        ] = (
            "Kota "
            + df.loc[
                is_kota,
                "wilayah"
            ]
        )


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


        # FINAL DATA

        df_final = pd.DataFrame({

            "kota":
                df["kota"],

            "provinsi":
                df["provinsi"],

            "nama_indikator":
                INDIKATOR,

            "nama_item":
                pd.NA,

            "idnamadata":
                id_nama_data,

            "data_x":
                pd.to_datetime(
                    df["data_x"]
                    .astype(str),
                    format="%Y",
                    errors="coerce",
                ).dt.strftime(
                    "31-12-%Y"
                ),

            "data_y":
                pd.to_numeric(
                    df["data_y"],
                    errors="coerce",
                ),

            "satuan":
                (
                    df["satuan"]
                    .fillna("Poin Indeks")
                    .astype(str)
                    .str.strip()
                    .replace(
                        "",
                        "Poin Indeks",
                    )
                ),

            "sumber":
                "Badan Pusat Statistik (BPS)",

            "note":
                (
                    "Data Gini Ratio "
                    "Kabupaten/Kota "
                    "berdasarkan BPS"
                ),

            "nama_data_import":
                (
                    "s3://maganghub/"
                    "bps/final/"
                    "bps_gini_kabupaten_kota.csv"
                ),
        })


        df_final = df_final[
            df_final["data_y"].notna()
        ].copy()


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
            FINAL_FILE,
        )


        print(
            "\nFINAL FILE:",
            FINAL_FILE,
        )


        return FINAL_FILE


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
            FINAL_FILE,
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


        df_nama = (
            self.get_nama_data()
        )


        id_nama_data = (
            self.match_nama_data(
                df_nama
            )
        )


        if id_nama_data is None:

            raise RuntimeError(
                "Gagal mendapatkan "
                "idnamadata."
            )


        df_final = self.transform(
            df_raw,
            id_nama_data,
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


def main():

    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Run scraping dan transform "
            "tanpa menyimpan/upload output"
        ),
    )


    args = parser.parse_args()


    print(
        "========================================"
    )

    print(
        "BPS GINI RATIO PIPELINE"
    )

    print(
        "========================================"
    )


    df_raw = scrape_all(
        save_raw=not args.dry_run
    )


    print(
        "\nRAW DATA SELESAI"
    )

    print(
        "JUMLAH RAW ROW:",
        len(df_raw),
    )


    etl = BPSETL()


    df_final = etl.run(
        df_raw=df_raw,
        save_outputs=not args.dry_run,
    )


    print(
        "\n========================================"
    )

    print(
        "PIPELINE SELESAI"
    )

    print(
        "========================================"
    )


    print(
        "RAW ROW:",
        len(df_raw),
    )

    print(
        "FINAL ROW:",
        len(df_final),
    )


if __name__ == "__main__":
    main()