import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from utils.data import (
    normalize_dataframe,
    find_duplicates,
)

from utils.file import (
    read_parquet,
    save_csv,
)

from utils.storage import (
    get_minio_client,
    upload_file,
)


MINIO_BUCKET = "maganghub"


RAW_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "bi"
    / "sknbi_regional"
)


FINAL_DATA_DIR = (
    PROJECT_ROOT
    / "final"
    / "bi"
    / "sknbi_regional"
)


RAW_FILE = (
    RAW_DATA_DIR
    / "bi_sknbi_regional_raw_Maret_September_2025.parquet"
)


NAMA_DATA = {

    "Kota Asal":
        "Nilai Transaksi (Kliring Kredit) "
        "Menurut Kota Asal Menurut Provinsi",

    "Kota Tujuan":
        "Nilai Transaksi (Kliring Kredit) "
        "Menurut Kota Tujuan Menurut Provinsi",
}


class SKNBIScraper:

    def __init__(self):

        self.minio_client = get_minio_client()

        self.RAW_DATA_DIR = RAW_DATA_DIR
        self.RAW_FILE = RAW_FILE


    def load_raw_data(self):

        print("\n==============================")
        print("LOAD RAW DATA BI")
        print("==============================")

        print("FILE:", self.RAW_FILE)

        if not self.RAW_FILE.exists():
            raise FileNotFoundError(
                f"Raw file tidak ditemukan: "
                f"{self.RAW_FILE}"
            )

        df = read_parquet(
            self.RAW_FILE
        )

        print(
            "TOTAL RAW ROWS:",
            len(df)
        )

        print(
            "KOLOM:",
            df.columns.tolist()
        )

        return df


    def prepare_dataframe(self, df):

        df = normalize_dataframe(
            df,
            text_columns=[
                "indikator",
                "blok",
                "wilayah",
                "satuan",
                "sumber",
            ],
            date_columns=[
                "data_x",
            ],
            numeric_columns=[
                "data_y",
            ],
        )

        return df


    def upload_existing_raw_to_minio(self):

        if not self.RAW_FILE.exists():
            raise FileNotFoundError(
                f"Raw file tidak ditemukan: "
                f"{self.RAW_FILE}"
            )

        object_name = (
            "bi/raw/"
            "bi_sknbi_regional_raw_Maret_September_2025.parquet"
        )

        upload_file(
            self.minio_client,
            MINIO_BUCKET,
            self.RAW_FILE,
            object_name,
            "application/octet-stream",
        )

        print(
            "\nRAW PARQUET DI-UPLOAD KE MINIO:"
        )

        print(
            f"{MINIO_BUCKET}/{object_name}"
        )


class SKNBIETL:

    def __init__(self):

        self.scraper = SKNBIScraper()

        self.minio_client = (
            self.scraper.minio_client
        )

        self.FINAL_DATA_DIR = FINAL_DATA_DIR

        self.FINAL_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )


    def filter_nilai_transaksi(self, df):

        return df[
            df["indikator"]
            == "Nilai Transaksi (Kliring Kredit)"
        ].copy()


    def create_data(
        self,
        df,
        blok,
    ):

        print("\n==============================")
        print(
            "CREATE DATA:",
            blok,
        )
        print("==============================")


        # FILTER INDIKATOR
        df = self.filter_nilai_transaksi(
            df
        )


        # FILTER BLOK
        df = df[
            df["blok"] == blok
        ].copy()


        if df.empty:

            print(
                "Tidak ada data untuk:",
                blok,
            )

            return pd.DataFrame()


        # BENTUK DATA STANDARD
        final_df = pd.DataFrame({

            "kabkota": pd.NA,

            "provinsi": (
                df["wilayah"]
            ),

            "nama_indikator": (
                NAMA_DATA[blok]
            ),

            "nama_item": pd.NA,

            "idnamadata": pd.NA,

            "data_x": (
                df["data_x"]
            ),

            "data_y": (
                pd.to_numeric(
                    df["data_y"],
                    errors="coerce",
                )
            ),

            "satuan": "Rp miliar",

            "sumber": (
                "Bank Indonesia (BI)"
            ),

            "nama_data_import": (
                "https://www.bi.go.id/"
                "id/statistik/ekonomi-keuangan/"
                "spip/Default.aspx"
            ),

            "note": (
                "Data SKNBI Regional "
                "berdasarkan SPIP "
                "September 2025"
            ),
        })


        # DATA Y KOSONG DIBUANG
        final_df = final_df[
            final_df["data_y"].notna()
        ].copy()


        # SORT
        final_df = (
            final_df
            .sort_values(
                [
                    "data_x",
                    "provinsi",
                ]
            )
            .reset_index(drop=True)
        )


        # FORMAT TANGGAL FINAL
        final_df["data_x"] = (
            final_df["data_x"]
            .dt.strftime("%d-%m-%Y")
        )


        # CEK DUPLICATE
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

            print(
                duplicates[
                    [
                        "provinsi",
                        "data_x",
                        "data_y",
                    ]
                ].to_string(
                    index=False
                )
            )

        else:

            print(
                "Tidak ada duplicate."
            )


        return final_df


    def generate_nama_data(self, blok):

        return NAMA_DATA[blok]


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

        print(filepath)


        object_name = (
            f"bi/final/{filename}"
        )


        upload_file(
            self.minio_client,
            MINIO_BUCKET,
            filepath,
            object_name,
            "text/csv",
        )


        print(
            "Final data di-upload ke MinIO:"
        )

        print(
            f"{MINIO_BUCKET}/{object_name}"
        )


        print(
            "Nama data:",
            nama_data,
        )

        print(
            "Total rows:",
            len(df),
        )


    def run(self):

        print(
            "\n========================================"
        )

        print("SKNBI ETL")

        print(
            "========================================"
        )


        # RAW → MINIO
        self.scraper.upload_existing_raw_to_minio()


        # LOAD RAW
        df = self.scraper.load_raw_data()


        # NORMALIZE
        df = self.scraper.prepare_dataframe(
            df
        )


        # PROCESS MASING-MASING BLOK
        for blok in [
            "Kota Asal",
            "Kota Tujuan",
        ]:

            print(
                f"\nPROCESS: {blok}"
            )


            final_df = self.create_data(
                df,
                blok,
            )


            if final_df.empty:
                continue


            print("\nSAMPLE:")

            print(
                final_df
                .head(10)
                .to_string(
                    index=False
                )
            )


            nama_data = (
                self.generate_nama_data(
                    blok
                )
            )


            if blok == "Kota Asal":

                filename = (
                    "bi_sknbi_nilai_transaksi_"
                    "kliring_kredit_"
                    "kota_asal_provinsi.csv"
                )

            else:

                filename = (
                    "bi_sknbi_nilai_transaksi_"
                    "kliring_kredit_"
                    "kota_tujuan_provinsi.csv"
                )


            self.save_final_data(
                final_df,
                filename,
                nama_data,
            )


        print(
            "\n========================================"
        )

        print("ETL SELESAI")

        print(
            "========================================"
        )

def fetch(params=None):
    params = params or {}

    blok = params.get("blok", "Kota Asal")

    etl = SKNBIETL()

    df = etl.scraper.load_raw_data()
    df = etl.scraper.prepare_dataframe()

    final_df = etl.create_data(df, blok)

    return final_df

def main():

    etl = SKNBIETL()

    etl.run()


if __name__ == "__main__":
    main()