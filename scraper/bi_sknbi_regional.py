import sys
import pandas as pd

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0,
    str(PROJECT_ROOT)
)

from utils.minio import create_client, write_file

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
        "Menurut Kota Tujuan Menurut Provinsi"
}

class SKNBIScraper:

    def __init__(self):

        self.minio_client = create_client()

        self.RAW_DATA_DIR = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "bi"
            / "sknbi_regional"
        )

        self.RAW_FILE = (
            self.RAW_DATA_DIR
            / "bi_sknbi_regional_raw_Maret_September_2025.parquet"
        )

    def load_raw_data(self):

        print("\n==============================")
        print("LOAD RAW DATA BI")
        print("==============================")

        print(
            "FILE:",
            self.RAW_FILE
        )

        if not self.RAW_FILE.exists():

            raise FileNotFoundError(
                f"Raw file tidak ditemukan: "
                f"{self.RAW_FILE}"
            )

        df = pd.read_parquet(
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

    def prepare_dataframe(
        self,
        df
    ):

        df = df.copy()

        # Pastikan tanggal
        df["data_x"] = pd.to_datetime(
            df["data_x"],
            errors="coerce"
        )

        df["data_y"] = pd.to_numeric(
            df["data_y"],
            errors="coerce"
        )

        text_columns = [
            "indikator",
            "blok",
            "wilayah",
            "satuan",
            "sumber"
        ]

        for column in text_columns:

            if column in df.columns:

                df[column] = (
                    df[column]
                    .astype("string")
                    .str.strip()
                )

        return df


    def save_raw_data(
        self,
        df
    ):

        self.RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        df.to_parquet(
            self.RAW_FILE,
            index=False
        )

        print("\n==============================")
        print("RAW PARQUET DISIMPAN")
        print("==============================")

        print(
            "FILE:",
            self.RAW_FILE
        )

        with open(
            self.RAW_FILE,
            "rb"
        ) as f:

            parquet_bytes = f.read()

        object_name = (
            "bi/raw/"
            "bi_sknbi_regional_raw_Maret_September_2025.parquet"
        )

        write_file(
            self.minio_client,
            MINIO_BUCKET,
            parquet_bytes,
            object_name,
            "application/octet-stream"
        )

        print(
            "\nRAW PARQUET DI-UPLOAD KE MINIO:"
        )

        print(
            f"{MINIO_BUCKET}/{object_name}"
        )


    def upload_existing_raw_to_minio(self):

        if not self.RAW_FILE.exists():

            raise FileNotFoundError(
                f"Raw file tidak ditemukan: "
                f"{self.RAW_FILE}"
            )

        with open(
            self.RAW_FILE,
            "rb"
        ) as f:

            parquet_bytes = f.read()

        object_name = (
            "bi/raw/"
            "bi_sknbi_regional_raw_Maret_September_2025.parquet"
        )

        write_file(
            self.minio_client,
            MINIO_BUCKET,
            parquet_bytes,
            object_name,
            "application/octet-stream"
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
        self.minio_client = create_client()

        self.FINAL_DATA_DIR = (
            PROJECT_ROOT
            / "final"
            / "bi"
            / "sknbi_regional"
        )

        self.FINAL_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

    def filter_nilai_transaksi(
        self,
        df
    ):

        return df[
            df["indikator"]
            == "Nilai Transaksi (Kliring Kredit)"
        ].copy()

    def create_data(
        self,
        df,
        blok
    ):

        print("\n==============================")
        print(
            "CREATE DATA:",
            blok
        )
        print("==============================")

        df = self.filter_nilai_transaksi(
            df
        )

        df = df[
            df["blok"] == blok
        ].copy()


        if df.empty:

            print(
                "Tidak ada data untuk:",
                blok
            )

            return pd.DataFrame()

        final_df = pd.DataFrame({

        "kabkota": pd.NA,

        "provinsi": df["wilayah"],

        "nama_indikator": NAMA_DATA[blok],

        "nama_item": pd.NA,

        "idnamadata": pd.NA,

        "data_x": pd.to_datetime(
            df["data_x"]
        ),

        "data_y": pd.to_numeric(
            df["data_y"],
            errors="coerce"
        ),

        "satuan": "Rp miliar",

         "sumber": "Bank Indonesia (BI)",

        "nama_data_import": "https://www.bi.go.id/id/statistik/ekonomi-keuangan/spip/Default.aspx",

        "note": (
            "Data SKNBI Regional "
            "berdasarkan SPIP "
            "September 2025"
        )
    })

        final_df = final_df[
            final_df["data_y"].notna()
        ].copy()

        final_df = (
            final_df
            .sort_values(
                [
                    "data_x",
                    "provinsi"
                ]
            )
            .reset_index(
                drop=True
            )
        )

        final_df["data_x"] = (
            final_df["data_x"]
            .dt.strftime("%d-%m-%Y")
        )

        key_columns = [
            "provinsi",
            "data_x"
        ]

        duplicates = final_df[
            final_df.duplicated(
                subset=key_columns,
                keep=False
            )
        ]


        if not duplicates.empty:

            print(
                "\nWARNING: "
                "Ditemukan duplicate!"
            )

            print(
                duplicates[
                    key_columns
                    + ["data_y"]
                ].to_string(
                    index=False
                )
            )

        else:

            print(
                "Tidak ada duplicate."
            )


        return final_df

    def generate_nama_data(
        self,
        blok
    ):

        return NAMA_DATA[
            blok
        ]

    def save_final_data(
        self,
        df,
        filename,
        nama_data
    ):
        """
        Simpan final CSV ke lokal
        dan upload ke MinIO.
        """

        FINAL_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        filepath = FINAL_DATA_DIR / filename

        df.to_csv(
            filepath,
            index=False,
            encoding="utf-8-sig"
        )

        print("\nFinal data disimpan:")
        print(filepath)

        csv_bytes = (
            df.to_csv(
                index=False,
                encoding="utf-8-sig"
            )
            .encode("utf-8-sig")
        )

        object_name = (
            f"bi/final/{filename}"
        )

        write_file(
            self.minio_client,
            MINIO_BUCKET,
            csv_bytes,
            object_name,
            "text/csv"
        )

        print(
            "Final data di-upload ke MinIO:"
        )
        print(
            f"{MINIO_BUCKET}/{object_name}"
        )

        print(
            "Nama data:",
            nama_data
        )

        print(
            "Total rows:",
            len(df)
        )


    def run(self):

        print(
            "\n========================================"
        )

        print(
            "SKNBI ETL"
        )

        print(
            "========================================"
        )

        self.scraper.upload_existing_raw_to_minio()

        df = self.scraper.load_raw_data()

        df = self.scraper.prepare_dataframe(
            df
        )

        for blok in [
            "Kota Asal",
            "Kota Tujuan"
        ]:

            print(
                f"\nPROCESS: {blok}"
            )


            final_df = self.create_data(
                df,
                blok
            )


            if final_df.empty:

                continue

            print("\nSAMPLE:")
            print(
                final_df.head(10).to_string(
                    index=False
                )
            )

            nama_data = self.generate_nama_data(
                blok
            )

            if blok == "Kota Asal":
                filename = (
                    "bi_sknbi_nilai_transaksi_"
                    "kliring_kredit_kota_asal_provinsi.csv"
                )
            else:
                filename = (
                    "bi_sknbi_nilai_transaksi_"
                    "kliring_kredit_kota_tujuan_provinsi.csv"
                )

            self.save_final_data(
                final_df,
                filename,
                nama_data
            )

        print(
            "\n========================================"
        )

        print(
            "ETL SELESAI"
        )

        print(
            "========================================"
        )

def main():

    etl = SKNBIETL()

    etl.run()

if __name__ == "__main__":

    main()