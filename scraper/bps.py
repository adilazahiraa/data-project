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
        "nama_data_import": (
            "s3://maganghub/bps/final/"
            "bps_gini_kabupaten_kota.csv"
        ),
    },

    # NANTI DATA BPS LAIN TINGGAL DITAMBAHKAN
}

def get_bps_config(indikator):
    if indikator not in BPS_CONFIG:
        raise ValueError(
            f"Indikator BPS tidak ditemukan: {indikator}"
        )

    return BPS_CONFIG[indikator]

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


    def run(
        self,
        tahun_awal=None,
        tahun_akhir=None,
        indikator=None,
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

    if save_raw:

        save_parquet(
            df_raw,
            RAW_FILE,
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
                indikator,

            "nama_item":
                pd.NA,

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
    """

    # Resolve id_nama_data
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

    df_raw = scrape_all(
        configs=config["configs"],
        indikator=config["indikator"],
        save_raw=False,
        tahun_awal=tahun_awal,
        tahun_akhir=tahun_akhir,
    )

    if df_raw.empty:
        raise RuntimeError(
            f"Tidak ada data yang berhasil diambil "
            f"untuk {config['indikator']}."
        )

    etl = BPSETL()

    df_final = etl.transform(
        df_raw,
        idnamadata,
        config,
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
        idnamadata = int(idnamadata)

    if isinstance(backfill, str):
        backfill = (
            backfill.strip().lower()
            in {"true", "1", "yes", "y"}
        )

    return update_idnamadata_ginirasio(
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Ambil seluruh histori data BPS",
    )

    args = parser.parse_args()

    print("\n========================================")
    print("BPS GINI RATIO PIPELINE")
    print("========================================")

    df_final = update_idnamadata_ginirasio(
        backfill=args.backfill
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