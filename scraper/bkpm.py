import re
import time
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from utils.api import (
    create_session,
)

from utils.data import (
    normalize_dataframe,
    filter_dataframe,
    filter_not_null,
    filter_nonzero,
    aggregate_dataframe,
    find_duplicates,
    concat_dataframes,
    format_year_end,
    format_quarter_end,
    sanitize_filename,
)

from utils.file import (
    read_parquet,
    save_csv,
    dataframe_to_bytes,
)

from utils.storage import (
    get_minio_client,
    upload_bytes,
)


BASE_URL = "https://data.bkpm.go.id"
SEARCH_URL = f"{BASE_URL}/cari-data"

QUERY = "Data Realisasi Investasi Triwulan"

USE_EXISTING_RAW = False

MINIO_BUCKET = "maganghub"


RAW_DATA_DIR = (
    PROJECT_ROOT / "data" / "raw" / "bkpm"
)

FINAL_DATA_DIR = (
    PROJECT_ROOT / "final"
)

# ============================================================
# BKPM CONFIGURATION
# ============================================================
#
# BKPM_CONFIG hanya menyimpan konfigurasi UMUM dataset.
#
# Nama sektor TIDAK disimpan di sini.
# Daftar sektor tetap berasal dari:
#
#     Mapping Sektor.xlsx
#
# idnamadata tetap menjadi target runtime.
# ============================================================

BKPM_CONFIG = {
    "realisasi_investasi": {
        "jenis_data": "investasi",

        "field_nilai": {
            "PMA": "investasi_us_ribu",
            "PMDN": "investasi_rp_juta",
        },

        "satuan": {
            "PMA": "US$",
            "PMDN": "Rp juta",
        },

        "group_by": {
            "Provinsi": [
                "periode",
                "provinsi",
            ],
            "Kabupaten Kota": [
                "periode",
                "kabupaten_kota",
            ],
            "Negara": [
                "periode",
                "negara",
            ],
        },

        "sector_mapping": {
            "source": "Mapping Sektor.xlsx",
            "raw_column": "nama_sektor",
            "bkpm_column": "sektor_bkpm",
            "utama_column": "sektor_utama",
        },
    },

    "tenaga_kerja": {
        "jenis_data": "tenaga_kerja",

        "field_nilai": "tki",
        "satuan": "Orang",

        "group_by": {
            "Provinsi": [
                "periode",
                "provinsi",
            ],
            "Kabupaten Kota": [
                "periode",
                "kabupaten_kota",
            ],
            "Negara": [
                "periode",
                "negara",
            ],
        },

        "sector_mapping": {
            "source": "Mapping Sektor.xlsx",
            "raw_column": "nama_sektor",
            "bkpm_column": "sektor_bkpm",
            "utama_column": "sektor_utama",
        },
    },
}


def get_bkpm_config(dataset):
    """
    Mengambil konfigurasi dasar dataset BKPM.

    Contoh:
        get_bkpm_config("realisasi_investasi")
        get_bkpm_config("tenaga_kerja")
    """

    if dataset not in BKPM_CONFIG:
        raise ValueError(
            f"Dataset BKPM tidak ditemukan: {dataset}"
        )

    return BKPM_CONFIG[dataset].copy()

def periode_to_key(periode):
    """
    Mengubah periode BKPM menjadi angka untuk sorting periode.

    Contoh:
        2025 Triwulan 1 -> 20251
        2025 Triwulan 2 -> 20252
        2025 Triwulan 3 -> 20253
        2025 Triwulan 4 -> 20254
        2025 Triwulan I -> 20251
        2025 Triwulan II -> 20252
        2025 Triwulan III -> 20253
        2025 Triwulan IV -> 20254
    """

    if pd.isna(periode):
        return pd.NA

    periode = str(periode).strip()

    match = re.search(
        r"(\d{4}).*?Triwulan\s*(IV|III|II|I|[1-4])\b",
        periode,
        re.IGNORECASE,
    )

    if not match:
        return pd.NA

    tahun = int(match.group(1))
    triwulan_raw = match.group(2).upper()

    triwulan_map = {
        "I": 1,
        "II": 2,
        "III": 3,
        "IV": 4,
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
    }

    triwulan = triwulan_map.get(triwulan_raw)

    if triwulan is None:
        return pd.NA

    return tahun * 10 + triwulan

def parse_bkpm_nama_data(nama_data):
    """
    Membaca target dataset BKPM dari nama_data XFlask.

    Fungsi ini tidak menyimpan daftar sektor.

    Nama sektor dibaca dari nama_data kemudian
    divalidasi terhadap Mapping Sektor.xlsx.
    """

    nama_data = str(nama_data).strip()

    # ========================================================
    # JENIS DATA
    # ========================================================

    if nama_data.startswith(
        "Realisasi Investasi"
    ):

        dataset = "realisasi_investasi"

    elif nama_data.startswith(
        "Jumlah Penyerapan Tenaga Kerja"
    ):

        dataset = "tenaga_kerja"

    else:

        raise ValueError(
            f"Jenis data BKPM tidak dikenali: "
            f"{nama_data}"
        )

    base_config = get_bkpm_config(
        dataset
    )

    # ========================================================
    # STATUS PMA / PMDN
    # ========================================================

    if " PMA " in f" {nama_data} ":

        status = "PMA"

    elif " PMDN " in f" {nama_data} ":

        status = "PMDN"

    else:

        status = None

    # ========================================================
    # LOKASI
    # ========================================================

    if "Menurut Kabupaten Kota" in nama_data:

        lokasi = "Kabupaten Kota"

    elif "Menurut Provinsi" in nama_data:

        lokasi = "Provinsi"

    elif "Menurut Negara" in nama_data:

        lokasi = "Negara"

    else:

        raise ValueError(
            f"Lokasi BKPM tidak dikenali: "
            f"{nama_data}"
        )

    # ========================================================
    # SEKTOR
    # ========================================================

    level = "total"

    sektor = None

    sektor_utama = None

    marker = " Sektor "

    if marker in nama_data:

        bagian_sektor = nama_data.split(
            marker,
            1,
        )[1]

        nama_sektor = bagian_sektor.split(
            " Menurut ",
            1,
        )[0].strip()

        if nama_sektor.lower() == "total":

            level = "total"

        elif dataset == "realisasi_investasi":

            level = "sektor_bkpm"

            sektor = nama_sektor

        elif dataset == "tenaga_kerja":

            level = "sektor_utama"

            sektor_utama = nama_sektor

    # ========================================================
    # INVESTASI HARUS PUNYA STATUS
    # ========================================================

    if (
        dataset == "realisasi_investasi"
        and status is None
    ):

        raise ValueError(
            "Realisasi investasi harus memiliki "
            "status PMA atau PMDN karena satuannya berbeda."
        )

    return {
        **base_config,

        "dataset": dataset,

        "jenis_data":
            base_config["jenis_data"],

        "status": status,

        "level": level,

        "sektor": sektor,

        "sektor_utama": sektor_utama,

        "lokasi": lokasi,

        "nama_data": nama_data,
    }

class BKPMScraper:

    def __init__(self):

        self.RAW_DATA_DIR = (
            RAW_DATA_DIR
        )

        self.session = (
            create_session()
        )

        self.minio_client = (
            get_minio_client()
        )


    def get_dataset_links(self):

        datasets = []

        page = 1


        while True:

            print(
                f"\nMencari dataset - "
                f"halaman {page}"
            )


            params = {
                "query": QUERY,
                "page": page,
            }


            response = self.session.get(
                SEARCH_URL,
                params=params,
                timeout=30,
            )


            print(
                "Status:",
                response.status_code,
            )


            if response.status_code != 200:

                print(
                    "Gagal mengambil "
                    "halaman pencarian."
                )

                break


            soup = BeautifulSoup(
                response.text,
                "html.parser",
            )


            links = soup.select(
                'a[href*="/dataset-detail/"]'
            )


            print(
                "Link dataset ditemukan:",
                len(links),
            )


            if not links:
                break


            new_links = 0


            for link in links:

                href = link.get(
                    "href"
                )


                if not href:
                    continue


                url = requests.compat.urljoin(
                    BASE_URL,
                    href,
                )


                title = link.find_parent(
                    class_="card-content"
                )


                if title:

                    title_element = (
                        title.find("h5")
                    )


                    if title_element:

                        title_text = (
                            title_element
                            .get_text(
                                strip=True
                            )
                        )

                    else:

                        title_text = (
                            link.get_text(
                                strip=True
                            )
                        )

                else:

                    title_text = (
                        link.get_text(
                            strip=True
                        )
                    )


                if (
                    "Data Realisasi "
                    "Investasi Triwulan"
                    not in title_text
                ):
                    continue


                if url not in [
                    item["url"]
                    for item in datasets
                ]:

                    datasets.append({

                        "judul":
                            title_text,

                        "url":
                            url,

                    })

                    new_links += 1


            print(
                "Dataset baru:",
                new_links,
            )


            if new_links == 0:
                break


            page += 1


            if page > 100:

                print(
                    "Berhenti karena "
                    "batas halaman."
                )

                break


            time.sleep(0.5)


        return datasets


    def get_parent_id(
        self,
        dataset_url,
    ):

        response = self.session.get(
            dataset_url,
            timeout=30,
        )


        if response.status_code != 200:

            print(
                "Gagal membuka:",
                dataset_url,
            )

            return None


        match = re.search(
            r'<input[^>]*value=["\']'
            r'([^"\']+)["\'][^>]*'
            r'name=["\']parent_id["\']',
            response.text,
        )


        if match:
            return match.group(1)


        return None


    def get_data(
        self,
        parent_id,
    ):

        all_rows = []

        start = 0

        length = 10000


        while True:

            print(
                f"  Mengambil data: "
                f"start={start}"
            )


            params = {
                "draw": 1,
                "start": start,
                "length": length,

                "order[0][column]": 0,
                "order[0][dir]": "asc",

                "search[value]": "",
                "search[regex]": "false",

                "dataset_detail_parent_id":
                    parent_id,
            }


            for i in range(14):

                params[
                    f"columns[{i}][data]"
                ] = "function"

                params[
                    f"columns[{i}][name]"
                ] = ""

                params[
                    f"columns[{i}][searchable]"
                ] = "true"

                params[
                    f"columns[{i}][orderable]"
                ] = "true"

                params[
                    f"columns[{i}][search][value]"
                ] = ""

                params[
                    f"columns[{i}][search][regex]"
                ] = "false"


            try:

                response = (
                    self.session.get(
                        f"{BASE_URL}/data",
                        params=params,
                        timeout=60,
                    )
                )

            except requests.exceptions.RequestException as e:

                print(
                    "  ERROR:",
                    e,
                )

                return []


            if response.status_code != 200:

                print(
                    "  Gagal mengambil data:",
                    response.status_code,
                )

                return []


            try:

                result = (
                    response.json()
                )

            except ValueError:

                print(
                    "  Response bukan JSON."
                )

                return []


            total = result.get(
                "recordsTotal",
                0,
            )


            rows = result.get(
                "data",
                [],
            )


            print(
                f"  Server: {total} | "
                f"Diterima batch: "
                f"{len(rows)}"
            )


            if not rows:
                break


            all_rows.extend(
                rows
            )


            start += len(rows)


            if start >= total:
                break


            time.sleep(1)


        print(
            f"  TOTAL DATASET: "
            f"{len(all_rows)}"
        )


        return all_rows

    def get_latest_bkpm_period(self, df):
        """
        Mengambil periode BKPM paling baru dari dataframe.
        """

        if df.empty or "periode" not in df.columns:
            raise RuntimeError(
                "Kolom periode tidak ditemukan."
            )

        temp = df[
            ["periode"]
        ].dropna().drop_duplicates().copy()

        temp["periode_key"] = (
            temp["periode"]
            .apply(periode_to_key)
        )

        temp = temp.dropna(
            subset=["periode_key"]
        )

        if temp.empty:
            raise RuntimeError(
                "Tidak ditemukan periode BKPM yang valid."
            )

        latest = temp.loc[
            temp["periode_key"].idxmax()
        ]

        return (
            latest["periode"],
            int(latest["periode_key"])
        )

    def save_raw_data(
        self,
        df,
        periode,
        index,
    ):

        safe_periode = sanitize_filename(periode)


        filename = (
            f"bkpm_investasi_"
            f"{safe_periode}_"
            f"{index}.parquet"
        )


        data = dataframe_to_bytes(
            df,
            format="parquet",
        )


        object_name = (
            f"bkpm/raw/{filename}"
        )


        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            data,
            object_name,
            "application/octet-stream",
        )


        print(
            f"Raw data disimpan ke MinIO: "
            f"{object_name}"
        )

    def prepare_dataframe(
        self,
        df,
    ):

        df = normalize_dataframe(
            df,
            numeric_columns=[
                "investasi_rp_juta",
                "investasi_us_ribu",
                "tki",
                "data_y",
            ],
        )


        if "data_y" in df.columns:

            df["data_y"] = (
                df["data_y"]
                .round(2)
            )


        return df


    def load_existing_raw_data(self):

        raw_files = list(
            self.RAW_DATA_DIR
            .glob("*.parquet")
        )


        print(
            f"Ditemukan "
            f"{len(raw_files)} "
            f"file raw."
        )


        if not raw_files:

            print(
                "Tidak ada file raw "
                "di data/raw."
            )

            return pd.DataFrame()


        all_dataframes = []


        for filepath in raw_files:

            print(
                "Membaca:",
                filepath.name,
            )


            df = read_parquet(
                filepath
            )


            df = self.prepare_dataframe(
                df
            )


            all_dataframes.append(
                df
            )


            full_df = concat_dataframes(
                all_dataframes
            )


        print(
            "\nKOLOM FULL DF:"
        )

        print(
            full_df.columns.tolist()
        )


        print(
            f"\nTotal raw rows: "
            f"{len(full_df):,}"
        )


        return full_df


    def save_processed_data(
        self,
        df,
        tahun_awal,
        tahun_akhir,
    ):

        filename = (
            f"bkpm_investasi_processed_"
            f"{tahun_awal}_"
            f"{tahun_akhir}.parquet"
        )


        data = dataframe_to_bytes(
            df,
            format="parquet",
        )


        object_name = (
            f"bkpm/processed/"
            f"{filename}"
        )


        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            data,
            object_name,
            "application/octet-stream",
        )


        print(
            f"\nProcessed data "
            f"disimpan ke MinIO: "
            f"{object_name}"
        )

        print(
            f"Total processed rows: "
            f"{len(df):,}"
        )

class BKPMETL:

    def __init__(self):

        self.scraper = (
            BKPMScraper()
        )

        self.minio_client = (
            self.scraper.minio_client
        )


    def create_aggregation(
        self,
        df,
        status_penanaman_modal=None,
        nama_sektor=None,
        sektor_utama=None,
        group_by=None,
    ):

        filtered_df = df.copy()


        if status_penanaman_modal:

            filtered_df = filtered_df[
                filtered_df[
                    "status_penanaman_modal"
                ]
                == status_penanaman_modal
            ]


        if nama_sektor:

            filtered_df = filtered_df[
                filtered_df[
                    "sektor_bkpm"
                ]
                == nama_sektor
            ]


        if sektor_utama:

            filtered_df = filtered_df[
                filtered_df[
                    "sektor_utama"
                ]
                == sektor_utama
            ]


        if filtered_df.empty:

            print(
                "Tidak ada data "
                "setelah filter."
            )

            return pd.DataFrame()


        if group_by is None:

            group_by = [
                "periode",
                "provinsi",
            ]

        result = aggregate_dataframe(
            filtered_df,
            group_by=group_by,
            aggregations={
                "investasi_rp_juta": "sum",
                "investasi_us_ribu": "sum",
                "tki": "sum",
            },
        )


        return result


    def check_duplicates(
        self,
        df,
    ):

        return find_duplicates(
            df,
            subset=[
                "negara",
                "provinsi",
                "kota",
                "item",
                "data_x",
            ],
        )


    def save_final_data(
        self,
        df,
        filename,
        status_penanaman_modal,
        nama_sektor,
        lokasi,
        jenis_data="investasi",
        level="sektor_bkpm",
    ):
        filepath = FINAL_DATA_DIR / filename

        filepath.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        save_csv(
            df,
            filepath,
        )

        csv_bytes = dataframe_to_bytes(
            df,
            format="csv",
        )

        object_name = f"bkpm/final/{filename}"

        upload_bytes(
            self.minio_client,
            MINIO_BUCKET,
            csv_bytes,
            object_name,
            "text/csv",
        )

        print(
            "Final data di-upload ke MinIO:",
            object_name,
        )

        print(
            "Total rows:",
            len(df),
        )

        self.add_nama_data(
            self.generate_nama_data(
                status_penanaman_modal,
                nama_sektor,
                lokasi,
                jenis_data,
                level=level,
            )
        )

    def periode_to_date(
        self,
        periode,
    ):

        if pd.isna(periode):
            return pd.NA


        periode = str(
            periode
        ).strip()


        tahun = periode[:4]


        if "Triwulan 1" in periode:

            return (
                f"31-03-{tahun}"
            )


        elif "Triwulan 2" in periode:

            return (
                f"30-06-{tahun}"
            )


        elif "Triwulan 3" in periode:

            return (
                f"30-09-{tahun}"
            )


        elif "Triwulan 4" in periode:

            return (
                f"31-12-{tahun}"
            )


        return pd.NA


    def generate_nama_data(
        self,
        status_penanaman_modal,
        nama,
        lokasi,
        jenis_data="investasi",
        level="sektor_bkpm",
    ):

        if jenis_data == "tenaga_kerja":

            return (
                f"Jumlah Penyerapan "
                f"Tenaga Kerja "
                f"{status_penanaman_modal} "
                f"Sektor {nama} "
                f"Menurut {lokasi} "
                f"(Triwulan)"
            )


        if (
            level == "total"
            or nama is None
        ):

            return (
                f"Realisasi Investasi "
                f"{status_penanaman_modal} "
                f"Total Sektor "
                f"Menurut {lokasi} "
                f"(Triwulan)"
            )


        return (
            f"Realisasi Investasi "
            f"{status_penanaman_modal} "
            f"Sektor {nama} "
            f"Menurut {lokasi} "
            f"(Triwulan)"
        )


    def add_nama_data(
        self,
        nama_data,
    ):

        nama_data_file = (
            PROJECT_ROOT
            / "nama_data.csv"
        )


        if nama_data_file.exists():

            df_nama = pd.read_csv(
                nama_data_file
            )

        else:

            df_nama = pd.DataFrame(
                columns=["nama_data"]
            )


        if (
            nama_data
            in df_nama[
                "nama_data"
            ].astype(str).values
        ):

            print(
                f"Nama data sudah ada: "
                f"{nama_data}"
            )

            return


        new_row = pd.DataFrame({

            "nama_data":
                [nama_data],

        })


        df_nama = pd.concat(
            [
                df_nama,
                new_row,
            ],
            ignore_index=True,
        )


        save_csv(
            df_nama,
            nama_data_file,
        )


        print(
            f"Nama data ditambahkan: "
            f"{nama_data}"
        )

    def get_nama_data(self):
        """
        Mengambil daftar nama_data dari XFlask.
        """

        url = "https://xflask.databoks.id/database/nama_data"

        response = self.scraper.session.get(
            url,
            timeout=60,
        )

        response.raise_for_status()

        data = response.json()

        if isinstance(data, dict):
            if "data" in data:
                data = data["data"]
            elif "result" in data:
                data = data["result"]
            else:
                data = [data]

        df = pd.DataFrame(data)

        if df.empty:
            raise ValueError(
                "Data nama_data dari XFlask kosong."
            )

        required_columns = [
            "id",
            "nama",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                "Response nama_data XFlask tidak memiliki "
                f"kolom: {missing_columns}"
            )

        return df


    def load_mapping_sektor(self):
        """
        Membaca mapping sektor dari Mapping Sektor.xlsx.

        Mapping digunakan untuk:
            - sektor_bkpm
            - sektor_utama

        Nama sektor tidak di-hardcode di Python.
        """

        mapping_file = (
            PROJECT_ROOT
            / "Mapping Sektor.xlsx"
        )

        if not mapping_file.exists():

            raise FileNotFoundError(
                "File Mapping Sektor.xlsx tidak ditemukan: "
                f"{mapping_file}"
            )

        mapping_df = pd.read_excel(
            mapping_file
        )

        required_columns = [
            "sektor_bkpm",
            "sektor_utama",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in mapping_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "Kolom Mapping Sektor.xlsx tidak lengkap: "
                f"{missing_columns}"
            )

        mapping_df = (
            mapping_df[
                required_columns
            ]
            .copy()
        )

        mapping_df[
            "sektor_bkpm"
        ] = (
            mapping_df[
                "sektor_bkpm"
            ]
            .astype(str)
            .str.strip()
        )

        mapping_df[
            "sektor_utama"
        ] = (
            mapping_df[
                "sektor_utama"
            ]
            .astype(str)
            .str.strip()
        )

        mapping_df = (
            mapping_df
            .replace({
                "nan": pd.NA,
                "None": pd.NA,
                "": pd.NA,
            })
            .dropna(
                subset=[
                    "sektor_bkpm",
                ]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        return mapping_df

    def filter_current_periods(
            self,
            df,
            jumlah_periode=3,
        ):
            """
            Mengambil periode terbaru + periode sebelumnya.

            Default:
                3 triwulan
            """

            if df.empty:
                return df

            temp = df.copy()

            temp["_periode_key"] = (
                temp["periode"]
                .apply(periode_to_key)
            )

            periode_keys = (
                temp["_periode_key"]
                .dropna()
                .drop_duplicates()
                .sort_values()
                .tolist()
            )

            if not periode_keys:
                raise RuntimeError(
                    "Tidak ada periode BKPM yang valid."
                )

            selected_keys = periode_keys[
                -jumlah_periode:
            ]

            print(
                "\nPERIODE CURRENT UPDATE:"
            )

            selected_periods = (
                temp.loc[
                    temp["_periode_key"].isin(
                        selected_keys
                    ),
                    "periode",
                ]
                .drop_duplicates()
                .tolist()
            )

            for periode in selected_periods:
                print("-", periode)

            result = temp[
                temp["_periode_key"].isin(
                    selected_keys
                )
            ].copy()

            result.drop(
                columns=["_periode_key"],
                inplace=True,
            )

            return result

def build_bkpm_dataset(
    df,
    config,
    idnamadata=None,
):
    """
    Transform satu target dataset BKPM.

    Config terdiri dari:
        - konfigurasi umum BKPM_CONFIG
        - target hasil parsing nama_data

    Nama sektor tidak berasal dari BKPM_CONFIG.
    """

    etl = BKPMETL()

    jenis_data = config[
        "jenis_data"
    ]

    status = config.get(
        "status"
    )

    lokasi = config[
        "lokasi"
    ]

    level = config[
        "level"
    ]

    nama_sektor = config.get(
        "sektor"
    )

    sektor_utama = config.get(
        "sektor_utama"
    )

    # ========================================================
    # GROUP BY
    # ========================================================

    group_by_map = config[
        "group_by"
    ]

    if lokasi not in group_by_map:

        raise ValueError(
            f"Lokasi BKPM tidak didukung: "
            f"{lokasi}"
        )

    group_by = group_by_map[
        lokasi
    ]

    # ========================================================
    # AGGREGATION
    # ========================================================

    hasil = etl.create_aggregation(
        df,
        status_penanaman_modal=status,
        nama_sektor=nama_sektor,
        sektor_utama=sektor_utama,
        group_by=group_by,
    )

    if hasil.empty:
        return pd.DataFrame()

    # ========================================================
    # TENAGA KERJA
    # ========================================================

    if jenis_data == "tenaga_kerja":

        field_nilai = config[
            "field_nilai"
        ]

        satuan = config[
            "satuan"
        ]

        data_y = (
            hasil[field_nilai]
            .round(2)
        )

    # ========================================================
    # INVESTASI
    # ========================================================

    elif jenis_data == "investasi":

        if status not in {
            "PMA",
            "PMDN",
        }:

            raise ValueError(
                "Status investasi BKPM "
                "harus PMA atau PMDN."
            )

        field_nilai = config[
            "field_nilai"
        ][status]

        satuan = config[
            "satuan"
        ][status]

        data_y = hasil[
            field_nilai
        ]

        # Raw PMA:
        # investasi_us_ribu
        #
        # Output:
        # US$
        if status == "PMA":

            data_y = data_y.mul(
                1000
            )

        data_y = data_y.round(
            2
        )

    else:

        raise ValueError(
            f"Jenis data BKPM tidak didukung: "
            f"{jenis_data}"
        )

    # ========================================================
    # FINAL DATAFRAME
    # ========================================================

    if lokasi == "Provinsi":

        final_df = pd.DataFrame({

            "kota": pd.NA,

            "provinsi":
                hasil["provinsi"],

            "negara":
                "Indonesia",

            "item": pd.NA,

            "satuan":
                satuan,

            "data_x": (
                hasil["periode"]
                .apply(
                    etl.periode_to_date
                )
            ),

            "data_y":
                data_y,

            "sumber":
                "BKPM",

            "nama_data_import":
                f"{BASE_URL}/dataset?status=publik",
        })

    elif lokasi == "Kabupaten Kota":

        final_df = pd.DataFrame({

            "kota":
                hasil["kabupaten_kota"],

            "provinsi":
                pd.NA,

            "negara":
                "Indonesia",

            "item": pd.NA,

            "satuan":
                satuan,

            "data_x": (
                hasil["periode"]
                .apply(
                    etl.periode_to_date
                )
            ),

            "data_y":
                data_y,

            "sumber":
                "BKPM",

            "nama_data_import":
                f"{BASE_URL}/dataset?status=publik",
        })

    elif lokasi == "Negara":

        final_df = pd.DataFrame({

            "kota": pd.NA,

            "provinsi": pd.NA,

            "negara":
                hasil["negara"],

            "item": pd.NA,

            "satuan":
                satuan,

            "data_x": (
                hasil["periode"]
                .apply(
                    etl.periode_to_date
                )
            ),

            "data_y":
                data_y,

            "sumber":
                "BKPM",

            "nama_data_import":
                f"{BASE_URL}/dataset?status=publik",
        })

    else:

        raise ValueError(
            f"Lokasi tidak didukung: "
            f"{lokasi}"
        )

    # ========================================================
    # CLEANING
    # ========================================================

    final_df = final_df[
        final_df["data_y"].notna()
        & (
            final_df["data_y"]
            != 0
        )
    ].copy()

    print(
        "\nBUILD BKPM DATASET"
    )

    print(
        "idnamadata:",
        idnamadata,
    )

    print(
        "jenis_data:",
        jenis_data,
    )

    print(
        "status:",
        status,
    )

    print(
        "level:",
        level,
    )

    print(
        "sektor:",
        nama_sektor,
    )

    print(
        "sektor_utama:",
        sektor_utama,
    )

    print(
        "lokasi:",
        lokasi,
    )

    print(
        "jumlah row:",
        len(final_df),
    )

    return final_df

def update_bkpm_dataset(
    config,
    idnamadata=None,
    iditem=None,
    backfill=True,
):
    """
    Menghasilkan dataframe BKPM untuk update.

    backfill=True
        Ambil seluruh histori.

    backfill=False
        Ambil data terbaru + 2 triwulan sebelumnya.
    """

    del iditem

    etl = BKPMETL()

    # ==========================================
    # LOAD RAW
    # ==========================================

    if USE_EXISTING_RAW:

        print(
            "\n========================================"
        )
        print(
            "BKPM - LOAD EXISTING RAW"
        )
        print(
            "========================================"
        )

        df_raw = (
            etl.scraper
            .load_existing_raw_data()
        )

    else:

        print(
            "\n========================================"
        )
        print(
            "BKPM - SCRAPE RAW"
        )
        print(
            "========================================"
        )

        datasets = (
            etl.scraper
            .get_dataset_links()
        )

        if not datasets:
            raise RuntimeError(
                "Tidak ada dataset BKPM."
            )

        all_dataframes = []

        for i, dataset in enumerate(
            datasets,
            start=1,
        ):

            print(
                f"\n[{i}/{len(datasets)}]"
            )

            print(
                dataset["judul"]
            )

            parent_id = (
                etl.scraper
                .get_parent_id(
                    dataset["url"]
                )
            )

            if not parent_id:
                continue

            rows = (
                etl.scraper
                .get_data(parent_id)
            )

            if not rows:
                continue

            df = pd.DataFrame(rows)

            df = (
                etl.scraper
                .prepare_dataframe(df)
            )

            all_dataframes.append(df)

            periode = "unknown"

            if "periode" in df.columns:

                values = (
                    df["periode"]
                    .dropna()
                    .astype(str)
                    .unique()
                )

                if len(values) > 0:
                    periode = values[0]

            etl.scraper.save_raw_data(
                df,
                periode,
                i,
            )

        if not all_dataframes:
            raise RuntimeError(
                "Tidak ada data BKPM yang berhasil diambil."
            )

        df_raw = pd.concat(
            all_dataframes,
            ignore_index=True,
        )

    if df_raw.empty:
        raise RuntimeError(
            "Raw BKPM kosong."
        )

    if not backfill:

        df_raw = (
            etl.filter_current_periods(
                df_raw,
                jumlah_periode=3,
            )
        )

    # ========================================================
    # LOAD MAPPING SEKTOR
    # ========================================================

    mapping_sektor = (
        etl.load_mapping_sektor()
    )

    if mapping_sektor.empty:

        raise RuntimeError(
            "Mapping Sektor.xlsx kosong."
        )

    nama_sektor = config.get(
        "sektor"
    )

    sektor_utama = config.get(
        "sektor_utama"
    )

    # ========================================================
    # VALIDASI TARGET SEKTOR
    # ========================================================

    if nama_sektor is not None:

        sektor_tersedia = set(
            mapping_sektor[
                "sektor_bkpm"
            ]
            .dropna()
            .astype(str)
            .str.strip()
        )

        if nama_sektor not in sektor_tersedia:

            raise ValueError(
                "Sektor BKPM tidak ditemukan "
                "di Mapping Sektor.xlsx: "
                f"{nama_sektor}"
            )

    if sektor_utama is not None:

        sektor_utama_tersedia = set(
            mapping_sektor[
                "sektor_utama"
            ]
            .dropna()
            .astype(str)
            .str.strip()
        )

        if (
            sektor_utama
            not in sektor_utama_tersedia
        ):

            raise ValueError(
                "Sektor utama tidak ditemukan "
                "di Mapping Sektor.xlsx: "
                f"{sektor_utama}"
            )

    # ========================================================
    # MERGE RAW DENGAN MAPPING
    # ========================================================

    if "nama_sektor" not in df_raw.columns:

        raise ValueError(
            "Kolom 'nama_sektor' tidak ditemukan "
            "pada raw BKPM."
        )

    df_raw = df_raw.merge(
        mapping_sektor,
        left_on="nama_sektor",
        right_on="sektor_bkpm",
        how="left",
    )

    # ==========================================
    # TRANSFORM
    # ==========================================

    df_final = build_bkpm_dataset(
        df=df_raw,
        config=config,
        idnamadata=idnamadata,
    )

    if df_final.empty:
        raise RuntimeError(
            "Hasil transform BKPM kosong."
        )

    print(
        "\n========================================"
    )
    print(
        "DATAFRAME UPDATE SIAP"
    )
    print(
        "JUMLAH ROW:",
        len(df_final),
    )
    print(
        "KOLOM:",
        df_final.columns.tolist(),
    )

    return df_final

def get_nama_data_by_id(idnamadata):
    etl = BKPMETL()

    df_nama = etl.get_nama_data()

    if df_nama.empty:
        raise ValueError("Data nama_data kosong.")

    df_nama["id"] = pd.to_numeric(
        df_nama["id"],
        errors="coerce"
    )

    idnamadata = int(idnamadata)

    row = df_nama[
        df_nama["id"] == idnamadata
    ]

    if row.empty:
        raise ValueError(
            f"idnamadata {idnamadata} tidak ditemukan."
        )

    return str(row.iloc[0]["nama"])

def get_bkpm_target_config(
    idnamadata
):
    """
    Mengambil konfigurasi target berdasarkan
    idnamadata.

    Alur:

        idnamadata
            ↓
        nama_data XFlask
            ↓
        parse_bkpm_nama_data()
            ↓
        BKPM_CONFIG
            ↓
        Mapping Sektor.xlsx
    """

    nama_data = (
        get_nama_data_by_id(
            idnamadata
        )
    )

    config = (
        parse_bkpm_nama_data(
            nama_data
        )
    )

    return config, nama_data

def update_idnamadata_bkpm(
    idnamadata,
    iditem=None,
    backfill=True,
):
    """
    Generic update satu id_nama_data BKPM.

    Dataset ditentukan berdasarkan nama_data
    dari XFlask.

    Config umum berasal dari BKPM_CONFIG.
    Target sektor berasal dari nama_data
    dan divalidasi melalui Mapping Sektor.xlsx.
    """

    config, nama_data = (
        get_bkpm_target_config(
            idnamadata
        )
    )

    print(
        "\nNAMA DATA:",
        nama_data,
    )

    print(
        "DATASET:",
        config["dataset"],
    )

    print(
        "STATUS:",
        config["status"],
    )

    print(
        "LEVEL:",
        config["level"],
    )

    print(
        "SEKTOR:",
        config.get("sektor"),
    )

    print(
        "SEKTOR UTAMA:",
        config.get("sektor_utama"),
    )

    print(
        "LOKASI:",
        config["lokasi"],
    )

    return update_bkpm_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def update_idnamadata_realisasi_investasi(
    idnamadata,
    iditem=None,
    backfill=True,
):
    """
    Update satu id_nama_data untuk
    realisasi investasi BKPM.
    """

    config, nama_data = (
        get_bkpm_target_config(
            idnamadata
        )
    )

    if config["jenis_data"] != "investasi":

        raise ValueError(
            f"idnamadata {idnamadata} bukan "
            "dataset realisasi investasi."
        )

    print(
        "\nNAMA DATA:",
        nama_data,
    )

    return update_bkpm_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )


def update_idnamadata_tenaga_kerja(
    idnamadata,
    iditem=None,
    backfill=True,
):
    """
    Update satu id_nama_data untuk
    tenaga kerja BKPM.
    """

    config, nama_data = (
        get_bkpm_target_config(
            idnamadata
        )
    )

    if config["jenis_data"] != "tenaga_kerja":

        raise ValueError(
            f"idnamadata {idnamadata} bukan "
            "dataset tenaga kerja."
        )

    print(
        "\nNAMA DATA:",
        nama_data,
    )

    return update_bkpm_dataset(
        config=config,
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def update_curr_realisasi_investasi(
    idnamadata,
    iditem=None,
):
    """
    Current update realisasi investasi.

    Mengambil:
        periode terbaru
        + dua periode sebelumnya.
    """

    return update_idnamadata_realisasi_investasi(
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=False,
    )


def update_curr_tenaga_kerja(
    idnamadata,
    iditem=None,
):
    """
    Current update tenaga kerja.

    Mengambil:
        periode terbaru
        + dua periode sebelumnya.
    """

    return update_idnamadata_tenaga_kerja(
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=False,
    )


def update_curr_bkpm(
    idnamadata,
    iditem=None,
):
    """
    Generic current update BKPM.

    Memilih update_curr_* berdasarkan
    jenis dataset.
    """

    config, _ = (
        get_bkpm_target_config(
            idnamadata
        )
    )

    if config["jenis_data"] == "investasi":

        return update_curr_realisasi_investasi(
            idnamadata=idnamadata,
            iditem=iditem,
        )

    elif config["jenis_data"] == "tenaga_kerja":

        return update_curr_tenaga_kerja(
            idnamadata=idnamadata,
            iditem=iditem,
        )

    else:

        raise ValueError(
            f"Jenis data BKPM tidak didukung: "
            f"{config['jenis_data']}"
        )

def fetch(params=None):
    params = params or {}

    idnamadata = params.get("idnamadata")
    iditem = params.get("iditem")
    backfill = params.get("backfill", True)

    if idnamadata is None:
        raise ValueError("Parameter 'idnamadata' wajib diisi.")

    try:
        idnamadata = int(idnamadata)
    except (TypeError, ValueError):
        raise ValueError(
            f"idnamadata harus berupa angka, bukan: {idnamadata}"
        )

    if isinstance(backfill, str):
        backfill = backfill.strip().lower() in {
            "true",
            "1",
            "yes",
            "y",
        }

    return update_idnamadata_bkpm(
        idnamadata=idnamadata,
        iditem=iditem,
        backfill=backfill,
    )

def test_main(
    dataset=1,
    tahun_awal=2010,
    tahun_akhir=2026,
):
    s = BKPMScraper()

    datasets = s.get_dataset_links()

    if not datasets:
        raise RuntimeError(
            "Tak ada dataset di data.bkpm.go.id "
            "(halaman pencarian berubah?)"
        )

    # ========================================
    # MODE DATASET TERBARU
    # ========================================

    if str(dataset).lower() == "last":

        kandidat = []

        triwulan_map = {
            "I": 1,
            "II": 2,
            "III": 3,
            "IV": 4,
        }

        for d in datasets:

            judul = str(
                d.get("judul", "")
            )

            match = re.search(
                r"Triwulan\s+"
                r"(I{1,3}|IV)"
                r"\s+Tahun\s+"
                r"(\d{4})",
                judul,
                re.IGNORECASE,
            )

            if not match:
                continue

            triwulan_romawi = (
                match.group(1).upper()
            )

            tahun = int(
                match.group(2)
            )

            triwulan = triwulan_map.get(
                triwulan_romawi
            )

            if triwulan is None:
                continue

            if not (
                tahun_awal
                <= tahun
                <= tahun_akhir
            ):
                continue

            kandidat.append({
                "dataset": d,
                "tahun": tahun,
                "triwulan": triwulan,
            })

        if not kandidat:
            raise RuntimeError(
                "Tidak ditemukan dataset "
                "dengan periode yang sesuai."
            )

        # Cari periode paling terbaru
        tahun_terbaru = max(
            x["tahun"]
            for x in kandidat
        )

        triwulan_terbaru = max(
            x["triwulan"]
            for x in kandidat
            if x["tahun"] == tahun_terbaru
        )

        # Ambil dataset dengan periode terbaru
        kandidat_terbaru = [
            x
            for x in kandidat
            if (
                x["tahun"]
                == tahun_terbaru
                and x["triwulan"]
                == triwulan_terbaru
            )
        ]

        print(
            "\nDATASET TERBARU:"
        )

        for x in kandidat_terbaru:
            print(
                x["dataset"]["judul"]
            )

        print(
            "PERIODE TERBARU:",
            f"{tahun_terbaru} - "
            f"Triwulan {triwulan_terbaru}",
        )

        # Kalau ada lebih dari satu dataset
        # dengan periode yang sama, ambil semuanya
        pilih = [
            x["dataset"]
            for x in kandidat_terbaru
        ]

    # ========================================
    # MODE DATASET BERDASARKAN NOMOR
    # ========================================

    else:

        dataset = int(dataset)

        pilih = datasets[:dataset]

    # ========================================
    # AMBIL DATA
    # ========================================

    bagian = []

    for d in pilih:

        pid = s.get_parent_id(
            d["url"]
        )

        if not pid:
            continue

        rows = s.get_data(pid)

        if rows:
            bagian.append(
                s.prepare_dataframe(
                    pd.DataFrame(rows)
                )
            )

    if not bagian:
        raise RuntimeError(
            "Dataset ditemukan tapi "
            "tak ada baris yang terbaca."
        )

    df = pd.concat(
        bagian,
        ignore_index=True,
    )

    # ========================================
    # FILTER TAHUN
    # ========================================

    tahun = (
        df["periode"]
        .astype(str)
        .str[:4]
    )

    df = df[
        tahun.str.isdigit()
    ]

    df = df[
        df["periode"]
        .astype(str)
        .str[:4]
        .astype(int)
        .between(
            tahun_awal,
            tahun_akhir,
        )
    ].copy()

    return df

def main(tahun_awal=2010, tahun_akhir=2026):
    """
    Full ETL BKPM.

    Flow:
        1. Load / scrape raw data
        2. Filter periode
        3. Load Mapping Sektor
        4. Merge raw dengan mapping
        5. Build dataset berdasarkan konfigurasi
        6. Save final dataset

    Logic transform utama dipusatkan di:
        build_bkpm_dataset()
    """

    print(
        "========================================"
    )
    print(
        "BKPM ETL"
    )
    print(
        "========================================"
    )

    etl = BKPMETL()

    # ========================================================
    # 1. LOAD RAW DATA
    # ========================================================

    if USE_EXISTING_RAW:

        print(
            "\nMODE: MENGGUNAKAN RAW DATA YANG SUDAH ADA"
        )

        full_df = (
            etl.scraper.load_existing_raw_data()
        )

        if full_df.empty:

            print(
                "Raw data kosong."
            )

            return

    else:

        print(
            "\nMODE: SCRAPING DATA DARI BKPM"
        )

        datasets = (
            etl.scraper.get_dataset_links()
        )

        print(
            "\nTotal dataset ditemukan:",
            len(datasets),
        )

        if not datasets:

            print(
                "Tidak ada dataset ditemukan."
            )

            return

        dataset_info = []

        # ----------------------------------------------------
        # Ambil parent ID setiap dataset
        # ----------------------------------------------------

        for dataset in datasets:

            parent_id = (
                etl.scraper.get_parent_id(
                    dataset["url"]
                )
            )

            dataset_info.append({
                "judul": dataset["judul"],
                "url": dataset["url"],
                "parent_id": parent_id,
            })

            time.sleep(1)

        all_dataframes = []

        failed_datasets = []

        # ----------------------------------------------------
        # Scrape seluruh dataset
        # ----------------------------------------------------

        for i, dataset in enumerate(
            dataset_info,
            start=1,
        ):

            print(
                f"\n[{i}/{len(dataset_info)}]",
                dataset["judul"],
            )

            parent_id = (
                dataset["parent_id"]
            )

            if not parent_id:

                failed_datasets.append(
                    dataset
                )

                continue

            rows = (
                etl.scraper.get_data(
                    parent_id
                )
            )

            if not rows:

                failed_datasets.append(
                    dataset
                )

                continue

            df = pd.DataFrame(
                rows
            )

            df = (
                etl.scraper.prepare_dataframe(
                    df
                )
            )

            if df.empty:

                failed_datasets.append(
                    dataset
                )

                continue

            all_dataframes.append(
                df
            )

            periode = "unknown"

            if "periode" in df.columns:

                values = (
                    df["periode"]
                    .dropna()
                    .astype(str)
                    .unique()
                )

                if len(values) > 0:

                    periode = values[0]

            etl.scraper.save_raw_data(
                df,
                periode,
                i,
            )

            time.sleep(2)

        # ----------------------------------------------------
        # Gabungkan raw data
        # ----------------------------------------------------

        if not all_dataframes:

            print(
                "Tidak ada data yang berhasil diambil."
            )

            return

        full_df = concat_dataframes(
            all_dataframes
        )

        print(
            "\nTOTAL RAW DATA:",
            f"{len(full_df):,}",
        )

    # ========================================================
    # 2. FILTER PERIODE
    # ========================================================

    if "periode" not in full_df.columns:

        raise ValueError(
            "Kolom 'periode' tidak ditemukan "
            "pada raw BKPM."
        )

    full_df = full_df[
        full_df["periode"]
        .astype(str)
        .str[:4]
        .astype(int)
        .between(
            tahun_awal,
            tahun_akhir,
        )
    ].copy()

    if full_df.empty:

        print(
            "\nTidak ada data dalam rentang periode:"
        )

        print(
            tahun_awal,
            "-",
            tahun_akhir,
        )

        return

    # ========================================================
    # 3. SAVE PROCESSED DATA
    # ========================================================

    etl.scraper.save_processed_data(
        full_df,
        tahun_awal,
        tahun_akhir,
    )

    # ========================================================
    # 4. LOAD MAPPING SEKTOR
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "MULAI TRANSFORM"
    )

    print(
        "========================================"
    )

    mapping_sektor = (
        etl.load_mapping_sektor()
    )

    if mapping_sektor.empty:

        print(
            "Mapping sektor kosong."
        )

        return

    print(
        "\nKolom full_df:"
    )

    print(
        full_df.columns.tolist()
    )

    print(
        "\nKolom mapping:"
    )

    print(
        mapping_sektor.columns.tolist()
    )

    # ========================================================
    # 5. MERGE MAPPING SEKTOR
    # ========================================================

    if "nama_sektor" not in full_df.columns:

        raise ValueError(
            "Kolom 'nama_sektor' tidak ditemukan "
            "pada full_df."
        )

    full_df = full_df.merge(
        mapping_sektor,
        left_on="nama_sektor",
        right_on="sektor_bkpm",
        how="left",
    )

    print(
        "\nTotal mapping sektor:",
        len(mapping_sektor),
    )

    # ========================================================
    # 6. CONFIG DASAR
    # ========================================================

    investasi_config = get_bkpm_config(
        "realisasi_investasi"
    )

    tenaga_kerja_config = get_bkpm_config(
        "tenaga_kerja"
    )

    lokasi_list = list(
        investasi_config[
            "group_by"
        ].keys()
    )

    # ========================================================
    # 7. REALISASI INVESTASI TOTAL
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "REALISASI INVESTASI - TOTAL"
    )

    print(
        "========================================"
    )

    for status in [
        "PMA",
        "PMDN",
    ]:

        for lokasi in lokasi_list:

            print(
                f"\nBuild TOTAL | "
                f"{status} | "
                f"{lokasi}"
            )

            config = investasi_config.copy()

            config.update({

                "dataset":
                    "realisasi_investasi",

                "status":
                    status,

                "level":
                    "total",

                "sektor":
                    None,

                "sektor_utama":
                    None,

                "lokasi":
                    lokasi,
            })

            final_df = build_bkpm_dataset(
                full_df,
                config,
            )

            if final_df.empty:

                print(
                    "Tidak ada data, dilewati."
                )

                continue

            nama_data = (
                etl.generate_nama_data(
                    status,
                    None,
                    lokasi,
                    level="total",
                )
            )

            filename = (
                "bkpm_realisasi_"
                f"{status.lower()}_"
                "total_sektor_"
                f"{lokasi.lower().replace(' ', '_')}"
                ".csv"
            )

            duplicates = (
                etl.check_duplicates(
                    final_df
                )
            )

            if not duplicates.empty:

                print(
                    "WARNING: ditemukan "
                    f"{len(duplicates)} "
                    "baris duplicate."
                )

            etl.save_final_data(
                final_df,
                filename,
                status,
                None,
                lokasi,
                level="total",
            )

            print(
                f"Nama data: {nama_data}"
            )

    # ========================================================
    # 8. REALISASI INVESTASI SEKTOR UTAMA
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "REALISASI INVESTASI - SEKTOR UTAMA"
    )

    print(
        "========================================"
    )

    sektor_utama_list = (
        mapping_sektor[
            "sektor_utama"
        ]
        .dropna()
        .astype(str)
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    for status in [
        "PMA",
        "PMDN",
    ]:

        for sektor_utama in (
            sektor_utama_list
        ):

            for lokasi in lokasi_list:

                print(
                    f"\nBuild SEKTOR UTAMA | "
                    f"{status} | "
                    f"{sektor_utama} | "
                    f"{lokasi}"
                )

                config = (
                    investasi_config.copy()
                )

                config.update({

                    "dataset":
                        "realisasi_investasi",

                    "status":
                        status,

                    "level":
                        "sektor_utama",

                    "sektor":
                        None,

                    "sektor_utama":
                        sektor_utama,

                    "lokasi":
                        lokasi,
                })

                final_df = (
                    build_bkpm_dataset(
                        full_df,
                        config,
                    )
                )

                if final_df.empty:

                    print(
                        "Tidak ada data, dilewati."
                    )

                    continue

                nama_data = (
                    etl.generate_nama_data(
                        status,
                        sektor_utama,
                        lokasi,
                        level="sektor_utama",
                    )
                )

                nama_file_sektor = (
                    sanitize_filename(
                        sektor_utama
                    )
                )

                filename = (
                    "bkpm_realisasi_"
                    f"{status.lower()}_"
                    f"{nama_file_sektor}_"
                    f"{lokasi.lower().replace(' ', '_')}"
                    ".csv"
                )

                duplicates = (
                    etl.check_duplicates(
                        final_df
                    )
                )

                if not duplicates.empty:

                    print(
                        "WARNING: ditemukan "
                        f"{len(duplicates)} "
                        "baris duplicate."
                    )

                etl.save_final_data(
                    final_df,
                    filename,
                    status,
                    sektor_utama,
                    lokasi,
                    level="sektor_utama",
                )

                print(
                    f"Nama data: {nama_data}"
                )

    # ========================================================
    # 9. TENAGA KERJA SEKTOR UTAMA
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "TENAGA KERJA - SEKTOR UTAMA"
    )

    print(
        "========================================"
    )

    for status in [
        "PMA",
        "PMDN",
    ]:

        for sektor_utama in (
            sektor_utama_list
        ):

            for lokasi in lokasi_list:

                print(
                    f"\nBuild TENAGA KERJA | "
                    f"{status} | "
                    f"{sektor_utama} | "
                    f"{lokasi}"
                )

                config = (
                    tenaga_kerja_config.copy()
                )

                config.update({

                    "dataset":
                        "tenaga_kerja",

                    "status":
                        status,

                    "level":
                        "sektor_utama",

                    "sektor":
                        None,

                    "sektor_utama":
                        sektor_utama,

                    "lokasi":
                        lokasi,
                })

                tenaga_kerja_df = (
                    build_bkpm_dataset(
                        full_df,
                        config,
                    )
                )

                if tenaga_kerja_df.empty:

                    print(
                        "Tidak ada data, dilewati."
                    )

                    continue

                nama_data = (
                    etl.generate_nama_data(
                        status,
                        sektor_utama,
                        lokasi,
                        jenis_data="tenaga_kerja",
                    )
                )

                nama_file_sektor = (
                    sanitize_filename(
                        sektor_utama
                    )
                )

                filename = (
                    "bkpm_jumlah_penyerapan_"
                    "tenaga_kerja_"
                    f"{status.lower()}_"
                    f"{nama_file_sektor}_"
                    f"{lokasi.lower().replace(' ', '_')}"
                    ".csv"
                )

                duplicates = (
                    etl.check_duplicates(
                        tenaga_kerja_df
                    )
                )

                if not duplicates.empty:

                    print(
                        "WARNING: ditemukan "
                        f"{len(duplicates)} "
                        "baris duplicate."
                    )

                etl.save_final_data(
                    tenaga_kerja_df,
                    filename,
                    status,
                    sektor_utama,
                    lokasi,
                    jenis_data="tenaga_kerja",
                    level="sektor_utama",
                )

                print(
                    f"Nama data: {nama_data}"
                )

    # ========================================================
    # 10. REALISASI INVESTASI SEKTOR BKPM
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "REALISASI INVESTASI - SEKTOR BKPM"
    )

    print(
        "========================================"
    )

    sektor_bkpm_list = (
        mapping_sektor[
            "sektor_bkpm"
        ]
        .dropna()
        .astype(str)
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    for status in [
        "PMA",
        "PMDN",
    ]:

        for sektor in (
            sektor_bkpm_list
        ):

            for lokasi in lokasi_list:

                print(
                    f"\nBuild SEKTOR BKPM | "
                    f"{status} | "
                    f"{sektor} | "
                    f"{lokasi}"
                )

                config = (
                    investasi_config.copy()
                )

                config.update({

                    "dataset":
                        "realisasi_investasi",

                    "status":
                        status,

                    "level":
                        "sektor_bkpm",

                    "sektor":
                        sektor,

                    "sektor_utama":
                        None,

                    "lokasi":
                        lokasi,
                })

                final_df = (
                    build_bkpm_dataset(
                        full_df,
                        config,
                    )
                )

                if final_df.empty:

                    print(
                        "Tidak ada data, dilewati."
                    )

                    continue

                nama_data = (
                    etl.generate_nama_data(
                        status,
                        sektor,
                        lokasi,
                        level="sektor_bkpm",
                    )
                )

                nama_file_sektor = (
                    sanitize_filename(
                        sektor
                    )
                )

                filename = (
                    "bkpm_realisasi_"
                    f"{status.lower()}_"
                    f"{nama_file_sektor}_"
                    f"{lokasi.lower().replace(' ', '_')}"
                    ".csv"
                )

                # ------------------------------------------------
                # Special case Kendari
                # ------------------------------------------------

                if (
                    filename
                    ==
                    "bkpm_realisasi_pmdn_"
                    "sektor_konstruksi_"
                    "kabupaten_kota.csv"
                ):

                    final_df = final_df[
                        ~final_df[
                            "kota"
                        ]
                        .astype(str)
                        .str.strip()
                        .isin([
                            "Kota Kendari",
                            "Kabupaten Kendari",
                        ])
                    ].copy()

                    print(
                        "Kendari tersisa:",
                        final_df[
                            final_df[
                                "kota"
                            ]
                            .astype(str)
                            .str.strip()
                            .isin([
                                "Kota Kendari",
                                "Kabupaten Kendari",
                            ])
                        ].shape[0],
                    )

                if final_df.empty:

                    print(
                        "Tidak ada data setelah "
                        "special cleaning, dilewati."
                    )

                    continue

                duplicates = (
                    etl.check_duplicates(
                        final_df
                    )
                )

                if not duplicates.empty:

                    print(
                        "WARNING: ditemukan "
                        f"{len(duplicates)} "
                        "baris duplicate."
                    )

                etl.save_final_data(
                    final_df,
                    filename,
                    status,
                    sektor,
                    lokasi,
                    level="sektor_bkpm",
                )

                print(
                    f"Nama data: {nama_data}"
                )

    # ========================================================
    # SELESAI
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "ETL SELESAI"
    )

    print(
        "========================================"
    )

if __name__ == "__main__":

    tahun_awal = int(
        input("Tahun awal [2010]: ") or 2010
    )

    tahun_akhir = int(
        input("Tahun akhir [2026]: ") or 2026
    )

    main(
        tahun_awal=tahun_awal,
        tahun_akhir=tahun_akhir,
    )