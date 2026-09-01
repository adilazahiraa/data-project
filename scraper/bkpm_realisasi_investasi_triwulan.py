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

USE_EXISTING_RAW = True

MINIO_BUCKET = "maganghub"


RAW_DATA_DIR = (
    PROJECT_ROOT / "data" / "raw" / "bkpm"
)

FINAL_DATA_DIR = (
    PROJECT_ROOT / "final"
)


GRANULARITIES = {
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


        # GENERIC AGGREGATION
        # DITANGANI UTILS

        result = aggregate_dataframe(
            filtered_df,
            group_by=group_by,
            aggregations={
                "investasi_rp_juta":
                    "sum",

                "investasi_us_ribu":
                    "sum",

                "tki":
                    "sum",
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
        sektor_utama=None,
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


    def load_mapping_sektor(self):

        mapping_file = (
            PROJECT_ROOT
            / "Mapping Sektor.xlsx"
        )


        mapping_df = pd.read_excel(
            mapping_file
        )


        mapping_df = (
            mapping_df[
                ["sektor_bkpm"]
            ]
            .dropna()
            .drop_duplicates()
            .reset_index(drop=True)
        )


        return mapping_df

def fetch(params=None):
    params = params or {}

    tahun_awal = int(
        params.get("tahun_awal", 2010)
    )

    tahun_akhir = int(
        params.get("tahun_akhir", 2026)
    )

    scraper = BKPMScraper()

    if USE_EXISTING_RAW:

        full_df = (
            scraper.load_existing_raw_data()
        )

        if full_df.empty:
            return pd.DataFrame()

    else:

        datasets = (
            scraper.get_dataset_links()
        )

        if not datasets:
            return pd.DataFrame()

        all_dataframes = []

        for i, dataset in enumerate(
            datasets,
            start=1,
        ):

            parent_id = (
                scraper.get_parent_id(
                    dataset["url"]
                )
            )

            if not parent_id:
                continue

            rows = scraper.get_data(
                parent_id
            )

            if not rows:
                continue

            df = pd.DataFrame(rows)

            df = scraper.prepare_dataframe(
                df
            )

            all_dataframes.append(df)

        if not all_dataframes:
            return pd.DataFrame()

        full_df = concat_dataframes(
            all_dataframes
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

    return full_df

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

    if USE_EXISTING_RAW:

        print(
            "\nMODE: MENGGUNAKAN RAW DATA YANG SUDAH ADA"
        )

        full_df = etl.scraper.load_existing_raw_data()

        if full_df.empty:
            return

    else:

        print(
            "\nMODE: SCRAPING DATA DARI BKPM"
        )

        datasets = etl.scraper.get_dataset_links()

        print(
            "\nTotal dataset ditemukan:",
            len(datasets)
        )

        if not datasets:
            print(
                "Tidak ada dataset ditemukan."
            )
            return

        dataset_info = []

        for dataset in datasets:

            parent_id = etl.scraper.get_parent_id(
                dataset["url"]
            )

            dataset_info.append({
                "judul": dataset["judul"],
                "url": dataset["url"],
                "parent_id": parent_id,
            })

            time.sleep(1)

        all_dataframes = []

        failed_datasets = []

        for i, dataset in enumerate(
            dataset_info,
            start=1,
        ):

            print(
                f"\n[{i}/{len(dataset_info)}]",
                dataset["judul"],
            )

            parent_id = dataset["parent_id"]

            if not parent_id:

                failed_datasets.append(
                    dataset
                )

                continue

            rows = etl.scraper.get_data(
                parent_id
            )

            if not rows:

                failed_datasets.append(
                    dataset
                )

                continue

            df = pd.DataFrame(rows)

            df = etl.scraper.prepare_dataframe(
                df
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

            time.sleep(2)

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
            f"{len(full_df):,}"
        )

    full_df = full_df[
        full_df["periode"]
        .astype(str)
        .str[:4]
        .astype(int)
        .between(
            tahun_awal,
            tahun_akhir
        )
    ].copy()

    etl.scraper.save_processed_data(
        full_df,
        tahun_awal,
        tahun_akhir,
    )

    print(
        "\n========================================"
    )

    print(
        "MULAI TRANSFORM"
    )

    print(
        "========================================"
    )

    mapping_sektor = etl.load_mapping_sektor()

    print("\nKolom full_df:")
    print(full_df.columns.tolist())

    print("\nKolom mapping:")
    print(mapping_sektor.columns.tolist())

    full_df = full_df.merge(
        mapping_sektor,
        left_on="nama_sektor",
        right_on="sektor_bkpm",
        how="left",
    )

    print(
        f"Total mapping sektor: {len(mapping_sektor)}"
    )

    for status in ["PMA", "PMDN"]:

        for lokasi, group_by in GRANULARITIES.items():

            print(
                f"\nAgregasi TOTAL SEKTOR | "
                f"{status} | {lokasi}"
            )

            hasil = etl.create_aggregation(
                full_df,
                status_penanaman_modal=status,
                group_by=group_by,
            )

            if hasil.empty:
                print("Tidak ada data, dilewati.")
                continue

            if status == "PMA":
                satuan = "US$"
                data_y = (
                    hasil["investasi_us_ribu"]
                    .mul(1000)
                    .round(2)
                )
            else:
                satuan = "Rp juta"
                data_y = (
                    hasil["investasi_rp_juta"].round(2)
                )

            if lokasi == "Provinsi":

                final_df = pd.DataFrame({
                    "kota": pd.NA,
                    "provinsi": hasil["provinsi"],
                    "negara": "Indonesia",
                    "item": pd.NA,
                    "satuan": satuan,
                    "data_x": (
                        hasil["periode"]
                        .apply(format_quarter_end)
                    ),
                    "data_y": data_y,
                    "sumber": "BKPM",
                    "nama_data_import":
                        "https://data.bkpm.go.id/dataset?status=publik",
                })

            elif lokasi == "Kabupaten Kota":

                final_df = pd.DataFrame({
                    "kota": hasil["kabupaten_kota"],
                    "provinsi": pd.NA,
                    "negara": "Indonesia",
                    "item": pd.NA,
                    "satuan": satuan,
                    "data_x": (
                        hasil["periode"]
                        .apply(format_quarter_end)
                    ),
                    "data_y": data_y,
                    "sumber": "BKPM",
                    "nama_data_import":
                        "https://data.bkpm.go.id/dataset?status=publik",
                })

            elif lokasi == "Negara":

                final_df = pd.DataFrame({
                    "kota": pd.NA,
                    "provinsi": pd.NA,
                    "negara": hasil["negara"],
                    "item": pd.NA,
                    "satuan": satuan,
                    "data_x": (
                        hasil["periode"]
                        .apply(format_quarter_end)
                    ),
                    "data_y": data_y,
                    "sumber": "BKPM",
                    "nama_data_import":
                        "https://data.bkpm.go.id/dataset?status=publik",
                })

            else:
                print(
                    f"Granularitas belum didukung: {lokasi}"
                )
                continue

            final_df = filter_nonzero(
                final_df,
                "data_y",
            )

            if final_df.empty:
                print(
                    "Tidak ada data setelah cleaning, "
                    "dilewati."
                )
                continue

            nama_data = etl.generate_nama_data(
                status,
                None,
                lokasi,
                level="total",
            )

            nama_file_status = status.lower()

            nama_file_lokasi = (
                lokasi.lower()
                .replace(" ", "_")
            )

            filename = (
                f"bkpm_realisasi_"
                f"{nama_file_status}_"
                f"total_sektor_"
                f"{nama_file_lokasi}.csv"
            )

            duplicates = etl.check_duplicates(
                final_df
            )

            if not duplicates.empty:
                print(
                    f"WARNING: ditemukan "
                    f"{len(duplicates)} baris duplicate."
                )

            etl.save_final_data(
                final_df,
                filename,
                status,
                None,
                lokasi,
            )

            print(
                f"Nama data: {nama_data}"
            )

    sektor_utama_list = (
        full_df["sektor_utama"]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    for status in ["PMA", "PMDN"]:

        for sektor_utama in sektor_utama_list:

            for lokasi, group_by in GRANULARITIES.items():

                print(
                    f"\nAgregasi SEKTOR UTAMA | "
                    f"{status} | "
                    f"{sektor_utama} | "
                    f"{lokasi}"
                )

                hasil = etl.create_aggregation(
                    full_df,
                    status_penanaman_modal=status,
                    sektor_utama=sektor_utama,
                    group_by=group_by,
                )

                if hasil.empty:
                    print("Tidak ada data, dilewati.")
                    continue

                if status == "PMA":
                    satuan = "US$"
                    data_y = (
                        hasil["investasi_us_ribu"]
                        .mul(1000)
                        .round(2)
                    )

                else:

                    satuan = "Rp juta"

                    data_y = (
                        hasil["investasi_rp_juta"].round(2)
                    )

                if lokasi == "Provinsi":

                    final_df = pd.DataFrame({
                        "kota": pd.NA,
                        "provinsi": hasil["provinsi"],
                        "negara": "Indonesia",
                        "item": pd.NA,
                        "satuan": satuan,
                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),
                        "data_y": data_y,
                        "sumber": "BKPM",
                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })


                elif lokasi == "Kabupaten Kota":

                    final_df = pd.DataFrame({
                        "kota": hasil["kabupaten_kota"],
                        "provinsi": pd.NA,
                        "negara": "Indonesia",
                        "item": pd.NA,
                        "satuan": satuan,
                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),
                        "data_y": data_y,
                        "sumber": "BKPM",
                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })


                elif lokasi == "Negara":

                    final_df = pd.DataFrame({
                        "kota": pd.NA,
                        "provinsi": pd.NA,
                        "negara": hasil["negara"],
                        "item": pd.NA,
                        "satuan": satuan,
                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),
                        "data_y": data_y,
                        "sumber": "BKPM",
                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })


                else:

                    print(
                        f"Granularitas belum didukung: {lokasi}"
                    )
                    continue

                final_df = filter_not_null(
                    final_df,
                    "data_y",
                )

                if final_df.empty:

                    print(
                        "Tidak ada data setelah cleaning, "
                        "dilewati."
                    )

                    continue

                nama_data = etl.generate_nama_data(
                    status,
                    sektor_utama,
                    lokasi,
                    level="sektor_utama",
                )

                nama_file_sektor_utama = sanitize_filename(
                    sektor_utama
                )

                nama_file_status = (
                    status.lower()
                )

                nama_file_lokasi = (
                    lokasi.lower()
                    .replace(" ", "_")
                )

                filename = (
                    f"bkpm_realisasi_"
                    f"{nama_file_status}_"
                    f"{nama_file_sektor_utama}_"
                    f"{nama_file_lokasi}.csv"
                )

                duplicates = etl.check_duplicates(final_df)

                if not duplicates.empty:
                    print(
                        f"WARNING: ditemukan "
                        f"{len(duplicates)} baris duplicate."
                    )

                etl.save_final_data(
                    final_df,
                    filename,
                    status,
                    sektor_utama,
                    lokasi,
                )

                print(
                    f"Nama data: {nama_data}"
                )


                if lokasi == "Provinsi":

                    tenaga_kerja_df = pd.DataFrame({
                        "kota": pd.NA,

                        "provinsi": hasil["provinsi"],

                        "negara": "Indonesia",

                        "item": pd.NA,

                        "satuan": "Orang",

                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),

                        "data_y": hasil["tki"].round(2),

                        "sumber": "BKPM",

                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })


                elif lokasi == "Kabupaten Kota":

                    tenaga_kerja_df = pd.DataFrame({
                        "kota": hasil["kabupaten_kota"],

                        "provinsi": pd.NA,

                        "negara": "Indonesia",

                        "item": pd.NA,

                        "satuan": "Orang",

                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),

                        "data_y": hasil["tki"].round(2),

                        "sumber": "BKPM",

                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })


                elif lokasi == "Negara":

                    tenaga_kerja_df = pd.DataFrame({
                        "kota": pd.NA,

                        "provinsi": pd.NA,

                        "negara": hasil["negara"],

                        "item": pd.NA,

                        "satuan": "Orang",

                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),

                        "data_y": hasil["tki"].round(2),

                        "sumber": "BKPM",

                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })


                else:

                    print(
                        f"Granularitas belum didukung: {lokasi}"
                    )

                    continue


                # Cleaning
                tenaga_kerja_df = (
                    tenaga_kerja_df[
                        tenaga_kerja_df["data_y"].notna()
                        & (
                            tenaga_kerja_df["data_y"] != 0
                        )
                    ]
                    .copy()
                )


                if not tenaga_kerja_df.empty:

                    nama_file_tki_sektor_utama = sanitize_filename(
                        sektor_utama
                    )

                    filename_tki = (
                        f"bkpm_jumlah_penyerapan_"
                        f"tenaga_kerja_"
                        f"{status.lower()}_"
                        f"{nama_file_tki_sektor_utama}_"
                        f"{lokasi.lower().replace(' ', '_')}.csv"
                    )

                    duplicates = etl.check_duplicates(
                        tenaga_kerja_df
                    )

                    if not duplicates.empty:
                        print(
                            f"WARNING: ditemukan "
                            f"{len(duplicates)} baris duplicate."
                        )

                    etl.save_final_data(
                        tenaga_kerja_df,
                        filename_tki,
                        status,
                        sektor_utama,
                        lokasi,
                        jenis_data="tenaga_kerja",
                    )

                    nama_data_tenaga_kerja = etl.generate_nama_data(
                        status,
                        sektor_utama,
                        lokasi,
                        "tenaga_kerja", 
                    )

                    print(
                        f"Nama data: {nama_data_tenaga_kerja}"
                    )

    for status in ["PMA", "PMDN"]:

        for _, mapping in mapping_sektor.iterrows():

            sektor = mapping["sektor_bkpm"]

            for lokasi, group_by in GRANULARITIES.items():

                print(
                    f"\nAgregasi SEKTOR BKPM | "
                    f"{status} | "
                    f"{sektor} | "
                    f"{lokasi}"
                )

                hasil = etl.create_aggregation(
                    full_df,
                    status_penanaman_modal=status,
                    nama_sektor=sektor,
                    group_by=group_by,
                )

                if hasil.empty:
                    print("Tidak ada data, dilewati.")
                    continue

                if status == "PMA":
                    satuan = "US$"
                    data_y = (
                        hasil["investasi_us_ribu"]
                        .mul(1000)
                        .round(2)
                    )
                else:
                    satuan = "Rp juta"
                    data_y = (
                        hasil["investasi_rp_juta"].round(2)
                    )

                if lokasi == "Provinsi":

                    final_df = pd.DataFrame({
                        "kota": pd.NA,
                        "provinsi": hasil["provinsi"],
                        "negara": "Indonesia",
                        "item": pd.NA,
                        "satuan": satuan,
                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),
                        "data_y": data_y,
                        "sumber": "BKPM",
                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })

                elif lokasi == "Kabupaten Kota":

                    final_df = pd.DataFrame({
                        "kota": hasil["kabupaten_kota"],
                        "provinsi": pd.NA,
                        "negara": "Indonesia",
                        "item": pd.NA,
                        "satuan": satuan,
                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),
                        "data_y": data_y,
                        "sumber": "BKPM",
                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })

                elif lokasi == "Negara":

                    final_df = pd.DataFrame({
                        "kota": pd.NA,
                        "provinsi": pd.NA,
                        "negara": hasil["negara"],
                        "item": pd.NA,
                        "satuan": satuan,
                        "data_x": (
                            hasil["periode"]
                            .apply(format_quarter_end)
                        ),
                        "data_y": data_y,
                        "sumber": "BKPM",
                        "nama_data_import":
                            "https://data.bkpm.go.id/dataset?status=publik",
                    })

                else:
                    print(
                        f"Granularitas belum didukung: {lokasi}"
                    )
                    continue

                final_df = filter_not_null(
                    final_df,
                    "data_y",
                )

                if final_df.empty:

                    print(
                        "Tidak ada data setelah cleaning, "
                        "dilewati."
                    )
                    continue

                nama_data = etl.generate_nama_data(
                    status,
                    sektor,
                    lokasi,
                    level="sektor_bkpm",
                )

                nama_file_sektor = sanitize_filename(
                    sektor
                )

                nama_file_status = status.lower()

                nama_file_lokasi = (
                    lokasi.lower()
                    .replace(" ", "_")
                )

                filename = (
                    f"bkpm_realisasi_"
                    f"{nama_file_status}_"
                    f"{nama_file_sektor}_"
                    f"{nama_file_lokasi}.csv"
                )

                duplicates = etl.check_duplicates(
                    final_df
                )

                if not duplicates.empty:
                    print(
                        f"WARNING: ditemukan "
                        f"{len(duplicates)} baris duplicate."
                    )

                if filename == "bkpm_realisasi_pmdn_sektor_konstruksi_kabupaten_kota.csv":
                    final_df = final_df[
                        ~final_df["kota"].astype(str).str.strip().isin([
                            "Kota Kendari",
                            "Kabupaten Kendari",
                        ])
                    ].copy()

                    print(
                        "Kendari tersisa:",
                        final_df[
                            final_df["kota"].astype(str).str.strip().isin([
                                "Kota Kendari",
                                "Kabupaten Kendari",
                            ])
                        ].shape[0]
                    )

                etl.save_final_data(
                    final_df,
                    filename,
                    status,
                    sektor,
                    lokasi,
                )

                print(
                    f"Nama data: {nama_data}"
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