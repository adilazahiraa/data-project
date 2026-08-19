import os
import re
import time
from pathlib import Path
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from minio import Minio


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
FINAL_DATA_DIR = PROJECT_ROOT / "final"

BASE_URL = "https://data.bkpm.go.id"
SEARCH_URL = f"{BASE_URL}/cari-data"

QUERY = "Data Realisasi Investasi Triwulan"

USE_EXISTING_RAW = False

load_dotenv(PROJECT_ROOT / ".env")

MINIO_BUCKET = "maganghub"

minio_client = Minio(
    f"{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)

def upload_to_minio(
    data,
    object_name,
    content_type="application/octet-stream",
):
    data_stream = BytesIO(data)

    minio_client.put_object(
        MINIO_BUCKET,
        object_name,
        data_stream,
        length=len(data),
        content_type=content_type,
    )

    print(f"MinIO upload: {object_name}")

def create_session():

    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "http://",
        adapter
    )

    session.mount(
        "https://",
        adapter
    )

    return session


session = create_session()

def get_dataset_links():

    datasets = []

    page = 1

    while True:

        print(
            f"\nMencari dataset - halaman {page}"
        )

        params = {
            "query": QUERY,
            "page": page,
        }

        response = session.get(
            SEARCH_URL,
            params=params,
            timeout=30,
        )

        print(
            "Status:",
            response.status_code
        )

        if response.status_code != 200:

            print(
                "Gagal mengambil halaman pencarian."
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

            href = link.get("href")

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

                title_element = title.find("h5")

                if title_element:

                    title_text = (
                        title_element.get_text(
                            strip=True
                        )
                    )

                else:

                    title_text = link.get_text(
                        strip=True
                    )

            else:

                title_text = link.get_text(
                    strip=True
                )

            if (
                "Data Realisasi Investasi Triwulan"
                not in title_text
            ):
                continue

            if url not in [
                item["url"]
                for item in datasets
            ]:

                datasets.append({
                    "judul": title_text,
                    "url": url,
                })

                new_links += 1

        print(
            "Dataset baru:",
            new_links
        )

        if new_links == 0:
            break

        page += 1

        if page > 100:

            print(
                "Berhenti karena batas halaman."
            )

            break

        time.sleep(0.5)

    return datasets


def get_parent_id(dataset_url):

    response = session.get(
        dataset_url,
        timeout=30,
    )

    if response.status_code != 200:

        print(
            "Gagal membuka:",
            dataset_url
        )

        return None

    html = response.text

    match = re.search(
        r'<input[^>]*value=["\']([^"\']+)["\'][^>]*name=["\']parent_id["\']',
        html,
    )

    if match:
        return match.group(1)

    return None


def get_data(parent_id):

    all_rows = []

    start = 0
    length = 10000

    while True:

        print(
            f"  Mengambil data: start={start}"
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

            response = session.get(
                f"{BASE_URL}/data",
                params=params,
                timeout=60,
            )

        except requests.exceptions.RequestException as e:

            print(
                "  ERROR:",
                e
            )

            return []

        if response.status_code != 200:

            print(
                "  Gagal mengambil data:",
                response.status_code,
            )

            return []

        try:

            result = response.json()

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
            f"Diterima batch: {len(rows)}"
        )

        if not rows:
            break

        all_rows.extend(rows)

        start += len(rows)

        if start >= total:
            break

        time.sleep(1)

    print(
        f"  TOTAL DATASET: {len(all_rows)}"
    )

    return all_rows

def save_raw_data(df, periode, index):
    safe_periode = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        periode,
    ).strip("_")

    filename = (
        f"bkpm_investasi_"
        f"{safe_periode}_"
        f"{index}.parquet"
    )

    # Convert dataframe -> parquet bytes
    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
    )

    data = buffer.getvalue()

    object_name = f"bkpm/raw/{filename}"

    upload_to_minio(
        data,
        object_name,
        "application/octet-stream",
    )

    print(
        f"Raw data disimpan ke MinIO: {object_name}"
    )

def prepare_dataframe(df):

    df = df.copy()

    df.columns = (
        df.columns
        .str.replace(
            "\ufeff",
            "",
            regex=False,
        )
    )

    numeric_columns = [
        "investasi_rp_juta",
        "investasi_us_ribu",
        "tki",
    ]

    for column in numeric_columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    return df

def save_processed_data(df, tahun_awal, tahun_akhir):
    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
    )

    data = buffer.getvalue()

    object_name = (
        f"bkpm/processed/"
        f"bkpm_investasi_processed_"
        f"{tahun_awal}_{tahun_akhir}.parquet"
    )

    upload_to_minio(
        data,
        object_name,
        "application/octet-stream",
    )

    print(
        f"Processed data disimpan ke MinIO: {object_name}"
    )

    print(
        f"Total processed rows: {len(df):,}"
    )

def create_aggregation(
    df,
    status_penanaman_modal=None,
    sektor_utama=None,
    nama_sektor=None,
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

    if sektor_utama:

        filtered_df = filtered_df[
            filtered_df[
                "sektor_utama"
            ]
            == sektor_utama
        ]

    if nama_sektor:

        filtered_df = filtered_df[
            filtered_df[
                "nama_sektor"
            ]
            == nama_sektor
        ]

    if filtered_df.empty:

        print(
            "Tidak ada data setelah filter."
        )

        return pd.DataFrame()

    if group_by is None:

        group_by = [
            "periode",
            "provinsi",
        ]

    result = (
        filtered_df
        .groupby(
            group_by,
            as_index=False,
        )
        .agg(
            investasi_rp_juta=(
                "investasi_rp_juta",
                "sum",
            ),
            investasi_us_ribu=(
                "investasi_us_ribu",
                "sum",
            ),
            tki=(
                "tki",
                "sum",
            ),
        )
    )

    return result


def save_final_data(df, filename):

    FINAL_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    filepath = FINAL_DATA_DIR / filename

    # Simpan CSV ke lokal
    df.to_csv(
        filepath,
        index=False,
        encoding="utf-8-sig",
    )

    print(
        "Final data disimpan:",
        filepath,
    )

    # Upload CSV ke MinIO
    csv_bytes = df.to_csv(
        index=False,
        encoding="utf-8-sig",
    ).encode("utf-8-sig")

    object_name = f"bkpm/final/{filename}"

    upload_to_minio(
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

def load_existing_raw_data():

    raw_files = list(
        RAW_DATA_DIR.glob("*.parquet")
    )

    print(
        f"Ditemukan {len(raw_files)} file raw."
    )

    if not raw_files:

        print(
            "Tidak ada file raw di data/raw."
        )

        return pd.DataFrame()

    all_dataframes = []

    for filepath in raw_files:

        print(
            "Membaca:",
            filepath.name
        )

        df = pd.read_parquet(
            filepath
        )

        df = prepare_dataframe(
            df
        )

        all_dataframes.append(
            df
        )

    full_df = pd.concat(
        all_dataframes,
        ignore_index=True
    )

    print(
        f"\nTotal raw rows: {len(full_df):,}"
    )

    return full_df

def periode_to_date(periode):
    """
    Mengubah periode triwulan menjadi tanggal akhir triwulan.

    2010 - Triwulan 1 -> 31-03-2010
    2010 - Triwulan 2 -> 30-06-2010
    2010 - Triwulan 3 -> 30-09-2010
    2010 - Triwulan 4 -> 31-12-2010
    """

    if pd.isna(periode):
        return pd.NA

    periode = str(periode).strip()

    tahun = periode[:4]

    if "Triwulan 1" in periode:
        return f"31-03-{tahun}"

    elif "Triwulan 2" in periode:
        return f"30-06-{tahun}"

    elif "Triwulan 3" in periode:
        return f"30-09-{tahun}"

    elif "Triwulan 4" in periode:
        return f"31-12-{tahun}"

    return pd.NA

def generate_nama_data(
    status_penanaman_modal,
    nama_sektor,
    lokasi,
):
    return (
        f"Realisasi Investasi "
        f"{status_penanaman_modal} "
        f"Sektor {nama_sektor} "
        f"Menurut {lokasi} "
        f"(Triwulan)"
    )

def add_nama_data(nama_data):
    nama_data_file = PROJECT_ROOT / "nama_data.csv"

    if nama_data_file.exists():
        df_nama = pd.read_csv(nama_data_file)
    else:
        df_nama = pd.DataFrame(columns=["nama_data"])

    if nama_data in df_nama["nama_data"].astype(str).values:
        print(f"Nama data sudah ada: {nama_data}")
        return

    new_row = pd.DataFrame({
        "nama_data": [nama_data],
    })

    df_nama = pd.concat(
        [df_nama, new_row],
        ignore_index=True,
    )

    df_nama.to_csv(
        nama_data_file,
        index=False,
        encoding="utf-8-sig",
    )

    print(f"Nama data ditambahkan: {nama_data}")


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

    if USE_EXISTING_RAW:

        print(
            "\nMODE: MENGGUNAKAN RAW DATA YANG SUDAH ADA"
        )

        full_df = load_existing_raw_data()

        if full_df.empty:

            return

    else:

        print(
            "\nMODE: SCRAPING DATA DARI BKPM"
        )

        datasets = get_dataset_links()

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

        for dataset in datasets:

            parent_id = get_parent_id(
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

            rows = get_data(
                parent_id
            )

            if not rows:

                failed_datasets.append(
                    dataset
                )

                continue

            df = pd.DataFrame(
                rows
            )

            df = prepare_dataframe(
                df
            )

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

            save_raw_data(
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

        full_df = pd.concat(
            all_dataframes,
            ignore_index=True
        )

        print(
            "\nTOTAL RAW DATA:",
            f"{len(full_df):,}"
        )

        # Filter tahun
        full_df = full_df[
            full_df["periode"]
            .astype(str)
            .str[:4]
            .astype(int)
            .between(tahun_awal, tahun_akhir)
        ].copy()

        # Simpan processed ke MinIO
        save_processed_data(
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

    pma_konstruksi_provinsi = create_aggregation(
        full_df,
        status_penanaman_modal="PMA",
        nama_sektor="Konstruksi",
        group_by=[
            "periode",
            "provinsi",
        ],
    )

    if not pma_konstruksi_provinsi.empty:

        pma_konstruksi_provinsi_final = pd.DataFrame({
            "kota": pd.NA,
            "provinsi": pma_konstruksi_provinsi["provinsi"],
            "negara": "Indonesia",
            "item": pd.NA,
            "satuan": "US$ ribu",
            "data_x": pma_konstruksi_provinsi["periode"].apply(
                periode_to_date
            ),
            "data_y": pma_konstruksi_provinsi["investasi_us_ribu"],
            "sumber": "BKPM",
            "nama_data_import": "https://data.bkpm.go.id/dataset?status=publik",
        })

        # Drop data_y = 0
        pma_konstruksi_provinsi_final = (
            pma_konstruksi_provinsi_final[
                pma_konstruksi_provinsi_final["data_y"].notna()
                & (pma_konstruksi_provinsi_final["data_y"] != 0)
            ]
            .copy()
        )

        save_final_data(
            pma_konstruksi_provinsi_final,
            "bkpm_realisasi_pma_konstruksi_provinsi.csv",
        )

        add_nama_data(
            generate_nama_data(
                "PMA",
                "Konstruksi",
                "Provinsi",
            )
        )        

    pmdn_konstruksi_provinsi = create_aggregation(
        full_df,
        status_penanaman_modal="PMDN",
        nama_sektor="Konstruksi",
        group_by=[
            "periode",
            "provinsi",
        ],
    )

    if not pmdn_konstruksi_provinsi.empty:

        pmdn_konstruksi_provinsi_final = pd.DataFrame({
            "kota": pd.NA,
            "provinsi": pmdn_konstruksi_provinsi["provinsi"],
            "negara": "Indonesia",
            "item": pd.NA,
            "satuan": "Rp juta",
            "data_x": pmdn_konstruksi_provinsi["periode"].apply(
                periode_to_date
            ),
            "data_y": pmdn_konstruksi_provinsi["investasi_rp_juta"],
            "sumber": "BKPM",
            "nama_data_import": "https://data.bkpm.go.id/dataset?status=publik",
        })

        # Drop data_y = 0
        pmdn_konstruksi_provinsi_final = (
            pmdn_konstruksi_provinsi_final[
                pmdn_konstruksi_provinsi_final["data_y"].notna()
                & (pmdn_konstruksi_provinsi_final["data_y"] != 0)
            ]
            .copy()
        )

        save_final_data(
            pmdn_konstruksi_provinsi_final,
            "bkpm_realisasi_pmdn_konstruksi_provinsi.csv",
        )

        add_nama_data(
            generate_nama_data(
                "PMDN",
                "Konstruksi",
                "Provinsi",
            )
        )        


    pma_konstruksi_kabupaten_kota = create_aggregation(
        full_df,
        status_penanaman_modal="PMA",
        nama_sektor="Konstruksi",
        group_by=[
            "periode",
            "kabupaten_kota",
        ],
    )

    if not pma_konstruksi_kabupaten_kota.empty:

        pma_konstruksi_kabupaten_kota_final = pd.DataFrame({
            "kota": pma_konstruksi_kabupaten_kota["kabupaten_kota"],
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "item": pd.NA,
            "satuan": "US$ ribu",
            "data_x": pma_konstruksi_kabupaten_kota["periode"].apply(
                periode_to_date
            ),
            "data_y": pma_konstruksi_kabupaten_kota["investasi_us_ribu"],
            "sumber": "BKPM",
            "nama_data_import": "https://data.bkpm.go.id/dataset?status=publik",
        })

        pma_konstruksi_kabupaten_kota_final = (
            pma_konstruksi_kabupaten_kota_final[
                pma_konstruksi_kabupaten_kota_final["data_y"].notna()
                & (pma_konstruksi_kabupaten_kota_final["data_y"] != 0)
            ]
            .copy()
        )

        pma_konstruksi_kabupaten_kota_final["kota"] = (
            pma_konstruksi_kabupaten_kota_final["kota"]
            .replace({
                "Kabupaten Kendari": "Kota Kendari"
            })
        )

        save_final_data(
            pma_konstruksi_kabupaten_kota_final,
            "bkpm_realisasi_pma_konstruksi_kabupaten_kota.csv",
        )

        add_nama_data(
            generate_nama_data(
                "PMA",
                "Konstruksi",
                "Kabupaten Kota",
            )
        )

    pmdn_konstruksi_kabupaten_kota = create_aggregation(
        full_df,
        status_penanaman_modal="PMDN",
        nama_sektor="Konstruksi",
        group_by=[
            "periode",
            "kabupaten_kota",
        ],
    )

    if not pmdn_konstruksi_kabupaten_kota.empty:

        pmdn_konstruksi_kabupaten_kota_final = pd.DataFrame({
            "kota": pmdn_konstruksi_kabupaten_kota["kabupaten_kota"],
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "item": pd.NA,
            "satuan": "Rp juta",
            "data_x": pmdn_konstruksi_kabupaten_kota["periode"].apply(
                periode_to_date
            ),
            "data_y": pmdn_konstruksi_kabupaten_kota["investasi_rp_juta"],
            "sumber": "BKPM",
            "nama_data_import": "https://data.bkpm.go.id/dataset?status=publik",
        })

        pmdn_konstruksi_kabupaten_kota_final = (
            pmdn_konstruksi_kabupaten_kota_final[
                pmdn_konstruksi_kabupaten_kota_final["data_y"].notna()
                & (pmdn_konstruksi_kabupaten_kota_final["data_y"] != 0)
            ]
            .copy()
        )

        pmdn_konstruksi_kabupaten_kota_final["kota"] = (
            pmdn_konstruksi_kabupaten_kota_final["kota"]
            .replace({
                "Kabupaten Kendari": "Kota Kendari"
            })
        )

        save_final_data(
            pmdn_konstruksi_kabupaten_kota_final,
            "bkpm_realisasi_pmdn_konstruksi_kabupaten_kota.csv",
        )

        add_nama_data(
            generate_nama_data(
                "PMDN",
                "Konstruksi",
                "Kabupaten Kota",
            )
        )        

    pma_tanaman_pangan_kabupaten_kota = create_aggregation(
        full_df,
        status_penanaman_modal="PMA",
        nama_sektor="Tanaman Pangan, Perkebunan, dan Peternakan",
        group_by=[
            "periode",
            "kabupaten_kota",
        ],
    )

    if not pma_tanaman_pangan_kabupaten_kota.empty:

        pma_tanaman_pangan_kabupaten_kota_final = pd.DataFrame({
            "kota": pma_tanaman_pangan_kabupaten_kota["kabupaten_kota"],
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "item": pd.NA,
            "satuan": "US$ ribu",
            "data_x": pma_tanaman_pangan_kabupaten_kota["periode"].apply(
                periode_to_date
            ),
            "data_y": pma_tanaman_pangan_kabupaten_kota["investasi_us_ribu"],
            "sumber": "BKPM",
            "nama_data_import": "https://data.bkpm.go.id/dataset?status=publik",
        })

        pma_tanaman_pangan_kabupaten_kota_final = (
            pma_tanaman_pangan_kabupaten_kota_final[
                pma_tanaman_pangan_kabupaten_kota_final["data_y"].notna()
                & (
                    pma_tanaman_pangan_kabupaten_kota_final["data_y"] != 0
                )
            ]
            .copy()
        )

        pma_tanaman_pangan_kabupaten_kota_final["kota"] = (
            pma_tanaman_pangan_kabupaten_kota_final["kota"]
            .replace({
                "Kabupaten Kendari": "Kota Kendari"
            })
        )

        save_final_data(
            pma_tanaman_pangan_kabupaten_kota_final,
            "bkpm_realisasi_pma_tanaman_pangan_perkebunan_peternakan_kabupaten_kota.csv",
        )

        add_nama_data(
            generate_nama_data(
                "PMA",
                "Tanaman Pangan, Perkebunan, dan Peternakan",
                "Kabupaten Kota",
            )
        )

    pma_kehutanan_kabupaten_kota = create_aggregation(
        full_df,
        status_penanaman_modal="PMA",
        nama_sektor="Kehutanan",
        group_by=[
            "periode",
            "kabupaten_kota",
        ],
    )

    if not pma_kehutanan_kabupaten_kota.empty:

        pma_kehutanan_kabupaten_kota_final = pd.DataFrame({
            "kota": pma_kehutanan_kabupaten_kota["kabupaten_kota"],
            "provinsi": pd.NA,
            "negara": "Indonesia",
            "item": pd.NA,
            "satuan": "US$ ribu",
            "data_x": pma_kehutanan_kabupaten_kota["periode"].apply(
                periode_to_date
            ),
            "data_y": pma_kehutanan_kabupaten_kota["investasi_us_ribu"],
            "sumber": "BKPM",
            "nama_data_import": "https://data.bkpm.go.id/dataset?status=publik",
        })

        pma_kehutanan_kabupaten_kota_final = (
            pma_kehutanan_kabupaten_kota_final[
                pma_kehutanan_kabupaten_kota_final["data_y"].notna()
                & (
                    pma_kehutanan_kabupaten_kota_final["data_y"] != 0
                )
            ]
            .copy()
        )

        pma_kehutanan_kabupaten_kota_final["kota"] = (
            pma_kehutanan_kabupaten_kota_final["kota"]
            .replace({
                "Kabupaten Kendari": "Kota Kendari"
            })
        )

        save_final_data(
            pma_kehutanan_kabupaten_kota_final,
            "bkpm_realisasi_pma_kehutanan_kabupaten_kota.csv",
        )

        add_nama_data(
            generate_nama_data(
                "PMA",
                "Kehutanan",
                "Kabupaten Kota",
            )
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