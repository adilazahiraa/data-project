import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
FINAL_DATA_DIR = PROJECT_ROOT / "final"

BASE_URL = "https://data.bkpm.go.id"
SEARCH_URL = f"{BASE_URL}/cari-data"

QUERY = "Data Realisasi Investasi Triwulan"

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

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

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

    filepath = RAW_DATA_DIR / filename

    df.to_parquet(
        filepath,
        index=False,
    )

    print(
        "Raw data disimpan:",
        filepath,
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

    df.to_csv(
        filepath,
        index=False,
        encoding="utf-8-sig",
    )

    print(
        "Final data disimpan:",
        filepath,
    )

    print(
        "Total rows:",
        len(df),
    )


def main():

    print(
        "========================================"
    )

    print(
        "BKPM AUTOMATED ETL"
    )

    print(
        "========================================"
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

        df = pd.DataFrame(rows)

        df = prepare_dataframe(df)

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
        ignore_index=True,
    )

    print(
        "\n========================================"
    )

    print(
        "TOTAL RAW DATA:",
        f"{len(full_df):,}",
    )

    print(
        "========================================"
    )

    tersier_provinsi = create_aggregation(
        full_df,
        status_penanaman_modal="PMA",
        sektor_utama="Sektor Tersier",
        group_by=[
            "periode",
            "provinsi",
        ],
    )

    if not tersier_provinsi.empty:

        save_final_data(
            tersier_provinsi,
            "bkpm_realisasi_pma_tersier_provinsi.csv",
        )

    konstruksi_provinsi = create_aggregation(
        full_df,
        status_penanaman_modal="PMA",
        nama_sektor="Konstruksi",
        group_by=[
            "periode",
            "provinsi",
        ],
    )

    if not konstruksi_provinsi.empty:

        save_final_data(
            konstruksi_provinsi,
            "bkpm_realisasi_pma_konstruksi_provinsi.csv",
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
    main()