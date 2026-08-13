import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

BASE_URL = "https://data.bkpm.go.id"
SEARCH_URL = f"{BASE_URL}/cari-data"

QUERY = "Data Realisasi Investasi Triwulan"

session = requests.Session()

retry_strategy = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

adapter = HTTPAdapter(
    max_retries=retry_strategy
)

session.mount("http://", adapter)
session.mount("https://", adapter)

def get_dataset_links():
    datasets = []
    page = 1

    while True:

        print(f"\nMencari dataset - halaman {page}")

        params = {
            "query": QUERY,
            "page": page,
        }

        try:
            response = session.get(
                SEARCH_URL,
                params=params,
                timeout=30,
            )

        except requests.exceptions.RequestException as e:
            print("ERROR:", e)
            break

        print("Status:", response.status_code)

        if response.status_code != 200:
            print("Gagal mengambil halaman pencarian.")
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

            url = urljoin(
                BASE_URL,
                href,
            )

            title = link.find_parent(
                class_="card-content"
            )

            if title:

                title_element = title.find("h5")

                if title_element:
                    title_text = title_element.get_text(
                        strip=True
                    )
                else:
                    title_text = link.get_text(
                        strip=True
                    )

            else:
                title_text = link.get_text(
                    strip=True
                )

            if "Data Realisasi Investasi Triwulan" not in title_text:
                continue

            if "Triwulan II Tahun 2026" in title_text:
                print("\n!!! Q2 2026 DITEMUKAN !!!")
                print("Judul :", title_text)
                print("URL   :", url)

            if url not in [x["url"] for x in datasets]:

                datasets.append({
                    "judul": title_text,
                    "url": url,
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
                "Berhenti karena batas halaman."
            )
            break

        time.sleep(0.5)

    return datasets

def get_parent_id(dataset_url):

    try:
        response = session.get(
            dataset_url,
            timeout=30,
        )

    except requests.exceptions.RequestException as e:
        print("ERROR:", e)
        return None

    if response.status_code != 200:

        print(
            "Gagal membuka:",
            dataset_url,
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

            "dataset_detail_parent_id": parent_id,
        }

        # Parameter DataTables
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
                e,
            )

            print(
                "  Dataset ini dilewati."
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

            print(
                "  Dataset ini dilewati."
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


# ============================================================
# CREATE SAFE FILE NAME
# ============================================================

def create_safe_filename(title):

    filename = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        title,
    ).strip("_").lower()

    return filename

def main():

    print(
        "========================================"
    )

    print(
        "BKPM AUTOMATED SCRAPER"
    )

    print(
        "========================================"
    )

    # Pastikan folder raw tersedia
    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    datasets = get_dataset_links()

    print(
        "\n========================================"
    )

    print(
        "TOTAL DATASET DITEMUKAN:",
        len(datasets),
    )

    print(
        "========================================"
    )

    if not datasets:

        print(
            "Tidak ada dataset ditemukan."
        )

        return

    dataset_info = []

    for i, dataset in enumerate(
        datasets,
        start=1,
    ):

        print(
            f"\n[{i}/{len(datasets)}]",
            dataset["judul"],
        )

        parent_id = get_parent_id(
            dataset["url"]
        )

        print(
            "Parent ID:",
            parent_id,
        )

        dataset_info.append({
            "judul": dataset["judul"],
            "url": dataset["url"],
            "parent_id": parent_id,
        })

        time.sleep(1)

    dataset_df = pd.DataFrame(
        dataset_info
    )

    dataset_list_path = (
        RAW_DATA_DIR /
        "bkpm_dataset_list.csv"
    )

    dataset_df.to_csv(
        dataset_list_path,
        index=False,
        encoding="utf-8-sig",
    )

    print(
        "\nDaftar dataset disimpan:",
        dataset_list_path,
    )


    failed_datasets = []

    print(
        "\n========================================"
    )

    print(
        "MULAI SCRAPING RAW DATA"
    )

    print(
        "========================================"
    )

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

            print(
                "Parent ID tidak ditemukan."
            )

            failed_datasets.append({
                "judul": dataset["judul"],
                "url": dataset["url"],
                "parent_id": None,
            })

            continue

        rows = get_data(
            parent_id
        )

        if not rows:

            print(
                "Dataset gagal diambil."
            )

            failed_datasets.append({
                "judul": dataset["judul"],
                "url": dataset["url"],
                "parent_id": parent_id,
            })

            continue

        df = pd.DataFrame(rows)

        df.columns = df.columns.str.replace(
            "\ufeff",
            "",
            regex=False,
        )

        safe_title = create_safe_filename(
            dataset["judul"]
        )

        filename = (
            f"{safe_title}.parquet"
        )

        filepath = (
            RAW_DATA_DIR /
            filename
        )

        df.to_parquet(
            filepath,
            index=False,
        )

        print(
            "Berhasil disimpan:",
            filepath,
        )

        print(
            "Rows:",
            len(df),
        )

        time.sleep(2)

    print(
        "\n========================================"
    )

    print(
        "SCRAPING SELESAI"
    )

    print(
        "========================================"
    )

    print(
        "Dataset berhasil:",
        len(dataset_info) - len(failed_datasets),
    )

    print(
        "Dataset gagal:",
        len(failed_datasets),
    )

    if failed_datasets:

        print(
            "\nDataset yang gagal:"
        )

        for item in failed_datasets:

            print(
                "-",
                item["judul"],
                "| Parent ID:",
                item["parent_id"],
            )

        failed_df = pd.DataFrame(
            failed_datasets
        )

        failed_path = (
            RAW_DATA_DIR /
            "bkpm_failed_datasets.csv"
        )

        failed_df.to_csv(
            failed_path,
            index=False,
            encoding="utf-8-sig",
        )

        print(
            "\nDaftar dataset gagal disimpan:",
            failed_path,
        )

if __name__ == "__main__":
    main()