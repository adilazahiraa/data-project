from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
FINAL_DATA_DIR = PROJECT_ROOT / "final"

OUTPUT_FILE = (
    FINAL_DATA_DIR
    / "bkpm_realisasi_pma_tersier_provinsi.csv"
)

def load_raw_data():

    files = sorted(
        RAW_DATA_DIR.glob("*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            "Tidak ditemukan file .parquet di data/raw/"
        )

    print(
        f"Ditemukan {len(files)} file parquet."
    )

    dataframes = []

    for file in files:

        print(
            f"Membaca: {file.name}"
        )

        df = pd.read_parquet(file)

        dataframes.append(df)

    combined_df = pd.concat(
        dataframes,
        ignore_index=True
    )

    print(
        f"\nTotal rows: {len(combined_df):,}"
    )

    print(
        f"Total columns: {len(combined_df.columns)}"
    )

    return combined_df

def check_columns(df):

    print("\nKolom yang tersedia:")

    for column in df.columns:
        print(f"- {column}")


def create_aggregation(df):

    # --------------------------------------------------------
    # FILTER
    # --------------------------------------------------------

    filtered_df = df[
        (df["status_penanaman_modal"] == "PMA")
        &
        (df["sektor_utama"] == "Sektor Tersier")
    ].copy()

    print(
        "\nRows setelah filter PMA + Sektor Tersier:",
        f"{len(filtered_df):,}"
    )

    if filtered_df.empty:

        raise ValueError(
            "Tidak ada data setelah filter."
        )

    # --------------------------------------------------------
    # CONVERT MEASURES TO NUMERIC
    # --------------------------------------------------------

    numeric_columns = [
        "investasi_rp_juta",
        "investasi_us_ribu",
        "tki",
    ]

    for column in numeric_columns:

        filtered_df[column] = pd.to_numeric(
            filtered_df[column],
            errors="coerce"
        )

    # Cek hasil konversi
    print("\nTipe data setelah konversi:")

    print(
        filtered_df[
            numeric_columns
        ].dtypes
    )

    # --------------------------------------------------------
    # AGGREGATION
    # --------------------------------------------------------

    result = (
        filtered_df
        .groupby(
            [
                "periode",
                "provinsi",
            ],
            as_index=False
        )
        .agg(
            investasi_rp_juta=(
                "investasi_rp_juta",
                "sum"
            ),
            investasi_us_ribu=(
                "investasi_us_ribu",
                "sum"
            ),
            tki=(
                "tki",
                "sum"
            ),
        )
    )

    # --------------------------------------------------------
    # SORT
    # --------------------------------------------------------

    result = (
        result
        .sort_values(
            [
                "periode",
                "provinsi",
            ]
        )
        .reset_index(drop=True)
    )

    return result

def save_result(df):

    FINAL_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        "\n========================================"
    )

    print(
        "HASIL AGREGASI BERHASIL DISIMPAN"
    )

    print(
        "========================================"
    )

    print(
        OUTPUT_FILE
    )

    print(
        f"Total baris: {len(df):,}"
    )

def main():

    print(
        "========================================"
    )

    print(
        "BKPM"
    )

    print(
        "REALISASI PMA SEKTOR TERSIER"
    )

    print(
        "MENURUT PROVINSI"
    )

    print(
        "========================================"
    )

    # 1. Load raw data
    df = load_raw_data()

    # 2. Check columns
    check_columns(df)

    # 3. Create aggregation
    result = create_aggregation(df)

    # 4. Preview result
    print(
        "\nPreview hasil agregasi:"
    )

    print(
        result.head(20).to_string(
            index=False
        )
    )

    # 5. Save to final/
    save_result(result)

if __name__ == "__main__":
    main()