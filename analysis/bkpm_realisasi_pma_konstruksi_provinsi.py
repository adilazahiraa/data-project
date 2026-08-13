from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
FINAL_DATA_DIR = PROJECT_ROOT / "final"

OUTPUT_FILE = (
    FINAL_DATA_DIR
    / "bkpm_realisasi_pma_konstruksi_provinsi.csv"
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
        f"\nTotal raw rows: {len(combined_df):,}"
    )

    return combined_df

def create_aggregation(df):

    filtered_df = df[
        (df["status_penanaman_modal"] == "PMA")
        &
        (df["nama_sektor"] == "Konstruksi")
    ].copy()

    print(
        "\nRows setelah filter PMA + Konstruksi:",
        f"{len(filtered_df):,}"
    )

    if filtered_df.empty:

        print("\nUnique nama_sektor:")

        print(
            df["nama_sektor"]
            .dropna()
            .unique()
        )

        raise ValueError(
            "Sektor Konstruksi tidak ditemukan."
        )

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
        f"File: {OUTPUT_FILE}"
    )

    print(
        f"Total rows: {len(df):,}"
    )

def main():

    print(
        "========================================"
    )

    print(
        "BKPM"
    )

    print(
        "REALISASI PMA SEKTOR KONSTRUKSI"
    )

    print(
        "MENURUT PROVINSI"
    )

    print(
        "========================================"
    )

    # 1. Load raw
    df = load_raw_data()

    # 2. Cek struktur sektor
    print(
        "\nContoh unique nama_sektor:"
    )

    print(
        df["nama_sektor"]
        .dropna()
        .unique()
    )

    # 3. Agregasi
    result = create_aggregation(df)

    # 4. Preview
    print(
        "\nPreview hasil agregasi:"
    )

    print(
        result.head(20).to_string(
            index=False
        )
    )

    # 5. Save
    save_result(result)


if __name__ == "__main__":
    main()