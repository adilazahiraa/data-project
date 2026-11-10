import re
import pandas as pd


# =========================================================
# COLUMN CLEANING
# =========================================================

def clean_columns(df):
    """Membersihkan nama kolom dari BOM dan whitespace."""
    df = df.copy()

    df.columns = (
        df.columns
        .astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    return df


def clean_text_columns(df, columns):
    """Membersihkan whitespace pada kolom teks."""
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
            )

    return df


def clean_numeric_columns(
    df,
    columns,
    round_digits=None,
):
    """Mengubah kolom menjadi numerik dan opsional membulatkan nilainya."""
    df = df.copy()

    for column in columns:
        if column not in df.columns:
            continue

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        if round_digits is not None:
            df[column] = df[column].round(round_digits)

    return df


# =========================================================
# DATAFRAME NORMALIZATION
# =========================================================

def normalize_dataframe(
    df,
    text_columns=None,
    numeric_columns=None,
    date_columns=None,
    round_columns=None,
    round_digits=2,
):
    """Menormalisasi kolom teks, numerik, tanggal, dan pembulatan."""
    df = clean_columns(df)

    if text_columns:
        df = clean_text_columns(
            df,
            text_columns,
        )

    if numeric_columns:
        df = clean_numeric_columns(
            df,
            numeric_columns,
        )

    if date_columns:
        for column in date_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column],
                    errors="coerce",
                )

    if round_columns:
        for column in round_columns:
            if column in df.columns:
                df[column] = (
                    pd.to_numeric(
                        df[column],
                        errors="coerce",
                    )
                    .round(round_digits)
                )

    return df


# =========================================================
# DATAFRAME FILTERING
# =========================================================

def filter_dataframe(df, **filters):
    """Memfilter DataFrame berdasarkan pasangan kolom dan nilai."""
    result = df.copy()

    for column, value in filters.items():
        if value is None:
            continue

        if column not in result.columns:
            continue

        result = result[result[column] == value]

    return result.copy()


def filter_not_null(df, column):
    """Mengambil baris dengan nilai kolom yang tidak null."""
    if column not in df.columns:
        return df.copy()

    return df[df[column].notna()].copy()


def filter_nonzero(df, column):
    """Mengambil baris dengan nilai kolom yang tidak null dan bukan nol."""
    if column not in df.columns:
        return df.copy()

    return df[
        df[column].notna()
        & (df[column] != 0)
    ].copy()


# =========================================================
# DATAFRAME AGGREGATION
# =========================================================

def aggregate_dataframe(df, group_by, aggregations):
    """Mengelompokkan dan mengagregasi data berdasarkan kolom tertentu."""
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby(
            group_by,
            as_index=False,
        )
        .agg(aggregations)
    )


# =========================================================
# DUPLICATE DETECTION
# =========================================================

def find_duplicates(
    df,
    key_columns=None,
    subset=None,
):
    """Mengembalikan seluruh baris yang memiliki kunci duplikat."""
    columns = (
        key_columns
        if key_columns is not None
        else subset
    )

    if df.empty:
        return pd.DataFrame(columns=df.columns)

    if columns is None:
        return pd.DataFrame(columns=df.columns)

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    if not existing_columns:
        return pd.DataFrame(columns=df.columns)

    return (
        df[
            df.duplicated(
                subset=existing_columns,
                keep=False,
            )
        ]
        .sort_values(existing_columns)
        .copy()
    )


# =========================================================
# DATAFRAME CONCATENATION
# =========================================================

def concat_dataframes(dataframes):
    """Menggabungkan DataFrame yang tidak None dan tidak kosong."""
    valid_dataframes = [
        df
        for df in dataframes
        if df is not None and not df.empty
    ]

    if not valid_dataframes:
        return pd.DataFrame()

    return pd.concat(
        valid_dataframes,
        ignore_index=True,
    )


# =========================================================
# PERIOD FORMATTING
# =========================================================

def format_year_end(value):
    """Mengubah tahun menjadi tanggal akhir tahun: 31-12-YYYY."""
    if pd.isna(value):
        return pd.NA

    value = str(value).strip()

    match = re.search(
        r"\b(\d{4})\b",
        value,
    )

    if not match:
        return pd.NA

    return f"31-12-{match.group(1)}"


def format_quarter_end(value):
    """Mengubah tahun dan triwulan menjadi tanggal akhir triwulan."""
    if pd.isna(value):
        return pd.NA

    value = str(value).strip()

    match = re.search(
        r"\b(\d{4})\b",
        value,
    )

    if not match:
        return pd.NA

    year = match.group(1)

    quarter_dates = {
        "Triwulan 1": "31-03",
        "Triwulan 2": "30-06",
        "Triwulan 3": "30-09",
        "Triwulan 4": "31-12",
    }

    for quarter, date in quarter_dates.items():
        if quarter in value:
            return f"{date}-{year}"

    return pd.NA


# =========================================================
# FILENAME SANITIZATION
# =========================================================
def sanitize_filename(value):
    """Mengubah nilai menjadi nama file yang lebih aman."""
    if pd.isna(value):
        return ""

    return (
        re.sub(
            r"[^A-Za-z0-9]+",
            "_",
            str(value),
        )
        .strip("_")
        .lower()
    )

def get_config(configs, indikator):
    if indikator not in configs:
        raise ValueError(
            f"Indikator tidak ditemukan: {indikator}"
        )

    return configs[indikator]