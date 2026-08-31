import re
import pandas as pd


def clean_columns(df):
    df = df.copy()

    df.columns = (
        df.columns
        .astype(str)
        .str.replace(
            "\ufeff",
            "",
            regex=False,
        )
        .str.strip()
    )

    return df


def clean_text_columns(df, columns):
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
    df = df.copy()

    for column in columns:
        if column not in df.columns:
            continue

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        if round_digits is not None:
            df[column] = df[column].round(
                round_digits
            )

    return df


def normalize_dataframe(
    df,
    text_columns=None,
    numeric_columns=None,
    date_columns=None,
    round_columns=None,
    round_digits=2,
):
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


def filter_dataframe(
    df,
    **filters,
):
    result = df.copy()

    for column, value in filters.items():
        if value is None:
            continue

        if column not in result.columns:
            continue

        result = result[
            result[column] == value
        ]

    return result.copy()


def filter_not_null(
    df,
    column,
):
    if column not in df.columns:
        return df.copy()

    return df[
        df[column].notna()
    ].copy()


def filter_nonzero(
    df,
    column,
):
    if column not in df.columns:
        return df.copy()

    return df[
        df[column].notna()
        & (df[column] != 0)
    ].copy()


def aggregate_dataframe(
    df,
    group_by,
    aggregations,
):
    if df.empty:
        return pd.DataFrame()

    return (
        df
        .groupby(
            group_by,
            as_index=False,
        )
        .agg(aggregations)
    )


def find_duplicates(
    df,
    key_columns=None,
    subset=None,
):
    columns = (
        key_columns
        if key_columns is not None
        else subset
    )

    if df.empty:
        return pd.DataFrame(
            columns=df.columns
        )

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    if not existing_columns:
        return pd.DataFrame(
            columns=df.columns
        )

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


def concat_dataframes(
    dataframes,
):
    valid_dataframes = [
        df
        for df in dataframes
        if df is not None
        and not df.empty
    ]

    if not valid_dataframes:
        return pd.DataFrame()

    return pd.concat(
        valid_dataframes,
        ignore_index=True,
    )


def format_year_end(value):
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


def sanitize_filename(value):
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