from io import BytesIO
from pathlib import Path

import pandas as pd
import re


def read_parquet(filepath):
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {filepath}"
        )

    return pd.read_parquet(
        filepath
    )


def save_parquet(
    df,
    filepath,
):
    filepath = Path(filepath)

    filepath.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        filepath,
        index=False,
    )

    return filepath


def safe_filename(value):
    """
    Mengubah text menjadi format aman untuk nama file.
    """
    if value is None:
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


def read_csv(
    filepath,
    encoding="utf-8-sig",
):
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {filepath}"
        )

    return pd.read_csv(
        filepath,
        encoding=encoding,
    )


def save_csv(
    df,
    filepath,
    encoding="utf-8-sig",
):
    filepath = Path(filepath)

    filepath.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_csv(
        filepath,
        index=False,
        encoding=encoding,
    )

    return filepath

def dataframe_to_bytes(
    df,
    format="csv",
):
    if format == "csv":
        return dataframe_to_csv_bytes(df)

    if format == "parquet":
        return dataframe_to_parquet_bytes(df)

    raise ValueError(
        f"Format tidak didukung: {format}"
    )

def dataframe_to_csv_bytes(
    df,
    encoding="utf-8-sig",
):
    return (
        df.to_csv(
            index=False,
            encoding=encoding,
        )
        .encode(encoding)
    )


def dataframe_to_parquet_bytes(
    df,
):
    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
    )

    return buffer.getvalue()