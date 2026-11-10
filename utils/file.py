from io import BytesIO
from pathlib import Path
import re

import pandas as pd


def get_storage_paths(config, project_root, source="bps"):
    storage_name = config["storage_name"]
    file_name = config["file_name"]
    source_name = config.get("storage_source", source)

    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("Nama source harus berupa string yang tidak kosong.")

    if not isinstance(storage_name, str) or not storage_name.strip():
        raise ValueError("config['storage_name'] harus berupa string yang tidak kosong.")

    if not isinstance(file_name, str) or not file_name.strip():
        raise ValueError("config['file_name'] harus berupa string yang tidak kosong.")

    root = Path(project_root)
    raw_dir = root / "data" / "raw" / source_name / storage_name
    final_dir = root / "data" / "final" / source_name / storage_name

    return {
        "raw_dir": raw_dir,
        "raw_file": raw_dir / f"{file_name}_raw.parquet",
        "final_dir": final_dir,
        "final_file": final_dir / f"{file_name}.csv",
    }


def read_parquet(filepath):
    filepath = Path(filepath)
    if not filepath.is_file():
        raise FileNotFoundError(f"File tidak ditemukan: {filepath}")
    return pd.read_parquet(filepath)


def save_parquet(df, filepath):
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath, index=False)
    return filepath


def safe_filename(value):
    """Mengubah teks menjadi format aman untuk nama file."""
    if value is None:
        return ""
    return re.sub(r"[^A-Za-z0-9]+", "_", str(value)).strip("_").lower()


def read_csv(filepath, encoding="utf-8-sig", **kwargs):
    filepath = Path(filepath)
    if not filepath.is_file():
        raise FileNotFoundError(f"File tidak ditemukan: {filepath}")
    return pd.read_csv(filepath, encoding=encoding, **kwargs)


def save_csv(df, filepath, encoding="utf-8-sig", **kwargs):
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False, encoding=encoding, **kwargs)
    return filepath


def dataframe_to_bytes(df, format="csv"):
    format_name = str(format).lower().strip()
    if format_name == "csv":
        return dataframe_to_csv_bytes(df)
    if format_name == "parquet":
        return dataframe_to_parquet_bytes(df)
    raise ValueError(f"Format tidak didukung: {format}")


def dataframe_to_csv_bytes(df, encoding="utf-8-sig"):
    return df.to_csv(index=False, encoding=encoding).encode(encoding)


def dataframe_to_parquet_bytes(df):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()
