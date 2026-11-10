
import os
from io import BytesIO
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

MINIO_BUCKET = "maganghub"
OBJECT_NOT_FOUND_CODES = {"NoSuchKey", "NoSuchObject", "NotFound"}


def create_client():
    required_config = {
        "MINIO_HOST": os.getenv("MINIO_HOST"),
        "MINIO_PORT": os.getenv("MINIO_PORT"),
        "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY"),
    }

    missing_config = [
        key for key, value in required_config.items() if not value
    ]
    if missing_config:
        raise RuntimeError(
            "Konfigurasi MinIO belum lengkap: "
            + ", ".join(missing_config)
        )

    host = required_config["MINIO_HOST"]
    port = required_config["MINIO_PORT"]

    return Minio(
        f"{host}:{port}",
        access_key=required_config["MINIO_ACCESS_KEY"],
        secret_key=required_config["MINIO_SECRET_KEY"],
        secure=False,
    )


def write_file(
    client, bucket_name, data, object_name,
    content_type="application/octet-stream",
):
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("data harus berupa bytes atau bytearray.")

    data = bytes(data)
    client.put_object(
        bucket_name,
        object_name,
        BytesIO(data),
        length=len(data),
        content_type=content_type,
    )
    print(f"MinIO write berhasil: {object_name}")


def read_file(client, bucket_name, object_name):
    response = client.get_object(bucket_name, object_name)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    print(f"MinIO read berhasil: {object_name}")
    return data


def upload_file(
    client, bucket_name, file_path, object_name,
    content_type="application/octet-stream",
):
    file_path = Path(file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    file_size = file_path.stat().st_size
    with file_path.open("rb") as file_data:
        client.put_object(
            bucket_name,
            object_name,
            file_data,
            length=file_size,
            content_type=content_type,
        )

    print(f"MinIO upload berhasil: {object_name}")


def list_files(client, bucket_name, prefix=""):
    objects = client.list_objects(
        bucket_name,
        prefix=prefix,
        recursive=True,
    )
    return [obj.object_name for obj in objects]


def object_exists(client, bucket_name, object_name):
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except S3Error as error:
        if error.code in OBJECT_NOT_FOUND_CODES:
            return False
        raise
