import os

from io import BytesIO
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

load_dotenv(
    PROJECT_ROOT / ".env"
)


MINIO_BUCKET = "maganghub"


def create_client():
    host = os.getenv(
        "MINIO_HOST"
    )

    port = os.getenv(
        "MINIO_PORT"
    )

    access_key = os.getenv(
        "MINIO_ACCESS_KEY"
    )

    secret_key = os.getenv(
        "MINIO_SECRET_KEY"
    )

    if not host:
        raise RuntimeError(
            "MINIO_HOST tidak ditemukan "
            "di .env"
        )

    if not port:
        raise RuntimeError(
            "MINIO_PORT tidak ditemukan "
            "di .env"
        )

    if not access_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY tidak ditemukan "
            "di .env"
        )

    if not secret_key:
        raise RuntimeError(
            "MINIO_SECRET_KEY tidak ditemukan "
            "di .env"
        )

    return Minio(
        f"{host}:{port}",
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def write_file(
    client,
    bucket_name,
    data,
    object_name,
    content_type="application/octet-stream",
):
    """
    Write bytes/data langsung
    ke object MinIO.
    """

    if not isinstance(
        data,
        (bytes, bytearray),
    ):
        raise TypeError(
            "data harus berupa bytes "
            "atau bytearray."
        )

    data = bytes(data)

    data_stream = BytesIO(
        data
    )

    client.put_object(
        bucket_name,
        object_name,
        data_stream,
        length=len(data),
        content_type=content_type,
    )

    print(
        f"MinIO write berhasil: "
        f"{object_name}"
    )


def read_file(
    client,
    bucket_name,
    object_name,
):
    """
    Read object dari MinIO
    dan mengembalikan bytes.
    """

    response = client.get_object(
        bucket_name,
        object_name,
    )

    try:
        data = response.read()

    finally:
        response.close()
        response.release_conn()

    print(
        f"MinIO read berhasil: "
        f"{object_name}"
    )

    return data


def upload_file(
    client,
    bucket_name,
    file_path,
    object_name,
    content_type="application/octet-stream",
):
    """
    Upload file lokal ke MinIO.
    """

    file_path = Path(
        file_path
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: "
            f"{file_path}"
        )

    file_size = (
        file_path.stat().st_size
    )

    with file_path.open("rb") as file_data:

        client.put_object(
            bucket_name,
            object_name,
            file_data,
            length=file_size,
            content_type=content_type,
        )

    print(
        f"MinIO upload berhasil: "
        f"{object_name}"
    )


def list_files(
    client,
    bucket_name,
    prefix="",
):
    """
    Mengambil daftar object/file
    di MinIO.
    """

    objects = client.list_objects(
        bucket_name,
        prefix=prefix,
        recursive=True,
    )

    return [
        obj.object_name
        for obj in objects
    ]