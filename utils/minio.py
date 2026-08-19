import os
from io import BytesIO
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")


MINIO_BUCKET = "maganghub"


def create_client():
    return Minio(
        f"{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
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
    Write bytes/data langsung ke object MinIO.
    """

    data_stream = BytesIO(data)

    client.put_object(
        bucket_name,
        object_name,
        data_stream,
        length=len(data),
        content_type=content_type,
    )

    print(
        f"MinIO write berhasil: {object_name}"
    )


def read_file(
    client,
    bucket_name,
    object_name,
):
    """
    Read object dari MinIO dan mengembalikan bytes.
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
        f"MinIO read berhasil: {object_name}"
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

    file_path = Path(file_path)

    with file_path.open("rb") as file_data:

        file_size = file_path.stat().st_size

        client.put_object(
            bucket_name,
            object_name,
            file_data,
            length=file_size,
            content_type=content_type,
        )

    print(
        f"MinIO upload berhasil: {object_name}"
    )


def list_files(
    client,
    bucket_name,
    prefix="",
):
    """
    Menampilkan daftar object/file di MinIO.
    """

    objects = client.list_objects(
        bucket_name,
        prefix=prefix,
        recursive=True,
    )

    files = [
        obj.object_name
        for obj in objects
    ]

    return files