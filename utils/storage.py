from utils.minio import (
    create_client,
    write_file,
    read_file,
    upload_file,
    list_files,
)


def get_minio_client():
    """
    Membuat client MinIO.
    """
    return create_client()


def upload_bytes(
    client,
    bucket_name,
    data,
    object_name,
    content_type="application/octet-stream",
):
    """
    Upload bytes langsung ke MinIO.
    """
    return write_file(
        client,
        bucket_name,
        data,
        object_name,
        content_type,
    )


def download_bytes(
    client,
    bucket_name,
    object_name,
):
    """
    Mengambil object dari MinIO
    dan mengembalikan bytes.
    """
    return read_file(
        client,
        bucket_name,
        object_name,
    )


def upload_local_file(
    client,
    bucket_name,
    file_path,
    object_name,
    content_type="application/octet-stream",
):
    """
    Upload file lokal ke MinIO.
    """
    return upload_file(
        client,
        bucket_name,
        file_path,
        object_name,
        content_type,
    )


def list_objects(
    client,
    bucket_name,
    prefix="",
):
    """
    Mengambil daftar object dari MinIO.
    """
    return list_files(
        client,
        bucket_name,
        prefix,
    )