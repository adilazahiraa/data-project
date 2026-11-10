

from utils.minio import (
    MINIO_BUCKET,
    create_client,
    write_file,
    read_file,
    upload_file as _upload_file,
    list_files,
    object_exists as _object_exists,
)

def get_minio_client():
    """Membuat client MinIO."""
    return create_client()


def upload_bytes(
    client,
    bucket_name,
    data,
    object_name,
    content_type="application/octet-stream",
):
    """Mengunggah data bytes ke MinIO."""
    return write_file(
        client=client,
        bucket_name=bucket_name,
        data=data,
        object_name=object_name,
        content_type=content_type,
    )


def download_bytes(client, bucket_name, object_name):
    """Mengunduh object MinIO sebagai bytes."""
    return read_file(
        client=client,
        bucket_name=bucket_name,
        object_name=object_name,
    )


def upload_local_file(
    client,
    bucket_name,
    file_path,
    object_name,
    content_type="application/octet-stream",
):
    """Mengunggah file lokal ke MinIO."""
    return _upload_file(
        client=client,
        bucket_name=bucket_name,
        file_path=file_path,
        object_name=object_name,
        content_type=content_type,
    )


# Backward compatibility
upload_file = upload_local_file


def list_objects(client, bucket_name, prefix=""):
    """Mengambil daftar nama object di MinIO."""
    return list_files(
        client=client,
        bucket_name=bucket_name,
        prefix=prefix,
    )


def object_exists(client, bucket_name, object_name):
    """Mengecek keberadaan object di MinIO."""
    return _object_exists(
        client=client,
        bucket_name=bucket_name,
        object_name=object_name,
    )
