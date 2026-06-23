from google.cloud import storage
from pathlib import Path


def upload_file_to_gcs(
    local_file_path: str,
    bucket_name: str,
    destination_blob_name: str,
) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(local_file_path)

    print(f"Uploaded {Path(local_file_path).name} to configured GCS bucket at {destination_blob_name}")


def upload_parquet_folder_to_gcs(
    local_folder: str,
    bucket_name: str,
    gcs_prefix: str
) -> None:
    local_path = Path(local_folder)

    parquet_files = list(local_path.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {local_folder}")

    for file_path in parquet_files:
        relative_path = file_path.relative_to(local_path)
        destination_blob_name = f"{gcs_prefix}/{relative_path}".replace("\\", "/")

        upload_file_to_gcs(
            local_file_path=str(file_path),
            bucket_name=bucket_name,
            destination_blob_name=destination_blob_name
        )
