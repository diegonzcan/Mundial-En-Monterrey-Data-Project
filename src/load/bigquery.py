import os
from google.cloud import bigquery


def load_parquet_from_gcs_to_bigquery(
    gcs_uri: str,
    dataset_id: str,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """
    Loads parquet files from GCS into a BigQuery table.

    This uses WRITE_TRUNCATE to ensure rerunning the pipeline for the same run_date replaces
    existing data instead of appending duplicates.
    """

    project_id = os.getenv("GCP_PROJECT_ID")

    if not project_id:
        raise ValueError("Missing environment variable: GCP_PROJECT_ID")

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    table = client.get_table(table_id)

    print(f"Loaded data from {gcs_uri}")
    print(f"Table: {table_id}")
    print(f"Total rows now: {table.num_rows}")