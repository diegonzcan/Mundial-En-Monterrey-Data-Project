import datetime
import os
from dateutil.utils import today
from load.bigquery import load_parquet_from_gcs_to_bigquery
import dotenv

dotenv.load_dotenv()


def _default_run_date():
    return (today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def load_all(run_date: str | None = None) -> None:
    """
    Load parquet files for videos, comments and channels into BigQuery.

    It looks for parquet files uploaded to `gs://{GCS_BUCKET_NAME}/youtube/raw/{run_date}/`
    and uses filename patterns to target the correct BigQuery table.
    """

    if run_date is None:
        from common import get_run_date
        run_date = get_run_date()

    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Missing environment variable: GCS_BUCKET_NAME")

    base_gcs = f"gs://{bucket_name}/youtube/raw/{run_date}"

    dataset_id = "mundial_trends_raw"

    patterns = {
        "videos_raw": f"{base_gcs}/youtube_worldcup_mty_videos_{run_date}.parquet",
        "comments_raw": f"{base_gcs}/youtube_worldcup_mty_comments_{run_date}.parquet",
        "channels_raw": f"{base_gcs}/youtube_worldcup_mty_channels_{run_date}.parquet",
    }

    for table_name, gcs_uri in patterns.items():
        print(f"Loading {table_name} from configured GCS bucket into {dataset_id}...")
        load_parquet_from_gcs_to_bigquery(
            gcs_uri=gcs_uri,
            dataset_id=dataset_id,
            table_name=table_name,
            write_disposition="WRITE_TRUNCATE",
        )


def main(run_date: str | None = None):
    try:
        load_all(run_date=run_date)
    except Exception as e:
        print(f"Error loading to BigQuery: {e}")
        raise


if __name__ == "__main__":
    main()
