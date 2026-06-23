import os
import datetime

from dateutil.utils import today
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = "mundial_trends_raw"
SILVER_DATASET = "mundial_trends_silver"


def get_default_run_date() -> str:
    return (today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def run_query(sql: str) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    job = client.query(sql)
    job.result()
    print("Query completed successfully.")


def create_comments_current_table() -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{SILVER_DATASET}.comments_current`
    (
      run_date DATE,
      comment_id STRING,
      parent_id STRING,
      video_id STRING,
      author_channel_id STRING,
      author_name STRING,
      comment_text STRING,
      like_count INT64,
      published_at TIMESTAMP,
      updated_at TIMESTAMP,
      extracted_at TIMESTAMP
    )
    PARTITION BY run_date
    CLUSTER BY video_id, comment_id
    """
    run_query(sql)


def merge_comments_current(run_date: str) -> None:
    sql = f"""
    MERGE `{PROJECT_ID}.{SILVER_DATASET}.comments_current` T
    USING (
      SELECT
        run_date,
        comment_id,
        parent_id,
        video_id,
        author_channel_id,
        author_name,
        comment_text,
        like_count,
        published_at,
        updated_at,
        extracted_at
      FROM `{PROJECT_ID}.{RAW_DATASET}.comments_raw`
      WHERE run_date = DATE('{run_date}')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY comment_id
        ORDER BY extracted_at DESC
      ) = 1
    ) S
    ON T.comment_id = S.comment_id

    WHEN MATCHED AND S.extracted_at > T.extracted_at THEN
      UPDATE SET
        run_date = S.run_date,
        parent_id = S.parent_id,
        video_id = S.video_id,
        author_channel_id = S.author_channel_id,
        author_name = S.author_name,
        comment_text = S.comment_text,
        like_count = S.like_count,
        published_at = S.published_at,
        updated_at = S.updated_at,
        extracted_at = S.extracted_at

    WHEN NOT MATCHED THEN
      INSERT (
        run_date,
        comment_id,
        parent_id,
        video_id,
        author_channel_id,
        author_name,
        comment_text,
        like_count,
        published_at,
        updated_at,
        extracted_at
      )
      VALUES (
        S.run_date,
        S.comment_id,
        S.parent_id,
        S.video_id,
        S.author_channel_id,
        S.author_name,
        S.comment_text,
        S.like_count,
        S.published_at,
        S.updated_at,
        S.extracted_at
      )
    """
    run_query(sql)


def create_videos_daily_table() -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{SILVER_DATASET}.videos_daily`
    (
      run_date DATE,
      video_id STRING,
      video_url STRING,
      title STRING,
      description STRING,
      channel_id STRING,
      channel_title STRING,
      published_at TIMESTAMP,
      search_query STRING,
      category_id STRING,
      tags STRING,
      default_language STRING,
      duration_seconds INT64,
      view_count INT64,
      like_count INT64,
      comment_count INT64,
      thumbnail_url STRING,
      published_after_utc TIMESTAMP,
      published_before_utc TIMESTAMP,
      extracted_at TIMESTAMP,
      raw_search_json STRING
    )
    PARTITION BY run_date
    CLUSTER BY video_id, channel_id
    """
    run_query(sql)


def merge_videos_daily(run_date: str) -> None:
    sql = f"""
    MERGE `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` T
    USING (
      SELECT
        run_date,
        video_id,
        video_url,
        title,
        description,
        channel_id,
        channel_title,
        published_at,
        search_query,
        category_id,
        tags,
        default_language,
        duration_seconds,
        view_count,
        like_count,
        comment_count,
        thumbnail_url,
        published_after_utc,
        published_before_utc,
        extracted_at,
        raw_search_json
      FROM `{PROJECT_ID}.{RAW_DATASET}.videos_raw`
      WHERE run_date = DATE('{run_date}')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY video_id, run_date
        ORDER BY extracted_at DESC
      ) = 1
    ) S
    ON T.video_id = S.video_id
    AND T.run_date = S.run_date

    WHEN MATCHED AND S.extracted_at > T.extracted_at THEN
      UPDATE SET
        video_url = S.video_url,
        title = S.title,
        description = S.description,
        channel_id = S.channel_id,
        channel_title = S.channel_title,
        published_at = S.published_at,
        search_query = S.search_query,
        category_id = S.category_id,
        tags = S.tags,
        default_language = S.default_language,
        duration_seconds = S.duration_seconds,
        view_count = S.view_count,
        like_count = S.like_count,
        comment_count = S.comment_count,
        thumbnail_url = S.thumbnail_url,
        published_after_utc = S.published_after_utc,
        published_before_utc = S.published_before_utc,
        extracted_at = S.extracted_at,
        raw_search_json = S.raw_search_json

    WHEN NOT MATCHED THEN
      INSERT (
        run_date,
        video_id,
        video_url,
        title,
        description,
        channel_id,
        channel_title,
        published_at,
        search_query,
        category_id,
        tags,
        default_language,
        duration_seconds,
        view_count,
        like_count,
        comment_count,
        thumbnail_url,
        published_after_utc,
        published_before_utc,
        extracted_at,
        raw_search_json
      )
      VALUES (
        S.run_date,
        S.video_id,
        S.video_url,
        S.title,
        S.description,
        S.channel_id,
        S.channel_title,
        S.published_at,
        S.search_query,
        S.category_id,
        S.tags,
        S.default_language,
        S.duration_seconds,
        S.view_count,
        S.like_count,
        S.comment_count,
        S.thumbnail_url,
        S.published_after_utc,
        S.published_before_utc,
        S.extracted_at,
        S.raw_search_json
      )
    """
    run_query(sql)


def create_channels_daily_table() -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{SILVER_DATASET}.channels_daily`
    (
      run_date DATE,
      channel_id STRING,
      channel_title STRING,
      description STRING,
      custom_url STRING,
      country STRING,
      subscriber_count INT64,
      total_view_count INT64,
      total_video_count INT64,
      published_at TIMESTAMP,
      extracted_at TIMESTAMP,
      raw_channel_json STRING
    )
    PARTITION BY run_date
    CLUSTER BY channel_id
    """
    run_query(sql)


def merge_channels_daily(run_date: str) -> None:
    sql = f"""
    MERGE `{PROJECT_ID}.{SILVER_DATASET}.channels_daily` T
    USING (
      SELECT
        run_date,
        channel_id,
        channel_title,
        description,
        custom_url,
        country,
        subscriber_count,
        total_view_count,
        total_video_count,
        published_at,
        extracted_at,
        raw_channel_json
      FROM `{PROJECT_ID}.{RAW_DATASET}.channels_raw`
      WHERE run_date = DATE('{run_date}')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY channel_id, run_date
        ORDER BY extracted_at DESC
      ) = 1
    ) S
    ON T.channel_id = S.channel_id
    AND T.run_date = S.run_date

    WHEN MATCHED AND S.extracted_at > T.extracted_at THEN
      UPDATE SET
        channel_title = S.channel_title,
        description = S.description,
        custom_url = S.custom_url,
        country = S.country,
        subscriber_count = S.subscriber_count,
        total_view_count = S.total_view_count,
        total_video_count = S.total_video_count,
        published_at = S.published_at,
        extracted_at = S.extracted_at,
        raw_channel_json = S.raw_channel_json

    WHEN NOT MATCHED THEN
      INSERT (
        run_date,
        channel_id,
        channel_title,
        description,
        custom_url,
        country,
        subscriber_count,
        total_view_count,
        total_video_count,
        published_at,
        extracted_at,
        raw_channel_json
      )
      VALUES (
        S.run_date,
        S.channel_id,
        S.channel_title,
        S.description,
        S.custom_url,
        S.country,
        S.subscriber_count,
        S.total_view_count,
        S.total_video_count,
        S.published_at,
        S.extracted_at,
        S.raw_channel_json
      )
    """
    run_query(sql)


def main(run_date: str | None = None) -> None:
    if not PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID environment variable.")

    run_date = run_date or get_default_run_date()

    print(f"Running silver transformations for run_date={run_date}")

    create_comments_current_table()
    merge_comments_current(run_date)

    create_videos_daily_table()
    merge_videos_daily(run_date)

    create_channels_daily_table()
    merge_channels_daily(run_date)

    print("Silver transformations completed.")
