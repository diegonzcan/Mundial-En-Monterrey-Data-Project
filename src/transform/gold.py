from google.cloud import bigquery
import os
from dotenv import load_dotenv


load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_SILVER = "mundial_trends_silver"
DATASET_GOLD = "mundial_trends_gold"


def get_client() -> bigquery.Client:
    if not PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID environment variable.")
    return bigquery.Client(project=PROJECT_ID)


def run_query(query: str):
    client = get_client()
    job = client.query(query)
    job.result()
    print("Query completed.")


def create_gold_dataset():
    client = get_client()
    dataset_id = f"{PROJECT_ID}.{DATASET_GOLD}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset ready: {dataset_id}")
    except Exception as e:
        print(f"Error creating dataset: {e}")
        raise

def build_gold_video_momentum():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.video_momentum` AS
    with base as (

    select
        run_date,
        video_id,
        video_url,
        title,
        channel_id,
        channel_title,
        description,
        thumbnail_url,

        view_count,
        like_count,
        comment_count,

        published_at,
        extracted_at,

        lag(view_count) over (
            partition by video_id
            order by extracted_at, run_date
        ) as previous_view_count,

        lag(like_count) over (
            partition by video_id
            order by extracted_at, run_date
        ) as previous_like_count,

        lag(comment_count) over (
            partition by video_id
            order by extracted_at, run_date
        ) as previous_comment_count

    from `{PROJECT_ID}.{DATASET_SILVER}.videos_daily`

    where published_at >= timestamp_sub(current_timestamp(), interval 3 day)

    )

    select
        *,

        view_count - previous_view_count as views_gained,
        like_count - previous_like_count as likes_gained,
        comment_count - previous_comment_count as comments_gained,

        safe_divide(
            view_count - previous_view_count,
            previous_view_count
        ) as pct_growth,

        coalesce(view_count - previous_view_count, 0)
        + coalesce(like_count - previous_like_count, 0) * 10
        + coalesce(comment_count - previous_comment_count, 0) * 25
        as momentum_score,

        timestamp_diff(
            extracted_at,
            published_at,
            hour
        ) as video_age_hours,

        current_timestamp() as gold_loaded_at

    from base

    where previous_view_count is not null
    ;
    """
    run_query(query)



def build_gold_video_discovery():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.video_discovery` AS
    select
    video_id,
    title,
    channel_title,
    description,
    view_count,
    views_gained,
    comments_gained,
    likes_gained,
    pct_growth,
    momentum_score,
    video_age_hours,

    safe_divide(comment_count, view_count) as comment_rate,
    safe_divide(like_count, view_count) as like_rate,

    case
        when view_count < 5000 and momentum_score > 0 then 'hidden_gem'
        when view_count < 20000 and pct_growth >= 0.5 then 'breakout_candidate'
        when comments_gained >= 5 and safe_divide(comment_count, view_count) >= 0.01 then 'conversation_driver'
        else 'normal'
    end as discovery_type

    from `{PROJECT_ID}.{DATASET_GOLD}.video_momentum`

    where 1 = 1
    and view_count between 100 and 100000
    and (
        pct_growth >= 0.25
        or comments_gained >= 3
        or momentum_score >= 500
    )

    order by
    case
        when discovery_type = 'conversation_driver' then 1
        when discovery_type = 'breakout_candidate' then 2
        when discovery_type = 'hidden_gem' then 3
        else 4
    end,
    momentum_score desc
    ;
    """
    run_query(query)


def main():
    create_gold_dataset()
    build_gold_video_momentum()
    build_gold_video_discovery()
    print("Gold layer completed successfully.")


if __name__ == "__main__":
    main()
