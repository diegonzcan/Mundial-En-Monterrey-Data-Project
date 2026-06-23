import argparse
import json
import logging
import os
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GOLD_DATASET = "mundial_trends_gold"
SILVER_DATASET = "mundial_trends_silver"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "apps" / "data"
MIRROR_OUTPUT_DIR = PROJECT_ROOT / "src" / "dashboard" / "data"
DATE_FORMAT = "%Y-%m-%d"
DATA_WINDOW_DAYS = 30
DEFAULT_TOPIC_LIMIT = 6

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_client():
    if not PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID environment variable.")
    return bigquery.Client(project=PROJECT_ID)


def run_query(client, query, job_config=None):
    logger.info("Running BigQuery query")
    return client.query(query, job_config=job_config).to_dataframe()


def clean_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if hasattr(value, "item"):
        return clean_value(value.item())

    return value


def clean_for_json(obj):
    if isinstance(obj, dict):
        return {key: clean_for_json(value) for key, value in obj.items()}

    if isinstance(obj, list):
        return [clean_for_json(value) for value in obj]

    return clean_value(obj)


def parse_date(value, name):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, DATE_FORMAT).date()
        except ValueError as exc:
            raise ValueError(f"Invalid {name}: {value}. Expected format YYYY-MM-DD") from exc
    raise ValueError(f"Invalid {name}: {value}. Expected YYYY-MM-DD string or date object.")


def build_topic_window(start_date=None, end_date=None, latest_run_date=None):
    if start_date is None:
        if latest_run_date is None:
            raise ValueError("start_date or latest_run_date must be provided")
        start_date = parse_date(latest_run_date, "start_date")
    else:
        start_date = parse_date(start_date, "start_date")

    if end_date is None:
        if latest_run_date is None:
            raise ValueError("end_date or latest_run_date must be provided")
        end_date = parse_date(latest_run_date, "end_date")
    else:
        end_date = parse_date(end_date, "end_date")

    if end_date < start_date:
        raise ValueError(f"end_date {end_date.isoformat()} must be on or after start_date {start_date.isoformat()}")

    return start_date, end_date


def generate_date_range(start_date, end_date):
    days = (end_date - start_date).days
    return [(start_date + timedelta(days=d)).isoformat() for d in range(days + 1)]


def build_topic_series(topic_df, date_range):
    records = {clean_value(row["run_date"]): row for row in topic_df.to_dict(orient="records")}
    series = []

    for run_date in date_range:
        row = records.get(run_date)
        if row is None:
            series.append(
                {
                    "run_date": run_date,
                    "videos_count": 0,
                    "total_comments": 0,
                    "total_views": 0,
                    "comments_change": 0,
                    "views_change": 0,
                }
            )
        else:
            series.append(
                {
                    "run_date": run_date,
                    "videos_count": clean_value(row.get("videos_count", 0)),
                    "total_comments": clean_value(row.get("total_comments", 0)),
                    "total_views": clean_value(row.get("total_views", 0)),
                    "comments_change": clean_value(row.get("comments_change", 0)),
                    "views_change": clean_value(row.get("views_change", 0)),
                }
            )

    return series


def write_json(data, filename):
    cleaned_data = clean_for_json(data)

    for output_dir in [OUTPUT_DIR, MIRROR_OUTPUT_DIR]:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / filename

        with path.open("w", encoding="utf-8") as f:
            json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

        logger.info("Created %s", path.relative_to(PROJECT_ROOT))


def df_to_records(df):
    if df.empty:
        return []
    return clean_for_json(df.to_dict(orient="records"))


def get_latest_run_date(client):
    query = f"""
    select max(run_date) as latest_run_date
    from `{PROJECT_ID}.{GOLD_DATASET}.dashboard_summary_daily`
    """

    df = run_query(client, query)

    if df.empty or pd.isna(df.loc[0, "latest_run_date"]):
        logger.warning("No latest run_date found in dashboard_summary_daily")
        return None

    return clean_value(df.loc[0, "latest_run_date"])


def get_earliest_topic_run_date(client):
    query = f"""
    select min(run_date) as earliest_run_date
    from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
    """

    df = run_query(client, query)

    if df.empty or pd.isna(df.loc[0, "earliest_run_date"]):
        logger.warning("No earliest run_date found in topic_daily")
        return None

    return clean_value(df.loc[0, "earliest_run_date"])


def get_top_relevant_topic_ids(client, latest_run_date, start_date=None, end_date=None, limit=6):
    if not latest_run_date:
        logger.warning("Cannot select relevant topics without latest_run_date")
        return []

    start_date, end_date = build_topic_window(start_date, end_date, latest_run_date)

    query = f"""
    select
        coalesce(macro_topic_id, topic_id) as topic_id,
        any_value(coalesce(macro_topic_name, topic_name)) as topic_name,
        sum(coalesce(total_comments, 0)) as window_total_comments,
        sum(coalesce(total_views, 0)) as window_total_views
    from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
    where run_date between date('{start_date.isoformat()}')
        and date('{end_date.isoformat()}')
    group by topic_id
    order by window_total_comments desc, window_total_views desc
    """

    df = run_query(client, query)

    if df.empty:
        logger.warning(
            "No topic rows found between %s and %s",
            start_date.isoformat(),
            end_date.isoformat(),
        )
        return []

    non_other = df[df["topic_id"] != "other"]
    selected = non_other if len(non_other) >= 3 else df

    return selected.head(limit)["topic_id"].astype(str).tolist()


def export_latest_metadata(client, latest_run_date, window_days=None):
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    query = f"""
    select
        min(run_date) as earliest_run_date,
        count(distinct run_date) as days_available
    from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
    """

    df = run_query(client, query)

    earliest_run_date = None
    days_available = 0

    if df.empty or pd.isna(df.loc[0, "earliest_run_date"]):
        logger.warning("No topic history found for latest_metadata.json")
    else:
        earliest_run_date = df.loc[0, "earliest_run_date"]
        days_available = df.loc[0, "days_available"]

    metadata = {
        "generated_at": generated_at,
        "latest_run_date": latest_run_date,
        "earliest_run_date": earliest_run_date,
        "days_available": days_available,
        "data_window_days": window_days if window_days is not None else DATA_WINDOW_DAYS,
    }

    write_json(metadata, "latest_metadata.json")


def export_dashboard_summary(client, latest_run_date):
    if not latest_run_date:
        logger.warning("Writing empty dashboard_summary.json")
        write_json({}, "dashboard_summary.json")
        return

    query = f"""
    select
        run_date,
        videos_monitored,
        channels_monitored,
        topics_detected,
        comments_available,
        total_views,
        total_likes,
        total_comments,
        top_topic_id,
        top_topic_name,
        top_topic_comments,
        top_channel_id,
        top_channel_title,
        top_channel_views
    from `{PROJECT_ID}.{GOLD_DATASET}.dashboard_summary_daily`
    where run_date = date('{latest_run_date}')
    order by processed_at desc
    limit 1
    """

    df = run_query(client, query)

    if df.empty:
        logger.warning("No dashboard summary found for %s", latest_run_date)
        write_json({}, "dashboard_summary.json")
        return

    write_json(df.iloc[0].to_dict(), "dashboard_summary.json")


def export_relevant_topics_over_time(client, latest_run_date, start_date=None, end_date=None, limit=6):
    start_date, end_date = build_topic_window(start_date, end_date, latest_run_date)
    date_range = generate_date_range(start_date, end_date)
    topic_ids = get_top_relevant_topic_ids(client, latest_run_date, start_date=start_date, end_date=end_date, limit=limit)

    if not topic_ids:
        write_json(
            {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "dates": date_range,
                "window_days": len(date_range),
                "metric": "total_comments",
                "topics": [],
            },
            "relevant_topics_over_time.json",
        )
        return topic_ids

    query = f"""
    with macro_daily as (
        select
            run_date,
            coalesce(macro_topic_id, topic_id) as topic_id,
            any_value(coalesce(macro_topic_name, topic_name)) as topic_name,
            sum(coalesce(videos_count, 0)) as videos_count,
            sum(coalesce(total_comments, 0)) as total_comments,
            sum(coalesce(total_views, 0)) as total_views
        from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
        where run_date between date('{start_date.isoformat()}')
            and date('{end_date.isoformat()}')
        group by run_date, topic_id
    ),
    macro_trends as (
        select
            *,
            total_comments - lag(total_comments) over (partition by topic_id order by run_date) as comments_change,
            total_views - lag(total_views) over (partition by topic_id order by run_date) as views_change
        from macro_daily
    )
    select
        run_date,
        topic_id,
        topic_name,
        videos_count,
        total_comments,
        total_views,
        comments_change,
        views_change
    from macro_trends
    where topic_id in unnest(@topic_ids)
    order by topic_id, run_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("topic_ids", "STRING", topic_ids),
        ]
    )

    df = run_query(client, query, job_config=job_config)

    if df.empty:
        logger.warning("No trend rows found for selected relevant topics")
        write_json(
            {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "dates": date_range,
                "window_days": len(date_range),
                "metric": "total_comments",
                "topics": [],
            },
            "relevant_topics_over_time.json",
        )
        return topic_ids

    topics = []
    for topic_id in topic_ids:
        topic_df = df[df["topic_id"] == topic_id].copy()

        if topic_df.empty:
            continue

        topics.append(
            {
                "topic_id": topic_id,
                "topic_name": topic_df.iloc[0]["topic_name"],
                "window_total_comments": topic_df["total_comments"].fillna(0).sum(),
                "window_total_views": topic_df["total_views"].fillna(0).sum(),
                "series": build_topic_series(topic_df, date_range),
            }
        )

    write_json(
        {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "dates": date_range,
            "window_days": len(date_range),
            "metric": "total_comments",
            "topics": topics,
        },
        "relevant_topics_over_time.json",
    )

    return topic_ids


def export_topic_share_today(client, latest_run_date):
    if not latest_run_date:
        write_json([], "topic_share_today.json")
        return

    query = f"""
    with latest_topics as (
        select
            coalesce(macro_topic_id, topic_id) as topic_id,
            any_value(coalesce(macro_topic_name, topic_name)) as topic_name,
            sum(coalesce(total_comments, 0)) as total_comments,
            sum(coalesce(total_views, 0)) as total_views,
            sum(coalesce(videos_count, 0)) as videos_count
        from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
        where run_date = date('{latest_run_date}')
        group by topic_id
    ),
    filtered_topics as (
        select *
        from latest_topics
        where topic_id != 'other'
    ),
    selected_topics as (
        select * from filtered_topics
        union all
        select * from latest_topics
        where not exists (select 1 from filtered_topics)
    )
    select
        topic_id,
        topic_name,
        total_comments,
        safe_divide(total_comments, sum(total_comments) over ()) as share_of_comments,
        total_views,
        videos_count
    from selected_topics
    order by total_comments desc, total_views desc
    limit 10
    """

    df = run_query(client, query)

    if df.empty:
        logger.warning("No topic share rows found for %s", latest_run_date)

    write_json(df_to_records(df), "topic_share_today.json")


def export_topic_changes_today(client, latest_run_date):
    if not latest_run_date:
        write_json([], "topic_changes_today.json")
        return

    query = f"""
    with macro_daily as (
        select
            run_date,
            coalesce(macro_topic_id, topic_id) as topic_id,
            any_value(coalesce(macro_topic_name, topic_name)) as topic_name,
            sum(coalesce(videos_count, 0)) as videos_count,
            sum(coalesce(total_comments, 0)) as total_comments,
            sum(coalesce(total_views, 0)) as total_views
        from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
        where run_date <= date('{latest_run_date}')
        group by run_date, topic_id
    ),
    latest_topics as (
        select
            *,
            lag(total_comments) over (partition by topic_id order by run_date) as previous_total_comments,
            lag(total_views) over (partition by topic_id order by run_date) as previous_total_views,
            lag(videos_count) over (partition by topic_id order by run_date) as previous_videos_count,
            total_comments - lag(total_comments) over (partition by topic_id order by run_date) as comments_change,
            total_views - lag(total_views) over (partition by topic_id order by run_date) as views_change,
            videos_count - lag(videos_count) over (partition by topic_id order by run_date) as videos_count_change
        from macro_daily
        qualify run_date = date('{latest_run_date}')
    ),
    filtered_topics as (
        select *
        from latest_topics
        where topic_id != 'other'
    ),
    selected_topics as (
        select * from filtered_topics
        union all
        select * from latest_topics
        where not exists (select 1 from filtered_topics)
    )
    select
        topic_id,
        topic_name,
        total_comments,
        previous_total_comments,
        comments_change,
        total_views,
        previous_total_views,
        views_change,
        videos_count,
        videos_count_change
    from selected_topics
    order by comments_change desc, views_change desc
    limit 10
    """

    df = run_query(client, query)

    if df.empty:
        logger.warning("No topic changes found for %s", latest_run_date)

    write_json(df_to_records(df), "topic_changes_today.json")


def export_top_topics_today(client, latest_run_date):
    if not latest_run_date:
        write_json([], "top_topics_today.json")
        return

    query = f"""
    with latest_topics as (
        select
            coalesce(macro_topic_id, topic_id) as topic_id,
            any_value(coalesce(macro_topic_name, topic_name)) as topic_name,
            sum(coalesce(videos_count, 0)) as videos_count,
            sum(coalesce(total_views, 0)) as total_views,
            sum(coalesce(total_likes, 0)) as total_likes,
            sum(coalesce(total_comments, 0)) as total_comments,
            safe_divide(sum(coalesce(total_comments, 0)), sum(coalesce(videos_count, 0))) as avg_comments_per_video
        from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
        where run_date = date('{latest_run_date}')
        group by topic_id
    ),
    filtered_topics as (
        select *
        from latest_topics
        where topic_id != 'other'
    ),
    selected_topics as (
        select * from filtered_topics
        union all
        select * from latest_topics
        where not exists (select 1 from filtered_topics)
    )
    select
        row_number() over (order by total_comments desc, total_views desc) as rank,
        topic_id,
        topic_name,
        videos_count,
        total_views,
        total_likes,
        total_comments,
        avg_comments_per_video
    from selected_topics
    order by rank
    limit 10
    """

    df = run_query(client, query)

    if df.empty:
        logger.warning("No top topics found for %s", latest_run_date)

    write_json(df_to_records(df), "top_topics_today.json")


def export_top_videos_by_topic(client, latest_run_date, topic_ids):
    if not latest_run_date or not topic_ids:
        write_json([], "top_videos_by_topic.json")
        return

    topic_ids = [topic_id for topic_id in topic_ids if topic_id != "other"]

    if not topic_ids:
        write_json([], "top_videos_by_topic.json")
        return

    query = f"""
    with ranked_videos as (
        select
            coalesce(vt.macro_topic_id, vt.topic_id) as topic_id,
            coalesce(vt.macro_topic_name, vt.topic_name) as topic_name,
            vt.sub_topic_id,
            vt.sub_topic_name,
            v.video_id,
            v.title,
            v.channel_title,
            v.video_url,
            v.thumbnail_url,
            v.view_count,
            v.like_count,
            v.comment_count,
            v.published_at,
            vt.topic_keywords,
            vt.matched_source,
            row_number() over (
                partition by coalesce(vt.macro_topic_id, vt.topic_id)
                order by v.comment_count desc, v.view_count desc
            ) as video_rank
        from `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` vt
        join `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
            on vt.run_date = v.run_date
            and vt.video_id = v.video_id
        where vt.run_date = date('{latest_run_date}')
            and coalesce(vt.macro_topic_id, vt.topic_id) in unnest(@topic_ids)
            and coalesce(vt.macro_topic_id, vt.topic_id) != 'other'
    )
    select
        topic_id,
        topic_name,
        sub_topic_id,
        sub_topic_name,
        video_id,
        title,
        channel_title,
        video_url,
        thumbnail_url,
        view_count,
        like_count,
        comment_count,
        published_at,
        topic_keywords,
        matched_source
    from ranked_videos
    where video_rank <= 5
    order by topic_id, comment_count desc, view_count desc
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("topic_ids", "STRING", topic_ids),
        ]
    )

    df = run_query(client, query, job_config=job_config)

    if df.empty:
        logger.warning("No top videos by topic found for %s", latest_run_date)
        write_json([], "top_videos_by_topic.json")
        return

    output = []
    for topic_id in topic_ids:
        topic_df = df[df["topic_id"] == topic_id].copy()

        if topic_df.empty:
            continue

        output.append(
            {
                "topic_id": topic_id,
                "topic_name": topic_df.iloc[0]["topic_name"],
                "videos": df_to_records(
                    topic_df[
                        [
                            "video_id",
                            "sub_topic_id",
                            "sub_topic_name",
                            "title",
                            "channel_title",
                            "video_url",
                            "thumbnail_url",
                            "view_count",
                            "like_count",
                            "comment_count",
                            "published_at",
                            "topic_keywords",
                            "matched_source",
                        ]
                    ]
                ),
            }
        )

    write_json(output, "top_videos_by_topic.json")


def export_sub_topics_over_time(client, latest_run_date, start_date=None, end_date=None):
    if not latest_run_date:
        write_json(
            {"window_days": 0, "metric": "total_comments", "macros": []},
            "sub_topics_over_time.json",
        )
        return

    start_date, end_date = build_topic_window(start_date, end_date, latest_run_date)
    date_range = generate_date_range(start_date, end_date)

    query = f"""
    with sub_topic_trends as (
        select
            run_date,
            coalesce(macro_topic_id, topic_id) as macro_topic_id,
            any_value(coalesce(macro_topic_name, topic_name)) as macro_topic_name,
            sub_topic_id,
            any_value(sub_topic_name) as sub_topic_name,
            any_value(keywords) as keywords,
            sum(coalesce(videos_count, 0)) as videos_count,
            sum(coalesce(total_comments, 0)) as total_comments,
            sum(coalesce(total_views, 0)) as total_views
        from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
        where run_date between date('{start_date.isoformat()}')
            and date('{end_date.isoformat()}')
            and coalesce(macro_topic_id, topic_id) != 'other'
            and sub_topic_id is not null
        group by run_date, macro_topic_id, sub_topic_id
    ),
    ranked_sub_topics as (
        select
            macro_topic_id,
            sub_topic_id,
            sum(total_comments) as window_total_comments,
            sum(total_views) as window_total_views,
            row_number() over (
                partition by macro_topic_id
                order by sum(total_comments) desc, sum(total_views) desc
            ) as sub_topic_rank
        from sub_topic_trends
        group by macro_topic_id, sub_topic_id
    )
    select s.*
    from sub_topic_trends s
    join ranked_sub_topics r
        using (macro_topic_id, sub_topic_id)
    where r.sub_topic_rank <= 8
    order by macro_topic_id, sub_topic_id, run_date
    """

    df = run_query(client, query)
    macros = []

    if not df.empty:
        for macro_topic_id, macro_df in df.groupby("macro_topic_id", sort=False):
            sub_topics = []
            for sub_topic_id, sub_df in macro_df.groupby("sub_topic_id", sort=False):
                sub_topics.append(
                    {
                        "sub_topic_id": sub_topic_id,
                        "sub_topic_name": sub_df.iloc[0]["sub_topic_name"],
                        "keywords": sub_df.iloc[0]["keywords"],
                        "window_total_comments": sub_df["total_comments"].fillna(0).sum(),
                        "window_total_views": sub_df["total_views"].fillna(0).sum(),
                        "series": build_topic_series(sub_df, date_range),
                    }
                )

            sub_topics.sort(key=lambda row: (row["window_total_comments"], row["window_total_views"]), reverse=True)
            macros.append(
                {
                    "macro_topic_id": macro_topic_id,
                    "macro_topic_name": macro_df.iloc[0]["macro_topic_name"],
                    "sub_topics": sub_topics,
                }
            )

    write_json(
        {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "dates": date_range,
            "window_days": len(date_range),
            "metric": "total_comments",
            "macros": macros,
        },
        "sub_topics_over_time.json",
    )


def export_top_sub_topics_by_macro(client, latest_run_date):
    if not latest_run_date:
        write_json([], "top_sub_topics_by_macro.json")
        return

    query = f"""
    with ranked as (
        select
            coalesce(macro_topic_id, topic_id) as macro_topic_id,
            coalesce(macro_topic_name, topic_name) as macro_topic_name,
            sub_topic_id,
            sub_topic_name,
            keywords,
            representative_videos,
            sample_comments,
            videos_count,
            total_views,
            total_comments,
            relevance_score,
            row_number() over (
                partition by coalesce(macro_topic_id, topic_id)
                order by total_comments desc, total_views desc
            ) as sub_topic_rank
        from `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
        where run_date = date('{latest_run_date}')
            and coalesce(macro_topic_id, topic_id) != 'other'
            and sub_topic_id is not null
    )
    select *
    from ranked
    where sub_topic_rank <= 8
    order by macro_topic_id, sub_topic_rank
    """

    df = run_query(client, query)
    if df.empty:
        write_json([], "top_sub_topics_by_macro.json")
        return

    output = []
    for macro_topic_id, macro_df in df.groupby("macro_topic_id", sort=False):
        output.append(
            {
                "macro_topic_id": macro_topic_id,
                "macro_topic_name": macro_df.iloc[0]["macro_topic_name"],
                "sub_topics": df_to_records(
                    macro_df[
                        [
                            "sub_topic_rank",
                            "sub_topic_id",
                            "sub_topic_name",
                            "keywords",
                            "representative_videos",
                            "sample_comments",
                            "videos_count",
                            "total_views",
                            "total_comments",
                            "relevance_score",
                        ]
                    ]
                ),
            }
        )

    write_json(output, "top_sub_topics_by_macro.json")


def export_videos_by_sub_topic(client, latest_run_date):
    if not latest_run_date:
        write_json([], "videos_by_sub_topic.json")
        return

    query = f"""
    with ranked_videos as (
        select
            coalesce(vt.macro_topic_id, vt.topic_id) as macro_topic_id,
            coalesce(vt.macro_topic_name, vt.topic_name) as macro_topic_name,
            vt.sub_topic_id,
            vt.sub_topic_name,
            v.video_id,
            v.title,
            v.channel_title,
            v.video_url,
            v.thumbnail_url,
            v.view_count,
            v.like_count,
            v.comment_count,
            v.published_at,
            vt.sub_topic_keywords as topic_keywords,
            row_number() over (
                partition by vt.sub_topic_id
                order by v.comment_count desc, v.view_count desc
            ) as video_rank
        from `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` vt
        join `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
            on vt.run_date = v.run_date
            and vt.video_id = v.video_id
        where vt.run_date = date('{latest_run_date}')
            and coalesce(vt.macro_topic_id, vt.topic_id) != 'other'
            and vt.sub_topic_id is not null
    )
    select *
    from ranked_videos
    where video_rank <= 5
    order by macro_topic_id, sub_topic_name, comment_count desc, view_count desc
    """

    df = run_query(client, query)
    if df.empty:
        write_json([], "videos_by_sub_topic.json")
        return

    output = []
    for sub_topic_id, sub_df in df.groupby("sub_topic_id", sort=False):
        output.append(
            {
                "macro_topic_id": sub_df.iloc[0]["macro_topic_id"],
                "macro_topic_name": sub_df.iloc[0]["macro_topic_name"],
                "sub_topic_id": sub_topic_id,
                "sub_topic_name": sub_df.iloc[0]["sub_topic_name"],
                "videos": df_to_records(
                    sub_df[
                        [
                            "video_id",
                            "title",
                            "channel_title",
                            "video_url",
                            "thumbnail_url",
                            "view_count",
                            "like_count",
                            "comment_count",
                            "published_at",
                            "topic_keywords",
                        ]
                    ]
                ),
            }
        )

    write_json(output, "videos_by_sub_topic.json")


def export_channel_daily(client, latest_run_date):
    if not latest_run_date:
        write_json([], "channel_daily.json")
        return

    query = f"""
    select
        row_number() over (order by total_comments desc, total_views desc) as rank,
        channel_id,
        channel_title,
        videos_count,
        total_views,
        total_likes,
        total_comments,
        subscriber_count,
        avg_comments_per_video
    from `{PROJECT_ID}.{GOLD_DATASET}.channel_daily`
    where run_date = date('{latest_run_date}')
    order by rank
    limit 10
    """

    df = run_query(client, query)

    if df.empty:
        logger.warning("No channel rows found for %s", latest_run_date)

    write_json(df_to_records(df), "channel_daily.json")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate dashboard JSON files")
    parser.add_argument("--start-date", help="Start date for topic evolution window (YYYY-MM-DD)", default=None)
    parser.add_argument("--end-date", help="End date for topic evolution window (YYYY-MM-DD)", default=None)
    parser.add_argument("--topic-limit", type=int, default=DEFAULT_TOPIC_LIMIT)
    return parser.parse_args()


def main():
    logger.info("Generating dashboard JSON files")
    args = parse_args()
    client = get_client()
    latest_run_date = get_latest_run_date(client)
    earliest_run_date = get_earliest_topic_run_date(client)

    start_date, end_date = build_topic_window(args.start_date or earliest_run_date, args.end_date, latest_run_date)
    window_days = len(generate_date_range(start_date, end_date))

    export_latest_metadata(client, latest_run_date, window_days=window_days)
    export_dashboard_summary(client, latest_run_date)
    topic_ids = export_relevant_topics_over_time(
        client,
        latest_run_date,
        start_date=start_date,
        end_date=end_date,
        limit=args.topic_limit,
    )
    export_topic_share_today(client, latest_run_date)
    export_topic_changes_today(client, latest_run_date)
    export_top_topics_today(client, latest_run_date)
    export_top_videos_by_topic(client, latest_run_date, topic_ids)
    export_sub_topics_over_time(client, latest_run_date, start_date=start_date, end_date=end_date)
    export_top_sub_topics_by_macro(client, latest_run_date)
    export_videos_by_sub_topic(client, latest_run_date)
    export_channel_daily(client, latest_run_date)

    logger.info("Dashboard JSON generation completed")


if __name__ == "__main__":
    main()
