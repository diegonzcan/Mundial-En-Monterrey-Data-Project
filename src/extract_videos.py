from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import isodate
import pandas as pd

import os

from common import PARQUET_DIR, load_env, write_parquet, youtube_get, get_run_date
from load.gcs import upload_parquet_folder_to_gcs


QUERIES = [
    "mundial monterrey",
    "world cup monterrey",
    "copa mundial monterrey",
    "mundial 2026 monterrey",
    "world cup 2026 monterrey",
]


def get_last_3_days_window_monterrey():
    tz = ZoneInfo("America/Monterrey")
    today = datetime.now(tz).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=2)

    start_local = datetime.combine(start_date, datetime.min.time(), tzinfo=tz)
    end_local = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=tz)

    start_utc = start_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    end_utc = end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    return end_date, start_utc, end_utc


def search_videos(query, published_after, published_before, max_pages=2):
    rows = []
    page_token = None

    for _ in range(max_pages):
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "order": "date",
            "regionCode": "MX",
            "relevanceLanguage": "es",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "maxResults": 50,
        }

        if page_token:
            params["pageToken"] = page_token

        data = youtube_get("search", params)

        for item in data.get("items", []):
            video_id = item["id"].get("videoId")
            snippet = item.get("snippet", {})

            if not video_id:
                continue

            rows.append({
                "search_query": query,
                "video_id": video_id,
                "channel_id": snippet.get("channelId"),
                "channel_title": snippet.get("channelTitle"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url"),
                "raw_search_json": str(item),
            })

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return rows


def get_video_details(video_ids):
    detail_rows = []

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]

        data = youtube_get("videos", {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(batch),
            "maxResults": 50,
        })

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            duration_iso = content.get("duration")
            duration_seconds = None

            if duration_iso:
                try:
                    duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())
                except Exception:
                    duration_seconds = None

            detail_rows.append({
                "video_id": item.get("id"),
                "category_id": snippet.get("categoryId"),
                "tags": ", ".join(snippet.get("tags", [])) if snippet.get("tags") else None,
                "default_language": snippet.get("defaultLanguage"),
                "duration_seconds": duration_seconds,
                "view_count": int(stats.get("viewCount", 0)) if stats.get("viewCount") else None,
                "like_count": int(stats.get("likeCount", 0)) if stats.get("likeCount") else None,
                "comment_count": int(stats.get("commentCount", 0)) if stats.get("commentCount") else None,
            })

    return detail_rows

def normalize_videos_schema(df: pd.DataFrame) -> pd.DataFrame:
    string_cols = [
        "video_id",
        "video_url",
        "title",
        "description",
        "channel_id",
        "channel_title",
        "search_query",
        "category_id",
        "tags",
        "default_language",
        "thumbnail_url",
        "raw_search_json",
    ]

    int_cols = [
        "duration_seconds",
        "view_count",
        "like_count",
        "comment_count",
    ]

    timestamp_cols = [
        "published_at",
        "published_after_utc",
        "published_before_utc",
        "extracted_at",
    ]

    date_cols = ["run_date"]

    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df

def main(run_date: str | None = None):
    load_env()

    if run_date is None:
        run_date_obj, published_after, published_before = get_last_3_days_window_monterrey()
        run_date = str(run_date_obj)
    else:
        # Calculate published_after and published_before from the run_date
        tz = ZoneInfo("America/Monterrey")
        run_date_obj = datetime.strptime(run_date, "%Y-%m-%d").date()
        start_date = run_date_obj - timedelta(days=2)
        start_local = datetime.combine(start_date, datetime.min.time(), tzinfo=tz)
        end_local = datetime.combine(run_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=tz)
        start_utc = start_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        end_utc = end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        published_after = start_utc
        published_before = end_utc

    extracted_at = datetime.now(timezone.utc).isoformat()

    all_rows = []

    for query in QUERIES:
        all_rows.extend(search_videos(query, published_after, published_before))

    if not all_rows:
        print("No videos found for the last 3 days.")
        return

    search_df = pd.DataFrame(all_rows)

    # Deduplicate because the same video can appear for multiple search queries.
    search_df = (
        search_df
        .sort_values(["video_id", "search_query"])
        .groupby("video_id", as_index=False)
        .agg({
            "search_query": lambda x: ", ".join(sorted(set(x))),
            "channel_id": "first",
            "channel_title": "first",
            "title": "first",
            "description": "first",
            "published_at": "first",
            "thumbnail_url": "first",
            "raw_search_json": "first",
        })
    )

    video_ids = search_df["video_id"].dropna().unique().tolist()
    details_df = pd.DataFrame(get_video_details(video_ids))

    final_df = search_df.merge(details_df, on="video_id", how="left")

    final_df["run_date"] = str(run_date)
    final_df["published_after_utc"] = published_after
    final_df["published_before_utc"] = published_before
    final_df["video_url"] = "https://www.youtube.com/watch?v=" + final_df["video_id"]
    final_df["extracted_at"] = extracted_at

    final_df = final_df[[
        "run_date",
        "video_id",
        "video_url",
        "title",
        "description",
        "channel_id",
        "channel_title",
        "published_at",
        "search_query",
        "category_id",
        "tags",
        "default_language",
        "duration_seconds",
        "view_count",
        "like_count",
        "comment_count",
        "thumbnail_url",
        "published_after_utc",
        "published_before_utc",
        "extracted_at",
        "raw_search_json",
    ]]

    final_df = normalize_videos_schema(final_df)
    parquet_path = write_parquet(final_df, f"youtube_worldcup_mty_videos_{run_date}.parquet", run_date=run_date)

    print(f"Exported {len(final_df)} rows to {parquet_path}")
    print("Saved 3-day video snapshot with possible duplicates across days.")



    # upload to GCS 
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if bucket_name:
        parquet_run_dir = PARQUET_DIR / str(run_date)
        upload_parquet_folder_to_gcs(
            local_folder=str(parquet_run_dir),
            bucket_name=bucket_name,
            gcs_prefix=f"youtube/raw/{run_date}"
        )
    else:
        print("GCS_BUCKET_NAME not set; skipping GCS upload")


if __name__ == "__main__":
    main()