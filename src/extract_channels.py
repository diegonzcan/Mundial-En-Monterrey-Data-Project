import os
from datetime import datetime, timezone

import pandas as pd

from common import PARQUET_DIR, load_env, load_latest_videos_df, get_latest_run_date, write_parquet, youtube_get
from load.gcs import upload_parquet_folder_to_gcs


def extract_channel_details(channel_ids):
    rows = []

    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]

        data = youtube_get(
            "channels",
            {
                "part": "snippet,statistics",
                "id": ",".join(batch),
                "maxResults": 50,
            }
        )

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})

            rows.append({
                "channel_id": item.get("id"),

                "channel_title": snippet.get("title"),
                "description": snippet.get("description"),
                "custom_url": snippet.get("customUrl"),
                "country": snippet.get("country"),

                "subscriber_count": int(stats.get("subscriberCount", 0))
                if stats.get("subscriberCount")
                else None,

                "total_view_count": int(stats.get("viewCount", 0))
                if stats.get("viewCount")
                else None,

                "total_video_count": int(stats.get("videoCount", 0))
                if stats.get("videoCount")
                else None,

                "published_at": snippet.get("publishedAt"),

                "extracted_at": datetime.now(timezone.utc).isoformat(),

                "raw_channel_json": str(item),
            })

    return pd.DataFrame(rows)

def normalize_channels_schema(df: pd.DataFrame) -> pd.DataFrame:
    string_cols = [
        "channel_id",
        "channel_title",
        "description",
        "custom_url",
        "country",
        "raw_channel_json",
    ]

    int_cols = [
        "subscriber_count",
        "total_view_count",
        "total_video_count",
    ]

    timestamp_cols = [
        "published_at",
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
        videos_df = load_latest_videos_df()
        if videos_df.empty:
            print("No videos found in latest videos parquet.")
            return
        run_date = get_latest_run_date(videos_df)
    else:
        videos_df = load_latest_videos_df(run_date=run_date)
        if videos_df.empty:
            print(f"No videos found in latest videos parquet for run_date {run_date}.")
            return

    channel_ids = videos_df["channel_id"].dropna().unique().tolist()

    if not channel_ids:
        print("No channel_ids found.")
        return

    print(f"Found {len(channel_ids)} channels.")

    channels_df = extract_channel_details(channel_ids)

    if channels_df.empty:
        print("No channel data extracted.")
        return

    channels_df = channels_df.drop_duplicates(subset=["channel_id"])
    channels_df["run_date"] = run_date
    channels_df = normalize_channels_schema(channels_df)

    parquet_path = write_parquet(channels_df, f"youtube_worldcup_mty_channels_{run_date}.parquet", run_date=run_date)
    print(f"Exported {len(channels_df)} channels to {parquet_path}")

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