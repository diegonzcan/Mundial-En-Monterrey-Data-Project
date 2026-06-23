import os
from datetime import datetime, timezone

import pandas as pd

from common import PARQUET_DIR, load_env, load_latest_videos_df, get_latest_run_date, write_parquet, youtube_get
from load.gcs import upload_parquet_folder_to_gcs


def extract_comments_for_video(video_id, max_pages=1):
    rows = []
    page_token = None

    for _ in range(max_pages):
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": 100,
            "order": "relevance",
            "textFormat": "plainText",
        }

        if page_token:
            params["pageToken"] = page_token

        data = youtube_get("commentThreads", params)

        if data is None:
            return rows

        for item in data.get("items", []):
            thread_snippet = item.get("snippet", {})
            top_comment = thread_snippet.get("topLevelComment", {})
            comment_snippet = top_comment.get("snippet", {})

            author_channel_id = None
            author_channel = comment_snippet.get("authorChannelId")

            if isinstance(author_channel, dict):
                author_channel_id = author_channel.get("value")

            rows.append({
                "video_id": video_id,
                "comment_id": top_comment.get("id"),
                "parent_id": None,
                "author_channel_id": author_channel_id,
                "author_name": comment_snippet.get("authorDisplayName"),
                "comment_text": comment_snippet.get("textDisplay"),
                "like_count": comment_snippet.get("likeCount"),
                "published_at": comment_snippet.get("publishedAt"),
                "updated_at": comment_snippet.get("updatedAt"),
            })

        page_token = data.get("nextPageToken")

        if not page_token:
            break

    return rows

def normalize_comments_schema(df: pd.DataFrame) -> pd.DataFrame:
    string_cols = [
        "video_id",
        "video_url",
        "video_title",
        "comment_id",
        "parent_id",
        "author_channel_id",
        "author_name",
        "comment_text",
    ]

    int_cols = ["like_count"]

    timestamp_cols = [
        "published_at",
        "updated_at",
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
    extracted_at = datetime.now(timezone.utc).isoformat()

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

    print(f"Extracting comments for videos from run_date {run_date}")

    all_comments = []
    for _, video in videos_df.iterrows():
        video_id = video["video_id"]
        comments = extract_comments_for_video(video_id)

        for comment in comments:
            comment["run_date"] = video["run_date"]
            comment["video_url"] = video["video_url"]
            comment["video_title"] = video.get("video_title") or video.get("title")
            comment["extracted_at"] = extracted_at

        all_comments.extend(comments)

    if not all_comments:
        print("No comments extracted. Could be disabled or low activity.")
        return

    comments_df = pd.DataFrame(all_comments)
    comments_df = comments_df[[
        "run_date",
        "video_id",
        "video_url",
        "video_title",
        "comment_id",
        "parent_id",
        "author_channel_id",
        "author_name",
        "comment_text",
        "like_count",
        "published_at",
        "updated_at",
        "extracted_at",
    ]]
    comments_df = comments_df.drop_duplicates(subset=["run_date", "comment_id"])
    comments_df = normalize_comments_schema(comments_df)

    parquet_path = write_parquet(comments_df, f"youtube_worldcup_mty_comments_{run_date}.parquet", run_date=run_date)
    print(f"Exported {len(comments_df)} comments to {parquet_path}")

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