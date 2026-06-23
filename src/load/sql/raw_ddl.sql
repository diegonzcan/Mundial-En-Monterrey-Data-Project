-- BigQuery RAW layer DDL.
-- Replace `your_project.your_raw_dataset` with your own project and dataset.
-- RAW tables store one pipeline run's extracted YouTube API results loaded from Parquet.

CREATE SCHEMA IF NOT EXISTS `your_project.your_raw_dataset`
OPTIONS (
  description = "Raw YouTube extraction tables loaded from Parquet snapshots."
);

CREATE TABLE IF NOT EXISTS `your_project.your_raw_dataset.videos_raw` (
  run_date DATE OPTIONS(description = "Logical extraction date."),
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
  raw_search_json STRING OPTIONS(description = "Raw YouTube search response for audit/debugging.")
)
PARTITION BY run_date
CLUSTER BY video_id, channel_id
OPTIONS(description = "Raw video search and statistics snapshots.");

CREATE TABLE IF NOT EXISTS `your_project.your_raw_dataset.comments_raw` (
  run_date DATE OPTIONS(description = "Logical extraction date."),
  video_id STRING,
  video_url STRING,
  video_title STRING,
  comment_id STRING,
  parent_id STRING,
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
OPTIONS(description = "Raw top-level public YouTube comments collected for monitored videos.");

CREATE TABLE IF NOT EXISTS `your_project.your_raw_dataset.channels_raw` (
  run_date DATE OPTIONS(description = "Logical extraction date."),
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
  raw_channel_json STRING OPTIONS(description = "Raw YouTube channel response for audit/debugging.")
)
PARTITION BY run_date
CLUSTER BY channel_id
OPTIONS(description = "Raw channel metadata snapshots for channels found in the video extraction.");
