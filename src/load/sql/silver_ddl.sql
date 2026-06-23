-- BigQuery SILVER layer DDL.
-- Replace `your_project.your_silver_dataset` with your own project and dataset.
-- SILVER tables are cleaned and deduplicated versions of RAW pipeline outputs.

CREATE SCHEMA IF NOT EXISTS `your_project.your_silver_dataset`
OPTIONS (
  description = "Cleaned and deduplicated YouTube trend monitoring tables."
);

CREATE TABLE IF NOT EXISTS `your_project.your_silver_dataset.videos_daily` (
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
OPTIONS(description = "Daily deduplicated video snapshots used for trend calculations.");

CREATE TABLE IF NOT EXISTS `your_project.your_silver_dataset.comments_current` (
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
OPTIONS(description = "Latest known version of extracted public comments.");

CREATE TABLE IF NOT EXISTS `your_project.your_silver_dataset.channels_daily` (
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
OPTIONS(description = "Daily channel metadata snapshots joined into dashboard aggregates.");
