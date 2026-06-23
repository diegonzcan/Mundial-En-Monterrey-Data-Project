-- BigQuery GOLD layer DDL.
-- Replace `your_project.your_gold_dataset` with your own project and dataset.
-- GOLD tables power the dashboard: momentum, discovery, topics, channels, and KPIs.

CREATE SCHEMA IF NOT EXISTS `your_project.your_gold_dataset`
OPTIONS (
  description = "Dashboard-ready YouTube trend analytics tables."
);

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.video_momentum` (
  run_date DATE,
  video_id STRING,
  video_url STRING,
  title STRING,
  channel_id STRING,
  channel_title STRING,
  description STRING,
  thumbnail_url STRING,
  view_count INT64,
  like_count INT64,
  comment_count INT64,
  published_at TIMESTAMP,
  extracted_at TIMESTAMP,
  previous_view_count INT64,
  previous_like_count INT64,
  previous_comment_count INT64,
  views_gained INT64,
  likes_gained INT64,
  comments_gained INT64,
  pct_growth FLOAT64,
  momentum_score FLOAT64,
  video_age_hours INT64,
  gold_loaded_at TIMESTAMP
)
PARTITION BY run_date
CLUSTER BY video_id, channel_id
OPTIONS(description = "Video-level velocity metrics calculated from daily snapshots.");

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.video_discovery` (
  video_id STRING,
  title STRING,
  channel_title STRING,
  description STRING,
  view_count INT64,
  views_gained INT64,
  comments_gained INT64,
  likes_gained INT64,
  pct_growth FLOAT64,
  momentum_score FLOAT64,
  video_age_hours INT64,
  comment_rate FLOAT64,
  like_rate FLOAT64,
  discovery_type STRING
)
OPTIONS(description = "Ranked candidate videos such as hidden gems and conversation drivers.");

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.video_topics_daily` (
  run_date DATE,
  video_id STRING,
  topic_id STRING,
  topic_name STRING,
  macro_topic_id STRING,
  macro_topic_name STRING,
  sub_topic_id STRING,
  sub_topic_name STRING,
  topic_keywords STRING,
  macro_topic_keywords STRING,
  sub_topic_keywords STRING,
  topic_confidence FLOAT64,
  topic_method STRING,
  matched_source STRING,
  source_text STRING,
  processed_at TIMESTAMP
)
PARTITION BY run_date
CLUSTER BY macro_topic_id, sub_topic_id, video_id
OPTIONS(description = "Daily hierarchical topic assignment per video.");

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.topic_daily` (
  run_date DATE,
  topic_id STRING,
  topic_name STRING,
  macro_topic_id STRING,
  macro_topic_name STRING,
  sub_topic_id STRING,
  sub_topic_name STRING,
  keywords STRING,
  representative_videos STRING,
  sample_comments STRING,
  videos_count INT64,
  video_count INT64,
  total_views INT64,
  view_count_total INT64,
  total_likes INT64,
  total_comments INT64,
  comment_count_total INT64,
  avg_views_per_video FLOAT64,
  avg_likes_per_video FLOAT64,
  avg_comments_per_video FLOAT64,
  sentiment_avg FLOAT64,
  relevance_score FLOAT64,
  processed_at TIMESTAMP
)
PARTITION BY run_date
CLUSTER BY macro_topic_id, sub_topic_id
OPTIONS(description = "Daily aggregate metrics by macro topic and sub-topic.");

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.topic_trends_daily` (
  run_date DATE,
  topic_id STRING,
  topic_name STRING,
  macro_topic_id STRING,
  macro_topic_name STRING,
  sub_topic_id STRING,
  sub_topic_name STRING,
  keywords STRING,
  videos_count INT64,
  video_count INT64,
  total_views INT64,
  view_count_total INT64,
  total_likes INT64,
  total_comments INT64,
  comment_count_total INT64,
  relevance_score FLOAT64,
  previous_videos_count INT64,
  previous_total_views INT64,
  previous_total_likes INT64,
  previous_total_comments INT64,
  videos_count_change INT64,
  views_change INT64,
  likes_change INT64,
  comments_change INT64,
  processed_at TIMESTAMP
)
PARTITION BY run_date
CLUSTER BY macro_topic_id, sub_topic_id
OPTIONS(description = "Topic-level daily changes calculated from topic_daily.");

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.channel_daily` (
  run_date DATE,
  channel_id STRING,
  channel_title STRING,
  videos_count INT64,
  total_views INT64,
  total_likes INT64,
  total_comments INT64,
  subscriber_count INT64,
  total_channel_views INT64,
  total_channel_videos INT64,
  avg_views_per_video FLOAT64,
  avg_comments_per_video FLOAT64,
  processed_at TIMESTAMP
)
PARTITION BY run_date
CLUSTER BY channel_id
OPTIONS(description = "Dashboard-ready channel performance aggregates.");

CREATE TABLE IF NOT EXISTS `your_project.your_gold_dataset.dashboard_summary_daily` (
  run_date DATE,
  videos_monitored INT64,
  channels_monitored INT64,
  topics_detected INT64,
  comments_available INT64,
  total_views INT64,
  total_likes INT64,
  total_comments INT64,
  top_topic_id STRING,
  top_topic_name STRING,
  top_topic_comments INT64,
  top_channel_id STRING,
  top_channel_title STRING,
  top_channel_views INT64,
  processed_at TIMESTAMP
)
PARTITION BY run_date
OPTIONS(description = "One-row-per-day KPI summary consumed by the static dashboard.");
