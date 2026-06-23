import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
PARQUET_DIR = DATA_DIR / "parquet"
ENV_FILE = BASE_DIR.parent / ".env"
BASE_URL = "https://www.googleapis.com/youtube/v3"


def load_env():
    load_dotenv(ENV_FILE)


def get_run_date() -> str:
    """Generate run_date based on Monterrey timezone (YYYY-MM-DD format)."""
    tz = ZoneInfo("America/Monterrey")
    today = datetime.now(tz).date()
    run_date = today - timedelta(days=1)
    return str(run_date)


def ensure_parquet_dir(path: Path = PARQUET_DIR):
    path.mkdir(parents=True, exist_ok=True)


def get_partition_dir(run_date: str | None = None) -> Path:
        return PARQUET_DIR if not run_date else PARQUET_DIR / str(run_date)


def resolve_parquet_path(filename: str, run_date: str | None = None) -> Path:
    path = get_partition_dir(run_date) / filename
    if path.exists():
        return path

    if run_date:
        raise FileNotFoundError(f"Parquet file not found for run_date {run_date}: {path}")

    root_file = PARQUET_DIR / filename
    if root_file.exists():
        return root_file

    candidate_files = sorted(
        PARQUET_DIR.glob(f"*/{filename}"),
        key=lambda p: p.parent.name,
    )
    if candidate_files:
        return candidate_files[-1]

    raise FileNotFoundError(f"Parquet file not found: {filename}")


def write_parquet(df: pd.DataFrame, filename: str, run_date: str | None = None) -> Path:
    target_dir = get_partition_dir(run_date)
    ensure_parquet_dir(target_dir)
    path = target_dir / filename
    df.to_parquet(path, index=False)
    return path


def read_parquet(filename: str, run_date: str | None = None) -> pd.DataFrame:
    path = resolve_parquet_path(filename, run_date)
    return pd.read_parquet(path)


def get_latest_videos_path(run_date: str | None = None) -> Path:
    if run_date:
        return resolve_parquet_path(f"youtube_worldcup_mty_videos_{run_date}.parquet")

    candidate_files = sorted(
        PARQUET_DIR.glob("*/youtube_worldcup_mty_videos_*.parquet"),
        key=lambda p: p.parent.name,
    )

    if candidate_files:
        return candidate_files[-1]

    raise FileNotFoundError("Latest videos parquet not found in data/parquet")


def load_latest_videos_df(run_date: str | None = None) -> pd.DataFrame:
    path = get_latest_videos_path(run_date)
    if not path.exists():
        raise FileNotFoundError(f"Latest videos parquet not found: {path}")
    return pd.read_parquet(path)


def get_latest_run_date(df: pd.DataFrame) -> str:
    if df.empty:
        raise ValueError("DataFrame is empty, no run_date available")
    if "run_date" not in df.columns:
        raise ValueError("DataFrame does not contain run_date")
    return df["run_date"].max()


def youtube_get(endpoint: str, params: dict) -> dict:
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY in environment")

    params["key"] = api_key
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)

    if response.status_code == 403 and endpoint == "commentThreads":
        return None

    response.raise_for_status()
    return response.json()
