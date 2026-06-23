import os
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Add src to path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent))

from extract_videos import main as extract_videos
from extract_comments import main as extract_comments
from extract_channels import main as extract_channels
from load_bq import main as load_bq
from transform.silver import main as run_silver
from transform.gold import main as run_gold
from transform.topic_gold import (
    create_gold_tables as create_topic_gold_tables,
    process_date as process_topic_gold,
    rebuild_topic_trends,
)
from dashboard.generate_dashboard_json import main as generate_dashboard_json
from common import get_run_date
from google.cloud import bigquery


def main():
    print("Starting YouTube data extraction pipeline...")

    try:
        # Generate run_date once and pass to all extract/load jobs
        run_date = get_run_date()
        print(f"\nUsing run_date: {run_date}")

        print("\n1. Extracting videos...")
        extract_videos(run_date=run_date)
        print("Videos extracted successfully.")

        print("\n2. Extracting comments...")
        extract_comments(run_date=run_date)
        print("Comments extracted successfully.")

        print("\n3. Extracting channels...")
        extract_channels(run_date=run_date)
        print("Channels extracted successfully.")

        print("\n4. Loading raw data to BigQuery...")
        load_bq(run_date=run_date)
        print("Loaded raw data to BigQuery successfully.")

        print("\n5. Running silver transformations...")
        run_silver(run_date=run_date)
        print("Silver transformations completed successfully.")

        print("\n6. Running gold transformations...")
        run_gold()
        print("Gold transformations completed successfully.")

        print("\n7. Processing topic gold layer...")
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("Missing GCP_PROJECT_ID environment variable.")
        client = bigquery.Client(project=project_id)
        create_topic_gold_tables(client)
        process_topic_gold(client, run_date)
        rebuild_topic_trends(client)
        print("Topic gold processing completed successfully.")

        print("\n8. Generating dashboard JSON...")
        generate_dashboard_json()
        print("Dashboard JSON generated successfully.")

        print("\nPipeline completed successfully!")

    except Exception as e:
        print(f"\nPipeline failed: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
