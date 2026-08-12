import os
import json
import glob
from pathlib import Path

from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.environ["BQ_PROJECT_ID"]
DATASET = os.environ["BQ_DATASET"]
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

client = bigquery.Client(project=PROJECT_ID)


def ensure_dataset():
    """Create the bronze dataset if it doesn't exist yet."""
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")


def load_function_folder(function_name: str):
    """Load all JSON files for one function (e.g. 'overview') into its own bronze table."""
    folder = RAW_DATA_DIR / function_name
    if not folder.exists():
        print(f"No folder for {function_name}, skipping")
        return

    files = sorted(glob.glob(str(folder / "*.json")))
    if not files:
        print(f"No files found in {folder}, skipping")
        return

    rows = []
    for filepath in files:
        with open(filepath) as f:
            payload = json.load(f)
        # Store the whole payload as a JSON string in one column —
        # keeps bronze truly raw, no assumptions about structure yet
        rows.append({
            "ingested_at": payload["_ingested_at"],
            "source": payload["_source"],
            "function": payload["_function"],
            "symbol": payload["_symbol"],
            "raw_json": json.dumps(payload["data"]),
        })

    table_id = f"{PROJECT_ID}.{DATASET}.{function_name.lower()}"

    schema = [
        bigquery.SchemaField("ingested_at", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("function", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # reload clean state from raw files
    )

    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()  # wait for the job to finish

    print(f"Loaded {len(rows)} rows into {table_id}")


def main():
    ensure_dataset()
    for function_folder in ["time_series_daily", "overview"]:
        load_function_folder(function_folder)


if __name__ == "__main__":
    main()