import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from dagster import asset, multi_asset, AssetSpec, Definitions, get_dagster_logger, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject, DagsterDbtTranslator

load_dotenv()

API_KEY = os.environ["ALPHA_VANTAGE_API_KEY"]
BASE_URL = "https://www.alphavantage.co/query"
PROJECT_ID = os.environ["BQ_PROJECT_ID"]
DATASET = os.environ["BQ_DATASET"]

RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
FUNCTIONS = ["TIME_SERIES_DAILY", "OVERVIEW"]

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_resource = DbtCliResource(project_dir=dbt_project)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def warehouse_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--exclude", "scd_company_overview"], context=context).stream()


def fetch_endpoint(function: str, symbol: str) -> dict:
    params = {"function": function, "symbol": symbol, "apikey": API_KEY}
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "Error Message" in data:
        raise ValueError(f"API error for {symbol}/{function}: {data['Error Message']}")
    if "Note" in data:
        raise RuntimeError(f"Rate limit hit: {data['Note']}")
    return data


def save_raw(data: dict, function: str, symbol: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = RAW_DATA_DIR / function.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{symbol}_{ts}.json"
    payload = {
        "_ingested_at": ts,
        "_source": "alpha_vantage",
        "_function": function,
        "_symbol": symbol,
        "data": data,
    }
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
    return out_path


@asset
def raw_stock_files():
    """Pull data from Alpha Vantage and dump JSON to local disk (Phase 1, step 1)."""
    logger = get_dagster_logger()
    saved_paths = []
    for symbol in TICKERS:
        for function in FUNCTIONS:
            logger.info(f"Fetching {function} for {symbol}...")
            try:
                data = fetch_endpoint(function, symbol)
                path = save_raw(data, function, symbol)
                saved_paths.append(str(path))
                logger.info(f"Saved to {path}")
            except (ValueError, RuntimeError) as e:
                logger.error(f"FAILED {symbol}/{function}: {e}")
            time.sleep(13)
    return saved_paths


@multi_asset(
    specs=[
        AssetSpec("overview", deps=[raw_stock_files]),
        AssetSpec("time_series_daily", deps=[raw_stock_files]),
    ],
)
def bronze_tables():
    """Load raw JSON files into BigQuery bronze tables (Phase 1, step 2)."""
    logger = get_dagster_logger()
    client = bigquery.Client(project=PROJECT_ID)

    dataset_id = f"{PROJECT_ID}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id}")

    for function_name in ["time_series_daily", "overview"]:
        folder = RAW_DATA_DIR / function_name
        if not folder.exists():
            continue
        files = sorted(folder.glob("*.json"))
        if not files:
            continue

        rows = []
        for filepath in files:
            with open(filepath) as f:
                payload = json.load(f)
            rows.append({
                "ingested_at": payload["_ingested_at"],
                "source": payload["_source"],
                "function": payload["_function"],
                "symbol": payload["_symbol"],
                "raw_json": json.dumps(payload["data"]),
            })

        table_id = f"{PROJECT_ID}.{DATASET}.{function_name}"
        schema = [
            bigquery.SchemaField("ingested_at", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("function", "STRING"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("raw_json", "STRING"),
        ]
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
        load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
        load_job.result()
        logger.info(f"Loaded {len(rows)} rows into {table_id}")


defs = Definitions(
    assets=[raw_stock_files, bronze_tables, warehouse_dbt_assets],
    resources={"dbt": dbt_resource},
)