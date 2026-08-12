import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["ALPHA_VANTAGE_API_KEY"]
BASE_URL = "https://www.alphavantage.co/query"
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# Start small — respect the 5 req/min, 25 req/day free tier limit
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]


def fetch_endpoint(function: str, symbol: str) -> dict:
    """Hit a single Alpha Vantage endpoint for a given symbol."""
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": API_KEY,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Alpha Vantage returns 200 even on errors — check for that
    if "Error Message" in data:
        raise ValueError(f"API error for {symbol}/{function}: {data['Error Message']}")
    if "Note" in data:
        # This usually means you hit the rate limit
        raise RuntimeError(f"Rate limit hit: {data['Note']}")

    return data


def save_raw(data: dict, function: str, symbol: str) -> Path:
    """Dump raw JSON to disk with an ingestion timestamp in the filename."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = RAW_DATA_DIR / function.lower()
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol}_{ts}.json"

    # Wrap with metadata so bronze layer has lineage info later
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


def main():
    for symbol in TICKERS:
        for function in ["TIME_SERIES_DAILY", "OVERVIEW"]:
            print(f"Fetching {function} for {symbol}...")
            try:
                data = fetch_endpoint(function, symbol)
                path = save_raw(data, function, symbol)
                print(f"  -> saved to {path}")
            except (ValueError, RuntimeError) as e:
                print(f"  -> FAILED: {e}")

            # Free tier: 5 requests/min = 1 every 12s minimum, pad to be safe
            time.sleep(13)


if __name__ == "__main__":
    main()