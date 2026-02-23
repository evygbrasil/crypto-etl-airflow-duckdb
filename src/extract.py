from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils import ensure_dir, partition_path

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=20))
def fetch_prices(ids: list[str], vs_currency: str = "usd") -> dict:
    params = {
        "ids": ",".join(ids),
        "vs_currencies": vs_currency,
        "include_last_updated_at": "true",
    }
    r = requests.get(COINGECKO_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_to_raw(ids: list[str], raw_base_dir: str = "/opt/airflow/data/raw") -> Path:
    now = datetime.now(timezone.utc)
    data = fetch_prices(ids=ids, vs_currency="usd")

    out_dir = ensure_dir(partition_path(raw_base_dir, now))
    out_file = out_dir / f"coingecko_prices_{now.strftime('%Y%m%dT%H%M%SZ')}.json"

    payload = {
        "extracted_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "coingecko",
        "data": data,
    }

    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_file

if __name__ == "__main__":
    p = extract_to_raw(ids=["bitcoin", "ethereum", "solana"])
    print(f"RAW saved: {p}")