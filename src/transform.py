from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from src.utils import ensure_dir, partition_path

def transform_raw_to_parquet(raw_json_path: str | Path, processed_base_dir: str = "/opt/airflow/data/processed") -> Path:
    raw_path = Path(raw_json_path)
    payload = json.loads(raw_path.read_text(encoding="utf-8"))

    extracted_at = payload["extracted_at_utc"]
    extracted_dt = datetime.strptime(extracted_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    records = []
    for coin_id, fields in payload["data"].items():
        records.append({
            "coin_id": coin_id,
            "price_usd": float(fields.get("usd")) if fields.get("usd") is not None else None,
            "last_updated_at_unix": int(fields.get("last_updated_at")) if fields.get("last_updated_at") is not None else None,
            "extracted_at_utc": extracted_at,
        })

    df = pd.DataFrame(records)

    out_dir = ensure_dir(partition_path(processed_base_dir, extracted_dt))
    out_file = out_dir / f"prices_{extracted_dt.strftime('%Y%m%dT%H%M%SZ')}.parquet"

    df.to_parquet(out_file, index=False)
    return out_file

if __name__ == "__main__":
    # exemplo manual
    import sys
    p = transform_raw_to_parquet(sys.argv[1])
    print(f"PARQUET saved: {p}")