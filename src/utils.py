from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def safe_filename(ts_iso: str) -> str:
    # 2026-02-22T12:00:00Z -> 20260222T120000Z
    return ts_iso.replace("-", "").replace(":", "").replace("T", "T").replace("Z", "Z")

def partition_path(base: str | Path, dt_utc: datetime) -> Path:
    return Path(base) / f"year={dt_utc.year:04d}" / f"month={dt_utc.month:02d}" / f"day={dt_utc.day:02d}"