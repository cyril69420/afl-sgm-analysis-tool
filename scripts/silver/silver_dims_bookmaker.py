#!/usr/bin/env python
"""
Create/refresh dim_bookmaker from Bronze/Silver odds.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
from pathlib import Path

import duckdb
import pandas as pd

def _setup_logger(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )

def _bk_id(name: str) -> str:
    return "bk_" + hashlib.md5(name.strip().lower().encode("utf-8")).hexdigest()[:10]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", default="bronze", type=Path)
    ap.add_argument("--silver-dir", default="silver", type=Path)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    _setup_logger(args.log_level)

    con = duckdb.connect()
    con.execute("PRAGMA threads=8;")

    live_glob = str((args.bronze_dir / "odds/snapshots/**/*.parquet")).replace("\\", "/")
    hist_glob = str((args.bronze_dir / "odds/history/**/*.parquet")).replace("\\", "/")
    silv_glob = str((args.silver_dir / "f_odds_snapshot/**/*.parquet")).replace("\\", "/")

    names = set()
    for src, glob in [("live", live_glob), ("hist", hist_glob), ("silver", silv_glob)]:
        try:
            df = con.execute(f"SELECT DISTINCT COALESCE(bookmaker, bookmaker_name) AS bookmaker FROM read_parquet('{glob}', hive_partitioning=1)").fetch_df()
            names |= set(df["bookmaker"].dropna().astype(str).str.strip())
        except Exception:
            pass

    if not names:
        logging.warning("No bookmaker names found; skipping dim_bookmaker.")
        return

    out = pd.DataFrame(sorted(names), columns=["bookmaker_name"])
    out["bookmaker_id"] = out["bookmaker_name"].map(_bk_id)

    out_path = args.silver_dir / "dim_bookmaker.parquet"
    out.to_parquet(out_path, index=False)
    logging.info("Wrote %s (%d rows)", out_path, len(out))

if __name__ == "__main__":
    main()
