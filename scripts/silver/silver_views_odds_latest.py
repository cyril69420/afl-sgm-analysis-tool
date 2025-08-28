#!/usr/bin/env python
"""
Materialize latest pre-kickoff odds per (game_key, bookmaker, market_group, market_name, selection, line).

Output:
  - silver/v_odds_latest_pre_ko.parquet
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

def _setup_logger(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )

def build(silver_dir: Path, overwrite: bool) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8;")

    odds_glob = str((silver_dir / "f_odds_snapshot/**/*.parquet")).replace("\\", "/")
    fixture_path = str((silver_dir / "f_fixture.parquet")).replace("\\", "/")
    out_path = silver_dir / "v_odds_latest_pre_ko.parquet"

    con.execute("""
    CREATE OR REPLACE TEMP VIEW s AS
      SELECT * FROM read_parquet($odds, hive_partitioning=1)
    """, {"odds": odds_glob})
    con.execute("""
    CREATE OR REPLACE TEMP VIEW f AS
      SELECT game_key, scheduled_time_utc FROM read_parquet($fixture)
    """, {"fixture": fixture_path})

    df = con.execute("""
      WITH pre AS (
        SELECT
          s.*,
          row_number() OVER (
            PARTITION BY game_key, bookmaker, market_group, market_name, selection, line
            ORDER BY captured_at_utc DESC
          ) AS rn_all,
          row_number() OVER (
            PARTITION BY game_key, bookmaker, market_group, market_name, selection, line
            ORDER BY captured_at_utc DESC
          ) FILTER (WHERE s.captured_at_utc <= f.scheduled_time_utc) AS rn_pre
        FROM s
        JOIN f USING (game_key)
      )
      SELECT * FROM pre
      WHERE rn_pre = 1  -- latest pre-KO
    """).fetch_df()

    if overwrite or not out_path.exists():
        df.to_parquet(out_path, index=False)
    else:
        logging.warning("Output exists and --overwrite not set: %s", out_path)

    logging.info("Wrote %s (%d rows)", out_path, len(df))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="silver", type=Path)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    _setup_logger(args.log_level)
    build(args.silver_dir, args.overwrite)

if __name__ == "__main__":
    main()
