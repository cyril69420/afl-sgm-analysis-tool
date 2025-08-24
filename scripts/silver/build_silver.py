#!/usr/bin/env python
"""
Build the Silver layer from the Bronze data.

This script reads raw Bronze tables (event URLs, fixtures, odds, and
optional weather) and produces a set of clean, deduplicated tables
ready for downstream analytics. The resulting Parquet files are
written into the ``silver`` directory and overwrite any existing
datasets. Idempotency is achieved by selecting only the most recent
records for each key and removing duplicate rows.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from datetime import datetime
import polars as pl
import duckdb

from schemas.bronze import BronzeEventUrl, BronzeFixtureRow, BronzeOddsSnapshotRow, BronzeOddsHistoryRow


def build_silver_event_urls(bronze_root: Path, silver_root: Path, season: int) -> None:
    """Construct the silver event_urls table from bronze data.

    This function selects the latest record for each (bookmaker, event_url)
    combination based on the ``last_seen_utc`` timestamp and computes
    additional fields such as ``is_upcoming`` and ``seen_span_s``.
    """
    bronze_dir = bronze_root / "event_urls"
    if not bronze_dir.exists():
        logging.warning("Bronze event_urls directory does not exist: %s", bronze_dir)
        return
    # Read all Parquet files using DuckDB for efficiency
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{bronze_dir}/**/*.parquet')")
    except Exception:
        logging.warning("No bronze event URL files found in %s", bronze_dir)
        return
    df = con.execute(
        """
        WITH ranked AS (
          SELECT *,
                 row_number() OVER (PARTITION BY bookmaker, event_url ORDER BY last_seen_utc DESC) AS rn
          FROM events
          WHERE season = ?
        )
        SELECT *,
               (CASE WHEN status ILIKE '%upcoming%' THEN TRUE ELSE FALSE END) AS is_upcoming,
               CAST(strftime(last_seen_utc, '%s') AS BIGINT) - CAST(strftime(first_seen_utc, '%s') AS BIGINT) AS seen_span_s
        FROM ranked
        WHERE rn = 1
        """,
        [season]
    ).pl()
    if df.height == 0:
        logging.info("No event URLs for season %s", season)
        return
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "event_urls.parquet"
    df.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver event_urls with %d rows to %s", df.height, out_path)


def build_silver_odds(bronze_root: Path, silver_root: Path, season: int) -> None:
    """Construct the silver odds table by combining snapshots and history.

    Deduplicate rows on the full key and normalise text casing. Raw payload
    is not retained in the Silver layer.
    """
    odds_snap_dir = bronze_root / "odds" / "snapshots"
    odds_hist_dir = bronze_root / "odds" / "history"
    files = []
    if odds_snap_dir.exists():
        files += [p.as_posix() for p in odds_snap_dir.glob("**/*.parquet")]
    if odds_hist_dir.exists():
        files += [p.as_posix() for p in odds_hist_dir.glob("**/*.parquet")]
    if not files:
        logging.info("No bronze odds files found; skipping silver odds")
        return
    df = pl.read_parquet(files)
    # Filter by season
    # Season is stored in separate path; not present in odds rows. We'll read season from event_urls.
    # To join season and round we need event_urls mapping. We'll derive later in Gold.
    # Remove raw_payload for Silver
    if "raw_payload" in df.columns:
        df = df.drop("raw_payload")
    # Normalise text fields
    df = df.with_columns([
        pl.col("market_group").str.to_lowercase().alias("market_group"),
        pl.col("market_name").str.to_lowercase().alias("market_name"),
        pl.col("selection").str.to_lowercase().alias("selection")
    ])
    # Deduplicate on key columns
    unique_cols = ["bookmaker", "event_url", "market_group", "market_name", "selection", "line", "decimal_odds", "captured_at_utc"]
    df = df.unique(subset=unique_cols)
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "odds.parquet"
    df.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver odds with %d rows to %s", df.height, out_path)


def build_silver_fixtures(bronze_root: Path, silver_root: Path, season: int) -> None:
    """Write the fixtures table into Silver without modification.

    Fixtures are assumed to already be clean in Bronze. Source timezone
    is preserved.
    """
    fixtures_dir = bronze_root / "fixtures"
    files = list(fixtures_dir.glob("*.parquet"))
    if not files:
        logging.info("No bronze fixtures found; skipping silver fixtures")
        return
    df = pl.read_parquet([p.as_posix() for p in files])
    df = df.filter(pl.col("season") == season)
    if df.height == 0:
        logging.info("No fixtures for season %s", season)
        return
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "fixtures.parquet"
    df.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver fixtures with %d rows to %s", df.height, out_path)


def build_silver_weather(bronze_root: Path, silver_root: Path, season: int) -> None:
    """Optional weather aggregation for Silver.

    This function aligns weather observations and forecasts to hourly bins
    around the fixture kickoff times. For now it concatenates forecast
    and history without alignment.
    """
    forecast_dir = bronze_root / "weather" / "forecast"
    history_dir = bronze_root / "weather" / "history"
    paths = []
    if forecast_dir.exists():
        paths += [p.as_posix() for p in forecast_dir.glob("**/*.parquet")]
    if history_dir.exists():
        paths += [p.as_posix() for p in history_dir.glob("**/*.parquet")]
    if not paths:
        logging.info("No bronze weather data; skipping silver weather")
        return
    df = pl.read_parquet(paths)
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "weather.parquet"
    df.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver weather with %d rows to %s", df.height, out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver layer from Bronze data")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season to build")
    parser.add_argument("--include-weather", action="store_true", help="Include weather in Silver")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    root = Path(args.root).resolve()
    bronze_root = root / "bronze"
    silver_root = root / "silver"
    build_silver_event_urls(bronze_root, silver_root, args.season)
    build_silver_odds(bronze_root, silver_root, args.season)
    build_silver_fixtures(bronze_root, silver_root, args.season)
    if args.include_weather:
        build_silver_weather(bronze_root, silver_root, args.season)


if __name__ == "__main__":
    main()