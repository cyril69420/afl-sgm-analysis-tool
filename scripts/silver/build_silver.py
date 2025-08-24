#!/usr/bin/env python
"""
Build the Silver layer from the Bronze data with enhancements.

The Silver layer provides clean, enriched and deduplicated datasets
suitable for analytics and SGM construction.  Compared to the original
implementation this script:

  * Adds quality flags to the ``event_urls`` table (``is_upcoming`` and
    ``seen_span_s``) and normalises status text.
  * Combines live and historical odds into a unified ``odds`` table
    with a ``source_kind`` column and joins season, round and fixture
    metadata (game_key, home, away, venue).
  * Enriches fixtures with venue coordinates and timezone metadata
    loaded from ``config/venues.yaml`` and computes the local scheduled
    time in the venue timezone.
  * Optionally includes concatenated weather data from forecast and
    observed feeds.

Outputs are written into the ``silver`` directory and overwrite
existing files.  Idempotency is achieved by selecting the most recent
record per key and deduplicating where appropriate.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List

import polars as pl
import duckdb
from zoneinfo import ZoneInfo

from schemas.bronze import (
    BronzeEventUrl,
    BronzeFixtureRow,
    BronzeOddsSnapshotRow,
    BronzeOddsHistoryRow,
    BronzeWeatherRow,
)
# We replicate load_yaml and load_venue_lookup here to avoid import issues
import yaml


def load_yaml(path: str | Path) -> dict:
    """Load a YAML file into a Python dictionary."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_venue_lookup(path: str | Path) -> dict:
    """Load the venue lookup table (lat, lon, tz) from YAML."""
    p = Path(path)
    if not p.exists():
        logging.warning("Venue lookup file not found at %s", p)
        return {}
    data = load_yaml(p)
    return {str(k): v for k, v in data.items() if isinstance(v, dict)}


def build_silver_event_urls(bronze_root: Path, silver_root: Path, season: int) -> None:
    """Construct the silver event_urls table from bronze data.

    The table selects the latest record for each (bookmaker, event_url)
    combination based on ``last_seen_utc`` and computes additional
    fields such as ``is_upcoming`` (boolean) and ``seen_span_s`` (age
    span in seconds).  Status values are normalised to lowercase.
    """
    bronze_dir = bronze_root / "event_urls"
    if not bronze_dir.exists():
        logging.warning("Bronze event_urls directory does not exist: %s", bronze_dir)
        return
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
        SELECT bookmaker, event_url, season, round, competition,
               LOWER(status) AS status,
               first_seen_utc, last_seen_utc,
               (last_seen_utc >= CURRENT_TIMESTAMP) AS is_upcoming,
               CAST(strftime(last_seen_utc, '%s') AS BIGINT) - CAST(strftime(first_seen_utc, '%s') AS BIGINT) AS seen_span_s,
               source_page, discovery_run_id
        FROM ranked WHERE rn = 1
        """,
        [season],
    ).pl()
    if df.height == 0:
        logging.info("No event URLs for season %s", season)
        return
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "event_urls.parquet"
    df.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver event_urls with %d rows to %s", df.height, out_path)


def build_silver_fixtures(bronze_root: Path, silver_root: Path, season: int) -> pl.DataFrame:
    """Clean and enrich the fixtures table.

    Fixtures are loaded from the Bronze layer and enriched with venue
    coordinates and timezone information.  A new column
    ``scheduled_time_local`` expresses the kick‑off in the venue's
    timezone.  The resulting DataFrame is returned for later joins and
    also written to disk.
    """
    fixtures_dir = bronze_root / "fixtures"
    files = list(fixtures_dir.glob("*.parquet"))
    if not files:
        logging.info("No bronze fixtures found; skipping silver fixtures")
        return pl.DataFrame()
    df = pl.read_parquet([p.as_posix() for p in files])
    df = df.filter(pl.col("season") == season)
    if df.height == 0:
        logging.info("No fixtures for season %s", season)
        return pl.DataFrame()
    # Load venue lookup for lat/lon/tz
    venue_lookup = load_venue_lookup(bronze_root.parent / "config" / "venues.yaml")
    # Add lat, lon, tz and local time
    def enrich(row: dict) -> dict:
        meta = venue_lookup.get(row["venue"], {})
        lat = meta.get("lat")
        lon = meta.get("lon")
        tzname = meta.get("tz") or row.get("source_tz")
        tzinfo = None
        try:
            tzinfo = ZoneInfo(tzname) if tzname else None
        except Exception:
            tzinfo = None
        # Compute local time if tz available
        sched_utc: datetime = row["scheduled_time_utc"]
        if tzinfo:
            sched_local = sched_utc.astimezone(tzinfo)
        else:
            sched_local = sched_utc
        row["lat"] = lat
        row["lon"] = lon
        row["tz"] = tzname
        row["scheduled_time_local"] = sched_local
        return row
    enriched = [enrich(r) for r in df.to_dicts()]
    out_df = pl.DataFrame(enriched)
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "fixtures.parquet"
    out_df.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver fixtures with %d rows to %s", out_df.height, out_path)
    return out_df


def build_silver_odds(bronze_root: Path, silver_root: Path, season: int, fixtures_df: pl.DataFrame) -> None:
    """Construct the silver odds table by combining snapshots and history.

    This function reads both live snapshots and historical odds from the
    Bronze layer, normalises text casing, deduplicates rows and
    attaches season, round and fixture metadata.  A ``source_kind``
    column distinguishes live versus historical prices.  Raw payloads
    are dropped in the Silver layer.
    """
    snap_dir = bronze_root / "odds" / "snapshots"
    hist_dir = bronze_root / "odds" / "history"
    paths: List[str] = []
    kind_labels: List[str] = []
    if snap_dir.exists():
        snap_files = [p.as_posix() for p in snap_dir.glob("**/*.parquet")]
        paths.extend(snap_files)
        kind_labels.extend(["live"] * len(snap_files))
    if hist_dir.exists():
        hist_files = [p.as_posix() for p in hist_dir.glob("**/*.parquet")]
        paths.extend(hist_files)
        kind_labels.extend(["historical"] * len(hist_files))
    if not paths:
        logging.info("No bronze odds files found; skipping silver odds")
        return
    # Read all odds files; attach source_kind based on file origin
    odds_list = []
    for path in paths:
        df = pl.read_parquet(path)
        kind = "live" if "snapshots" in path else "historical"
        df = df.with_columns([pl.lit(kind).alias("source_kind")])
        odds_list.append(df)
    odds = pl.concat(odds_list, how="vertical")
    # Normalise text fields
    for col in ["market_group", "market_name", "selection"]:
        if col in odds.columns:
            odds = odds.with_columns(pl.col(col).cast(str).str.to_lowercase().alias(col))
    # Remove raw_payload
    if "raw_payload" in odds.columns:
        odds = odds.drop("raw_payload")
    # Deduplicate on key columns (bookmaker,event_url,market_group,market_name,selection,line,decimal_odds,captured_at_utc,source_kind)
    unique_cols = [
        "bookmaker",
        "event_url",
        "market_group",
        "market_name",
        "selection",
        "line",
        "decimal_odds",
        "captured_at_utc",
        "source_kind",
    ]
    odds = odds.unique(subset=unique_cols)
    # Join season, round from event_urls for partitioning
    event_urls_path = bronze_root / "event_urls"
    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{event_urls_path}/**/*.parquet')")
    events_df = con.execute("SELECT event_url, season, round FROM events WHERE season = ?", [season]).df()
    if events_df.empty:
        logging.warning("No event URL records for odds join")
        events_pl = pl.DataFrame()
    else:
        events_pl = pl.from_pandas(events_df)
    if not events_pl.is_empty():
        odds = odds.join(events_pl, on="event_url", how="left")
    # Join fixture metadata (game_key, home, away, venue) if available
    if fixtures_df.height > 0:
        fixtures_subset = fixtures_df.select(["game_key", "home", "away", "venue", "season", "round", "bookmaker_event_url"])
        fixtures_subset = fixtures_subset.rename({"bookmaker_event_url": "event_url"})
        odds = odds.join(fixtures_subset, on=["event_url", "season", "round"], how="left")
    # Write to Parquet
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "odds.parquet"
    odds.write_parquet(out_path, compression="zstd")
    logging.info("Wrote silver odds with %d rows to %s", odds.height, out_path)


def build_silver_weather(bronze_root: Path, silver_root: Path, season: int) -> None:
    """Aggregate weather data for Silver.

    Concatenate forecast and history data from the Bronze layer.  A
    future enhancement could align weather to fixtures; for now we
    simply union the two feeds.
    """
    forecast_dir = bronze_root / "weather" / "forecast"
    history_dir = bronze_root / "weather" / "history"
    paths: List[str] = []
    if forecast_dir.exists():
        paths += [p.as_posix() for p in forecast_dir.glob("**/*.parquet")]
    if history_dir.exists():
        paths += [p.as_posix() for p in history_dir.glob("**/*.parquet")]
    if not paths:
        logging.info("No bronze weather data; skipping silver weather")
        return
    df = pl.read_parquet(paths)
    # Partition by season/venue if season column exists; otherwise write as is
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
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    root = Path(args.root).resolve()
    bronze_root = root / "bronze"
    silver_root = root / "silver"
    # Build components
    build_silver_event_urls(bronze_root, silver_root, args.season)
    fixtures_df = build_silver_fixtures(bronze_root, silver_root, args.season)
    build_silver_odds(bronze_root, silver_root, args.season, fixtures_df)
    if args.include_weather:
        build_silver_weather(bronze_root, silver_root, args.season)


if __name__ == "__main__":
    main()