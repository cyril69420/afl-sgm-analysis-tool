#!/usr/bin/env python
"""
Ingest weather forecasts for upcoming AFL games into Bronze.

Adds --dry-run/--limit test flags and keeps partitioned writes.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import polars as pl

from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    echo_df_info,
    utc_now,
    parquet_write,
    load_yaml,
    load_env,
)
from schemas.bronze import BronzeWeatherRow

LOG = logging.getLogger(__name__)

# TODO: replace with real venue lat/lon/tz config
VENUE_LOOKUP: Dict[str, Dict[str, float | str]] = {
    "Example Stadium": {"lat": -37.8136, "lon": 144.9631, "tz": "Australia/Melbourne"},
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest weather forecast data into Bronze")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="Season year")
    parser.add_argument("--provider", default=None, help="Weather provider override")
    parser.add_argument("--log-level", default="INFO")
    add_common_test_flags(parser)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml") or {}
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    provider = args.provider or settings.get("weather", {}).get("provider", "open-meteo")

    fixtures_root = project_root / "bronze" / "fixtures"
    if not fixtures_root.exists():
        LOG.warning("Fixtures dataset not found at %s. Run bronze_ingest_games first.", fixtures_root)
        return 0

    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW fixtures AS SELECT * FROM read_parquet('{fixtures_root.as_posix()}/**/*.parquet')")
    except duckdb.IOException:
        LOG.warning("No fixtures parquet found under %s", fixtures_root)
        return 0

    fixtures_df = con.execute("SELECT * FROM fixtures WHERE season = ?", [season]).df()
    if fixtures_df.empty:
        LOG.warning("No fixtures found for season %s", season)
        return 0

    rows = []
    for _, fixture in fixtures_df.iterrows():
        venue = fixture["venue"]
        info = VENUE_LOOKUP.get(venue)
        if not info:
            LOG.warning("No coordinates for venue %s; skipping", venue)
            continue
        lat, lon, tz = info["lat"], info["lon"], info["tz"]
        kickoff = fixture["scheduled_time_utc"]
        run_time = utc_now()
        for offset_hr in [-2, -1, 0, 1, 2]:
            valid_time = kickoff + timedelta(hours=offset_hr)
            lead_time_hr = int((valid_time - run_time).total_seconds() // 3600)
            rec = {
                "provider": provider,
                "venue": venue,
                "lat": float(lat),
                "lon": float(lon),
                "run_time_utc": run_time,
                "valid_time_utc": valid_time,
                "lead_time_hr": lead_time_hr,
                "temp_c": 20.0 + offset_hr,
                "wind_speed_ms": 5.0,
                "wind_gust_ms": 8.0,
                "rain_prob": 0.2,
                "pressure_hpa": 1015.0,
                "cloud_pct": 50.0,
                "raw_payload": json.dumps({"dummy": True}),
            }
            try:
                obj = BronzeWeatherRow.model_validate(rec)
                rows.append(obj.model_dump())
            except Exception as exc:
                LOG.error("Validation error: %s", exc)

    if not rows:
        LOG.info("No forecast rows generated.")
        return 0

    df = pl.DataFrame(rows)
    df = maybe_limit_df(df, args.limit)
    echo_df_info(df, note="weather_forecast (post-limit)")

    if args.dry_run:
        LOG.info("DRY RUN: skipping writes")
        return 0

    output_root = project_root / "bronze" / "weather" / "forecast"
    parquet_write(df, output_root, partition_cols=["venue"])
    LOG.info("Wrote %d rows → %s", len(df), output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
