#!/usr/bin/env python
"""
Ingest historical (observed) weather for completed AFL games into the Bronze layer.

The script queries a weather provider for observations at the time of
each past fixture. The schema matches that of the forecast ingestion
but with ``source_kind`` implicitly set to ``'observed'``. Dummy data is
generated in this example; extend the provider call to retrieve real
observations.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import duckdb

from ._shared import (
    parquet_write,
    load_yaml,
    load_env,
)
from schemas.bronze import BronzeWeatherRow

logger = logging.getLogger(__name__)

VENUE_LOOKUP: Dict[str, Dict[str, float]] = {
    "Example Stadium": {"lat": -37.8136, "lon": 144.9631, "tz": "Australia/Melbourne"},
}


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest historical weather data into Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="Season year")
    parser.add_argument("--provider", default=None, help="Weather provider override (default from settings.yaml)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    provider = args.provider or settings.get("weather", {}).get("provider", "open-meteo")

    # Read fixtures
    fixtures_root = project_root / "bronze" / "fixtures"
    if not fixtures_root.exists():
        logger.error("Fixtures dataset not found. Run bronze_ingest_games first.")
        return
    con = duckdb.connect()
    con.execute(f"CREATE VIEW fixtures AS SELECT * FROM read_parquet('{fixtures_root}/**/*.parquet')")
    fixtures_df = con.execute("SELECT * FROM fixtures WHERE season = ?", [season]).df()
    if fixtures_df.empty:
        logger.warning("No fixtures found for season %s", season)
        return

    rows = []
    for _, fixture in fixtures_df.iterrows():
        venue = fixture["venue"]
        info = VENUE_LOOKUP.get(venue)
        if not info:
            logger.warning("No coordinates for venue %s; skipping", venue)
            continue
        lat, lon, tz = info["lat"], info["lon"], info["tz"]
        # Observed weather at scheduled kick‑off time
        observation_time = fixture["scheduled_time_utc"]
        row = {
            "provider": provider,
            "venue": venue,
            "lat": lat,
            "lon": lon,
            "run_time_utc": observation_time,  # for observations run_time == valid_time
            "valid_time_utc": observation_time,
            "lead_time_hr": 0,
            "temp_c": 18.0,
            "wind_speed_ms": 4.0,
            "wind_gust_ms": 6.0,
            "rain_prob": 0.1,
            "pressure_hpa": 1017.0,
            "cloud_pct": 30.0,
            "raw_payload": json.dumps({"observed": True}),
        }
        try:
            obj = BronzeWeatherRow.model_validate(row)
            rows.append(obj.model_dump())
        except Exception as exc:
            logger.error("Validation error for weather history row %s: %s", row, exc)

    if not rows:
        logger.info("No weather history rows to write")
        return

    df = pl.DataFrame(rows)
    output_root = project_root / "bronze" / "weather" / "history"
    partition_cols = ["season", "round", "venue"] if "round" in df.columns else ["venue"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d weather history rows to %s", len(df), output_root)


if __name__ == "__main__":
    main()