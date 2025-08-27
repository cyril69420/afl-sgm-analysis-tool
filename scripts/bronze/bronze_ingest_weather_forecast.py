#!/usr/bin/env python
"""
Ingest weather forecasts for upcoming AFL games into the Bronze layer.

The script looks up each fixture venue’s latitude and longitude from a
predefined lookup table and queries the selected weather provider for
hourly forecasts around the scheduled kickoff time. Each forecast row
includes the provider name, venue, coordinates, the model run time,
valid time, lead time in hours and a handful of standard meteorological
variables. Raw JSON payloads are retained for provenance.

Currently only a dummy provider is implemented to illustrate the
expected schema. You can extend this script to call APIs such as
Open‑Meteo, BoM, or other services by reading API keys from
environment variables.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import duckdb

from ._shared import (
    utc_now,
    parquet_write,
    load_yaml,
    load_env,
)

from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    preview_or_write,
    echo_df_info,
)

from schemas.bronze import BronzeWeatherRow

logger = logging.getLogger(__name__)


VENUE_LOOKUP: Dict[str, Dict[str, float]] = {
    # Example venue coordinates. Replace with real data or load from a file.
    "Example Stadium": {"lat": -37.8136, "lon": 144.9631, "tz": "Australia/Melbourne"},
}


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest weather forecast data into Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="Season year")
    parser.add_argument("--provider", default=None, help="Weather provider override (default from settings.yaml)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
add_common_test_flags(parser)  # adds --dry-run and --limit

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
            logger.warning("No coordinates found for venue %s; skipping", venue)
            continue
        lat, lon, tz = info["lat"], info["lon"], info["tz"]
        kickoff = fixture["scheduled_time_utc"]
        # For demonstration we simulate forecasts at kickoff time ± 2 hours
        run_time = utc_now()
        for offset_hr in [-2, -1, 0, 1, 2]:
            valid_time = kickoff + timedelta(hours=offset_hr)
            lead_time_hr = int((valid_time - run_time).total_seconds() // 3600)
            row = {
                "provider": provider,
                "venue": venue,
                "lat": lat,
                "lon": lon,
                "run_time_utc": run_time,
                "valid_time_utc": valid_time,
                "lead_time_hr": lead_time_hr,
                "temp_c": 20.0 + offset_hr,  # dummy gradient
                "wind_speed_ms": 5.0,
                "wind_gust_ms": 8.0,
                "rain_prob": 0.2,
                "pressure_hpa": 1015.0,
                "cloud_pct": 50.0,
                "raw_payload": json.dumps({"dummy": True}),
            }
            try:
                obj = BronzeWeatherRow.model_validate(row)
                rows.append(obj.model_dump())
            except Exception as exc:
                logger.error("Validation error for weather row %s: %s", row, exc)

    if not rows:
        logger.info("No weather forecast rows to write")
        return

    df_out = maybe_limit_df(df_out, args.limit)
preview_or_write(
    df_out,
    dry_run=args.dry_run,
    out_subdir="<bronze-subdir>",   # see mapping below
    out_stem="<file-stem>",         # short, deterministic
    fmt="parquet"                   # keep CSV if you must, parquet preferred
)



if __name__ == "__main__":
    main()