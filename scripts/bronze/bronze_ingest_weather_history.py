#!/usr/bin/env python
"""
Enhanced historical weather ingestion for the Bronze layer.

This script fetches observed (historical) weather at the scheduled
kick‑off time for completed fixtures.  It calls the Open‑Meteo
Archive API to retrieve hourly observations on the day of the match
and extracts the record closest to kick‑off.  The resulting rows
conform to the :class:`schemas.bronze.BronzeWeatherRow` schema.  They
are written to ``bronze/weather/history`` partitioned by venue.

If the API is unavailable or a record is not found, the script falls
back to a deterministic default row.  Additional providers could be
added via ``settings.yaml`` in the future.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import duckdb
import requests

from ._shared import (
    parquet_write,
    load_yaml,
    load_env,
    load_venue_lookup,
)
from schemas.bronze import BronzeWeatherRow

logger = logging.getLogger(__name__)


def fetch_open_meteo_observation(lat: float, lon: float, date_str: str) -> dict:
    """Fetch historical weather observations for a given date.

    Parameters
    ----------
    lat, lon : float
        Coordinates.
    date_str : str
        Date in YYYY-MM-DD format.  Only one day is retrieved.

    Returns
    -------
    dict
        Parsed JSON response from Open‑Meteo Archive API.
    """
    hourly_vars = [
        "temperature_2m",
        "precipitation_probability",
        "wind_speed_10m",
        "wind_gusts_10m",
        "surface_pressure",
        "cloud_cover",
    ]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": ",".join(hourly_vars),
        "timezone": "UTC",
    }
    url = "https://archive-api.open-meteo.com/v1/archive"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("Open‑Meteo archive request failed for (%s, %s, %s): %s", lat, lon, date_str, exc)
        return {}


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest historical weather data into Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="Season year")
    parser.add_argument("--provider", default=None, help="Weather provider override (default from settings.yaml)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
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
    # Select only fixtures that have already occurred (scheduled_time_utc < now)
    now_utc = datetime.now(timezone.utc)
    fixtures_df = con.execute(
        "SELECT * FROM fixtures WHERE season = ? AND scheduled_time_utc < ?",
        [season, now_utc],
    ).df()
    if fixtures_df.empty:
        logger.warning("No past fixtures found for season %s", season)
        return
    venue_lookup = load_venue_lookup(project_root / "config" / "venues.yaml")
    rows: List[dict] = []
    for _, fix in fixtures_df.iterrows():
        venue = fix["venue"]
        meta = venue_lookup.get(venue)
        if not meta:
            logger.warning("No coordinates for venue %s; skipping", venue)
            continue
        lat, lon, tz = meta["lat"], meta["lon"], meta.get("tz", "UTC")
        kickoff: datetime = fix["scheduled_time_utc"]
        date_str = kickoff.date().isoformat()
        if provider.lower() == "open-meteo":
            obs = fetch_open_meteo_observation(lat, lon, date_str)
        else:
            logger.error("Unsupported provider %s", provider)
            return
        row_added = False
        if obs and "hourly" in obs:
            hourly = obs["hourly"]
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            rain_probs = hourly.get("precipitation_probability", [])
            wind_speeds = hourly.get("wind_speed_10m", [])
            gusts = hourly.get("wind_gusts_10m", [])
            pressures = hourly.get("surface_pressure", [])
            clouds = hourly.get("cloud_cover", [])
            # Find time equal or nearest to kick‑off
            best_idx = None
            min_diff = None
            for i, tstr in enumerate(times):
                try:
                    vt = datetime.fromisoformat(tstr.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    continue
                diff = abs((vt - kickoff).total_seconds())
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    best_idx = i
            if best_idx is not None and min_diff is not None and min_diff <= 3 * 3600:
                i = best_idx
                valid_time = datetime.fromisoformat(times[i].replace("Z", "+00:00")).astimezone(timezone.utc)
                row = {
                    "provider": provider,
                    "venue": venue,
                    "lat": lat,
                    "lon": lon,
                    "run_time_utc": valid_time,  # for observations run_time == valid_time
                    "valid_time_utc": valid_time,
                    "lead_time_hr": 0,
                    "temp_c": temps[i] if i < len(temps) else None,
                    "wind_speed_ms": wind_speeds[i] if i < len(wind_speeds) else None,
                    "wind_gust_ms": gusts[i] if i < len(gusts) else None,
                    "rain_prob": (rain_probs[i] / 100.0) if i < len(rain_probs) else None,
                    "pressure_hpa": pressures[i] if i < len(pressures) else None,
                    "cloud_pct": clouds[i] if i < len(clouds) else None,
                    "raw_payload": json.dumps(obs),
                }
                try:
                    obj = BronzeWeatherRow.model_validate(row)
                    rows.append(obj.model_dump())
                    row_added = True
                except Exception as exc:
                    logger.error("Validation error for weather history row %s: %s", row, exc)
        if not row_added:
            # Fallback: write default observed row
            row = {
                "provider": provider,
                "venue": venue,
                "lat": lat,
                "lon": lon,
                "run_time_utc": kickoff,
                "valid_time_utc": kickoff,
                "lead_time_hr": 0,
                "temp_c": 18.0,
                "wind_speed_ms": 4.0,
                "wind_gust_ms": 6.0,
                "rain_prob": 0.1,
                "pressure_hpa": 1015.0,
                "cloud_pct": 40.0,
                "raw_payload": json.dumps({"fallback": True}),
            }
            try:
                obj = BronzeWeatherRow.model_validate(row)
                rows.append(obj.model_dump())
            except Exception as exc:
                logger.error("Validation error for fallback weather row %s: %s", row, exc)
    if not rows:
        logger.info("No weather history rows to write")
        return
    df = pl.DataFrame(rows)
    output_root = project_root / "bronze" / "weather" / "history"
    partition_cols = ["venue"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d weather history rows to %s", len(df), output_root)


if __name__ == "__main__":
    main()