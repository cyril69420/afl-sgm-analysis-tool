#!/usr/bin/env python
"""
Enhanced weather forecast ingestion for the Bronze layer.

This script queries the Open‑Meteo API for hourly weather forecasts
around the scheduled kick‑off time of upcoming fixtures.  It replaces
the dummy implementation with a real HTTP integration.  Forecast data
include temperature, wind speed and gust, precipitation probability,
surface pressure and cloud cover.  Units are normalised and the raw
payload from the API is retained for provenance.

Each row conforms to the :class:`schemas.bronze.BronzeWeatherRow`
schema and is written into ``bronze/weather/forecast`` partitioned by
venue.  If no matching fixtures or weather data are found, the script
logs a warning and exits gracefully.

You may configure additional providers via ``config/settings.yaml``.
Open‑Meteo does not require an API key, but some providers may.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import duckdb
import requests

from ._shared import (
    utc_now,
    parquet_write,
    load_yaml,
    load_env,
    load_venue_lookup,
)
from schemas.bronze import BronzeWeatherRow

logger = logging.getLogger(__name__)


def fetch_open_meteo_forecast(lat: float, lon: float, tz: str) -> dict:
    """Fetch hourly forecast from Open‑Meteo for the next 7 days.

    Parameters
    ----------
    lat, lon : float
        Coordinates of the venue.
    tz : str
        IANA timezone name; used for output times.  Open‑Meteo accepts
        ``timezone`` parameter to return times in the specified zone or
        ``UTC``.

    Returns
    -------
    dict
        The parsed JSON response.
    """
    # Request hourly variables; you can extend this list as needed
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
        "hourly": ",".join(hourly_vars),
        "forecast_days": 7,
        "past_days": 0,
        "timezone": "UTC",
    }
    url = "https://api.open-meteo.com/v1/forecast"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("Open‑Meteo request failed for (%s, %s): %s", lat, lon, exc)
        return {}


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest weather forecast data into Bronze layer")
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
    fixtures_df = con.execute("SELECT * FROM fixtures WHERE season = ?", [season]).df()
    if fixtures_df.empty:
        logger.warning("No fixtures found for season %s", season)
        return
    venue_lookup = load_venue_lookup(project_root / "config" / "venues.yaml")
    rows: List[dict] = []
    for _, fix in fixtures_df.iterrows():
        venue = fix["venue"]
        meta = venue_lookup.get(venue)
        if not meta:
            logger.warning("No coordinates found for venue %s; skipping", venue)
            continue
        lat = meta["lat"]
        lon = meta["lon"]
        tz = meta.get("tz", "UTC")
        kickoff: datetime = fix["scheduled_time_utc"]
        # Fetch forecast once per fixture; could be cached per venue
        if provider.lower() == "open-meteo":
            forecast = fetch_open_meteo_forecast(lat, lon, tz)
        else:
            logger.error("Unsupported provider %s", provider)
            return
        if not forecast or "hourly" not in forecast:
            logger.warning("No forecast data returned for venue %s", venue)
            continue
        hourly = forecast.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        rain_probs = hourly.get("precipitation_probability", [])
        wind_speeds = hourly.get("wind_speed_10m", [])
        gusts = hourly.get("wind_gusts_10m", [])
        pressures = hourly.get("surface_pressure", [])
        clouds = hourly.get("cloud_cover", [])
        # Build a lookup by timestamp for convenience
        for i, tstr in enumerate(times):
            try:
                valid_time = datetime.fromisoformat(tstr.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue
            # Filter to ±2 hours around kickoff
            if abs((valid_time - kickoff).total_seconds()) > 2 * 3600:
                continue
            run_time = utc_now()
            lead_time_hr = int((valid_time - run_time).total_seconds() // 3600)
            row = {
                "provider": provider,
                "venue": venue,
                "lat": lat,
                "lon": lon,
                "run_time_utc": run_time,
                "valid_time_utc": valid_time,
                "lead_time_hr": lead_time_hr,
                "temp_c": temps[i] if i < len(temps) else None,
                "wind_speed_ms": wind_speeds[i] if i < len(wind_speeds) else None,
                "wind_gust_ms": gusts[i] if i < len(gusts) else None,
                "rain_prob": (rain_probs[i] / 100.0) if i < len(rain_probs) else None,
                "pressure_hpa": pressures[i] if i < len(pressures) else None,
                "cloud_pct": clouds[i] if i < len(clouds) else None,
                "raw_payload": json.dumps(forecast),
            }
            try:
                obj = BronzeWeatherRow.model_validate(row)
                rows.append(obj.model_dump())
            except Exception as exc:
                logger.error("Validation error for weather row %s: %s", row, exc)
    if not rows:
        logger.info("No weather forecast rows to write")
        return
    df = pl.DataFrame(rows)
    # Partition by venue; season and round are not present in weather rows
    partition_cols = ["venue"]
    output_root = project_root / "bronze" / "weather" / "forecast"
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d weather forecast rows to %s", len(df), output_root)


if __name__ == "__main__":
    main()