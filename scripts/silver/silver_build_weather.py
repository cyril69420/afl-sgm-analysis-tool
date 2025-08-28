#!/usr/bin/env python
"""
Curate Bronze hourly weather forecasts into Silver:
  - f_weather_forecast_hourly  (Hive: season/round/venue_id)
  - v_weather_at_kickoff       (one row per game_key, nearest hour; tie -> later time)

Assumptions:
  Bronze forecast has hive season/round/venue=* and typical Open-Meteo fields.
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

def build(silver_dir: Path, bronze_dir: Path, overwrite: bool) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8;")

    hourly_glob = str((bronze_dir / "weather/forecast/**/*.parquet")).replace("\\", "/")
    fixture_path = str((silver_dir / "f_fixture.parquet")).replace("\\", "/")
    dim_venue = str((silver_dir / "dim_venue.parquet")).replace("\\", "/")

    out_hourly = str((silver_dir / "f_weather_forecast_hourly")).replace("\\", "/")
    out_atko = silver_dir / "v_weather_at_kickoff.parquet"

    # Read, normalise, and join venue IDs if present
    con.execute("""
    CREATE OR REPLACE TEMP VIEW h AS
    WITH raw AS (
      SELECT * FROM read_parquet($hourly, hive_partitioning=1)
    )
    SELECT
      COALESCE(game_key, match_key)                                      AS game_key,
      TRY_CAST(season AS INTEGER)                                        AS season,
      TRY_CAST(round AS INTEGER)                                         AS round,
      COALESCE(venue_id, venue_key)                                      AS venue_id,
      COALESCE(venue, venue_name, stadium)                               AS venue_name,
      TRY_CAST(COALESCE(forecast_time_utc, time_utc, ts_utc) AS TIMESTAMP) AS forecast_time_utc,
      TRY_CAST(COALESCE(temp_c, temperature_c, temperature) AS DOUBLE)   AS temp_c,
      TRY_CAST(COALESCE(wind_speed_ms, wind_ms, windspeed_ms) AS DOUBLE) AS wind_speed_ms,
      TRY_CAST(COALESCE(wind_gust_ms, windgust_ms) AS DOUBLE)            AS wind_gust_ms,
      TRY_CAST(COALESCE(rain_prob, precipitation_probability, precip_prob) AS DOUBLE) AS rain_prob,
      TRY_CAST(COALESCE(pressure_hpa, pressure) AS DOUBLE)               AS pressure_hpa,
      TRY_CAST(COALESCE(cloud_pct, cloud_cover_pct, cloud) AS DOUBLE)    AS cloud_pct,
      TRY_CAST(COALESCE(captured_at_utc, captured_utc, ts_capture) AS TIMESTAMP) AS captured_at_utc,
      COALESCE(provider, 'open-meteo')                                   AS provider
    FROM raw
    WHERE game_key IS NOT NULL
    """, {"hourly": hourly_glob})

    # If venue_id missing, derive from Silver dim_venue by name
    con.execute("""
    CREATE OR REPLACE TEMP VIEW h2 AS
    SELECT
      h.game_key, h.season, h.round,
      COALESCE(h.venue_id, v.venue_id) AS venue_id,
      h.forecast_time_utc, h.temp_c, h.wind_speed_ms, h.wind_gust_ms, h.rain_prob,
      h.pressure_hpa, h.cloud_pct, h.captured_at_utc, h.provider
    FROM h
    LEFT JOIN (SELECT * FROM read_parquet($dim_venue)) v
      ON lower(trim(h.venue_name)) = lower(trim(v.venue_name))
    """, {"dim_venue": dim_venue})

    # Write hourly partitioned
    duck_opts = f"(FORMAT PARQUET, PARTITION_BY (season, round, venue_id){', OVERWRITE_OR_IGNORE TRUE' if overwrite else ''})"
    Path(out_hourly).mkdir(parents=True, exist_ok=True)
    con.execute(f"""
      COPY (
        SELECT * FROM h2
      ) TO '{out_hourly}' {duck_opts};
    """)
    logging.info("Wrote f_weather_forecast_hourly -> %s", out_hourly)

    # Build at-kickoff view
    fixture_sql = """
      WITH h AS (SELECT * FROM read_parquet($hourly_out, hive_partitioning=1)),
           f AS (SELECT game_key, scheduled_time_utc FROM read_parquet($fixture))
      SELECT *
      FROM (
        SELECT
          h.*,
          abs(date_diff('minute', h.forecast_time_utc, f.scheduled_time_utc)) AS dist_min,
          row_number() OVER (
            PARTITION BY h.game_key
            ORDER BY dist_min ASC, h.forecast_time_utc DESC
          ) AS rn
        FROM h JOIN f USING (game_key)
      )
      WHERE rn = 1
    """
    atko = con.execute(
        fixture_sql,
        {"hourly_out": out_hourly, "fixture": fixture_path}
    ).fetch_df()

    if overwrite or not out_atko.exists():
        atko.to_parquet(out_atko, index=False)
    logging.info("Wrote v_weather_at_kickoff -> %s", out_atko)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", default="bronze", type=Path)
    ap.add_argument("--silver-dir", default="silver", type=Path)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    _setup_logger(args.log_level)
    build(args.silver_dir, args.bronze_dir, args.overwrite)

if __name__ == "__main__":
    main()
