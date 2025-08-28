#!/usr/bin/env python
"""
Unify Bronze live odds snapshots + historic closing odds into a clean, time-aware Silver fact.

Output:
  - silver/f_odds_snapshot/  (Hive: season/round/bookmaker)

Schema:
  game_key, season, round, bookmaker, market_group, market_name, selection,
  line, decimal_odds, captured_at_utc, source_kind
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

    live_glob = str((bronze_dir / "odds/snapshots/**/*.parquet")).replace("\\", "/")
    hist_glob = str((bronze_dir / "odds/history/**/*.parquet")).replace("\\", "/")
    fixture_path = str((silver_dir / "f_fixture.parquet")).replace("\\", "/")
    out_dir = str((silver_dir / "f_odds_snapshot")).replace("\\", "/")

    logging.info("Reading live snapshots: %s", live_glob)
    logging.info("Reading historic odds  : %s", hist_glob)

    # Robust column mapping for live snapshots
    # Expect captured_at_utc, decimal_odds, line, selection, market_*; tolerate variants.
    con.execute("""
    CREATE OR REPLACE TEMP VIEW v_live AS
    WITH raw AS (
      SELECT * FROM read_parquet($live, hive_partitioning=1)
    )
    SELECT
      COALESCE(game_key, match_key)                                  AS game_key,
      TRY_CAST(season AS INTEGER)                                    AS season,
      TRY_CAST(round AS INTEGER)                                     AS round,
      COALESCE(bookmaker, bookmaker_name)                            AS bookmaker,
      COALESCE(market_group, market_type, market)                    AS market_group,
      COALESCE(market_name, sub_market, market_group)                AS market_name,
      COALESCE(selection, outcome, runner, team)                     AS selection,
      TRY_CAST(COALESCE(line, handicap, total, points, spread) AS DOUBLE) AS line,
      TRY_CAST(COALESCE(decimal_odds, odds_decimal, price) AS DOUBLE) AS decimal_odds,
      TRY_CAST(COALESCE(captured_at_utc, captured_utc, ts_utc) AS TIMESTAMP) AS captured_at_utc,
      'live' AS source_kind
    FROM raw
    WHERE game_key IS NOT NULL AND bookmaker IS NOT NULL
    """, {"live": live_glob})

    # Historic: keep closing lines only; if no timestamp is present, align to fixture kickoff.
    con.execute("""
    CREATE OR REPLACE TEMP VIEW v_hist_base AS
    WITH raw AS (
      SELECT * FROM read_parquet($hist, hive_partitioning=1)
    )
    SELECT
      COALESCE(game_key, match_key)                                  AS game_key,
      TRY_CAST(season AS INTEGER)                                    AS season,
      TRY_CAST(round AS INTEGER)                                     AS round,
      COALESCE(bookmaker, 'market_consensus')                        AS bookmaker,
      COALESCE(market_group, market_type, market)                    AS market_group,
      COALESCE(market_name, sub_market, market_group)                AS market_name,
      COALESCE(selection, outcome, runner, team)                     AS selection,
      TRY_CAST(COALESCE(close_line, closing_line, line_close, line) AS DOUBLE) AS line,
      TRY_CAST(COALESCE(close, closing_odds, decimal_close, price_close, decimal_odds) AS DOUBLE) AS decimal_odds,
      COALESCE(captured_at_utc, captured_utc, ts_utc)                AS captured_at_utc_raw
    FROM raw
    WHERE game_key IS NOT NULL
    """, {"hist": hist_glob})

    # Bring in fixture kickoff to stamp captured_at when missing
    con.execute("""
    CREATE OR REPLACE TEMP VIEW v_fixture AS
      SELECT game_key, scheduled_time_utc
      FROM read_parquet($fixture)
    """, {"fixture": fixture_path})

    con.execute("""
    CREATE OR REPLACE TEMP VIEW v_hist AS
    SELECT
      h.game_key, h.season, h.round, h.bookmaker, h.market_group, h.market_name, h.selection,
      h.line, h.decimal_odds,
      COALESCE(
        TRY_CAST(h.captured_at_utc_raw AS TIMESTAMP),
        f.scheduled_time_utc
      ) AS captured_at_utc,
      'historic' AS source_kind
    FROM v_hist_base h
    LEFT JOIN v_fixture f USING (game_key)
    """)

    # Union and minimal de-dup (keep last write if same ts)
    con.execute("""
    CREATE OR REPLACE TEMP VIEW v_all AS
    SELECT * FROM v_live
    UNION ALL
    SELECT * FROM v_hist
    """)

    # Enforce uniqueness on (game_key,...,captured_at_utc)
    con.execute("""
    CREATE OR REPLACE TEMP VIEW v_dedup AS
    SELECT *
    FROM (
      SELECT
        *,
        row_number() OVER (
          PARTITION BY game_key, bookmaker, market_group, market_name, selection, line, captured_at_utc
          ORDER BY captured_at_utc DESC
        ) AS rn
      FROM v_all
    )
    WHERE rn = 1
    """)

    # Write partitioned by season/round/bookmaker
    duck_opts = f"(FORMAT PARQUET, PARTITION_BY (season, round, bookmaker){', OVERWRITE_OR_IGNORE TRUE' if overwrite else ''})"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    sql = f"COPY (SELECT game_key, season, round, bookmaker, market_group, market_name, selection, line, decimal_odds, captured_at_utc, source_kind FROM v_dedup) TO '{out_dir}' {duck_opts};"
    con.execute(sql)

    logging.info("Wrote Silver f_odds_snapshot partitions to %s", out_dir)


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
