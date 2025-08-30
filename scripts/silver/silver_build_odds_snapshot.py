#!/usr/bin/env python
"""
Silver: Build f_odds_snapshot by mapping live/historic odds to fixtures.

Inputs:
  - bronze/event_urls/season=*/round=*/bookmaker=*/*.parquet   (per-game URLs)
  - bronze/odds/snapshots/**/*.parquet                         (live odds; must carry real event_url)
  - bronze/odds/history/**/*.parquet                           (historic where available)
  - silver/dim_team.parquet, silver/f_fixture.parquet          (for game_key & team names)

Output:
  - silver/f_odds_snapshot/season=Y/round=R/bookmaker=.../*.parquet
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


def build(
    silver_dir: Path,
    bronze_dir: Path,
    overwrite: bool,
    season: int | None,
    round_no: int | None,
) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8;")

    # Paths
    ev_hive_glob = str((bronze_dir / "event_urls" / "season=*").as_posix()) + "/**/*.parquet"
    live_glob    = str((bronze_dir / "odds" / "snapshots").as_posix()) + "/**/*.parquet"
    hist_glob    = str((bronze_dir / "odds" / "history").as_posix()) + "/**/*.parquet"
    dim_team_path = (silver_dir / "dim_team.parquet").as_posix()
    f_fixture_path = (silver_dir / "f_fixture.parquet").as_posix()

    logging.info("Reading live snapshots: %s", live_glob)
    logging.info("Reading historic odds : %s", hist_glob)

    # ---------- helpers ----------
    URL_NORM = (
        "LOWER(TRIM("
        "REGEXP_REPLACE("
        "  REGEXP_REPLACE("
        "    REGEXP_REPLACE("
        "      REGEXP_REPLACE(CAST({col} AS VARCHAR), '^https?://(www\\.)?', ''),"
        "      '\\\\?.*$', ''"
        "    ),"
        "    '#.*$', ''"
        "  ),"
        "  '/+$', ''"
        ")"
        "))"
    )

    def EVENT_KEY(expr: str) -> str:
        return (
            "CASE "
            f"WHEN {expr} LIKE 'sportsbet.com.au/%' THEN 'sportsbet.com.au:' || REGEXP_EXTRACT({expr}, '(\\\\d+)$') "
            f"WHEN {expr} LIKE 'pointsbet.com.au/%' THEN 'pointsbet.com.au:' || REGEXP_EXTRACT({expr}, '(\\\\d+)$') "
            "ELSE NULL END"
        )

    def NORM_TEAM(expr: str) -> str:
        return "TRIM(REGEXP_REPLACE(LOWER(CAST((" + expr + ") AS VARCHAR)), '[^a-z0-9]+', ' '))"

    # ---------- Fixtures + team names ----------
    con.execute("CREATE OR REPLACE TEMP VIEW dim_team AS SELECT * FROM read_parquet('" + dim_team_path + "')")
    con.execute("CREATE OR REPLACE TEMP VIEW f_fixture AS SELECT * FROM read_parquet('" + f_fixture_path + "')")

    con.execute(
        "CREATE OR REPLACE TEMP VIEW fx_teams AS "
        "SELECT "
        "  CAST(f.game_key AS VARCHAR) AS game_key, "
        "  TRY_CAST(f.season AS INTEGER) AS season, "
        "  TRY_CAST(f.round  AS INTEGER) AS round, "
        "  f.scheduled_time_utc, "
        "  " + NORM_TEAM("t_home.team_name") + " AS home_norm, "
        "  " + NORM_TEAM("t_away.team_name") + " AS away_norm "
        "FROM f_fixture f "
        "JOIN dim_team t_home ON f.home_team_id = t_home.team_id "
        "JOIN dim_team t_away ON f.away_team_id = t_away.team_id "
    )

    # ---------- Event URL map (SB/PB only) ----------
    season_filter_ev = f"WHERE TRY_CAST(season AS INTEGER) = {int(season)}" if season is not None else ""
    con.execute(
        "CREATE OR REPLACE TEMP VIEW ev0 AS "
        "SELECT * FROM read_parquet('" + ev_hive_glob + "', hive_partitioning=1) "
        + season_filter_ev
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW ev_sbpb AS "
        "SELECT "
        + URL_NORM.format(col="event_url") + " AS event_url_norm, "
        + EVENT_KEY(URL_NORM.format(col="event_url")) + " AS event_key, "
        "  TRY_CAST(season AS INTEGER) AS season, "
        "  TRY_CAST(round  AS INTEGER) AS round, "
        "  CAST(bookmaker AS VARCHAR)  AS bookmaker, "
        # Sportsbet slug parsing
        "  CASE WHEN " + URL_NORM.format(col="event_url") + " LIKE 'sportsbet.com.au/%' "
        "       THEN REGEXP_EXTRACT(" + URL_NORM.format(col="event_url") + ", '.*/([^/]+)-(\\\\d+)$', 1) "
        "       ELSE NULL END AS sb_slug, "
        "  CASE WHEN " + URL_NORM.format(col="event_url") + " LIKE 'sportsbet.com.au/%' "
        "       THEN REGEXP_EXTRACT(REPLACE(REPLACE(sb_slug, '-vs-', '-v-'), '_', '-'), '^(.*?)-(?:v)-.*$', 1) "
        "       ELSE NULL END AS sb_t1_raw, "
        "  CASE WHEN " + URL_NORM.format(col="event_url") + " LIKE 'sportsbet.com.au/%' "
        "       THEN REGEXP_EXTRACT(REPLACE(REPLACE(sb_slug, '-vs-', '-v-'), '_', '-'), '.*-(?:v)-(.*)$', 1) "
        "       ELSE NULL END AS sb_t2_raw "
        "FROM ev0 "
        "WHERE event_url IS NOT NULL AND ("
        "  CAST(event_url AS VARCHAR) LIKE 'https://%sportsbet.com.au/%' OR "
        "  CAST(event_url AS VARCHAR) LIKE 'https://%pointsbet.com.au/%'"
        ")"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW ev_map AS "
        "SELECT "
        "  event_url_norm, event_key, season, round, bookmaker, "
        "  " + NORM_TEAM("REPLACE(COALESCE(sb_t1_raw, ''), '-', ' ')") + " AS t1_norm, "
        "  " + NORM_TEAM("REPLACE(COALESCE(sb_t2_raw, ''), '-', ' ')") + " AS t2_norm "
        "FROM ev_sbpb"
    )
    ev_rows = con.execute("SELECT COUNT(*) FROM ev_map").fetchone()[0]
    logging.info("ev_map rows (SB/PB): %s", ev_rows)

    # ---------- LIVE snapshots (must carry per-game event_url) ----------
    con.execute("CREATE OR REPLACE TEMP VIEW live_raw AS SELECT * FROM read_parquet('" + live_glob + "', hive_partitioning=1)")
    # Assert we actually have URLs in live snapshots
    live_nonblank = con.execute(
        "SELECT COUNT(*) FROM read_parquet('" + live_glob + "', hive_partitioning=1) "
        "WHERE event_url IS NOT NULL AND LENGTH(TRIM(CAST(event_url AS VARCHAR)))>0"
    ).fetchone()[0]
    if live_nonblank == 0:
        logging.error("All live snapshot rows have blank/NULL event_url. Re-run bronze_ingest_odds_live; it now fails fast if event_url is blank.")
        return

    con.execute(
        "CREATE OR REPLACE TEMP VIEW live_enriched AS "
        "SELECT "
        + URL_NORM.format(col="event_url") + " AS event_url_norm, "
        + EVENT_KEY(URL_NORM.format(col="event_url")) + " AS event_key, "
        "  LOWER(TRIM(CAST(bookmaker AS VARCHAR))) AS bookmaker, "
        "  CAST(market_group AS VARCHAR)   AS market_group, "
        "  CAST(market_name  AS VARCHAR)   AS market_name, "
        "  CAST(selection    AS VARCHAR)   AS selection, "
        "  TRY_CAST(line AS DOUBLE)        AS line, "
        "  TRY_CAST(decimal_odds AS DOUBLE) AS decimal_odds, "
        "  TRY_CAST(captured_at_utc AS TIMESTAMP) AS captured_at_utc "
        "FROM live_raw "
        "WHERE event_url IS NOT NULL AND LENGTH(TRIM(CAST(event_url AS VARCHAR))) > 0"
    )

    # Map live → ev_map → fixtures
    con.execute(
        "CREATE OR REPLACE TEMP VIEW live_with_teams AS "
        "SELECT l.*, e.season, e.round, e.t1_norm, e.t2_norm "
        "FROM live_enriched l "
        "JOIN ev_map e ON l.event_url_norm = e.event_url_norm"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW v_live_map AS "
        "SELECT "
        "  f.game_key, f.season, f.round, "
        "  l.bookmaker, l.market_group, l.market_name, l.selection, "
        "  l.line, l.decimal_odds, l.captured_at_utc, "
        "  'live' AS source_kind "
        "FROM live_with_teams l "
        "JOIN fx_teams f "
        "  ON l.season = f.season AND l.round = f.round "
        " AND ( (f.home_norm = l.t1_norm AND f.away_norm = l.t2_norm) "
        "    OR (f.home_norm = l.t2_norm AND f.away_norm = l.t1_norm) )"
    )

    live_total  = con.execute("SELECT COUNT(*) FROM live_enriched").fetchone()[0]
    live_mapped = con.execute("SELECT COUNT(*) FROM v_live_map").fetchone()[0]
    logging.info("LIVE rows: total=%s, mapped=%s", live_total, live_mapped)
    if live_mapped == 0:
        # Show what URLs we actually have
        sample = con.execute(
            "SELECT event_url_norm, COUNT(*) n FROM live_enriched GROUP BY 1 ORDER BY n DESC LIMIT 10"
        ).fetchdf()
        logging.warning("No LIVE rows mapped. Top live URL norms:\n%s", sample)

        sample_ev = con.execute(
            "SELECT event_url_norm, t1_norm, t2_norm, season, round FROM ev_map LIMIT 10"
        ).fetchdf()
        logging.warning("Sample ev_map rows:\n%s", sample_ev)

    # ---------- HISTORIC (optional) ----------
    con.execute("CREATE OR REPLACE TEMP VIEW hist_raw AS SELECT * FROM read_parquet('" + hist_glob + "', hive_partitioning=1)")
    con.execute(
        "CREATE OR REPLACE TEMP VIEW hist_sbpb AS "
        "SELECT "
        + URL_NORM.format(col="event_url") + " AS event_url_norm, "
        + EVENT_KEY(URL_NORM.format(col="event_url")) + " AS event_key, "
        "  LOWER(TRIM(CAST(bookmaker AS VARCHAR))) AS bookmaker, "
        "  CAST(market_group AS VARCHAR)   AS market_group, "
        "  CAST(market_name  AS VARCHAR)   AS market_name, "
        "  CAST(selection    AS VARCHAR)   AS selection, "
        "  TRY_CAST(line AS DOUBLE)        AS line, "
        "  TRY_CAST(decimal_odds AS DOUBLE) AS decimal_odds, "
        "  TRY_CAST(captured_at_utc AS TIMESTAMP) AS captured_at_utc "
        "FROM hist_raw "
        "WHERE event_url IS NOT NULL AND LENGTH(TRIM(CAST(event_url AS VARCHAR)))>0 "
        "  AND (CAST(event_url AS VARCHAR) LIKE 'https://%sportsbet.com.au/%' "
        "       OR CAST(event_url AS VARCHAR) LIKE 'https://%pointsbet.com.au/%')"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW hist_with_teams AS "
        "SELECT h.*, e.season, e.round, e.t1_norm, e.t2_norm "
        "FROM hist_sbpb h "
        "JOIN ev_map e ON h.event_url_norm = e.event_url_norm"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW v_hist_map AS "
        "SELECT "
        "  f.game_key, f.season, f.round, "
        "  h.bookmaker, h.market_group, h.market_name, h.selection, "
        "  h.line, h.decimal_odds, "
        "  COALESCE(h.captured_at_utc, f.scheduled_time_utc) AS captured_at_utc, "
        "  'historic' AS source_kind "
        "FROM hist_with_teams h "
        "JOIN fx_teams f "
        "  ON h.season = f.season AND h.round = f.round "
        " AND ( (f.home_norm = h.t1_norm AND f.away_norm = h.t2_norm) "
        "    OR (f.home_norm = h.t2_norm AND f.away_norm = h.t1_norm) )"
    )

    hist_mapped = con.execute("SELECT COUNT(*) FROM v_hist_map").fetchone()[0]
    logging.info("HIST rows mapped=%s", hist_mapped)

    # ---------- UNION + DEDUP ----------
    con.execute(
        "CREATE OR REPLACE TEMP VIEW v_all AS "
        "SELECT * FROM v_live_map "
        "UNION ALL SELECT * FROM v_hist_map"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW v_dedup AS "
        "SELECT * FROM ( "
        "  SELECT "
        "    *, "
        "    row_number() OVER ( "
        "      PARTITION BY game_key, bookmaker, market_group, market_name, selection, line, captured_at_utc "
        "      ORDER BY captured_at_utc DESC "
        "    ) AS rn "
        "  FROM v_all "
        ") WHERE rn = 1"
    )

    # ---------- Filter & write ----------
    filters = []
    if season is not None:
        filters.append(f"season = {int(season)}")
    if round_no is not None:
        filters.append(f"round = {int(round_no)}")
    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

    final_rows = con.execute(f"SELECT COUNT(*) FROM v_dedup {where_clause}").fetchone()[0]
    logging.info("Final f_odds_snapshot rows: %s", final_rows)

    if final_rows == 0:
        logging.warning("No rows to write for f_odds_snapshot (after mapping and filters). Skipping write.")
        return

    out_dir = (silver_dir / "f_odds_snapshot").as_posix()
    con.execute(
        "COPY ( "
        "  SELECT "
        "    game_key, season, round, bookmaker, market_group, market_name, selection, "
        "    line, decimal_odds, captured_at_utc, source_kind "
        "  FROM v_dedup "
        f"  {where_clause} "
        ") TO '" + out_dir.replace("'", "''") + "' "
        "(FORMAT PARQUET, PARTITION_BY (season, round, bookmaker) "
        + (", OVERWRITE_OR_IGNORE TRUE" if overwrite else "") + ")"
    )
    logging.info("Wrote Silver f_odds_snapshot → %s", out_dir)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", default="bronze", type=Path)
    ap.add_argument("--silver-dir", default="silver", type=Path)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--round", dest="round_no", type=int, default=None)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    _setup_logger(args.log_level)
    build(args.silver_dir, args.bronze_dir, args.overwrite, args.season, args.round_no)


if __name__ == "__main__":
    main()
