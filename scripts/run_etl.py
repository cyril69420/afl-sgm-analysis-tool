#!/usr/bin/env python
"""
Run Bronze → Silver pipeline in dependency order for a given season.

Examples:
  python -m scripts.run_etl --season 2025 --overwrite --csv-mirror --headless --log-level INFO
  python -m scripts.run_etl --season 2025 --phase silver --log-level DEBUG
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from typing import List


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def sh(cmd: List[str]) -> None:
    """Run a shell command with logging and failure surfacing."""
    logging.info("RUN: %s", " ".join(cmd))
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def common_flags(args: argparse.Namespace) -> List[str]:
    flags: List[str] = ["--log-level", args.log_level]
    if args.overwrite:
        flags.append("--overwrite")
    if args.csv_mirror:
        flags.append("--csv-mirror")
    return flags


def main() -> None:
    ap = argparse.ArgumentParser(description="Bronze → Silver ETL runner")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--phase", choices=["all", "bronze", "silver"], default="all")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--csv-mirror", action="store_true")
    ap.add_argument("--headless", action="store_true", help="(reserved) keep for compatibility")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    setup_logging(args.log_level)
    py = [sys.executable, "-m"]

    logging.info("ETL start | season=%s phase=%s overwrite=%s csv_mirror=%s",
                 args.season, args.phase, args.overwrite, args.csv_mirror)

    # -------------------- BRONZE --------------------
    if args.phase in ("all", "bronze"):
        logging.info("=== BRONZE: begin ===")

        # Upcoming fixtures
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_games",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Event URL discovery (bookmakers)
        sh(
            py
            + [
                "scripts.bronze.bronze_discover_event_urls",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Live odds
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_odds_live",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Historic odds
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_historic_odds",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Weather forecast
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_weather_forecast",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # 🔥 NEW: Historical match results (Squiggle by default)
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_results",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        logging.info("=== BRONZE: done ===")

    # -------------------- SILVER --------------------
    if args.phase in ("all", "silver"):
        logging.info("=== SILVER: begin ===")

        # Core (dims + upcoming + final + union)
        sh(
            py
            + [
                "scripts.silver.silver_build_core",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Odds snapshot (from bronze odds)
        sh(
            py
            + [
                "scripts.silver.silver_build_odds_snapshot",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Latest odds view
        sh(
            py
            + [
                "scripts.silver.silver_views_odds_latest",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Weather build
        sh(
            py
            + [
                "scripts.silver.silver_build_weather",
                "--season",
                str(args.season),
            ]
            + common_flags(args)
        )

        # Bookmaker dims
        sh(
            py
            + [
                "scripts.silver.silver_dims_bookmaker",
            ]
            + ["--log-level", args.log_level]
        )

        logging.info("=== SILVER: done ===")

    logging.info("ETL complete.")


if __name__ == "__main__":
    main()
