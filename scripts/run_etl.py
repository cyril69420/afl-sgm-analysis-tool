#!/usr/bin/env python
"""
Run Bronze → Silver → Gold pipeline in dependency order for a given season.

Examples:
  python -m scripts.pipeline.run_etl --season 2025 --overwrite --csv-mirror --headless --log-level INFO
  python -m scripts.pipeline.run_etl --season 2025 --phase silver --log-level DEBUG
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys


def sh(cmd: list[str]) -> None:
    """Run a subprocess command and exit on failure."""
    logging.info("$ %s", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        logging.error("Command failed with exit code %s", r.returncode)
        sys.exit(r.returncode)


def common_flags(args: argparse.Namespace) -> list[str]:
    flags: list[str] = ["--log-level", args.log_level]
    if getattr(args, "overwrite", False):
        flags.append("--overwrite")
    if getattr(args, "csv_mirror", False):
        flags.append("--csv-mirror")
    if getattr(args, "headless", False):
        flags.append("--headless")
    return flags


def main():
    ap = argparse.ArgumentParser(description="Bronze → Silver → Gold ETL Orchestrator")
    ap.add_argument("--season", type=int, required=True, help="Season to process, e.g., 2025")
    ap.add_argument(
        "--phase",
        choices=["all", "bronze", "silver", "gold"],
        default="all",
        help="Run only bronze, only silver, only gold, or the full pipeline",
    )
    ap.add_argument("--overwrite", action="store_true", help="Allow writers to overwrite Parquet partitions")
    ap.add_argument("--csv-mirror", action="store_true", help="If supported, also mirror outputs to CSV")
    ap.add_argument("--headless", action="store_true", help="Run scraping steps that support it in headless mode")
    ap.add_argument("--log-level", default="INFO", help="Logging level for child processes")
    args = ap.parse_args()

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=getattr(logging, args.log_level.upper(), logging.INFO),
    )

    py = [sys.executable, "-m"]  # invoke modules consistently

    # -----------------------
    # BRONZE
    # -----------------------
    if args.phase in ("all", "bronze"):
        logging.info("=== BRONZE: begin ===")

        # 1) Fixtures & URLs
        sh(
            py
            + ["scripts.bronze.bronze_ingest_games", "--season", str(args.season)]
            + common_flags(args)
        )

        sh(
            py
            + [
                "scripts.bronze.bronze_discover_event_urls",
                "--season",
                str(args.season),
                "--bookmakers",
                "sportsbet,pointsbet",
            ]
            + common_flags(args)
        )

        # 2) Odds & Weather
        sh(
            py
            + ["scripts.bronze.bronze_ingest_odds_live", "--season", str(args.season)]
            + common_flags(args)
        )
        sh(
            py
            + ["scripts.bronze.bronze_ingest_historic_odds", "--season", str(args.season)]
            + common_flags(args)
        )
        sh(
            py
            + ["scripts.bronze.bronze_ingest_weather_forecast", "--season", str(args.season)]
            + common_flags(args)
        )

        # 3) Player stats (AFLTables GBG) — include prior season for form windows
        prev_season = args.season - 1
        logging.info("Bronze: ingesting player stats for seasons %s and %s", prev_season, args.season)
        # Uses your existing bronze_ingest_player_stats.py interface (action='append' for --season). :contentReference[oaicite:5]{index=5}
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_player_stats",
                "--season",
                str(prev_season),
                "--season",
                str(args.season),
            ]
            + (["--verbose"] if args.log_level.upper() == "DEBUG" else [])
        )

        logging.info("=== BRONZE: done ===")

    # -----------------------
    # SILVER
    # -----------------------
    if args.phase in ("all", "silver"):
        logging.info("=== SILVER: begin ===")

        sh(py + ["scripts.silver.silver_build_core"] + ["--log-level", args.log_level])
        sh(py + ["scripts.silver.silver_build_odds_snapshot"] + (["--overwrite"] if args.overwrite else []) + ["--log-level", args.log_level])
        sh(py + ["scripts.silver.silver_views_odds_latest"] + (["--overwrite"] if args.overwrite else []) + ["--log-level", args.log_level])
        sh(py + ["scripts.silver.silver_build_weather"] + (["--overwrite"] if args.overwrite else []) + ["--log-level", args.log_level])
        sh(py + ["scripts.silver.silver_dims_bookmaker"] + ["--log-level", args.log_level])

        # NEW: Silver player stats (standardize Bronze GBG → f_player_game_stats for Gold)
        sh(py + ["scripts.silver.silver_build_player_stats", "--season", str(args.season)] + (["--overwrite"] if args.overwrite else []) + ["--log-level", args.log_level])

        logging.info("=== SILVER: done ===")

    # -----------------------
    # GOLD
    # -----------------------
    if args.phase in ("all", "gold"):
        logging.info("=== GOLD: begin ===")
        # Devig latest odds (Sportsbet only), build positive-EV SGMs, allocate $ stakes
        sh(py + ["scripts.gold.gold_devig_latest", "--season", str(args.season)] + common_flags(args))
        sh(py + ["scripts.gold.gold_build_sgms", "--season", str(args.season)] + common_flags(args))
        sh(py + ["scripts.gold.gold_allocate_stakes", "--season", str(args.season)] + common_flags(args))
        logging.info("=== GOLD: done ===")

    logging.info("ETL complete.")


if __name__ == "__main__":
    main()
