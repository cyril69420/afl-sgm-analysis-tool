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


def sh(cmd: list[str]) -> None:
    """Run a subprocess command and exit on failure."""
    logging.info("$ %s", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        logging.error("Command failed with exit code %s", r.returncode)
        sys.exit(r.returncode)


def main():
    ap = argparse.ArgumentParser(description="Bronze → Silver ETL Orchestrator")
    ap.add_argument("--season", type=int, required=True, help="Season to process, e.g., 2025")
    ap.add_argument(
        "--phase",
        choices=["all", "bronze", "silver"],
        default="all",
        help="Run only bronze, only silver, or the full pipeline",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow writers to overwrite existing Parquet partitions",
    )
    ap.add_argument(
        "--csv-mirror",
        action="store_true",
        help="If supported by the called script, also mirror outputs to CSV",
    )
    ap.add_argument(
        "--headless",
        action="store_true",
        help="Run scraping steps that support it in headless mode (Playwright)",
    )
    ap.add_argument("--log-level", default="INFO", help="Logging level for child processes")
    args = ap.parse_args()

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=getattr(logging, args.log_level.upper(), logging.INFO),
    )

    py = [sys.executable, "-m"]  # invoke modules consistently

    if args.phase in ("all", "bronze"):
        # 1) Bronze – Fixtures & URLs
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_games",
                "--season",
                str(args.season),
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
            + (["--csv-mirror"] if args.csv_mirror else [])
        )

        # Playwright-backed discovery (supports --headless)
        sh(
            py
            + [
                "scripts.bronze.bronze_discover_event_urls",
                "--season",
                str(args.season),
                "--log-level",
                args.log_level,
                "--bookmakers",
                "sportsbet,pointsbet",
            ]
            + (["--overwrite"] if args.overwrite else [])
            + (["--csv-mirror"] if args.csv_mirror else [])
            + (["--headless"] if args.headless else [])
        )

        # 2) Bronze – Odds & Weather
        # If your odds scraper also supports --headless, you can safely add the same flag below.
        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_odds_live",
                "--season",
                str(args.season),
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
            + (["--csv-mirror"] if args.csv_mirror else [])
            # + (["--headless"] if args.headless else [])  # uncomment if supported by your script
        )

        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_historic_odds",
                "--season",
                str(args.season),
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
            + (["--csv-mirror"] if args.csv_mirror else [])
        )

        sh(
            py
            + [
                "scripts.bronze.bronze_ingest_weather_forecast",
                "--season",
                str(args.season),
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
            + (["--csv-mirror"] if args.csv_mirror else [])
        )

    if args.phase in ("all", "silver"):
        # 3) Silver – core dims/facts
        sh(
            py
            + [
                "scripts.silver.silver_build_core",
                "--log-level",
                args.log_level,
            ]
        )

        sh(
            py
            + [
                "scripts.silver.silver_build_odds_snapshot",
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
        )

        sh(
            py
            + [
                "scripts.silver.silver_views_odds_latest",
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
        )

        sh(
            py
            + [
                "scripts.silver.silver_build_weather",
                "--log-level",
                args.log_level,
            ]
            + (["--overwrite"] if args.overwrite else [])
        )

        # Optional: bookmaker dim for referential integrity
        sh(
            py
            + [
                "scripts.silver.silver_dims_bookmaker",
                "--log-level",
                args.log_level,
            ]
        )

    logging.info("ETL complete.")


if __name__ == "__main__":
    main()
