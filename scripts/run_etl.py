#!/usr/bin/env python
"""
Run Bronze → Silver pipeline in dependency order for a season.

Examples:
  python run_etl.py --season 2025 --overwrite
  python run_etl.py --season 2025 --phase silver   # only Silver
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

def sh(cmd: list[str]) -> None:
    logging.info("$ %s", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(r.returncode)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--phase", choices=["all", "bronze", "silver"], default="all")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--csv-mirror", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s",
                        level=getattr(logging, args.log_level.upper(), logging.INFO))

    # paths
    py = [sys.executable, "-m"]

    if args.phase in ("all", "bronze"):
        # 1) Fixtures & URLs
        sh(py + ["scripts.bronze.bronze_ingest_games",
                 "--season", str(args.season),
                 "--log-level", args.log_level] + (["--overwrite"] if args.overwrite else []) + (["--csv-mirror"] if args.csv_mirror else []))
        sh(py + ["scripts.bronze.bronze_discover_event_urls",
                 "--season", str(args.season),
                 "--log-level", args.log_level,
                 "--bookmakers", "sportsbet,pointsbet"] + (["--overwrite"] if args.overwrite else []) + (["--csv-mirror"] if args.csv_mirror else []))

        # 2) Odds & Weather
        sh(py + ["scripts.bronze.bronze_ingest_odds_live",
                 "--season", str(args.season),
                 "--log-level", args.log_level] + (["--overwrite"] if args.overwrite else []) + (["--csv-mirror"] if args.csv_mirror else []))
        sh(py + ["scripts.bronze.bronze_ingest_historic_odds",
                 "--season", str(args.season),
                 "--log-level", args.log_level] + (["--overwrite"] if args.overwrite else []) + (["--csv-mirror"] if args.csv_mirror else []))
        sh(py + ["scripts.bronze.bronze_ingest_weather_forecast",
                 "--season", str(args.season),
                 "--log-level", args.log_level] + (["--overwrite"] if args.overwrite else []) + (["--cs]()_
