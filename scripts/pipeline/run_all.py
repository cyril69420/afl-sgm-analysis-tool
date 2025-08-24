#!/usr/bin/env python
"""
Pipeline orchestrator for the AFL SGM analysis tool.

This script coordinates the execution of all ingestion and modelling
steps from Bronze through Gold.  It is designed to be idempotent and
safe to run multiple times; each stage writes append‑only Parquet
datasets partitioned by season and round.

By default the orchestrator discovers event URLs, ingests fixtures,
scrapes live and historical odds, fetches weather (if enabled),
constructs the Silver layer, builds the Gold SGMs and then runs QA on
each layer.  Command line flags allow selective inclusion of
bookmakers, seasons, rounds and weather.

Example usage:

    python scripts/pipeline/run_all.py --season 2025 --rounds 1-5 --bookmakers sportsbet --include-weather

See also the Makefile target ``make pipeline`` which invokes this
script with sensible defaults.
"""

from __future__ import annotations

import argparse
import subprocess
import logging
import shlex
from pathlib import Path
from typing import List


def run(cmd: str) -> None:
    """Run a shell command and raise on failure.

    The command string is split using ``shlex.split`` to avoid shell
    injection.  Output and errors are streamed directly to the
    terminal.  If the command exits with a non‑zero status a
    ``RuntimeError`` is raised to abort the pipeline.
    """
    logging.info("Executing: %s", cmd)
    result = subprocess.run(shlex.split(cmd), capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the full AFL SGM pipeline")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season year to process (e.g. 2025)")
    parser.add_argument(
        "--rounds",
        type=str,
        default="all",
        help="Comma or range separated list of rounds to process (e.g. '1,2,3' or '1-5' or 'all')",
    )
    parser.add_argument(
        "--bookmakers",
        type=str,
        default=None,
        help="Comma separated list of bookmakers to scrape (default: all configured)",
    )
    parser.add_argument(
        "--include-weather", action="store_true", help="Whether to fetch weather data for fixtures"
    )
    parser.add_argument("--headless", action="store_true", help="Run Playwright in headless mode for scraping")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(message)s")
    root = Path(args.root).resolve()
    season = args.season
    # Bronze stage: discovery
    discover_cmd = f"python scripts/bronze/bronze_discover_event_urls.py --root {root} --season {season}"
    if args.rounds:
        discover_cmd += f" --rounds {args.rounds}"
    if args.bookmakers:
        discover_cmd += f" --bookmakers {args.bookmakers}"
    if args.headless:
        discover_cmd += " --headless"
    run(discover_cmd)
    # Bronze stage: fixtures
    fixtures_cmd = f"python scripts/bronze/bronze_ingest_games.py --root {root} --season {season}"
    if args.rounds:
        fixtures_cmd += f" --rounds {args.rounds}"
    run(fixtures_cmd)
    # Bronze stage: live odds
    odds_live_cmd = f"python scripts/bronze/bronze_ingest_odds_live.py --root {root} --season {season}"
    run(odds_live_cmd)
    # Bronze stage: historical odds
    odds_hist_cmd = f"python scripts/bronze/bronze_ingest_historic_odds.py --root {root} --season {season}"
    run(odds_hist_cmd)
    # Bronze stage: weather (optional)
    if args.include_weather:
        weather_fc_cmd = f"python scripts/bronze/bronze_ingest_weather_forecast.py --root {root} --season {season}"
        run(weather_fc_cmd)
        weather_hist_cmd = f"python scripts/bronze/bronze_ingest_weather_history.py --root {root} --season {season}"
        run(weather_hist_cmd)
    # Silver stage
    silver_cmd = f"python scripts/silver/build_silver.py --root {root} --season {season}"
    if args.include_weather:
        silver_cmd += " --include-weather"
    run(silver_cmd)
    # Gold stage
    gold_cmd = f"python scripts/gold/build_gold.py --root {root} --season {season}"
    run(gold_cmd)
    # Quality assurance
    qa_bronze_cmd = f"python scripts/bronze/qa_bronze.py {root}"
    run(qa_bronze_cmd)
    qa_silver_cmd = f"python scripts/silver/qa_silver.py {root}"
    run(qa_silver_cmd)
    qa_gold_cmd = f"python scripts/gold/qa_gold.py {root}"
    run(qa_gold_cmd)
    logging.info("Pipeline run completed successfully")


if __name__ == "__main__":
    main()