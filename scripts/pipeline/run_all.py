#!/usr/bin/env python
"""
Orchestrate the full AFL SGM analysis tool pipeline.

This script runs the Bronze ingestion scripts followed by the Silver and
Gold builders to produce a complete set of data artefacts. It is
designed to be idempotent: running it multiple times with the same
parameters will not create duplicate records. Use this as the main
entry point for automated workflows or local testing.

Usage:
    python scripts/pipeline/run_all.py --season 2025 --rounds all --bookmakers sportsbet,pointsbet --include-weather

Flags:
    --season: The AFL season year (e.g. 2025). Required.
    --rounds: Comma or range separated rounds to ingest (default: all).
    --bookmakers: Comma separated list of bookmakers to scrape (default from config).
    --include-weather: Include weather ingestion (forecast and history).
    --root: Project root directory (default: current directory).
    --log-level: Logging level (default: INFO).

The Bronze scripts rely on stubbed data in this repo for demonstration.
To ingest real data you will need to implement network calls in the
Bronze scripts.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path) -> None:
    """Execute a subprocess and raise on failure."""
    logging.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("Command failed with exit code %s: %s", result.returncode, result.stderr)
        raise RuntimeError(f"Command {' '.join(cmd)} failed")
    if result.stdout:
        logging.debug(result.stdout)
    if result.stderr:
        logging.debug(result.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bronze, Silver and Gold ingestion steps")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="AFL season year (e.g. 2025)")
    parser.add_argument("--rounds", default="all", help="Comma or range separated rounds (e.g. '1-5' or 'all')")
    parser.add_argument("--bookmakers", default="", help="Comma separated list of bookmakers (defaults from settings)")
    parser.add_argument("--include-weather", action="store_true", help="Include weather ingestion (forecast & history)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    root = Path(args.root).resolve()

    py_exec = sys.executable
    bronze_dir = root / "scripts" / "bronze"
    silver_dir = root / "scripts" / "silver"
    gold_dir = root / "scripts" / "gold"

    # Build argument lists
    rounds_flag = ["--rounds", args.rounds] if args.rounds else []
    bookmakers_flag = ["--bookmakers", args.bookmakers] if args.bookmakers else []

    # Bronze: discover event URLs
    run([py_exec, str(bronze_dir / "bronze_discover_event_urls.py"), "--season", str(args.season), "--root", str(root), *rounds_flag, *bookmakers_flag], root)
    # Bronze: ingest games
    run([py_exec, str(bronze_dir / "bronze_ingest_games.py"), "--season", str(args.season), "--root", str(root)], root)
    # Bronze: ingest live odds snapshots
    run([py_exec, str(bronze_dir / "bronze_ingest_odds_live.py"), "--season", str(args.season), "--root", str(root)], root)
    # Bronze: ingest historic odds
    run([py_exec, str(bronze_dir / "bronze_ingest_historic_odds.py"), "--season", str(args.season), "--root", str(root)], root)
    # Bronze: ingest weather (optional)
    if args.include_weather:
        run([py_exec, str(bronze_dir / "bronze_ingest_weather_forecast.py"), "--season", str(args.season), "--root", str(root)], root)
        run([py_exec, str(bronze_dir / "bronze_ingest_weather_history.py"), "--season", str(args.season), "--root", str(root)], root)

    # Silver: build from Bronze
    run([py_exec, str(silver_dir / "build_silver.py"), "--season", str(args.season), "--root", str(root)], root)

    # Gold: build SGMs
    run([py_exec, str(gold_dir / "build_gold.py"), "--season", str(args.season), "--root", str(root)], root)

    logging.info("Pipeline run completed successfully")


if __name__ == "__main__":
    main()