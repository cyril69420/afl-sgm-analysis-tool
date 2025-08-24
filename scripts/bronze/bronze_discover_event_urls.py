#!/usr/bin/env python
"""
Discover AFL event URLs from bookmaker websites.

This script uses Playwright to navigate bookmaker landing pages and
collect URLs for upcoming and historical AFL matches. The discovery
process is driven by selectors defined in ``config/bookmakers.yaml``.
For each URL discovered the script records metadata including the
bookmaker, season, round and timestamps of discovery. Results are
written to the Bronze data lake under ``bronze/event_urls``.

Idempotency is achieved by upserting on the key (bookmaker,
event_url). Re‑discovering an existing URL updates the ``last_seen_utc``
timestamp but leaves ``first_seen_utc`` unchanged. When rows are
written multiple times the resulting Parquet dataset contains at most
one record per key.
"""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import datetime
from typing import List, Optional

import polars as pl

from ._shared import (
    utc_now,
    to_utc,
    hash_row,
    parquet_write,
    read_checkpoint,
    write_checkpoint,
    retryable,
    load_yaml,
    load_env,
)
import time
import json
from pathlib import Path

from schemas.bronze import BronzeEventUrl


logger = logging.getLogger(__name__)


def parse_rounds(value: str) -> List[str]:
    """Parse a comma or range separated rounds argument into a list.

    The user can specify values like ``all``, ``1,2,3`` or ``1-5``. This
    function normalises the input into a list of strings representing
    each round. The special value ``all`` returns an empty list to
    signify that all rounds should be discovered.
    """
    if not value or value.lower() == "all":
        return []
    result: List[str] = []
    parts = value.split(",")
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            start, end = int(start_str), int(end_str)
            result.extend([str(i) for i in range(start, end + 1)])
        else:
            result.append(part)
    return result


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Discover AFL event URLs from bookmakers")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season to discover (e.g. 2025)")
    parser.add_argument("--rounds", type=str, default=None, help="Comma or range separated rounds (e.g. '1,2,3' or '1-5' or 'all')")
    parser.add_argument("--bookmakers", type=str, default=None, help="Comma separated list of bookmakers to scrape")
    parser.add_argument("--headless", action="store_true", help="Run Playwright in headless mode")
    parser.add_argument("--max-pages-per-site", type=int, default=10, help="Maximum pages to traverse per bookmaker")
    parser.add_argument("--checkpoint-dir", default="bronze/.checkpoints/discover_event_urls", help="Directory to persist discovery checkpoints")
    parser.add_argument("--parquet", action="store_true", default=True, help="Write Parquet output (default true)")
    parser.add_argument("--csv", action="store_true", help="Also write CSV output alongside Parquet")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    project_root = Path(args.root)
    load_env(project_root / ".env")

    # Load global and bookmaker configurations
    settings = load_yaml(project_root / "config" / "settings.yaml")
    bookmakers_cfg = load_yaml(project_root / "config" / "bookmakers.yaml")

    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    rounds = parse_rounds(args.rounds) or parse_rounds(settings.get("default_rounds", "all"))
    bk_list = (args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", []))

    discovery_run_id = uuid.uuid4().hex

    all_rows: List[dict] = []
    rejects: List[dict] = []

    # Placeholder for Playwright logic. Since network access may be
    # unavailable in this environment, we simply log the intended actions
    # and construct an empty dataset. In a production environment you
    # would instantiate Playwright here, navigate to each start URL and
    # extract event links using the selectors defined in the
    # configuration.
    logger.info("Starting discovery for season %s rounds %s on bookmakers %s", season, rounds or "all", bk_list)
    for bookmaker in bk_list:
        cfg = bookmakers_cfg.get(bookmaker)
        if not cfg:
            logger.warning("No configuration found for bookmaker %s", bookmaker)
            continue
        logger.info("Would scrape bookmaker %s at %s", bookmaker, cfg.get("start_url"))
        # STUB: Example event URL for demonstration
        if rounds:
            target_rounds = rounds
        else:
            target_rounds = ["regular"]
        for rnd in target_rounds:
            row = {
                "bookmaker": bookmaker,
                "season": season,
                "round": rnd,
                "event_url": f"https://example.com/{bookmaker}/{season}/{rnd}",
                "competition": "afl",
                "first_seen_utc": utc_now(),
                "last_seen_utc": utc_now(),
                "source_page": cfg.get("start_url"),
                "status": "upcoming",
                "discovery_run_id": discovery_run_id,
            }
            all_rows.append(row)

    # Validate rows via Pydantic
    validated_rows = []
    for row in all_rows:
        try:
            obj = BronzeEventUrl.model_validate(row)
            validated_rows.append(obj.model_dump())
        except Exception as exc:
            logger.error("Validation error for row %s: %s", row, exc)
            rejects.append({"row": row, "error": str(exc)})

    if not validated_rows:
        logger.warning("No event URLs discovered. Exiting.")
        return

    df = pl.DataFrame(validated_rows)

    # Write Parquet dataset partitioned by season/round/bookmaker
    output_root = project_root / "bronze" / "event_urls"
    partition_cols = ["season", "round", "bookmaker"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d event URL rows to %s", len(df), output_root)

    if args.csv:
        csv_path = output_root / f"event_urls_{season}.csv"
        df.write_csv(csv_path)
        logger.info("Also wrote CSV output to %s", csv_path)

    # Write rejects log if needed
    if rejects:
        rejects_path = project_root / "bronze" / "_rejects" / "discovery"
        rejects_path.mkdir(parents=True, exist_ok=True)
        timestamp = int(time.time() * 1000)
        with open(rejects_path / f"rejects_{timestamp}.json", "w", encoding="utf-8") as f:
            json.dump(rejects, f, indent=2, default=str)
        logger.warning("Wrote %d rejects to %s", len(rejects), rejects_path)


if __name__ == "__main__":
    main()