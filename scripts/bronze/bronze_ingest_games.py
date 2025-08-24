#!/usr/bin/env python
"""
Ingest AFL fixtures into the Bronze layer.

This script reads event URLs (discovered via bronze_discover_event_urls) or
external fixture sources and produces a normalised fixtures table.
Each row represents a single game with its scheduled kickoff time in
UTC, the home and away team names, the venue and optional mapping
back to the bookmaker event URL. Rows are validated via the
BronzeFixtureRow pydantic model before being written to Parquet.

The script is idempotent: game keys are deterministic and repeated
ingestion will not duplicate data. You can filter by season or round
via CLI flags.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import polars as pl

from ._shared import (
    utc_now,
    to_utc,
    parquet_write,
    load_yaml,
    load_env,
)
from schemas.bronze import BronzeFixtureRow

logger = logging.getLogger(__name__)


def slugify(value: str) -> str:
    """Simple slugification: lowercases and replaces spaces with hyphens."""
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def build_game_key(season: int, round_id: str, home: str, away: str, kickoff_utc: datetime) -> str:
    slug_home = slugify(home)
    slug_away = slugify(away)
    ts = kickoff_utc.strftime("%Y%m%dT%H%M%SZ")
    return f"{season}_{round_id}_{slug_home}_{slug_away}_{ts}"


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest AFL fixtures into the Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season to ingest (e.g. 2025)")
    parser.add_argument("--rounds", type=str, default=None, help="Comma or range separated rounds to ingest")
    parser.add_argument("--since", type=str, default=None, help="Only include games scheduled on/after this date (YYYY-MM-DD)")
    parser.add_argument("--until", type=str, default=None, help="Only include games scheduled before this date (YYYY-MM-DD)")
    parser.add_argument("--csv", action="store_true", help="Also write CSV output")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]

    # Determine rounds to ingest
    def parse_rounds(value: Optional[str]) -> List[str]:
        if not value or value.lower() == "all":
            return []
        parts = value.split(",")
        out: List[str] = []
        for part in parts:
            part = part.strip()
            if "-" in part:
                s, e = part.split("-", 1)
                out.extend([str(i) for i in range(int(s), int(e) + 1)])
            else:
                out.append(part)
        return out
    rounds = parse_rounds(args.rounds) or parse_rounds(settings.get("default_rounds", "all"))

    # Placeholder fixtures. In a real implementation this would be
    # derived either by parsing bookmaker event pages or by consuming an
    # official fixture feed. Here we construct a single example game
    # for demonstration.
    example_fixtures = []
    if rounds:
        target_rounds = rounds
    else:
        target_rounds = ["regular"]
    for rnd in target_rounds:
        local_time = datetime(season, 3, 25, 19, 40)  # 7:40pm local
        source_tz = "Australia/Melbourne"
        kickoff_utc = to_utc(local_time, source_tz)
        home_team = "Example Home"
        away_team = "Example Away"
        venue = "Example Stadium"
        game_key = build_game_key(season, rnd, home_team, away_team, kickoff_utc)
        row = {
            "game_key": game_key,
            "season": season,
            "round": rnd,
            "home": home_team,
            "away": away_team,
            "venue": venue,
            "scheduled_time_utc": kickoff_utc,
            "source_tz": source_tz,
            "discovered_utc": utc_now(),
            "bookmaker_event_url": None,
        }
        example_fixtures.append(row)

    # Apply optional date filters
    if args.since:
        since_date = datetime.fromisoformat(args.since).date()
        example_fixtures = [r for r in example_fixtures if r["scheduled_time_utc"].date() >= since_date]
    if args.until:
        until_date = datetime.fromisoformat(args.until).date()
        example_fixtures = [r for r in example_fixtures if r["scheduled_time_utc"].date() < until_date]

    if not example_fixtures:
        logger.warning("No fixtures to ingest. Exiting.")
        return

    validated_rows = []
    for row in example_fixtures:
        try:
            obj = BronzeFixtureRow.model_validate(row)
            validated_rows.append(obj.model_dump())
        except Exception as exc:
            logger.error("Validation error for fixture %s: %s", row, exc)

    if not validated_rows:
        logger.error("All fixture rows failed validation. Nothing to write.")
        return

    df = pl.DataFrame(validated_rows)
    output_root = project_root / "bronze" / "fixtures"
    parquet_write(df, output_root, partition_cols=["season", "round"])
    logger.info("Wrote %d fixture rows to %s", len(df), output_root)

    if args.csv:
        csv_path = output_root / f"fixtures_{season}.csv"
        df.write_csv(csv_path)
        logger.info("Also wrote CSV output to %s", csv_path)


if __name__ == "__main__":
    main()