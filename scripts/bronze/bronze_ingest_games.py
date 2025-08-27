#!/usr/bin/env python
"""
Ingest AFL fixtures into Bronze (adds --dry-run/--limit; keeps partitioned writes).
"""

from __future__ import annotations

import argparse
import logging
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
from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    echo_df_info,
)

from schemas.bronze import BronzeFixtureRow

logger = logging.getLogger(__name__)


def _parse_rounds(value: Optional[str]) -> List[str]:
    if not value or value.lower() == "all":
        return []
    out: List[str] = []
    for part in value.split(","):
        part = part.strip()
        if "-" in part:
            s, e = part.split("-", 1)
            out.extend([str(i) for i in range(int(s), int(e) + 1)])
        else:
            out.append(part)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest AFL fixtures into Bronze")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season (e.g., 2025)")
    parser.add_argument("--rounds", type=str, default=None, help="Comma/range or 'all'")
    parser.add_argument("--since", type=str, default=None, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--until", type=str, default=None, help="YYYY-MM-DD (exclusive)")
    parser.add_argument("--csv", action="store_true", help="Also write CSV")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing bronze output dir")
    add_common_test_flags(parser)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]

    rounds = _parse_rounds(args.rounds) or _parse_rounds(settings.get("default_rounds", "all"))
    target_rounds = rounds or ["regular"]

    # Stub fixture rows (keeps your original validation + partitioned write)
    example_fixtures = []
    for rnd in target_rounds:
        local_time = datetime(season, 3, 25, 19, 40)
        source_tz = "Australia/Melbourne"
        kickoff_utc = to_utc(local_time, source_tz)
        row = {
            "game_key": f"{season}_{rnd}_example-home_example-away_{kickoff_utc.strftime('%Y%m%dT%H%M%SZ')}",
            "season": season,
            "round": rnd,
            "home": "Example Home",
            "away": "Example Away",
            "venue": "Example Stadium",
            "scheduled_time_utc": kickoff_utc,
            "source_tz": source_tz,
            "discovered_utc": utc_now(),
            "bookmaker_event_url": None,
        }
        try:
            obj = BronzeFixtureRow.model_validate(row)
            example_fixtures.append(obj.model_dump())
        except Exception as exc:
            logger.error("Validation error: %s", exc)

    # Optional date filters
    if args.since:
        since_d = datetime.fromisoformat(args.since).date()
        example_fixtures = [r for r in example_fixtures if r["scheduled_time_utc"].date() >= since_d]
    if args.until:
        until_d = datetime.fromisoformat(args.until).date()
        example_fixtures = [r for r in example_fixtures if r["scheduled_time_utc"].date() < until_d]

    if not example_fixtures:
        logger.warning("No fixtures to ingest.")
        return 0

    df = pl.DataFrame(example_fixtures)
    df = maybe_limit_df(df, args.limit)
    echo_df_info(df, note="fixtures (post-limit)")

    if args.dry_run:
        logger.info("DRY RUN: skipping writes")
        return 0

    output_root = project_root / "bronze" / "fixtures"
    parquet_write(df, output_root, partition_cols=["season", "round"], overwrite=args.overwrite)
    logger.info("Wrote %d rows → %s", len(df), output_root)

    if args.csv:
        (output_root / f"fixtures_{season}.csv").write_text(df.write_csv())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
