#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import polars as pl

# Use ONLY the helpers that exist in your _shared.py
from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    preview_or_write,
    echo_df_info,
    load_yaml,
    load_env,
)

logger = logging.getLogger(__name__)


def _parse_rounds(value: Optional[str]) -> List[str]:
    if not value or value.lower() == "all":
        return []
    out: List[str] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            s, e = part.split("-", 1)
            out.extend([str(i) for i in range(int(s), int(e) + 1)])
        else:
            out.append(part)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Discover AFL event URLs (Bronze)")
    parser.add_argument("--root", default=".", help="Project root")
    parser.add_argument("--season", type=int, help="Season e.g. 2025")
    parser.add_argument("--rounds", type=str, help="Comma list or range, or 'all'")
    parser.add_argument("--bookmakers", type=str, help="Comma-separated")
    parser.add_argument("--csv", action="store_true", help="Also mirror CSV")
    parser.add_argument("--log-level", default="INFO")
    add_common_test_flags(parser)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    root = Path(args.root)
    load_env(root / ".env")

    settings = load_yaml(root / "config" / "settings.yaml") or {}
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    rounds = _parse_rounds(args.rounds) or _parse_rounds(settings.get("default_rounds", "all"))
    bookmakers = (args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", [])) or []

    now_utc = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex

    # NOTE: Keep your real scraping here; below is a deterministic placeholder
    rows = []
    target_rounds = rounds or ["R1", "R2"]
    for bk in bookmakers:
        for rnd in target_rounds:
            rows.append(
                {
                    "bookmaker": bk,
                    "season": season,
                    "round": rnd,
                    "event_url": f"https://example.com/{bk}/{season}/{rnd}",
                    "competition": "afl",
                    "first_seen_utc": now_utc,
                    "last_seen_utc": now_utc,
                    "status": "upcoming",
                    "discovery_run_id": run_id,
                }
            )

    if not rows:
        logger.warning("No rows discovered (check bookmakers/rounds).")
        return 0

    df = pl.DataFrame(rows)
    df = maybe_limit_df(df, args.limit)
    echo_df_info(df, note="event_urls (post-limit)")

    preview_or_write(
        df,
        dry_run=args.dry_run,
        out_subdir="event_urls",
        out_stem=f"event_urls_{season}",
        fmt="parquet",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
