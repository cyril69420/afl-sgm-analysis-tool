#!/usr/bin/env python
"""
Backfill historical odds into Bronze (adds --dry-run/--limit; keeps partitioned writes).
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
import polars as pl

# shared helpers
from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    echo_df_info,
    utc_now,
    hash_row,
    parquet_write,
    load_yaml,
    load_env,
)
from schemas.bronze import BronzeOddsHistoryRow

LOG = logging.getLogger(__name__)


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
    parser = argparse.ArgumentParser(description="Ingest historical odds into Bronze")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season")
    parser.add_argument("--rounds", type=str, default=None, help="Comma/range or 'all'")
    parser.add_argument("--bookmakers", type=str, default=None, help="Comma-separated")
    parser.add_argument("--force", action="store_true", help="Ignore existing hashes")
    parser.add_argument("--log-level", default="INFO")
    add_common_test_flags(parser)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml") or {}
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]

    rounds = _parse_rounds(args.rounds) or _parse_rounds(settings.get("default_rounds", "all"))
    bookmakers = args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", [])

    # Event URLs
    event_root = (project_root / "bronze" / "event_urls")
    if not event_root.exists():
        LOG.warning("Event URLs dataset not found at %s. Run bronze_discover_event_urls first.", event_root)
        return 0

    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{event_root.as_posix()}/**/*.parquet')")
    except duckdb.IOException:
        LOG.warning("No event_urls parquet found under %s; nothing to backfill.", event_root)
        return 0

    q = "SELECT * FROM events WHERE season = ?"
    params: List = [season]
    if rounds:
        q += f" AND round IN ({','.join(['?'] * len(rounds))})"
        params.extend(rounds)
    if bookmakers:
        q += f" AND bookmaker IN ({','.join(['?'] * len(bookmakers))})"
        params.extend(bookmakers)
    event_df = con.execute(q, params).df()
    if event_df.empty:
        LOG.warning("No event URLs matched historical-odds filters.")
        return 0

    output_root = project_root / "bronze" / "odds" / "history"

    # Existing hash keys
    existing_hashes: set[str] = set()
    if output_root.exists() and not args.force:
        try:
            con.execute(f"CREATE VIEW hist AS SELECT * FROM read_parquet('{output_root.as_posix()}/**/*.parquet')")
            existing_hashes = {h[0] for h in con.execute("SELECT DISTINCT hash_key FROM hist").fetchall()}
        except Exception:
            pass

    new_rows = []
    # (stub) synthesize a couple of historical snapshots relative to a fake kickoff
    for _, row in event_df.iterrows():
        bookmaker = row["bookmaker"]
        event_url = row["event_url"]
        kickoff = utc_now() + timedelta(days=2)  # placeholder for demo
        for t in [kickoff - timedelta(hours=48), kickoff - timedelta(hours=24)]:
            rec = {
                "captured_at_utc": t,
                "bookmaker": bookmaker,
                "event_url": event_url,
                "market_group": "match",
                "market_name": "Head to Head",
                "selection": "Example",
                "line": None,
                "decimal_odds": 1.90,
                "raw_payload": json.dumps({"historical": True}),
                "source_kind": "historical",
            }
            h = hash_row([bookmaker, event_url, rec["market_group"], rec["market_name"], rec["selection"], rec["line"], t.isoformat()])
            rec["hash_key"] = h
            if not args.force and h in existing_hashes:
                continue
            try:
                obj = BronzeOddsHistoryRow.model_validate(rec)
                new_rows.append(obj.model_dump())
            except Exception as exc:
                LOG.error("Validation error: %s", exc)

    if not new_rows:
        LOG.info("No new historical odds rows to write.")
        return 0

    df = pl.DataFrame(new_rows)
    events_pl = pl.from_pandas(event_df[["event_url", "season", "round"]])
    df = df.join(events_pl, on="event_url", how="left")

    df = maybe_limit_df(df, args.limit)
    echo_df_info(df, note="odds_history (post-limit)")

    if args.dry_run:
        LOG.info("DRY RUN: skipping writes")
        return 0

    parquet_write(df, output_root, partition_cols=["season", "round", "bookmaker"])
    LOG.info("Wrote %d rows → %s", len(df), output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
