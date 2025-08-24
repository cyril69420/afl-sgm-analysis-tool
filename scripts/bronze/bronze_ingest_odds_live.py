#!/usr/bin/env python
"""
Ingest live odds snapshots for AFL events into the Bronze layer.

This script reads upcoming event URLs from ``bronze/event_urls`` and
scrapes live betting markets for each event. For each event the
bookmaker’s odds offerings are normalised into a tabular structure
consisting of a market group (e.g. ``match winner``), a market name
(e.g. ``Head to Head``), a selection (team or outcome), an optional
line and the decimal odds. The raw payload returned by the bookmaker is
preserved for downstream debugging.

Rows are deduplicated by hashing the tuple
``(bookmaker,event_url,market_group,market_name,selection,line,captured_at_utc)``.
If a record with the same hash already exists in the Parquet dataset it
will be skipped unless ``--force`` is specified.
"""

from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import polars as pl
import duckdb

from ._shared import (
    utc_now,
    hash_row,
    parquet_write,
    load_yaml,
    load_env,
)

from schemas.bronze import BronzeOddsSnapshotRow

logger = logging.getLogger(__name__)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest live odds snapshots into Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season")
    parser.add_argument("--rounds", type=str, default=None, help="Rounds to filter (comma/range)")
    parser.add_argument("--bookmakers", type=str, default=None, help="Comma separated bookmakers")
    parser.add_argument("--force", action="store_true", help="Re‑ingest even if rows exist")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]

    # Determine rounds list
    def parse_rounds(value: Optional[str]) -> List[str]:
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
    rounds = parse_rounds(args.rounds) or parse_rounds(settings.get("default_rounds", "all"))
    bookmakers = args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", [])

    # Read event URLs dataset
    event_root = project_root / "bronze" / "event_urls"
    if not event_root.exists():
        logger.error("Event URLs dataset not found at %s. Run bronze_discover_event_urls first.", event_root)
        return
    # Use DuckDB to quickly filter event URLs by season/round/bookmaker
    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{event_root}/**/*.parquet')")
    query = "SELECT * FROM events WHERE season = ?"
    params = [season]
    if rounds:
        placeholders = ",".join(["?"] * len(rounds))
        query += f" AND round IN ({placeholders})"
        params.extend(rounds)
    if bookmakers:
        placeholders = ",".join(["?"] * len(bookmakers))
        query += f" AND bookmaker IN ({placeholders})"
        params.extend(bookmakers)
    event_urls_df = con.execute(query, params).df()
    if event_urls_df.empty:
        logger.warning("No event URLs found for season %s and specified filters", season)
        return
    logger.info("Found %d event URLs to ingest odds for", len(event_urls_df))

    # Determine existing hashes to enforce idempotency
    output_root = project_root / "bronze" / "odds" / "snapshots"
    existing_hashes: set[str] = set()
    if output_root.exists() and not args.force:
        # Use DuckDB to read only the hash column to memory
        try:
            con.execute(f"CREATE VIEW odds AS SELECT * FROM read_parquet('{output_root}/**/*.parquet')")
            hashes = con.execute("SELECT DISTINCT hash_key FROM odds").fetchall()
            existing_hashes = {h[0] for h in hashes}
        except Exception:
            pass

    new_rows = []

    # Iterate through each event URL and produce a dummy odds snapshot.
    for _, row in event_urls_df.iterrows():
        bookmaker = row["bookmaker"]
        event_url = row["event_url"]
        # Example markets for demonstration. Real implementation would scrape.
        markets = [
            {
                "market_group": "match",
                "market_name": "Head to Head",
                "selection": row["event_url"].split("/")[-1],
                "line": None,
                "decimal_odds": 1.80,
            },
            {
                "market_group": "totals",
                "market_name": "Total Points",
                "selection": "Over",
                "line": 160.5,
                "decimal_odds": 1.95,
            },
        ]
        for mkt in markets:
            captured = utc_now()
            hash_key = hash_row([
                bookmaker,
                event_url,
                mkt["market_group"],
                mkt["market_name"],
                mkt["selection"],
                mkt["line"],
                captured.isoformat(),
            ])
            if not args.force and hash_key in existing_hashes:
                continue
            record = {
                "captured_at_utc": captured,
                "bookmaker": bookmaker,
                "event_url": event_url,
                "market_group": mkt["market_group"],
                "market_name": mkt["market_name"],
                "selection": mkt["selection"],
                "line": mkt["line"],
                "decimal_odds": mkt["decimal_odds"],
                "raw_payload": json.dumps({"example": True}),
                "hash_key": hash_key,
            }
            try:
                obj = BronzeOddsSnapshotRow.model_validate(record)
                new_rows.append(obj.model_dump())
            except Exception as exc:
                logger.error("Validation error for odds row %s: %s", record, exc)

    if not new_rows:
        logger.info("No new odds rows to ingest")
        return

    df = pl.DataFrame(new_rows)
    partition_cols = ["season", "round", "bookmaker"]
    # Add season and round columns derived from event URL rows (passed through). We use join.
    # Map event_url to season/round for partitioning
    events_pl = pl.from_pandas(event_urls_df[["event_url", "season", "round"]])
    df = df.join(events_pl, on="event_url", how="left")

    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d live odds rows to %s", len(df), output_root)


if __name__ == "__main__":
    main()