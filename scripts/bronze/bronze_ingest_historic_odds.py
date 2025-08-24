#!/usr/bin/env python
"""
Enhanced historical odds ingestion for the Bronze layer.

This script backfills historical price movements for AFL events.  It
extends the stub implementation by generating a richer time series of
odds snapshots across multiple markets and at several points in time
leading up to kick‑off.  Each row conforms to the
``BronzeOddsHistoryRow`` schema and includes a ``source_kind`` of
``'historical'``.  Outputs are written to ``bronze/odds/history``
partitioned by season, round and bookmaker.

In the absence of a real historical odds API the script synthesises
time series data relative to the scheduled kick‑off time.  You can
extend the generation logic to integrate real data sources via HTTP
requests or scraping archived pages.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
from datetime import datetime, timedelta
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
from schemas.bronze import BronzeOddsHistoryRow

logger = logging.getLogger(__name__)


def parse_rounds(value: Optional[str]) -> List[str]:
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


def synthesize_historic_markets(event_url: str) -> List[dict]:
    """Generate a small set of historical markets mirroring the live stub.

    We reuse the synthetic generator from ``bronze_ingest_odds_live`` by
    importing it lazily to avoid a hard dependency between modules.  If
    the generator cannot be imported we return a basic head‑to‑head
    market only.
    """
    try:
        from .bronze_ingest_odds_live import synthesize_markets as live_gen
        return live_gen(event_url)
    except Exception:
        # Fall back to simple match market
        return [
            {
                "market_group": "match",
                "market_name": "head to head",
                "selection": "home",
                "line": None,
                "decimal_odds": round(random.uniform(1.50, 2.50), 2),
            }
        ]


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest historical odds into Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season")
    parser.add_argument("--rounds", type=str, default=None, help="Rounds to filter")
    parser.add_argument("--bookmakers", type=str, default=None, help="Comma separated bookmakers")
    parser.add_argument("--force", action="store_true", help="Re‑ingest even if rows exist")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    rounds = parse_rounds(args.rounds) or parse_rounds(settings.get("default_rounds", "all"))
    bookmakers = args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", [])
    # Read event URLs dataset
    event_root = project_root / "bronze" / "event_urls"
    if not event_root.exists():
        logger.error("Event URLs dataset not found. Run bronze_discover_event_urls first.")
        return
    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{event_root}/**/*.parquet')")
    query = "SELECT * FROM events WHERE season = ?"
    params: List = [season]
    if rounds:
        placeholders = ",".join(["?"] * len(rounds))
        query += f" AND round IN ({placeholders})"
        params.extend(rounds)
    if bookmakers:
        placeholders = ",".join(["?"] * len(bookmakers))
        query += f" AND bookmaker IN ({placeholders})"
        params.extend(bookmakers)
    event_df = con.execute(query, params).df()
    if event_df.empty:
        logger.warning("No event URLs found for historical odds ingestion")
        return
    output_root = project_root / "bronze" / "odds" / "history"
    existing_hashes: set[str] = set()
    if output_root.exists() and not args.force:
        try:
            con.execute(f"CREATE VIEW odds_hist AS SELECT hash_key FROM read_parquet('{output_root}/**/*.parquet')")
            hashes = con.execute("SELECT DISTINCT hash_key FROM odds_hist").fetchall()
            existing_hashes = {h[0] for h in hashes}
        except Exception:
            pass
    new_rows = []
    for _, row in event_df.iterrows():
        bookmaker = row["bookmaker"]
        event_url = row["event_url"]
        # Use synthetic generator for markets
        markets = synthesize_historic_markets(event_url)
        # Simulate 5 timepoints: 1 week, 3 days, 1 day, 6h, 1h prior to now (proxy for kickoff)
        now = utc_now()
        offsets = [timedelta(days=7), timedelta(days=3), timedelta(days=1), timedelta(hours=6), timedelta(hours=1)]
        for offset in offsets:
            captured_time = now - offset
            for mkt in markets:
                record = {
                    "captured_at_utc": captured_time,
                    "bookmaker": bookmaker,
                    "event_url": event_url,
                    "market_group": mkt["market_group"],
                    "market_name": mkt["market_name"],
                    "selection": mkt["selection"],
                    "line": mkt["line"],
                    "decimal_odds": mkt["decimal_odds"],
                    "raw_payload": json.dumps({"historical": True}),
                    "source_kind": "historical",
                }
                hash_key = hash_row([
                    bookmaker,
                    event_url,
                    record["market_group"],
                    record["market_name"],
                    record["selection"],
                    record["line"],
                    record["captured_at_utc"].isoformat(),
                ])
                record["hash_key"] = hash_key
                if not args.force and hash_key in existing_hashes:
                    continue
                try:
                    obj = BronzeOddsHistoryRow.model_validate(record)
                    new_rows.append(obj.model_dump())
                except Exception as exc:
                    logger.error("Validation error for historical odds row %s: %s", record, exc)
    if not new_rows:
        logger.info("No new historical odds rows to ingest")
        return
    df = pl.DataFrame(new_rows)
    # Join season and round information
    events_pl = pl.from_pandas(event_df[["event_url", "season", "round"]])
    df = df.join(events_pl, on="event_url", how="left")
    partition_cols = ["season", "round", "bookmaker"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d historical odds rows to %s", len(df), output_root)


if __name__ == "__main__":
    main()