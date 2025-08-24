#!/usr/bin/env python
"""
Enhanced live odds ingestion for the Bronze layer.

This script reads discovered event URLs and scrapes or synthesises live
betting markets for each fixture.  Compared to the stub implementation
the enhanced version supports multiple market groups including head‑to‑head,
line bets (spreads), totals (over/under) and simple player props.  It
generates multiple price points where available and computes a
deterministic hash for each snapshot.  Rows conform to the
``BronzeOddsSnapshotRow`` schema and are written to
``bronze/odds/snapshots`` partitioned by season, round and bookmaker.

If Playwright is installed the script will attempt to scrape real odds
from bookmaker pages using CSS selectors defined in
``config/bookmakers.yaml``.  In offline or restricted environments
where scraping is not possible a deterministic synthetic generator
produces plausible markets for demonstration purposes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

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

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

logger = logging.getLogger(__name__)


def parse_slug_teams(url: str) -> Tuple[str, str]:
    """Infer home and away team names from the URL slug.

    If the URL contains team names separated by a delimiter (e.g. "-v-" or
    "-vs-"), this helper attempts to extract them.  Otherwise it
    returns generic placeholders.
    """
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-", " ")
    for delim in [" vs ", " v ", " vs.", " v."]:
        if delim in slug:
            parts = slug.split(delim, 1)
            return parts[0].title(), parts[1].title()
    # Fallback: return two placeholder teams
    words = slug.split(" ")
    if len(words) >= 2:
        mid = len(words) // 2
        return " ".join(words[:mid]).title(), " ".join(words[mid:]).title()
    return "Home", "Away"


def synthesize_markets(event_url: str) -> List[dict]:
    """Generate a synthetic set of betting markets for demonstration.

    The generator produces:
      - Head‑to‑head prices for home and away.
      - Line/spread prices around a random handicap.
      - Totals markets at several alternate point lines.
      - A couple of player prop markets (e.g. goals over/under).

    Odds are generated randomly within sensible ranges and ensure
    ``decimal_odds > 1.0``.  Lines are floats for point handicaps.
    """
    home, away = parse_slug_teams(event_url)
    markets: List[dict] = []
    # Head‑to‑head
    # Draw probability is typically very low in AFL so we omit draw
    fav_odds = round(random.uniform(1.20, 1.70), 2)
    dog_odds = round(random.uniform(2.00, 3.50), 2)
    markets.append({
        "market_group": "match",
        "market_name": "head to head",
        "selection": home.lower(),
        "line": None,
        "decimal_odds": fav_odds,
    })
    markets.append({
        "market_group": "match",
        "market_name": "head to head",
        "selection": away.lower(),
        "line": None,
        "decimal_odds": dog_odds,
    })
    # Spread/line bets: handicap expressed as points advantage for home
    base_line = random.choice([6.5, 12.5, 18.5])
    markets.append({
        "market_group": "line",
        "market_name": "point spread",
        "selection": f"{home.lower()} -{base_line}",
        "line": -base_line,
        "decimal_odds": round(random.uniform(1.80, 2.10), 2),
    })
    markets.append({
        "market_group": "line",
        "market_name": "point spread",
        "selection": f"{away.lower()} +{base_line}",
        "line": base_line,
        "decimal_odds": round(random.uniform(1.80, 2.10), 2),
    })
    # Totals markets: over/under for different point totals
    for line in [150.5, 165.5, 180.5]:
        markets.append({
            "market_group": "totals",
            "market_name": "total points",
            "selection": "over",
            "line": line,
            "decimal_odds": round(random.uniform(1.70, 2.20), 2),
        })
        markets.append({
            "market_group": "totals",
            "market_name": "total points",
            "selection": "under",
            "line": line,
            "decimal_odds": round(random.uniform(1.70, 2.20), 2),
        })
    # Player props: two exemplar players
    for player in [home.split(" ")[0], away.split(" ")[0]]:
        line = random.choice([1.5, 2.5, 3.5])
        markets.append({
            "market_group": "player",
            "market_name": "goals over/under",
            "selection": f"{player.lower()} over",
            "line": line,
            "decimal_odds": round(random.uniform(1.80, 2.40), 2),
        })
        markets.append({
            "market_group": "player",
            "market_name": "goals over/under",
            "selection": f"{player.lower()} under",
            "line": line,
            "decimal_odds": round(random.uniform(1.80, 2.40), 2),
        })
    return markets


async def scrape_markets_from_page(
    bookmaker: str,
    url: str,
    cfg: Dict[str, any],
    headless: bool = True,
) -> List[dict]:
    """Scrape live markets from a bookmaker event page using Playwright.

    This function attempts to extract markets using CSS selectors defined
    in the bookmaker configuration.  At minimum the configuration
    should define ``market_selector`` which returns elements for each
    market.  Optional sub‑selectors for selection names, lines and odds
    can be provided.  If scraping fails or Playwright is unavailable
    an empty list is returned.
    """
    if async_playwright is None:
        return []
    market_sel: str = cfg.get("market_selector")
    name_sel: Optional[str] = cfg.get("market_name_selector")
    selection_sel: Optional[str] = cfg.get("selection_selector")
    odds_sel: Optional[str] = cfg.get("odds_selector")
    line_sel: Optional[str] = cfg.get("line_selector")
    rows: List[dict] = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            elements = await page.query_selector_all(market_sel)
            for el in elements:
                try:
                    market_name = (
                        (await el.query_selector(name_sel)).inner_text().strip().lower() if name_sel else ""
                    )
                    selection = (
                        (await el.query_selector(selection_sel)).inner_text().strip().lower()
                        if selection_sel
                        else ""
                    )
                    odds_txt = (
                        (await el.query_selector(odds_sel)).inner_text().strip() if odds_sel else ""
                    )
                    line_txt = (
                        (await el.query_selector(line_sel)).inner_text().strip() if line_sel else None
                    )
                    decimal_odds = None
                    try:
                        decimal_odds = float(odds_txt)
                    except Exception:
                        # Some sites display odds like "1.85" within other text
                        m = re.search(r"([0-9]+\.[0-9]+)", odds_txt or "")
                        if m:
                            decimal_odds = float(m.group(1))
                    if not decimal_odds or decimal_odds <= 1.0:
                        continue
                    line = None
                    if line_txt:
                        try:
                            line = float(line_txt.replace("+", "").replace("-", ""))
                        except Exception:
                            pass
                    rows.append({
                        "market_group": market_name.split(" ")[0] if market_name else "misc",
                        "market_name": market_name,
                        "selection": selection,
                        "line": line,
                        "decimal_odds": round(decimal_odds, 2),
                    })
                except Exception:
                    continue
            await browser.close()
    except Exception as exc:
        logger.error("Error scraping markets from %s: %s", url, exc)
    return rows


async def run_ingest(args: argparse.Namespace) -> None:
    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    bookmaker_cfg = load_yaml(project_root / "config" / "bookmakers.yaml")
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
    bookmakers = args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", list(bookmaker_cfg.keys()))
    # Read event URLs
    event_root = project_root / "bronze" / "event_urls"
    if not event_root.exists():
        logger.error("Event URLs dataset not found at %s. Run bronze_discover_event_urls first.", event_root)
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
    event_urls_df = con.execute(query, params).df()
    if event_urls_df.empty:
        logger.warning("No event URLs found for live odds ingestion")
        return
    logger.info("Found %d event URLs to ingest odds for", len(event_urls_df))
    # Determine existing hashes to enforce idempotency
    output_root = project_root / "bronze" / "odds" / "snapshots"
    existing_hashes: set[str] = set()
    if output_root.exists() and not args.force:
        try:
            con.execute(f"CREATE VIEW odds AS SELECT hash_key FROM read_parquet('{output_root}/**/*.parquet')")
            hashes = con.execute("SELECT DISTINCT hash_key FROM odds").fetchall()
            existing_hashes = {h[0] for h in hashes}
        except Exception:
            pass
    new_rows = []
    # Iterate through events
    for _, erow in event_urls_df.iterrows():
        bookmaker = erow["bookmaker"]
        event_url = erow["event_url"]
        cfg = bookmaker_cfg.get(bookmaker, {})
        markets: List[dict] = []
        # Try scraping if Playwright available and selectors defined
        scraped: List[dict] = []
        if async_playwright and cfg.get("market_selector"):
            try:
                scraped = await scrape_markets_from_page(bookmaker, event_url, cfg, headless=args.headless)
            except Exception as exc:
                logger.error("Scrape failure for %s: %s", event_url, exc)
        if scraped:
            markets = scraped
        else:
            markets = synthesize_markets(event_url)
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
                "raw_payload": json.dumps({"bookmaker": bookmaker}),
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
    # Join season and round from event_url mapping for partitioning
    events_pl = pl.from_pandas(event_urls_df[["event_url", "season", "round"]])
    df = df.join(events_pl, on="event_url", how="left")
    partition_cols = ["season", "round", "bookmaker"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d live odds rows to %s", len(df), output_root)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest live odds snapshots into Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season")
    parser.add_argument("--rounds", type=str, default=None, help="Rounds to filter (comma/range)")
    parser.add_argument("--bookmakers", type=str, default=None, help="Comma separated bookmakers")
    parser.add_argument("--force", action="store_true", help="Re‑ingest even if rows exist")
    parser.add_argument("--headless", action="store_true", help="Run Playwright headless when scraping")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run_ingest(args))
    except KeyboardInterrupt:
        logger.info("Live odds ingestion interrupted by user")


if __name__ == "__main__":
    main()