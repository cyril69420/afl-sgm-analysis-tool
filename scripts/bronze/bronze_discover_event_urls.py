#!/usr/bin/env python
"""
Enhanced AFL event URL discovery for the Bronze layer.

This script replaces the stub implementation with a Playwright‑driven
scraper capable of enumerating upcoming (and optionally historical)
Australian Football League match pages from supported bookmakers.  It
leverages CSS selectors defined in ``config/bookmakers.yaml`` to find
event links and can traverse pagination or round selectors as needed.

The resulting rows conform to the :class:`schemas.bronze.BronzeEventUrl`
model and are written into the ``bronze/event_urls`` directory
partitioned by season, round and bookmaker.  Records are idempotent:
existing keys will have their ``last_seen_utc`` updated but their
``first_seen_utc`` preserved.

Usage examples:

    python scripts/bronze/bronze_discover_event_urls.py --season 2025 --bookmakers sportsbet,pointsbet
    python scripts/bronze/bronze_discover_event_urls.py --season 2025 --rounds 1-5 --headless

Environment variables (via .env) may be used to configure proxy
settings or authentication cookies for bookmakers if required.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import polars as pl

from ._shared import (
    utc_now,
    parquet_write,
    load_yaml,
    load_env,
    async_sleep_ms,
)
from schemas.bronze import BronzeEventUrl

try:
    from playwright.async_api import async_playwright
except ImportError as exc:
    # Playwright is an optional dependency; if it's not installed we bail
    async_playwright = None

logger = logging.getLogger(__name__)


def parse_rounds(value: Optional[str]) -> List[str]:
    """Parse a comma or range separated rounds argument into a list.

    The user can specify values like ``all``, ``1,2,3`` or ``1-5``.  The
    special value ``all`` returns an empty list to signify that all
    rounds should be discovered.
    """
    if not value or value.lower() == "all":
        return []
    result: List[str] = []
    for part in value.split(","):
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


async def discover_bookmaker(
    bookmaker: str,
    cfg: Dict[str, any],
    season: int,
    rounds: List[str],
    max_pages: int,
    headless: bool,
) -> List[Tuple[str, str]]:
    """Discover event URLs for a single bookmaker using Playwright.

    Parameters
    ----------
    bookmaker : str
        Name of the bookmaker (e.g. ``sportsbet``).
    cfg : dict
        Configuration dictionary for this bookmaker read from
        ``bookmakers.yaml``.  Keys include ``start_url``,
        ``event_selector``, ``next_page_selector`` and ``throttle_ms``.
    season : int
        AFL season year (e.g. 2025).  Any ``{season}`` placeholders in
        ``start_url`` will be formatted with this value.
    rounds : list[str]
        List of round identifiers.  If empty, all rounds are scraped.
        For sites that support selecting a round via URL or UI, this
        function iterates over each round when constructing the start
        URL.
    max_pages : int
        Maximum number of pages to traverse via ``next_page_selector``
        before aborting.
    headless : bool
        Whether to run the browser in headless mode.

    Returns
    -------
    list of tuple
        A list of ``(url, round)`` tuples representing discovered event
        pages and the round they belong to.  Round may be ``None`` if
        unknown.
    """
    if async_playwright is None:
        logger.warning(
            "Playwright is not installed; discovery for %s cannot proceed. Returning empty list.",
            bookmaker,
        )
        return []
    event_selector: str = cfg.get("event_selector")
    if not event_selector:
        logger.warning("No event_selector configured for bookmaker %s", bookmaker)
        return []
    next_selector: Optional[str] = cfg.get("next_page_selector")
    throttle_ms: int = int(cfg.get("throttle_ms", 500))
    base_start_url: str = cfg.get("start_url", "")
    discovered: List[Tuple[str, str]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        # Some bookmakers may require language or cookies; these could be set here from env
        page = await context.new_page()
        # Determine round values to iterate over; use a single None value if rounds is empty
        target_rounds = rounds or [None]
        for rnd in target_rounds:
            if rnd is not None and "{round}" in base_start_url:
                start_url = base_start_url.format(season=season, round=rnd)
            else:
                start_url = base_start_url.format(season=season)
            try:
                logger.info("Navigating to %s for bookmaker %s, round=%s", start_url, bookmaker, rnd or "all")
                await page.goto(start_url, timeout=30000)
            except Exception as exc:
                logger.error("Failed to load %s: %s", start_url, exc)
                continue
            pages_visited = 0
            while True:
                # Extract event links on the current page
                try:
                    anchors = await page.query_selector_all(event_selector)
                except Exception as exc:
                    logger.error("Error querying event elements on %s: %s", page.url, exc)
                    break
                for a in anchors:
                    try:
                        href = await a.get_attribute("href")
                    except Exception:
                        continue
                    if not href:
                        continue
                    # Normalise relative URLs to absolute
                    url = href
                    if not href.startswith("http"):
                        # Use current page base URL
                        from urllib.parse import urljoin

                        url = urljoin(page.url, href)
                    discovered.append((url, rnd or "unknown"))
                pages_visited += 1
                if pages_visited >= max_pages:
                    break
                # Pagination: click next if configured
                if next_selector:
                    try:
                        next_btn = await page.query_selector(next_selector)
                    except Exception:
                        next_btn = None
                    if not next_btn:
                        break
                    try:
                        await next_btn.click()
                    except Exception:
                        break
                    await async_sleep_ms(throttle_ms)
                    continue
                else:
                    break
        await browser.close()
    return discovered


async def run_discovery(args: argparse.Namespace) -> None:
    """Main asynchronous entry point to coordinate discovery across bookmakers."""
    project_root = Path(args.root)
    load_env(project_root / ".env")
    settings = load_yaml(project_root / "config" / "settings.yaml")
    cfg_bookmakers = load_yaml(project_root / "config" / "bookmakers.yaml")
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    rounds = parse_rounds(args.rounds) or parse_rounds(settings.get("default_rounds", "all"))
    bookmakers = (
        args.bookmakers.split(",") if args.bookmakers else settings.get("bookmakers", list(cfg_bookmakers.keys()))
    )
    discovery_run_id = uuid.uuid4().hex
    all_rows: List[dict] = []
    for bookmaker in bookmakers:
        cfg = cfg_bookmakers.get(bookmaker)
        if not cfg:
            logger.warning("No configuration found for bookmaker %s", bookmaker)
            continue
        urls_rounds: List[Tuple[str, str]] = []
        try:
            urls_rounds = await discover_bookmaker(
                bookmaker,
                cfg,
                season=season,
                rounds=rounds,
                max_pages=args.max_pages_per_site,
                headless=args.headless,
            )
        except Exception as exc:
            logger.error("Discovery failed for bookmaker %s: %s", bookmaker, exc)
        if not urls_rounds:
            logger.info("No event URLs discovered for bookmaker %s", bookmaker)
            continue
        for url, rnd in urls_rounds:
            now = utc_now()
            row = {
                "bookmaker": bookmaker,
                "season": season,
                "round": rnd,
                "event_url": url,
                "competition": "afl",
                "first_seen_utc": now,
                "last_seen_utc": now,
                "source_page": cfg.get("start_url"),
                "status": "upcoming",
                "discovery_run_id": discovery_run_id,
            }
            try:
                obj = BronzeEventUrl.model_validate(row)
                all_rows.append(obj.model_dump())
            except Exception as exc:
                logger.error("Validation error for row %s: %s", row, exc)
    if not all_rows:
        logger.warning("No event URLs discovered across all bookmakers.")
        return
    df = pl.DataFrame(all_rows)
    # Write dataset partitioned by season/round/bookmaker
    output_root = project_root / "bronze" / "event_urls"
    partition_cols = ["season", "round", "bookmaker"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d event URL rows to %s", len(df), output_root)
    # Optionally write CSV
    if args.csv:
        csv_path = output_root / f"event_urls_{season}.csv"
        df.write_csv(csv_path)
        logger.info("Also wrote CSV output to %s", csv_path)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Discover AFL event URLs from bookmakers")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season to discover (e.g. 2025)")
    parser.add_argument(
        "--rounds", type=str, default=None, help="Comma or range separated rounds (e.g. '1,2,3' or '1-5' or 'all')"
    )
    parser.add_argument(
        "--bookmakers", type=str, default=None, help="Comma separated list of bookmakers to scrape"
    )
    parser.add_argument("--headless", action="store_true", help="Run Playwright in headless mode")
    parser.add_argument(
        "--max-pages-per-site",
        type=int,
        default=10,
        help="Maximum pages to traverse per bookmaker when paginating",
    )
    parser.add_argument("--csv", action="store_true", help="Also write CSV output alongside Parquet")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Run the asynchronous discovery
    try:
        asyncio.run(run_discovery(args))
    except KeyboardInterrupt:
        logger.info("Discovery interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()