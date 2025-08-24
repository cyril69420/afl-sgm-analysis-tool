#!/usr/bin/env python
"""
Enhanced AFL fixture ingestion for the Bronze layer.

This script reads event URLs discovered by ``bronze_discover_event_urls`` and
attempts to extract detailed fixture information from each event page.
Where possible it uses Playwright to parse the bookmaker's event page;
otherwise it falls back to heuristics based on the URL slug.  Each
fixture row includes the home and away teams, venue, scheduled kick‑off
time in UTC and the originating timezone.  Venue metadata (timezone
and coordinates) are loaded from ``config/venues.yaml``.

Rows are validated against :class:`schemas.bronze.BronzeFixtureRow` and
written to the ``bronze/fixtures`` directory partitioned by season and
round.  Idempotency is achieved by computing a deterministic
``game_key`` from the season, round, teams and scheduled time.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl

from ._shared import (
    utc_now,
    to_utc,
    parquet_write,
    load_yaml,
    load_env,
    load_venue_lookup,
)
from schemas.bronze import BronzeFixtureRow

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

logger = logging.getLogger(__name__)


def slugify(value: str) -> str:
    """Simple slugification: lowercases and replaces spaces with hyphens."""
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def build_game_key(season: int, round_id: str, home: str, away: str, kickoff_utc: datetime) -> str:
    slug_home = slugify(home)
    slug_away = slugify(away)
    ts = kickoff_utc.strftime("%Y%m%dT%H%M%SZ")
    return f"{season}_{round_id}_{slug_home}_{slug_away}_{ts}"


async def parse_fixture_from_url(
    bookmaker: str,
    url: str,
    season: int,
    round_id: str,
    venue_lookup: Dict[str, Dict[str, any]],
    headless: bool = True,
) -> Optional[dict]:
    """Attempt to extract fixture details from a bookmaker event page.

    Parameters
    ----------
    bookmaker : str
        Name of the bookmaker.
    url : str
        URL of the event page.
    season : int
        AFL season year.
    round_id : str
        Round identifier.
    venue_lookup : dict
        Mapping of venue names to metadata (lat, lon, tz).
    headless : bool
        Whether to run Playwright in headless mode when scraping.

    Returns
    -------
    dict or None
        A dictionary matching the BronzeFixtureRow fields, or None if
        parsing failed.
    """
    # Heuristic fallback: parse slug from URL
    def fallback() -> dict:
        slug = url.rstrip("/").split("/")[-1]
        slug_clean = slug.replace("-", " ")
        # Attempt to split on ' vs ' or ' v '
        home, away = None, None
        for delim in [" vs ", " v ", " vs.", " v."]:
            if delim in slug_clean:
                parts = slug_clean.split(delim, 1)
                home = parts[0].replace("-", " ").title().strip()
                away = parts[1].replace("-", " ").title().strip()
                break
        if home is None or away is None:
            # Fallback to splitting on hyphens; assume last two teams separated by 'and'
            parts = slug_clean.split(" ")
            if len(parts) >= 4:
                mid = len(parts) // 2
                home = " ".join(parts[:mid]).title()
                away = " ".join(parts[mid:]).title()
            else:
                home = slug_clean.title()
                away = "Unknown"
        # Kickoff time heuristic: schedule at 7:40pm local on Saturday of round number
        try:
            rnd_int = int(round_id) if isinstance(round_id, str) and round_id.isdigit() else 1
        except Exception:
            rnd_int = 1
        # Schedule on first Saturday of March + round index
        kickoff_local = datetime(season, 3, 1) + timedelta(days=(rnd_int - 1) * 7)
        kickoff_local = kickoff_local.replace(hour=19, minute=40)
        # Determine venue by matching any known venue substring in slug
        venue = "Unknown"
        for vname in venue_lookup.keys():
            key = vname.lower().replace(" ", "")
            if key in slug.lower().replace("-", ""):
                venue = vname
                break
        # Source timezone from venue lookup or default
        tz = venue_lookup.get(venue, {}).get("tz", "Australia/Melbourne")
        kickoff_utc = to_utc(kickoff_local, tz)
        return {
            "bookmaker_event_url": url,
            "season": season,
            "round": round_id,
            "home": home,
            "away": away,
            "venue": venue,
            "scheduled_time_utc": kickoff_utc,
            "source_tz": tz,
            "discovered_utc": utc_now(),
        }

    # Attempt Playwright scraping if installed
    if async_playwright is None:
        return fallback()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            # Attempt generic selectors for team names and kick‑off time.  These
            # may need adjustment per bookmaker.  If they fail we fall back.
            home = None
            away = None
            venue = None
            ko_str = None
            try:
                # Many AFL event pages contain the team names within <h1> or
                # <h2> elements separated by a 'v' or 'vs'.  We'll query the
                # heading text and split accordingly.
                heading = await page.query_selector("h1, h2")
                if heading:
                    text = (await heading.inner_text()).strip()
                    # Normalise unicode dash
                    text_clean = re.sub(r"\s+", " ", text)
                    for delim in [" vs ", " v ", " vs.", " v."]:
                        if delim in text_clean.lower():
                            parts = text_clean.split(delim, 1)
                            home = parts[0].strip().title()
                            away = parts[1].strip().title()
                            break
            except Exception:
                pass
            # Kickoff time: look for datetime attribute or element with 'time'
            try:
                time_el = await page.query_selector("time")
                if time_el:
                    ko_str = await time_el.get_attribute("datetime")
                if not ko_str:
                    # Many sites use data-start or aria-label on a container
                    for attr in ["data-start", "aria-label"]:
                        el = await page.query_selector(f"[ {attr} ]")
                        if el:
                            ko_str = await el.get_attribute(attr)
                            if ko_str:
                                break
            except Exception:
                pass
            # Venue: search for any known venue name on the page text
            try:
                body_text = await page.inner_text("body")
                for vname in venue_lookup.keys():
                    if vname.lower() in body_text.lower():
                        venue = vname
                        break
            except Exception:
                pass
            await browser.close()
        # Fallback if any field missing
        if not home or not away or not ko_str or not venue:
            return fallback()
        # Parse kickoff time string into UTC
        # Attempt ISO 8601 parsing; if fails, use fallback
        try:
            # Some sites provide a full ISO string; parse via fromisoformat
            local_dt = datetime.fromisoformat(ko_str)
        except Exception:
            return fallback()
        # Determine timezone from venue lookup
        tz = venue_lookup.get(venue, {}).get("tz", "Australia/Melbourne")
        kickoff_utc = to_utc(local_dt, tz)
        return {
            "bookmaker_event_url": url,
            "season": season,
            "round": round_id,
            "home": home,
            "away": away,
            "venue": venue,
            "scheduled_time_utc": kickoff_utc,
            "source_tz": tz,
            "discovered_utc": utc_now(),
        }
    except Exception as exc:
        logger.error("Error scraping fixture from %s: %s", url, exc)
        return fallback()


async def run_ingest(args: argparse.Namespace) -> None:
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
    # Load event URLs
    event_dir = project_root / "bronze" / "event_urls"
    if not event_dir.exists():
        logger.error("Event URLs dataset not found at %s. Run bronze_discover_event_urls first.", event_dir)
        return
    con = pl.scan_parquet(f"{event_dir}/**/*.parquet").collect(streaming=True)
    events_df = con.filter(pl.col("season") == season)
    if rounds:
        events_df = events_df.filter(pl.col("round").is_in(rounds))
    if events_df.height == 0:
        logger.warning("No event URLs found for fixtures ingestion.")
        return
    venue_lookup = load_venue_lookup(project_root / "config" / "venues.yaml")
    rows: List[dict] = []
    # Process each event sequentially to avoid overwhelming the browser
    for rec in events_df.to_dicts():
        url = rec.get("event_url")
        bookmaker = rec.get("bookmaker")
        rnd = rec.get("round")
        fixture = await parse_fixture_from_url(bookmaker, url, season, str(rnd), venue_lookup, headless=args.headless)
        if fixture:
            # Compute game_key
            game_key = build_game_key(season, str(rnd), fixture["home"], fixture["away"], fixture["scheduled_time_utc"])
            fixture["game_key"] = game_key
            rows.append(fixture)
    if not rows:
        logger.warning("No fixtures to ingest.")
        return
    # Validate via Pydantic
    validated: List[dict] = []
    for row in rows:
        try:
            obj = BronzeFixtureRow.model_validate(row)
            validated.append(obj.model_dump())
        except Exception as exc:
            logger.error("Validation error for fixture %s: %s", row, exc)
    if not validated:
        logger.error("All fixture rows failed validation. Nothing to write.")
        return
    df = pl.DataFrame(validated)
    output_root = project_root / "bronze" / "fixtures"
    partition_cols = ["season", "round"]
    parquet_write(df, output_root, partition_cols=partition_cols)
    logger.info("Wrote %d fixture rows to %s", len(df), output_root)
    if args.csv:
        csv_path = output_root / f"fixtures_{season}.csv"
        df.write_csv(csv_path)
        logger.info("Also wrote CSV output to %s", csv_path)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest AFL fixtures into the Bronze layer")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, help="AFL season to ingest (e.g. 2025)")
    parser.add_argument("--rounds", type=str, default=None, help="Comma or range separated rounds to ingest")
    parser.add_argument("--headless", action="store_true", help="Run Playwright in headless mode for scraping")
    parser.add_argument("--csv", action="store_true", help="Also write CSV output")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run_ingest(args))
    except KeyboardInterrupt:
        logger.info("Fixture ingestion interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()