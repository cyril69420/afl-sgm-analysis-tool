#!/usr/bin/env python3
"""
Bronze: ingest live odds snapshots (Sportsbet, PointsBet) using
declanwalpole/sportsbook-odds-scraper EventScraper.

Repo & usage:
- https://github.com/declanwalpole/sportsbook-odds-scraper
  Example usage in README: from event_scraper import EventScraper; scraper.scrape(url); pandas df in scraper.odds_df. :contentReference[oaicite:0]{index=0}
- Supported books include SportsBet (AU), PointsBet (AU). :contentReference[oaicite:1]{index=1}

Assumptions:
- Event URLs live under bronze/event_urls/season=<year>/round=*/bookmaker=*/
- Output written to bronze/odds/snapshots/ partitioned by season/round/bookmaker.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb
import polars as pl

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
DEFAULT_BRONZE_DIR = REPO / "bronze"
DEFAULT_CSV_MIRROR_DIR = REPO / "bronze_csv_mirror"

# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze: ingest live odds snapshots via EventScraper.")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--round", type=str, default=None, help="Optional round filter (e.g. 'R1' or 'finals_w1').")
    p.add_argument("--bookmakers", type=str, default="sportsbet,pointsbet")
    p.add_argument("--bronze-dir", type=str, default=str(DEFAULT_BRONZE_DIR))
    p.add_argument("--csv-mirror", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--log-level", type=str, default="INFO")
    return p.parse_args()

# ---------- logging ----------

def get_logger(name: str, level: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%dT%H:%M:%S"))
        logger.addHandler(h)
    return logger

# ---------- small utils ----------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def stable_hash(*parts: Any) -> str:
    s = "|".join("" if p is None else str(p) for p in parts)
    return sha256(s.encode("utf-8")).hexdigest()

def _duck() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()

def _quote_list_for_duckdb(paths: Iterable[str]) -> str:
    esc = []
    for p in paths:
        esc.append("'" + str(p).replace("'", "''") + "'")
    return "[" + ", ".join(esc) + "]"

def _list_partitioned_event_url_files(bronze_dir: Path, season: int) -> List[str]:
    season_dir = bronze_dir / "event_urls" / ("season=" + str(season))
    return [p.as_posix() for p in season_dir.rglob("*.parquet")]

def _read_event_urls(bronze_dir: Path, season: int, round_filter: Optional[str], requested_books: List[str]) -> pl.DataFrame:
    files = _list_partitioned_event_url_files(bronze_dir, season)
    if not files:
        return pl.DataFrame([])
    con = _duck()
    file_list_sql = _quote_list_for_duckdb(files)
    pdf = con.execute("SELECT * FROM read_parquet(" + file_list_sql + ", hive_partitioning=1)").df()
    if pdf is None or pdf.empty:
        return pl.DataFrame([])
    df = pl.from_pandas(pdf)
    df = df.filter(pl.col("season") == season)
    if round_filter:
        df = df.filter(pl.col("round") == round_filter)
    if "bookmaker" in df.columns:
        df = df.filter(pl.col("bookmaker").str.to_lowercase().is_in([b.lower() for b in requested_books]))
    # Ensure only the columns we use are present
    keep = [c for c in ["event_url", "bookmaker", "round", "season"] if c in df.columns]
    return df.select(keep).unique()

def _write_partitioned_parquet(df: pl.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = _duck()
    con.register("df", df.to_arrow())
    opts = "FORMAT PARQUET, PARTITION_BY (" + ", ".join(partition_cols) + ")"
    if overwrite:
        opts += ", OVERWRITE_OR_IGNORE"
    target = str(out_dir).replace("'", "''")
    con.execute("COPY df TO '" + target + "' (" + opts + ")")
    con.unregister("df")

def _write_csv_mirror(df: pl.DataFrame, csv_dir: Path, stem: str) -> None:
    csv_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df.write_csv(csv_dir / (stem + "_" + ts + ".csv"))

# ---------- EventScraper integration ----------

def _import_event_scraper(log: logging.Logger):
    """
    Try import from environment; else try typical vendored locations;
    else print clear instructions to install from GitHub.
    """
    try:
        from event_scraper import EventScraper  # type: ignore
        return EventScraper
    except Exception:
        pass

    # Try vendored paths
    candidates = [
        REPO / "external" / "sportsbook-odds-scraper",
        REPO / "sportsbook-odds-scraper",
        REPO.parent / "sportsbook-odds-scraper",
    ]
    for c in candidates:
        if c.exists():
            sys.path.insert(0, str(c))
            try:
                from event_scraper import EventScraper  # type: ignore
                log.info("Imported EventScraper from %s", c)
                return EventScraper
            except Exception:
                pass

    log.error(
        "Could not import EventScraper. Install it with:\n"
        "  pip install git+https://github.com/declanwalpole/sportsbook-odds-scraper\n"
        "Repo & usage example in README. :contentReference[oaicite:2]{index=2}"
    )
    return None

def _normalise_from_odds_df(url: str, season: int, round_of_url: Optional[str], odds_pdf) -> List[Dict[str, Any]]:
    """
    Map sportsbook-odds-scraper columns to our Bronze schema.
    We handle multiple possible column names defensively.
    """
    rows: List[Dict[str, Any]] = []
    if odds_pdf is None or getattr(odds_pdf, "empty", True):
        return rows

    cols = list(odds_pdf.columns)
    has = lambda name: name in cols

    # likely columns based on README/examples
    for _, r in odds_pdf.iterrows():
        bookmaker = (r.get("sportsbook_name") or r.get("sportsbook") or "").strip().lower()
        market_group = r.get("market_group") or r.get("market_type") or r.get("market_name") or "Unknown"
        market_name = r.get("market_name") or market_group
        selection = r.get("selection_name") or r.get("selection") or r.get("outcome_name") or ""
        line_val = r.get("line")
        try:
            line = float(line_val) if line_val is not None and str(line_val) != "" else None
        except Exception:
            line = None
        dec_val = r.get("odds") or r.get("decimal_odds")
        try:
            decimal_odds = float(dec_val) if dec_val is not None and str(dec_val) != "" else None
        except Exception:
            decimal_odds = None

        payload_keys = [k for k in ["event_name", "event_id", "market_id", "selection_id"] if has(k)]
        payload = {k: r.get(k) for k in payload_keys}
        raw_payload = json.dumps(payload) if payload else None

        captured = r.get("timestamp") or r.get("scrape_ts") or None
        if captured is None:
            captured_at_utc = now_utc_iso()
        else:
            try:
                # pandas timestamp or string to ISO
                ts = str(captured)
                if "T" in ts:
                    captured_at_utc = ts
                else:
                    captured_at_utc = ts
            except Exception:
                captured_at_utc = now_utc_iso()

        hk = stable_hash(bookmaker, url, market_group, market_name, selection, line, decimal_odds)
        rows.append({
            "captured_at_utc": captured_at_utc,
            "bookmaker": bookmaker,
            "event_url": url,
            "market_group": market_group,
            "market_name": market_name,
            "selection": selection,
            "line": line,
            "decimal_odds": decimal_odds,
            "raw_payload": raw_payload,
            "hash_key": hk,
            "season": season,
            "round": round_of_url,
        })
    return rows

def main() -> int:
    args = parse_args()
    log = get_logger("odds_live", args.log_level)

    bronze_dir = Path(args.bronze_dir)
    requested = [b.strip().lower() for b in (args.bookmakers or "").split(",") if b.strip()]
    if not requested:
        requested = ["sportsbet", "pointsbet"]

    ev = _read_event_urls(bronze_dir, args.season, args.round, requested)
    if ev.is_empty():
        log.error("No event_urls under %s/season=%s/** for bookmakers=%s", bronze_dir / "event_urls", args.season, requested)
        return 2

    # Collect URL -> round mapping
    url_round: Dict[str, Optional[str]] = {}
    for rec in ev.select(["event_url", "round"]).unique().iter_rows(named=True):
        url_round[rec["event_url"]] = rec.get("round")

    urls = [r["event_url"] for r in ev.select(["event_url"]).unique().iter_rows(named=True)]
    if args.limit:
        urls = urls[: args.limit]

    EventScraper = _import_event_scraper(log)
    if EventScraper is None:
        return 3

    all_rows: List[Dict[str, Any]] = []
    for i, url in enumerate(urls, 1):
        try:
            scraper = EventScraper()  # type: ignore
            scraper.scrape(url)       # per README usage pattern :contentReference[oaicite:3]{index=3}
            if getattr(scraper, "error_message", None):
                log.warning("Scraper error for %s: %s", url, scraper.error_message)
                continue
            odds_pdf = getattr(scraper, "odds_df", None)
            rows = _normalise_from_odds_df(url, args.season, url_round.get(url), odds_pdf)
            all_rows.extend(rows)
            if i % 5 == 0:
                log.info("Scraped %d/%d urls", i, len(urls))
        except Exception:
            log.exception("Failed to scrape %s", url)

    if not all_rows:
        log.warning("No odds rows scraped; nothing to write.")
        return 0

    df = pl.DataFrame(all_rows).unique(subset=["hash_key"])

    if args.dry_run:
        log.info("DRY RUN: %d rows, columns=%s", df.height, df.columns)
        if df.height:
            log.info("Sample:\n%s", df.head(12))
        return 0

    out_dir = bronze_dir / "odds" / "snapshots"
    _write_partitioned_parquet(df, out_dir, partition_cols=["season", "round", "bookmaker"], overwrite=args.overwrite)
    if args.csv_mirror:
        _write_csv_mirror(df, DEFAULT_CSV_MIRROR_DIR / "odds_snapshots", stem="odds_snapshots")
    log.info("Wrote odds snapshots → %s (partitioned by season/round/bookmaker)", out_dir)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
