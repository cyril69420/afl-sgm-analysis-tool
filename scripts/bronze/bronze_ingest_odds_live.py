#!/usr/bin/env python3
"""
Bronze: ingest live odds snapshots (Sportsbet, PointsBet) using
declanwalpole/sportsbook-odds-scraper EventScraper.

- Reads per-game event URLs from:
    bronze/event_urls/season=<year>/round=*/bookmaker=*/
- Scrapes each URL and writes snapshots to:
    bronze/odds/snapshots/season=<year>/round=<round>/bookmaker=<bookmaker>/

Guarantees:
- Each output row has the real per-game event_url (the page you scraped).
- Deterministic dedupe via stable hash on key columns.
- Fails fast if any row would have a blank/NULL event_url.
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
    p.add_argument("--bookmakers", type=str, default="sportsbet,pointsbet", help="Comma-separated (default: sportsbet,pointsbet)")
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


# ---------- utils ----------

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
    pdf = con.execute("SELECT * FROM read_parquet(" + _quote_list_for_duckdb(files) + ", hive_partitioning=1)").df()
    if pdf is None or pdf.empty:
        return pl.DataFrame([])

    df = pl.from_pandas(pdf)
    df = df.filter(pl.col("season") == season)
    if round_filter:
        df = df.filter(pl.col("round") == round_filter)

    # Keep only requested bookmakers (lowercased)
    req = [b.lower() for b in requested_books]
    if "bookmaker" in df.columns:
        df = df.with_columns(pl.col("bookmaker").cast(pl.Utf8).str.to_lowercase())
        df = df.filter(pl.col("bookmaker").is_in(req))

    # Sportsbet/PointsBet per-game URLs only
    df = df.filter(
        pl.col("event_url").is_not_null()
        & pl.col("event_url").cast(pl.Utf8).str.contains("sportsbet.com.au|pointsbet.com.au")
    )

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


# ---------- EventScraper loader ----------

def _import_event_scraper(log: logging.Logger):
    try:
        from event_scraper import EventScraper  # type: ignore
        return EventScraper
    except Exception:
        pass
    # try repo-relative fallbacks
    for cand in [
        REPO / "external" / "sportsbook-odds-scraper",
        REPO / "sportsbook-odds-scraper",
        REPO.parent / "sportsbook-odds-scraper",
    ]:
        if cand.exists():
            sys.path.insert(0, str(cand))
            try:
                from event_scraper import EventScraper  # type: ignore
                log.info("Imported EventScraper from %s", cand)
                return EventScraper
            except Exception:
                continue
    log.error(
        "Could not import EventScraper. Install it with:\n"
        "  pip install git+https://github.com/declanwalpole/sportsbook-odds-scraper\n"
    )
    return None


# ---------- normalize one scrape (pandas -> rows dicts) ----------

def _normalize_scrape(url: str, season: int, round_code: str, odds_pdf) -> List[Dict[str, Any]]:
    """
    Map sportsbook-odds-scraper pandas DataFrame to our Bronze schema.
    Ensures event_url is the exact page we scraped (url).
    """
    rows: List[Dict[str, Any]] = []
    if odds_pdf is None:
        return rows
    # pandas only
    try:
        _ = odds_pdf.iterrows  # attribute check
    except Exception:
        return rows

    cols = list(odds_pdf.columns)
    has = set(cols)

    for _, r in odds_pdf.iterrows():
        # bookmaker
        bookmaker = (r.get("sportsbook_name") or r.get("sportsbook") or "").strip().lower()
        if not bookmaker:
            if "sportsbet.com.au" in url:
                bookmaker = "sportsbet"
            elif "pointsbet.com.au" in url:
                bookmaker = "pointsbet"
            else:
                bookmaker = "unknown"

        market_group = r.get("market_group") or r.get("market_type") or r.get("market_name") or "Unknown"
        market_name  = r.get("market_name") or market_group
        selection    = r.get("selection_name") or r.get("selection") or r.get("outcome_name") or ""

        # numerics
        def _to_float(x):
            try:
                return float(x) if x not in (None, "", "None") else None
            except Exception:
                return None

        line         = _to_float(r.get("line"))
        decimal_odds = _to_float(r.get("odds") or r.get("decimal_odds"))

        payload_keys = [k for k in ["event_name", "event_id", "market_id", "selection_id"] if k in has]
        raw_payload  = json.dumps({k: r.get(k) for k in payload_keys}) if payload_keys else None

        captured     = r.get("timestamp") or r.get("scrape_ts")
        captured_at  = str(captured) if captured else now_utc_iso()

        hk = stable_hash(bookmaker, url, market_group, market_name, selection, line, decimal_odds)

        rows.append({
            "captured_at_utc": captured_at,
            "bookmaker": bookmaker,
            "event_url": url,                # <— the actual page we scraped
            "market_group": market_group,
            "market_name": market_name,
            "selection": selection,
            "line": line,
            "decimal_odds": decimal_odds,
            "raw_payload": raw_payload,
            "hash_key": hk,
            "season": int(season),
            "round": str(round_code),
        })
    return rows


# ---------- main ----------

def main() -> int:
    args = parse_args()
    log = get_logger("odds_live", args.log_level)

    bronze_dir = Path(args.bronze_dir)
    requested = [b.strip().lower() for b in (args.bookmakers or "").split(",") if b.strip()]
    if not requested:
        requested = ["sportsbet", "pointsbet"]

    ev = _read_event_urls(bronze_dir, args.season, args.round, requested)
    if ev.is_empty():
        log.error("No per-game event_urls under %s/season=%s/** for bookmakers=%s",
                  bronze_dir / "event_urls", args.season, requested)
        return 2

    # URL -> round map
    url_round = {r["event_url"]: r.get("round") for r in ev.select(["event_url", "round"]).iter_rows(named=True)}
    urls      = [r["event_url"] for r in ev.select(["event_url"]).unique().iter_rows(named=True)]
    if args.limit:
        urls = urls[: args.limit]

    EventScraper = _import_event_scraper(log)
    if EventScraper is None:
        return 3

    all_rows: List[Dict[str, Any]] = []
    for i, url in enumerate(urls, 1):
        try:
            scraper = EventScraper()  # type: ignore
            scraper.scrape(url)
            if getattr(scraper, "error_message", None):
                log.warning("Scraper error for %s: %s", url, scraper.error_message)
                continue
            odds_pdf = getattr(scraper, "odds_df", None)
            rows = _normalize_scrape(url, args.season, url_round.get(url, None) or "unknown", odds_pdf)
            all_rows.extend(rows)
            if i % 5 == 0:
                log.info("Scraped %d/%d urls", i, len(urls))
        except Exception:
            log.exception("Failed to scrape %s", url)

    if not all_rows:
        log.warning("No odds rows scraped; nothing to write.")
        return 0

    df = pl.DataFrame(all_rows)

    # Hard guarantees: event_url exists and is non-empty everywhere
    df = df.with_columns(
        pl.col("event_url")
          .cast(pl.Utf8)
          .fill_null("")
          .str.strip_chars()   # <<-- use strip_chars for wider Polars compatibility
          .alias("event_url")
    )
    total = df.height
    bad   = df.filter(pl.col("event_url") == "").height
    if bad > 0:
        sample = df.filter(pl.col("event_url") == "").select(["bookmaker", "season", "round"]).head(10)
        log.error("FATAL: %d/%d rows missing event_url. Sample:\n%s", bad, total, sample)
        return 4

    # Dedup deterministically if hash_key present
    if "hash_key" in df.columns:
        df = df.unique(subset=["hash_key"])

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
