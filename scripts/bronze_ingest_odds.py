# -*- coding: utf-8 -*-
"""
bronze_ingest_odds.py — drop‑in v0.9

Fix: Stop calling a non‑existent CLI/module. Import and use the repo's
EventScraper class directly from the **repo root** (e.g.
C:\\Users\\Ethan\\sportsbook-odds-scraper), as described in the
project README.

Usage
  # optional (Git Bash accepts either style)
  set ODDS_SCRAPER_ROOT=C:\\Users\\Ethan\\sportsbook-odds-scraper   (cmd)
  $env:ODDS_SCRAPER_ROOT = 'C:\\Users\\Ethan\\sportsbook-odds-scraper' (PowerShell)
  export ODDS_SCRAPER_ROOT='/c/Users/Ethan/sportsbook-odds-scraper'       (Git Bash)

  python scripts/bronze_ingest_odds.py

Notes
  • No subprocess/PATH required; WinError 2 goes away.
  • Threaded, but serialises per‑scrape call within EventScraper to be safe.
"""
from __future__ import annotations

import csv
import os
import sys
import io
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# --- Project paths ------------------------------------------------------------
THIS = Path(__file__).resolve()
ROOT = THIS.parents[1]
BRONZE = ROOT / "bronze"
URLS_ROOT = BRONZE / "urls"
ODDS_ROOT = BRONZE / "odds"
CONFIG = ROOT / "config"
MARKETS_MAP_CSV = CONFIG / "markets_map.csv"

DEFAULT_MIN_MATCH_QUALITY = int(os.getenv("MIN_MATCH_QUALITY", "60"))

# --- Helpers ------------------------------------------------------------------

def latest_discovery_csv() -> Path:
    snaps = sorted(URLS_ROOT.glob("snapshot_ts=*/event_urls.csv"))
    if not snaps:
        raise SystemExit("[error] no URL discovery snapshots found under bronze/urls/")
    return snaps[-1]


def load_market_map() -> Dict[str, str]:
    if not MARKETS_MAP_CSV.exists():
        return {}
    df = pd.read_csv(MARKETS_MAP_CSV)
    lk: Dict[str, str] = {}
    for _, r in df.iterrows():
        src = str(r.get("raw_market", "")).strip().lower()
        dst = str(r.get("market_code", "")).strip()
        if src and dst:
            lk[src] = dst
    return lk


# --- Repo location + EventScraper import -------------------------------------

IMPORT_LOCK = threading.Lock()


def _msys_to_windows(path_str: str) -> str:
    if os.name == "nt" and path_str.startswith("/") and len(path_str) > 3 and path_str[2] == "/":
        # '/c/Users/...' -> 'C:/Users/...'
        return f"{path_str[1].upper()}:{path_str[2:]}"
    return path_str


def _candidate_roots() -> List[Path]:
    hints: List[Path] = []
    env_root = os.getenv("ODDS_SCRAPER_ROOT") or os.getenv("ODDS_SCRAPER_SRC")
    if env_root:
        hints.append(Path(_msys_to_windows(env_root)))
    # common installs
    hints += [
        Path(r"C:\\Users\\Ethan\\sportsbook-odds-scraper"),
        Path.home() / "sportsbook-odds-scraper",
        (ROOT.parent / "sportsbook-odds-scraper").resolve(),
    ]
    # also accept if user points to a 'src' subdir; step up to repo root
    roots: List[Path] = []
    for p in hints:
        if not p:
            continue
        if (p / "event_scraper.py").exists():
            roots.append(p)
        elif (p.name == "src") and (p.parent / "event_scraper.py").exists():
            roots.append(p.parent)
    # dedupe preserving order
    seen = set()
    out: List[Path] = []
    for p in roots:
        if str(p) not in seen:
            seen.add(str(p))
            out.append(p)
    return out


class _EventScraperProxy:
    _cached = None

    @classmethod
    def get(cls):
        if cls._cached is not None:
            return cls._cached
        with IMPORT_LOCK:
            if cls._cached is not None:
                return cls._cached
            for root in _candidate_roots():
                try:
                    sys.path.insert(0, str(root))
                    import importlib
                    es = importlib.import_module("event_scraper")
                    # verify class exists
                    getattr(es, "EventScraper")
                    print(f"[scraper] using EventScraper from {root}")
                    cls._cached = es
                    return es
                except Exception:
                    pass
                finally:
                    try:
                        sys.path.remove(str(root))
                    except ValueError:
                        pass
            # last resort: try current sys.path (maybe user installed module)
            import importlib
            es = importlib.import_module("event_scraper")
            getattr(es, "EventScraper")
            print("[scraper] using EventScraper from installed module on sys.path")
            cls._cached = es
            return es


# --- Scrape one URL -----------------------------------------------------------

SCRAPE_LOCK = threading.Lock()  # EventScraper mutates globals/prints; serialize calls


def scrape_to_df(url: str) -> pd.DataFrame:
    es = _EventScraperProxy.get()
    with SCRAPE_LOCK:
        scraper = es.EventScraper()
        scraper.scrape(url)
        if getattr(scraper, "error_message", None):
            raise RuntimeError(str(scraper.error_message))
        df = getattr(scraper, "odds_df", None)
        if df is None or df.empty:
            raise ValueError("empty dataframe from scraper")
        # ensure expected columns exist (market_name, selection_name, odds, line...)
        return df.copy()


# --- Normalisation ------------------------------------------------------------

BRONZE_COLS = [
    "ts",
    "game_id",
    "bookmaker",
    "market_code",
    "selection_name",
    "price_decimal",
    "line",
    "raw_market",
    "raw_selection",
]


def normalise_df(df: pd.DataFrame, game_id: str, book: str, market_map: Dict[str, str]):
    now = datetime.now(timezone.utc).isoformat()

    # soft rename common columns if casing differs
    cols = {c.lower(): c for c in df.columns}
    get = lambda name: cols.get(name)

    mkt_col = get("market_name") or get("market") or get("markettype") or get("market")
    sel_col = get("selection_name") or get("selection") or get("runner") or get("outcome")
    odds_col = get("odds") or get("price") or get("decimal_odds")
    line_col = get("line") or get("points")

    if not (mkt_col and sel_col and odds_col):
        raise KeyError("unexpected dataframe schema from EventScraper")

    out_rows: List[Dict[str, object]] = []
    for _, r in df.iterrows():
        raw_market = str(r[mkt_col]).strip()
        raw_sel = str(r[sel_col]).strip()
        market_key = raw_market.lower()
        market_code = market_map.get(market_key, market_key or "unknown")
        try:
            price_val = float(str(r[odds_col]).replace("$", "").strip())
        except Exception:
            continue  # skip unparsable odds
        try:
            line_val = float(str(r[line_col]).strip()) if line_col in df.columns else float("nan")
        except Exception:
            line_val = float("nan")
        out_rows.append({
            "ts": now,
            "game_id": game_id,
            "bookmaker": book,
            "market_code": market_code,
            "selection_name": raw_sel,
            "price_decimal": price_val,
            "line": line_val,
            "raw_market": raw_market,
            "raw_selection": raw_sel,
        })

    return pd.DataFrame(out_rows, columns=BRONZE_COLS)


# --- Main ---------------------------------------------------------------------

def main() -> None:
    urls_csv = latest_discovery_csv()

    # Prepare output snapshot dir
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = ODDS_ROOT / f"snapshot_ts={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "odds.csv"
    err_log = out_dir / "errors.log"

    market_map = load_market_map()

    # Read tasks
    tasks = []  # (game_id, bookmaker, url)
    with urls_csv.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            gid = str(r.get("game_id", "")).strip()
            book = str(r.get("bookmaker", "")).strip()
            url = str(r.get("event_url", "")).strip()
            mq = r.get("match_quality")
            try:
                mq = float(mq) if mq is not None and str(mq) != "" else 0.0
            except Exception:
                mq = 0.0
            if not (gid and book and url):
                continue
            if mq < DEFAULT_MIN_MATCH_QUALITY:
                continue
            tasks.append((gid, book, url))

    if not tasks:
        raise SystemExit("[error] no eligible (game,book,url) rows found — check discovery output and thresholds")

    dfs: List[pd.DataFrame] = []

    max_workers = min(12, max(2, (os.cpu_count() or 4)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex, err_log.open("w", encoding="utf-8") as elog:
        futs = {ex.submit(scrape_to_df, url): (gid, book, url) for gid, book, url in tasks}
        for fut in as_completed(futs):
            gid, book, url = futs[fut]
            try:
                df_raw = fut.result()
                df_norm = normalise_df(df_raw, gid, book, market_map)
                dfs.append(df_norm)
            except Exception as e:
                msg = f"{gid},{book},{url} -> {type(e).__name__}: {e}\n"
                print(f"[warn] {msg.strip()}")
                elog.write(msg)

    out_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=BRONZE_COLS)

    # Filter rows with valid numeric price
    if not out_df.empty:
        out_df = out_df[pd.to_numeric(out_df["price_decimal"], errors="coerce").notna()].copy()

    out_df.to_csv(out_path, index=False)

    print(str(out_path))
    if err_log.exists() and err_log.stat().st_size > 0:
        print(f"[note] some URLs failed — see {err_log}")


if __name__ == "__main__":
    main()
