# -*- coding: utf-8 -*-
"""
bronze_ingest_historical_odds.py — v1.3 (AUSSB Excel + GitHub + 2020+ filter + Line/Total mapping)

Improvements vs v1.2
- Adds **AUS Sports Betting Excel** back in (now that openpyxl is installed).
- Filters to **season >= 2020** (configurable via --start-year).
- Attempts to parse **Line** and **Total** (O/U) markets in addition to H2H
  where columns exist (2013+ in the Excel, per AUSSB notes).
- Keeps GitHub fallbacks (akareen + betfair DS + mirrors) as before for H2H.

Usage
  python scripts/bronze_ingest_historical_odds.py --start-year 2020

Output
  bronze/historical_odds/snapshot_ts=YYYYmmddTHHMMSSZ/historical_odds.csv
"""
from __future__ import annotations

import io
import os
import re
import sys
import csv
import zipfile
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import requests
import pandas as pd

THIS = Path(__file__).resolve()
ROOT = THIS.parents[1]
BRONZE = ROOT / "bronze"
OUT_ROOT = BRONZE / "historical_odds"
OUT_ROOT.mkdir(parents=True, exist_ok=True)

AUSBET_EXCEL_URL = "https://www.aussportsbetting.com/historical_data/afl.xlsx"
AUSBET_PAGE = "https://www.aussportsbetting.com/data/historical-afl-results-and-odds-data/"

GITHUB_ZIPS = [
    ("github_akareen", "https://codeload.github.com/akareen/AFL-Data-Analysis/zip/refs/heads/main", "AFL-Data-Analysis-main", ["odds_data/*.csv", "data/odds/*.csv"]),
    ("github_betfair_ds", "https://codeload.github.com/betfair-datascientists/predictive-models/zip/refs/heads/master", "predictive-models-master", ["afl/data/afl_odds.csv", "afl/data/weekly_odds.csv"]),
]

META_GUESSES = {
    "date_local": ["date", "match date", "kick off (local)", "kickoff (local)", "matchdate"],
    "season":     ["season", "year"],
    "round":      ["round"],
    "venue":      ["venue", "ground", "stadium"],
    "home_team":  ["home team", "home", "team home", "home_team", "hometeam"],
    "away_team":  ["away team", "away", "team away", "away_team", "awayteam"],
}

# H2H price columns
H2H_GUESSES = {
    "home_close": ["home odds", "home closing odds", "closing home odds", "home_close", "home_closing_odds"],
    "away_close": ["away odds", "away closing odds", "closing away odds", "away_close", "away_closing_odds"],
    "home_open":  ["home opening odds", "opening home odds", "home_opening_odds"],
    "away_open":  ["away opening odds", "opening away odds", "away_opening_odds"],
    "home_min":   ["home min odds", "home minimum odds", "home_min"],
    "home_max":   ["home max odds", "home maximum odds", "home_max"],
    "away_min":   ["away min odds", "away minimum odds", "away_min"],
    "away_max":   ["away max odds", "away maximum odds", "away_max"],
}

# Line & Total mapping — two parts: number (line/total) and prices (over/under or home/away)
LT_NUMBER_GUESSES = {
    "line":  ["line", "handicap", "spread", "hcap"],
    "total": ["total", "o/u", "over/under", "points total", "total points"],
}
LT_PRICE_GUESSES = {
    # For handicap we often see home/away prices
    "line_home":  ["home line odds", "home handicap odds", "home_line_odds"],
    "line_away":  ["away line odds", "away handicap odds", "away_line_odds"],
    # For totals we often see over/under prices
    "total_over": ["over odds", "o odds", "over_odds"],
    "total_under":["under odds", "u odds", "under_odds"],
    # Sometimes generics
    "line_price_home": ["home line price", "home spread price"],
    "line_price_away": ["away line price", "away spread price"],
}

PAIR_TYPES = {
    "closing": ("home_close", "away_close"),
    "opening": ("home_open",  "away_open"),
    "min":     ("home_min",   "away_min"),
    "max":     ("home_max",   "away_max"),
}


# ---------- utils ----------

def _canonicalise(df: pd.DataFrame, guesses: Dict[str, List[str]]) -> Dict[str, str]:
    cols = {c.lower().strip(): c for c in df.columns}
    out: Dict[str, str] = {}
    def find_one(cands: List[str]):
        for cand in cands:
            key = cand.lower()
            if key in cols:
                return cols[key]
            for ck, raw in cols.items():
                if re.sub(r"[^a-z0-9]", "", ck) == re.sub(r"[^a-z0-9]", "", key):
                    return raw
        return None
    for k, cands in guesses.items():
        hit = find_one(cands)
        if hit:
            out[k] = hit
    return out


def _emit(rows: List[Dict[str,object]], meta: Dict[str,object], market: str, odt: str, sel: str, price, line=None):
    try:
        pr = float(str(price).replace("$", "").strip())
    except Exception:
        return
    ln = None
    if line is not None:
        try:
            ln = float(str(line).strip())
        except Exception:
            ln = None
    rows.append({**meta, "market_code": market, "odds_type": odt, "selection_name": sel, "price_decimal": pr, "line": ln})


def _normalise_sheet(df: pd.DataFrame, source: str, start_year: int) -> List[Dict[str,object]]:
    out: List[Dict[str,object]] = []
    if df is None or df.empty:
        return out
    lk_meta = _canonicalise(df, META_GUESSES)
    lk_h2h  = _canonicalise(df, H2H_GUESSES)
    lk_num  = _canonicalise(df, LT_NUMBER_GUESSES)
    lk_p    = _canonicalise(df, LT_PRICE_GUESSES)

    must = ["date_local","home_team","away_team"]
    if not all(k in lk_meta for k in must):
        return out

    for _, r in df.iterrows():
        try:
            d = pd.to_datetime(r[lk_meta["date_local"]])
            y = int(d.year)
        except Exception:
            continue
        if y < start_year:
            continue
        meta = {
            "ts": datetime.utcnow().isoformat(),
            "source": source,
            "season": y,
            "round": r.get(lk_meta.get("round")) if lk_meta.get("round") else None,
            "date_local": str(d.date()),
            "home_team": str(r.get(lk_meta["home_team"])) if lk_meta.get("home_team") else None,
            "away_team": str(r.get(lk_meta["away_team"])) if lk_meta.get("away_team") else None,
            "venue": r.get(lk_meta.get("venue")) if lk_meta.get("venue") else None,
        }
        # H2H pairs
        for odt, (hk, ak) in PAIR_TYPES.items():
            hc = lk_h2h.get(hk); ac = lk_h2h.get(ak)
            if hc and ac:
                _emit(out, meta, "h2h", odt, "home", r.get(hc))
                _emit(out, meta, "h2h", odt, "away", r.get(ac))
        # Line (handicap) — needs a number and (home/away) prices
        line_num = None
        for key in ("line","handicap","spread"):
            if lk_num.get(key):
                line_num = r.get(lk_num[key])
                break
        lh = lk_p.get("line_home"); la = lk_p.get("line_away")
        if line_num is not None and (lh or la):
            # If we don't know opening/min/max mapping here, emit as closing
            _emit(out, meta, "line", "closing", "home", r.get(lh) if lh else None, line=line_num)
            _emit(out, meta, "line", "closing", "away", r.get(la) if la else None, line=line_num)
        # Totals (O/U)
        tot_num = None
        for key in ("total","over/under"):
            if lk_num.get(key):
                tot_num = r.get(lk_num[key])
                break
        to = lk_p.get("total_over"); tu = lk_p.get("total_under")
        if tot_num is not None and (to or tu):
            _emit(out, meta, "total", "closing", "over", r.get(to) if to else None, line=tot_num)
            _emit(out, meta, "total", "closing", "under", r.get(tu) if tu else None, line=tot_num)

    return out


# ---------- fetchers ----------

def fetch_ausbet_excel() -> Optional[List[pd.DataFrame]]:
    try:
        r = requests.get(AUSBET_EXCEL_URL, timeout=90)
        r.raise_for_status()
        bio = io.BytesIO(r.content)
        xls = pd.read_excel(bio, sheet_name=None)  # requires openpyxl
        frames = []
        for name, df in xls.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        return frames
    except Exception as e:
        print(f"[warn] AUSSB Excel fetch failed -> {type(e).__name__}: {e}")
        return None


def fetch_github_zip(zip_url: str, root: str, patterns: List[str]) -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    try:
        with requests.get(zip_url, stream=True, timeout=90) as r:
            r.raise_for_status()
            bio = io.BytesIO(r.content)
        with zipfile.ZipFile(bio) as z:
            names = z.namelist()
            wanted: List[str] = []
            for pat in patterns:
                if "*" in pat:
                    prefix = f"{root}/" + pat.split("*")[0]
                    for nm in names:
                        if nm.startswith(prefix) and nm.lower().endswith(".csv"):
                            wanted.append(nm)
                else:
                    nm = f"{root}/{pat}"
                    if nm in names:
                        wanted.append(nm)
            for nm in wanted:
                with z.open(nm) as fh:
                    try:
                        df = pd.read_csv(fh)
                        if not df.empty:
                            dfs.append(df)
                    except Exception:
                        pass
    except Exception as e:
        print(f"[warn] GitHub fetch failed for {zip_url} -> {type(e).__name__}: {e}")
    return dfs


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-year", type=int, default=2020)
    args = ap.parse_args()

    out_rows: List[Dict[str,object]] = []

    # 1) AUS Sports Betting Excel (preferred; has line/total from 2013+)
    sheets = fetch_ausbet_excel()
    if sheets:
        for df in sheets:
            out_rows += _normalise_sheet(df, "aussportsbetting_excel", args.start_year)

    # 2) GitHub fallbacks to boost H2H coverage if needed
    if not out_rows:
        for name, url, root, globs in GITHUB_ZIPS:
            dfs = fetch_github_zip(url, root, globs)
            for df in dfs:
                out_rows += _normalise_sheet(df, name, args.start_year)
            if out_rows:
                break

    if not out_rows:
        print(f"[error] unable to extract any historical odds from sources.\n  - AUSSB: {AUSBET_PAGE}")
        for name, url, *_ in GITHUB_ZIPS:
            print(f"  - GitHub: {name} {url}")
        sys.exit(1)

    # Write
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = OUT_ROOT / f"snapshot_ts={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "historical_odds.csv"

    cols = ["ts","source","season","round","date_local","home_team","away_team","venue","market_code","odds_type","selection_name","price_decimal","line"]
    df_out = pd.DataFrame(out_rows)
    # keep numeric prices only
    df_out = df_out[pd.to_numeric(df_out["price_decimal"], errors="coerce").notna()].copy()
    for c in cols:
        if c not in df_out.columns:
            df_out[c] = pd.NA
    df_out = df_out[cols].sort_values(["date_local","home_team","away_team","market_code","odds_type","selection_name"]).reset_index(drop=True)
    df_out.to_csv(out_csv, index=False)
    print(str(out_csv))


if __name__ == "__main__":
    main()
