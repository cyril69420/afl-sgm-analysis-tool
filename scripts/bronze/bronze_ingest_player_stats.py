#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import re
import argparse
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    echo_df_info,
)

TIMEOUT = 30

# AFL Tables team slug mapping (quirks handled)
TEAM_SLUGS: Dict[str, str] = {
    "Adelaide": "adelaide",
    "Brisbane Lions": "brisbanel",
    "Carlton": "carlton",
    "Collingwood": "collingwood",
    "Essendon": "essendon",
    "Fremantle": "fremantle",
    "Geelong": "geelong",
    "Gold Coast": "goldcoast",
    "Greater Western Sydney": "gws",
    "Hawthorn": "hawthorn",
    "Melbourne": "melbourne",
    "North Melbourne": "kangaroos",
    "Port Adelaide": "padelaide",
    "Richmond": "richmond",
    "St Kilda": "stkilda",
    "Sydney": "swans",
    "West Coast": "westcoast",
    "Western Bulldogs": "bullldogs",
}

GBG_URL_TMPL = "https://afltables.com/afl/stats/teams/{slug}/{year}_gbg.html"

# Section titles -> short codes for output
STAT_FAMILIES = {
    "Disposals": "DI",
    "Kicks": "KI",
    "Marks": "MK",
    "Handballs": "HB",
    "Goals": "GL",
    "Behinds": "BH",
    "Hit Outs": "HO",
    "Tackles": "TK",
    "Rebounds": "RB",
    "Inside 50s": "I5",
    "Clearances": "CL",
    "Clangers": "CG",
    "Frees": "FF",
    "Frees Against": "FA",
    "Contested Possessions": "CP",
    "Uncontested Possessions": "UP",
    "Contested Marks": "CM",
    "Marks Inside 50": "MI",
    "One Percenters": "OP",
    "Bounces": "BO",
    "Goal Assists": "GA",
    "% Played": "%P",
}

ROUND_LABEL_PAT = re.compile(r"^(R\d+|EF|QF|SF|PF|GF)$", re.I)
IS_ROUND_NUM = re.compile(r"^R(\d+)$", re.I)


def get(url: str, session: Optional[requests.Session] = None) -> requests.Response:
    sess = session or requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; afl-sgm-analysis-tool/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = sess.get(url, headers=headers, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp


def _flatten_columns(cols) -> List[str]:
    out = []
    for c in cols:
        if isinstance(c, tuple):
            c = [str(x) for x in c if str(x) != "nan"]
            out.append(c[-1] if c else "")
        else:
            out.append(str(c))
    return [x.strip() for x in out]


def _is_gbg_table(df: pd.DataFrame) -> bool:
    cols = _flatten_columns(df.columns)
    if "Player" not in cols:
        return False
    value_cols = [c for c in cols if ROUND_LABEL_PAT.match(c)]
    return len(value_cols) >= 5  # must look like R2..R24 or finals codes


def _find_stat_table(soup: BeautifulSoup, section_title: str) -> Optional[pd.DataFrame]:
    """
    Find the first valid GBG table after a section title (e.g., 'Disposals').
    We verify structure to avoid grabbing 'abbreviations' or control tables.
    """
    title_node = soup.find(string=re.compile(rf"^\s*{re.escape(section_title)}\s*$", re.I))
    if not title_node:
        return None

    el = title_node.parent if hasattr(title_node, "parent") else None
    nxt = el.find_next("table") if el else soup.find("table")
    while nxt is not None:
        try:
            dfs = pd.read_html(StringIO(str(nxt)))
        except Exception:
            nxt = nxt.find_next("table")
            continue
        for df in dfs:
            df.columns = _flatten_columns(df.columns)
            if _is_gbg_table(df):
                return df
        nxt = nxt.find_next("table")
    return None


def _extract_opponent_row(df: pd.DataFrame) -> Dict[str, str]:
    """Get mapping round_label -> opponent code from 'Opponent' row (if present)."""
    df = df.copy()
    df.columns = _flatten_columns(df.columns)
    if "Player" not in df.columns:
        return {}
    opp_rows = df[df["Player"].astype(str).str.strip().str.lower().eq("opponent")]
    if opp_rows.empty:
        return {}
    row = opp_rows.iloc[0]
    opp_map: Dict[str, str] = {}
    for c in df.columns:
        if ROUND_LABEL_PAT.match(str(c)):
            v = str(row[c]).strip()
            if v and v.lower() != "nan":
                opp_map[str(c)] = v
    return opp_map


def _cell_to_int_first(s: object) -> Optional[int]:
    """
    Coerce a cell like '5', ' 12 ', '5-9', '20 2', '—', '-' to int or NaN.
    We take the FIRST integer we see; if none, return NaN.
    """
    txt = "" if s is None else str(s)
    txt = txt.replace("–", "-").replace("—", "-")  # normalize dashes
    m = re.search(r"\d+", txt)
    return int(m.group(0)) if m else None


def _melt_stat_table(raw: pd.DataFrame, team: str, season: int, stat_code: str,
                     opp_map: Dict[str, str]) -> pd.DataFrame:
    df = raw.copy()
    df.columns = _flatten_columns(df.columns)

    if "Player" not in df.columns:
        return pd.DataFrame()

    value_cols = [c for c in df.columns if ROUND_LABEL_PAT.match(str(c))]
    df = df[["Player"] + value_cols]

    mask = df["Player"].astype(str).str.strip().str.lower().isin({"totals", "opponent"})
    df = df[~mask].copy()

    long_df = df.melt(id_vars=["Player"], value_vars=value_cols,
                      var_name="round_label", value_name=stat_code)
    long_df[stat_code] = long_df[stat_code].map(_cell_to_int_first)
    long_df = long_df.dropna(subset=[stat_code])

    long_df = long_df.rename(columns={"Player": "player"})
    long_df["season"] = season
    long_df["team"] = team

    round_nums: List[Optional[int]] = []
    finals_flags: List[bool] = []
    for lbl in long_df["round_label"].astype(str):
        m = re.match(r"^R(\d+)$", lbl, flags=re.I)
        if m:
            round_nums.append(int(m.group(1)))
            finals_flags.append(False)
        else:
            round_nums.append(None)
            finals_flags.append(lbl.upper() in {"EF", "QF", "SF", "PF", "GF"})
    long_df["round_num"] = round_nums
    long_df["is_finals"] = finals_flags

    long_df["opponent_code"] = long_df["round_label"].map(opp_map) if opp_map else None
    return long_df


def parse_team_season(team: str, slug: str, year: int, session: Optional[requests.Session] = None,
                      verbose: bool = False) -> pd.DataFrame:
    url = GBG_URL_TMPL.format(slug=slug, year=year)
    resp = get(url, session=session)
    if verbose:
        print(f"  • {team}: {url} ({resp.status_code})")

    soup = BeautifulSoup(resp.text, "html.parser")

    fam_tables: Dict[str, pd.DataFrame] = {}
    opp_map: Dict[str, str] = {}

    for title, code in STAT_FAMILIES.items():
        tbl = _find_stat_table(soup, title)
        if tbl is None:
            if verbose:
                print(f"    ! Missing/invalid table after '{title}'")
            continue
        if not opp_map:
            opp_map = _extract_opponent_row(tbl)
        fam_tables[code] = tbl

    if "DI" not in fam_tables:
        return pd.DataFrame()

    base = _melt_stat_table(fam_tables["DI"], team, year, "DI", opp_map)
    if base.empty:
        return base

    out = base.copy()
    for code in ("KI", "MK", "HB", "GL", "BH", "HO", "TK", "RB", "I5", "CL", "CG", "FF", "FA", "CP", "UP", "CM", "MI", "OP", "GA", "%P"):
        tbl = fam_tables.get(code)
        if tbl is None:
            continue
        long_tbl = _melt_stat_table(tbl, team, year, code, opp_map)
        if long_tbl.empty:
            continue
        out = out.merge(long_tbl[["player", "round_label", code]],
                        on=["player", "round_label"], how="left")

    desired = ["season", "team", "player", "round_label", "round_num", "is_finals",
               "opponent_code", "DI", "KI", "MK", "HB", "GL", "BH", "HO", "TK", "RB", "I5", "CL", "CG",
               "FF", "FA", "CP", "UP", "CM", "MI", "OP", "GA", "%P"]
    out = out[[c for c in desired if c in out.columns]].sort_values(
        ["season", "team", "round_num", "player"], na_position="last"
    ).reset_index(drop=True)
    return out


def filter_last_rounds(df: pd.DataFrame, last_rounds: Optional[int]) -> pd.DataFrame:
    if last_rounds is None or df.empty:
        return df
    mask_reg = (~df["is_finals"]) & (df["round_num"].notna())
    if not mask_reg.any():
        return df
    max_r = int(df.loc[mask_reg, "round_num"].max())
    low = max_r - int(last_rounds) + 1
    return df[mask_reg & df["round_num"].between(low, max_r)].copy()


def write_partitioned(df: pd.DataFrame, out_root: str, season: int, minimize: bool = False, verbose: bool = False):
    part_dir = os.path.join(out_root, f"season={season}")
    os.makedirs(part_dir, exist_ok=True)
    out_parquet = os.path.join(part_dir, "players.parquet")
    out_csv = os.path.join(part_dir, "players.csv")

    write_df = df.copy()
    if minimize:
        keep = ["season", "team", "player", "round_label", "round_num", "is_finals", "opponent_code",
                "DI", "KI", "MK", "HB", "GL", "BH", "HO", "TK", "RB", "I5", "CL", "CG", "FF", "FA", "CP", "UP", "CM", "MI", "OP", "GA", "%P"]
        write_df = write_df[[c for c in keep if c in write_df.columns]]

    # Prefer Parquet, mirror CSV
    try:
        write_df.to_parquet(out_parquet, index=False)
        if verbose:
            print(f"  ✓ Wrote {len(write_df):,} rows → {out_parquet}")
    except Exception as e:
        if verbose:
            print(f"  ! Parquet write failed ({e}); continuing…")

    try:
        write_df.to_csv(out_csv, index=False)
        if verbose:
            print(f"  ✓ Wrote {len(write_df):,} rows → {out_csv}")
    except Exception as e:
        if verbose:
            print(f"  ! CSV write failed ({e})")


def build_season(year: int, last_rounds: Optional[int] = None, verbose: bool = False) -> pd.DataFrame:
    if verbose:
        print(f"\n=== Season {year} ===")
        print("  • Sanity-check a few team URLs:")

    sess = requests.Session()
    frames: List[pd.DataFrame] = []

    if verbose:
        shown = 0
        for team, slug in TEAM_SLUGS.items():
            url = GBG_URL_TMPL.format(slug=slug, year=year)
            try:
                r = get(url, session=sess)
                print(f"    - {team}: {url} ({r.status_code})")
            except Exception as e:
                print(f"    - {team}: {url} (ERROR: {e})")
            shown += 1
            if shown >= 4:
                break

    for team, slug in TEAM_SLUGS.items():
        try:
            df_team = parse_team_season(team, slug, year, session=sess, verbose=verbose)
            if df_team.empty:
                continue
            if last_rounds is not None:
                df_team = filter_last_rounds(df_team, last_rounds)
            if not df_team.empty:
                frames.append(df_team)
        except requests.HTTPError as e:
            if verbose:
                print(f"  ✖ {team} — HTTP error: {e}")
        except Exception as e:
            if verbose:
                print(f"  ✖ {team} — Parse error: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    ap = argparse.ArgumentParser(description="Ingest AFLTables team Game-by-Game into Bronze.")
    add_common_test_flags(ap)  # <-- fix: register --dry-run/--limit
    ap.add_argument("--season", type=int, action="append", required=True,
                    help="e.g., --season 2024 --season 2025")
    ap.add_argument("--last-rounds", type=int, default=None,
                    help="Keep only the last N regular-season rounds per team (finals unaffected).")
    ap.add_argument("--minimize", action="store_true", help="Write only key columns + DI/KI/MK/HB.")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging & URL checks.")
    ap.add_argument("--out-root", default=os.path.join("bronze", "player_stats"),
                    help="Output root (partitioned by season=YYYY).")

    args = ap.parse_args()

    any_rows = 0
    for yr in args.season:
        df = build_season(yr, last_rounds=args.last_rounds, verbose=args.verbose)
        if df.empty:
            print(f"  ✖ No player rows assembled for season {yr}.")
            # Keep deterministic output structure for downstream
            part_dir = os.path.join(args.out_root, f"season={yr}")
            os.makedirs(part_dir, exist_ok=True)
            pd.DataFrame().to_csv(os.path.join(part_dir, "players.csv"), index=False)
            print(f"  ✓ Wrote 0 rows → {os.path.join(part_dir, 'players.csv')}")
            continue

        # Apply test limit + log
        df = maybe_limit_df(df, args.limit)  # works with pandas too
        echo_df_info(df, note=f"player_stats S{yr} (post-limit)")

        if args.dry_run:
            print("DRY RUN: skipping writes")
        else:
            write_partitioned(df, args.out_root, yr, minimize=args.minimize, verbose=args.verbose)
        any_rows += len(df)

    print("\n✅ Bronze player stats ingested." if any_rows else "\n⚠️ Bronze: no rows written (empty or dry-run).")


if __name__ == "__main__":
    main()
