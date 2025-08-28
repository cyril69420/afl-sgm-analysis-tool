#!/usr/bin/env python
from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl
import requests
from bs4 import BeautifulSoup  # pip install beautifulsoup4

from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    echo_df_info,
    parquet_write,
    ensure_dir,
    load_yaml,
    utc_now,
    to_utc,
)

from schemas.bronze import BronzeFixtureRow

LOG = logging.getLogger("bronze.fixtures")

# Week-1 finals articles (either may contain the full week; we parse both)
AFL_WEEK1_URLS = [
    "https://www.afl.com.au/news/1403462/schedule-ticket-details-fixture-for-week-one-of-2025-afl-finals-series-confirmed",
    "https://www.afl.com.au/news/1404480/elimination-finals-locked-in-for-2025-toyota-afl-finals-series",
]

_TZ_MAP = {
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "AWST": "Australia/Perth",
    "ACST": "Australia/Adelaide",
    "ACDT": "Australia/Adelaide",
}

_TEAM_MAP = {
    "Greater Western Sydney": "GWS Giants",
    "GWS": "GWS Giants",
    "Gold Coast": "Gold Coast Suns",
    "Gold Coast SUNS": "Gold Coast Suns",
    "Sydney": "Sydney Swans",
    "Brisbane": "Brisbane Lions",
    "Adelaide": "Adelaide Crows",
    "Geelong": "Geelong Cats",
    "Hawthorn": "Hawthorn Hawks",
    "Fremantle": "Fremantle Dockers",
    "Collingwood": "Collingwood",
}

def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

def _game_key(season: int, round_id: str, home: str, away: str, kickoff_utc: datetime) -> str:
    return f"{season}_{round_id}_{_slugify(home)}_{_slugify(away)}_{kickoff_utc.strftime('%Y%m%dT%H%M%SZ')}"

def _parse_rounds(value: Optional[str]) -> List[str]:
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

# -----------------------
# Squiggle source (primary)
# -----------------------

def _squiggle_games(year: int) -> List[Dict]:
    """Try incomplete first (upcoming), then full year."""
    urls = [
        f"https://api.squiggle.com.au/?q=games;year={year};complete=0",
        f"https://api.squiggle.com.au/?q=games;year={year}",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            js = r.json() or {}
            games = js.get("games") or js.get("Games") or []
            LOG.info("[squiggle] %s -> %d games", url, len(games))
            if games:
                return games
        except Exception as e:
            LOG.warning("[squiggle] %s failed: %s", url, e)
    return []

def _normalise_squiggle(raw_games: List[Dict]) -> List[Dict]:
    rows: List[Dict] = []
    now = utc_now()
    for g in raw_games:
        home = g.get("hteam") or g.get("homeTeam") or g.get("hteamname")
        away = g.get("ateam") or g.get("awayTeam") or g.get("ateamname")
        venue = g.get("venue") or g.get("venueName") or "Unknown"
        season = int(g.get("year") or g.get("season") or 0)
        rnd = str(g.get("round") or g.get("Round") or "unknown")
        date_str = g.get("date") or g.get("localtime")
        tz = g.get("tz") or "Australia/Melbourne"
        if not (home and away and season and date_str):
            continue
        try:
            local_dt = datetime.fromisoformat(str(date_str).replace("Z", "").replace("T", " "))
        except Exception:
            try:
                local_dt = datetime.strptime(str(date_str).replace("T", " "), "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
        kickoff_utc = to_utc(local_dt, tz)
        row = {
            "game_key": _game_key(season, rnd, home, away, kickoff_utc),
            "season": season,
            "round": rnd,
            "home": home,
            "away": away,
            "venue": venue,
            "scheduled_time_utc": kickoff_utc,
            "source_tz": tz,
            "discovered_utc": now,
            "bookmaker_event_url": None,
        }
        try:
            BronzeFixtureRow.model_validate(row)
            rows.append(row)
        except Exception as e:
            LOG.debug("Squiggle row failed validation: %s", e)
    return rows

# -----------------------
# AFL.com.au Week-1 fallback (BeautifulSoup + line regex)
# -----------------------

DAY_RE = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?$",
    re.I,
)
FIX_RE = re.compile(
    r"^([A-Z][A-Za-z’'&\-\s]+)\s+v\s+([A-Z][A-Za-z’'&\-\s]+)\s+at\s+([^,]+),\s+(\d{1,2}[:.]\d{2})\s*(am|pm)\s+([A-Z]{3,4})(?:\s*\([^)]*\))?$",
    re.I,
)

def _clean_team(name: str) -> str:
    name = re.sub(r"\s+", " ", name.strip())
    return _TEAM_MAP.get(name, name)

def _parse_time(date_str: str, hhmm: str, ampm: str, tz_code: str) -> Optional[datetime]:
    m = re.match(r"(\d{1,2})[:.](\d{2})", hhmm)
    if not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    ap = ampm.lower()
    if ap == "pm" and hh != 12:
        hh += 12
    if ap == "am" and hh == 12:
        hh = 0
    try:
        local_dt = datetime.strptime(f"{date_str} {hh:02d}:{mm:02d}:00", "%A, %B %d, %Y %H:%M:%S")
    except Exception:
        return None
    tz_name = _TZ_MAP.get(tz_code.upper(), "Australia/Melbourne")
    return to_utc(local_dt, tz_name)

def _afl_week1_from_page(url: str, expected_year: int) -> List[Dict]:
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        html = r.text
    except Exception as e:
        LOG.warning("[afl.com.au] fetch failed for %s: %s", url, e)
        return []

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    raw_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # NORMALISE: drop leading '#' (so '##### THURSDAY, ...' becomes 'THURSDAY, ...')
    lines = [re.sub(r"^#+\s*", "", ln) for ln in raw_lines]

    rows: List[Dict] = []
    now = utc_now()
    current_date_str: Optional[str] = None

    for ln in lines:
        m_day = DAY_RE.match(ln)
        if m_day:
            dayname = m_day.group(1).title()
            month   = m_day.group(2).title()
            day     = int(m_day.group(3))
            year    = int(m_day.group(4)) if m_day.group(4) else expected_year
            current_date_str = f"{dayname}, {month} {day}, {year}"
            continue

        m_fix = FIX_RE.match(ln)
        if m_fix and current_date_str:
            home  = _clean_team(m_fix.group(1))
            away  = _clean_team(m_fix.group(2))
            venue = re.sub(r"\s+", " ", m_fix.group(3)).strip()
            hhmm, ampm, tz = m_fix.group(4), m_fix.group(5), m_fix.group(6).upper()

            kickoff_utc = _parse_time(current_date_str, hhmm, ampm, tz)
            if not kickoff_utc:
                continue

            rnd = "finals_w1"
            row = {
                "game_key": _game_key(expected_year, rnd, home, away, kickoff_utc),
                "season": expected_year,
                "round": rnd,
                "home": home,
                "away": away,
                "venue": venue,
                "scheduled_time_utc": kickoff_utc,
                "source_tz": _TZ_MAP.get(tz, "Australia/Melbourne"),
                "discovered_utc": now,
                "bookmaker_event_url": None,
            }
            try:
                BronzeFixtureRow.model_validate(row)
                rows.append(row)
            except Exception as e:
                LOG.debug("AFL row failed validation: %s", e)

    LOG.info("[afl.com.au] %s -> parsed %d finals fixtures", url, len(rows))
    return rows

def _afl_week1_finals(season: int) -> List[Dict]:
    """Aggregate over known Week-1 articles and de-duplicate."""
    out: List[Dict] = []
    for url in AFL_WEEK1_URLS:
        out += _afl_week1_from_page(url, season)
    seen = set()
    dedup: List[Dict] = []
    for r in out:
        key = (r["season"], r["home"], r["away"], r["scheduled_time_utc"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)
    return dedup

# -----------------------
# Main
# -----------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Ingest AFL fixtures into Bronze")
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--season", type=int, help="AFL season (e.g., 2025)")
    ap.add_argument("--rounds", type=str, default=None, help="Filter rounds '1,2,3' or '1-5' or 'all'")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing Parquet partitions")
    ap.add_argument("--csv-mirror", action="store_true", help="Write CSV mirror for quick inspection")
    ap.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARN/ERROR")
    add_common_test_flags(ap)  # --dry-run, --limit
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | bronze.fixtures | %(message)s",
    )

    root = Path(args.root)
    settings = load_yaml(root / "config" / "settings.yaml") or {}
    season = args.season or (settings.get("seasons") or [datetime.utcnow().year])[0]
    rounds = _parse_rounds(args.rounds) or _parse_rounds(settings.get("default_rounds", "all"))

    # 1) Squiggle (primary)
    raw = _squiggle_games(season)
    rows = _normalise_squiggle(raw)

    # 2) Finals fallback if Squiggle is thin (Aug/Sep) -> parse AFL pages
    finals_window = datetime.utcnow().month in (8, 9)
    if finals_window and len([r for r in rows if str(r["round"]).lower().startswith("final")]) < 4:
        LOG.info("Squiggle appears incomplete for finals; augmenting from AFL.com.au Week-1 schedule.")
        rows += _afl_week1_finals(season)
        # De-dupe by (season, home, away, kickoff_utc)
        seen = set()
        dedup: List[Dict] = []
        for r in rows:
            key = (r["season"], r["home"], r["away"], r["scheduled_time_utc"])
            if key in seen:
                continue
            seen.add(key)
            dedup.append(r)
        rows = dedup

    # Optional round filtering
    if rounds:
        rows = [r for r in rows if str(r["round"]) in set(rounds)]

    if not rows:
        LOG.warning("No fixtures to ingest.")
        return 0

    df = pl.DataFrame(rows)
    df = maybe_limit_df(df, args.limit)
    echo_df_info(df, label="fixtures (post-limit)")

    if args.dry_run:
        return 0

    out_root = root / "bronze" / "fixtures"
    parquet_write(df, out_root, partition_cols=["season", "round"], overwrite=args.overwrite)
    LOG.info("Wrote %d rows \u2192 %s", df.height, out_root.as_posix())

    if args.csv_mirror:
        csv_dir = root / "bronze_csv_mirror" / "fixtures"
        ensure_dir(csv_dir)
        csv_path = csv_dir / f"fixtures_{season}.csv"
        df.write_csv(csv_path)
        LOG.info("CSV mirror \u2192 %s", csv_path.as_posix())

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
