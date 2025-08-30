# scripts/bronze/bronze_ingest_team_sheets.py
from __future__ import annotations

import argparse
import datetime as dt
import re
from typing import Any, Dict, List, Optional

import polars as pl
from bs4 import BeautifulSoup
import requests

# Only use helpers that exist in your _shared.py
from scripts.bronze._shared import (
    get_logger,
    add_common_test_flags,  # adds --dry-run and --limit
    fetch_json,
    fetch_text,
    parquet_write,
    write_csv_mirror,
    utc_now,
)

LOG = get_logger("bronze.team_sheets")

SQUIGGLE_GAMES_URL = "https://api.squiggle.com.au/?q=games"
AFL_LINEUPS_URL = "https://www.afl.com.au/matches/team-lineups"
# Fallback fixture feed if Squiggle is slow/unreachable
FIXTUREDOWNLOAD_URL_TMPL = "https://fixturedownload.com/feed/json/afl-{season}"

# Minimal team-name normalization so fallback matches Squiggle-ish names
NAME_FIX = {
    "Gold Coast SUNS": "Gold Coast",
    "GWS GIANTS": "GWS",
    "Brisbane Lions": "Brisbane",
    "Port Adelaide Power": "Port Adelaide",
    "Sydney Swans": "Sydney",                # Squiggle uses 'Sydney'
    "Greater Western Sydney": "GWS",
    # pass-through examples
    "Western Bulldogs": "Western Bulldogs",
}


def _normalize_team(n: Optional[str]) -> Optional[str]:
    if n is None:
        return None
    n = str(n).strip()
    return NAME_FIX.get(n, n)


def fetch_upcoming_games_squiggle(season: int, days_ahead: int = 14, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Primary source: Squiggle (fixtures/uncompleted). Uses single numeric timeout to match _shared.
    """
    data = fetch_json(SQUIGGLE_GAMES_URL, params={"year": season, "complete": 0}, timeout=30)
    games = data.get("games", [])
    if not games:
        return pl.DataFrame()

    df = pl.DataFrame(games)

    # Coerce 'date' -> datetime (version-agnostic). Docs: Expr.str.to_datetime. :contentReference[oaicite:0]{index=0}
    if "date" in df.columns:
        df = df.with_columns(pl.col("date").str.to_datetime(strict=False).alias("scheduled_local"))

    if days_ahead is not None:
        now = dt.datetime.now()
        horizon = now + dt.timedelta(days=days_ahead)
        df = df.filter(
            pl.col("scheduled_local").is_not_null()
            & (pl.col("scheduled_local") < pl.lit(horizon))
        )

    if limit:
        df = df.head(limit)

    keep = [c for c in ["id", "round", "year", "hteam", "ateam", "venue", "scheduled_local"] if c in df.columns]
    return df.select(keep) if keep else pl.DataFrame()


def fetch_upcoming_games_fallback(season: int, days_ahead: int = 14, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Fallback source: fixturedownload.com JSON (daily). Schema can vary, so be defensive.
    Produces columns: id, round, year, hteam, ateam, venue, scheduled_local
    """
    url = FIXTUREDOWNLOAD_URL_TMPL.format(season=season)
    try:
        data = fetch_json(url, timeout=30)
    except Exception as e:
        LOG.warning("Fallback fixture fetch failed: %s", e)
        return pl.DataFrame()

    if not isinstance(data, list) or not data:
        return pl.DataFrame()

    df = pl.DataFrame(data)

    # Likely column names
    round_col = next((c for c in ["RoundNumber", "Round", "RoundId"] if c in df.columns), None)
    date_col = next((c for c in ["DateUtc", "Date", "DateTimeUtc"] if c in df.columns), None)
    h_col = next((c for c in ["HomeTeam", "HomeTeamName", "HomeTeamValue"] if c in df.columns), None)
    a_col = next((c for c in ["AwayTeam", "AwayTeamName", "AwayTeamValue"] if c in df.columns), None)
    v_col = next((c for c in ["Venue", "VenueName"] if c in df.columns), None)

    if not (round_col and date_col and h_col and a_col):
        LOG.warning("Fallback feed missing expected keys (have: %s).", df.columns)
        return pl.DataFrame()

    df = df.with_columns(
        pl.col(date_col).str.to_datetime(strict=False).alias("scheduled_local")
    )

    if days_ahead is not None:
        now = dt.datetime.now()
        horizon = now + dt.timedelta(days=days_ahead)
        df = df.filter(
            pl.col("scheduled_local").is_not_null()
            & (pl.col("scheduled_local") < pl.lit(horizon))
        )

    if df.is_empty():
        return pl.DataFrame()

    # Normalize names to (roughly) match Squiggle
    df = df.with_columns(
        [
            pl.col(h_col).map_elements(_normalize_team).alias("hteam"),
            pl.col(a_col).map_elements(_normalize_team).alias("ateam"),
        ]
    )

    # Create a stable synthetic id that won't collide with Squiggle (prefix with large offset)
    df = df.with_row_count(name="_rn")
    df = df.with_columns(
        [
            (pl.lit(season, dtype=pl.Int64) * 10_000
             + pl.col(round_col).cast(pl.Int64) * 100
             + pl.col("_rn").cast(pl.Int64)).alias("id"),
            pl.col(round_col).cast(pl.Int64).alias("round"),
            pl.lit(season, dtype=pl.Int64).alias("year"),
            (pl.col(v_col) if v_col in df.columns else pl.lit(None)).alias("venue"),
        ]
    )

    keep = ["id", "round", "year", "hteam", "ateam", "venue", "scheduled_local"]
    out = df.select([c for c in keep if c in df.columns])

    if limit:
        out = out.head(limit)

    return out


def fetch_upcoming_games(season: int, days_ahead: int = 14, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Try Squiggle first; on network errors/timeouts, fall back to fixturedownload.
    """
    try:
        df = fetch_upcoming_games_squiggle(season, days_ahead, limit)
        if not df.is_empty():
            return df
        LOG.warning("Squiggle returned no games; trying fallback feed…")
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
        LOG.warning("Squiggle fetch failed (%s). Trying fallback feed…", e)

    fb = fetch_upcoming_games_fallback(season, days_ahead, limit)
    if not fb.is_empty():
        LOG.info("Using fallback fixture feed (fixturedownload).")
        return fb

    LOG.error("No fixtures available from Squiggle or fallback.")
    return pl.DataFrame()


def scrape_team_lineups_html() -> str:
    """Get the AFL team line-ups page HTML (single numeric timeout)."""
    try:
        return fetch_text(AFL_LINEUPS_URL, timeout=30)
    except Exception as e:
        LOG.warning("Failed to fetch AFL line-ups page: %s", e)
        return ""


def parse_team_lineups(html: str) -> List[Dict[str, Any]]:
    """
    Parse AFL team-lineups page and return rows:
    {
      'team_name': str,
      'player_name': str,
      'role': 'onfield' | 'interchange' | 'sub' | 'emergency',
      'published_dt': str (ISO),
    }
    Defensive (site markup can change). Returns [] if nothing found.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, Any]] = []

    role_labels = {
        "onfield": re.compile(r"Backs|Half[- ]Backs|Centres|Half[- ]Forwards|Forwards|Followers", re.I),
        "interchange": re.compile(r"Interchange", re.I),
        "sub": re.compile(r"(Substitute|Sub)\b", re.I),
        "emergency": re.compile(r"Emergenc", re.I),
    }

    team_panels = soup.find_all(["section", "article", "div"], class_=re.compile(r"team|club|panel|squad", re.I))
    if not team_panels:
        return rows

    def extract_players(container) -> List[str]:
        players: List[str] = []
        for el in container.find_all(["li", "p", "div"]):
            txt = el.get_text(" ", strip=True)
            if not txt or re.search(r"Backs|Interchange|Emergenc|Sub", txt, re.I):
                continue
            if len(txt.split()) <= 6:
                players.append(txt)
        seen, out = set(), []
        for p in players:
            if p not in seen:
                out.append(p)
                seen.add(p)
        return out

    for panel in team_panels:
        h = panel.find(["h2", "h3", "h4"])
        team_name = (h.get_text(strip=True) if h else "").strip()
        if not team_name:
            img = panel.find("img", alt=True)
            if img:
                team_name = img["alt"].strip()
        if not team_name:
            continue
        team_name = _normalize_team(team_name)

        for role, patt in role_labels.items():
            for label_node in panel.find_all(string=patt):
                label_el = getattr(label_node, "parent", panel)
                group_container = getattr(label_el, "parent", panel)
                for p in extract_players(group_container):
                    rows.append(
                        {
                            "team_name": team_name,
                            "player_name": p,
                            "role": role,
                            "published_dt": utc_now().isoformat(),
                        }
                    )

    return rows


def reconcile_to_games(team_rows: List[Dict[str, Any]], games: pl.DataFrame) -> pl.DataFrame:
    """
    Best-effort: join scraped team names to games by matching hteam/ateam (case-insensitive).
    Output columns: season, round, match_id, hteam, ateam, team_name, player_name, role, published_dt
    """
    if not team_rows or games.is_empty():
        return pl.DataFrame(
            schema={
                "season": pl.Int64,
                "round": pl.Int64,
                "match_id": pl.Int64,
                "hteam": pl.Utf8,
                "ateam": pl.Utf8,
                "team_name": pl.Utf8,
                "player_name": pl.Utf8,
                "role": pl.Utf8,
                "published_dt": pl.Utf8,
            }
        )

    df_scrape = pl.DataFrame(team_rows).with_columns(pl.col("team_name").str.strip())
    g = games.select(
        [
            pl.col("year").alias("season"),
            pl.col("round"),
            pl.col("id").alias("match_id"),
            pl.col("hteam"),
            pl.col("ateam"),
        ]
    )

    out = (
        df_scrape.join(g, how="cross")
        .filter(
            (pl.col("team_name").str.to_lowercase() == pl.col("hteam").str.to_lowercase())
            | (pl.col("team_name").str.to_lowercase() == pl.col("ateam").str.to_lowercase())
        )
        .select(
            [
                "season",
                "round",
                "match_id",
                "hteam",
                "ateam",
                "team_name",
                "player_name",
                "role",
                "published_dt",
            ]
        )
        .unique()
    )

    return out


def build_placeholders(games: pl.DataFrame, season: int) -> pl.DataFrame:
    """
    If no teams are announced, emit one placeholder row per round to keep Bronze partitions stable.
    Accept `season` explicitly to avoid relying on `.item()` from a multi-row Series.
    """
    if games.is_empty():
        rounds: List[int] = []
    else:
        rounds = (
            games["round"]
            .drop_nulls()
            .cast(pl.Int64, strict=False)
            .unique()
            .to_list()
        )
        rounds = sorted(int(r) for r in rounds)

    rows = []
    for rnd in rounds or [None]:
        rows.append(
            {
                "season": season,
                "round": rnd,
                "match_id": None,
                "hteam": None,
                "ateam": None,
                "team_name": None,
                "player_name": None,
                "role": None,
                "published_dt": utc_now().isoformat(),
            }
        )

    df = pl.DataFrame(rows)

    # Cast to a stable schema to keep parquet partitions predictable.
    schema = {
        "season": pl.Int64,
        "round": pl.Int64,
        "match_id": pl.Int64,
        "hteam": pl.Utf8,
        "ateam": pl.Utf8,
        "team_name": pl.Utf8,
        "player_name": pl.Utf8,
        "role": pl.Utf8,
        "published_dt": pl.Utf8,
    }
    df = df.select([pl.col(c).cast(t) for c, t in schema.items()])
    return df


def _write_outputs(df: pl.DataFrame, out_root: str, season: int, overwrite: bool, csv_mirror: bool, dry_run: bool) -> None:
    """Centralized writer that respects --dry-run (skip all disk writes)."""
    if dry_run:
        LOG.info("DRY RUN: would write %d rows to %s partitioned by (season, round).", df.height, out_root)
        if csv_mirror:
            LOG.info("DRY RUN: would also write CSV mirror to bronze_csv_mirror/teamsheets/teamsheets_%s.csv", season)
        return
    parquet_write(df, out_root, partition_cols=["season", "round"], overwrite=overwrite)
    LOG.info("Wrote Parquet partitions -> %s (partitioned by season, round)", out_root)
    if csv_mirror:
        write_csv_mirror(df, subdir="teamsheets", stem=f"teamsheets_{season}")


def main():
    ap = argparse.ArgumentParser(description="Bronze ingest: AFL team line-ups (team sheets).")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--days-ahead", type=int, default=14, help="Fixture lookahead window (days)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing Parquet partitions")
    ap.add_argument("--csv-mirror", action="store_true", help="Also write a flat CSV mirror for eyeballing")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    # standard test flags from your _shared.py (e.g., --dry-run, --limit)
    add_common_test_flags(ap)

    args = ap.parse_args()
    LOG.setLevel(args.log_level.upper())

    LOG.info("Fetching upcoming games: season=%s days_ahead=%s limit=%s", args.season, args.days_ahead, args.limit)
    games = fetch_upcoming_games(args.season, days_ahead=args.days_ahead, limit=args.limit)
    out_root = "bronze/teamsheets"

    if games.is_empty():
        LOG.warning("No upcoming games available from either source; writing a single season-level placeholder.")
        df = build_placeholders(games, args.season)
        _write_outputs(df, out_root, args.season, args.overwrite, args.csv_mirror, args.dry_run)
        LOG.info("Done.")
        return

    LOG.info("Scraping AFL team line-ups page …")
    html = scrape_team_lineups_html()
    team_rows = parse_team_lineups(html)

    if not team_rows:
        LOG.warning("No team line-ups detected (likely not announced yet). Writing placeholders per round.")
        df = build_placeholders(games, args.season)
    else:
        LOG.info("Parsed %d player rows from AFL team line-ups. Reconciling to fixtures …", len(team_rows))
        df = reconcile_to_games(team_rows, games)
        if df.is_empty():
            LOG.warning("Parsed team-sheet rows could not be reconciled to fixtures. Writing placeholders per round.")
            df = build_placeholders(games, args.season)

    _write_outputs(df, out_root, args.season, args.overwrite, args.csv_mirror, args.dry_run)
    LOG.info("Done.")


if __name__ == "__main__":
    main()
