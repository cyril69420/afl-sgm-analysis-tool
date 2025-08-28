# scripts/bronze/bronze_ingest_team_sheets.py
from __future__ import annotations

import argparse
import datetime as dt
import re
from typing import Any, Dict, List, Optional

import polars as pl
from bs4 import BeautifulSoup

# Use ONLY helpers that exist in your _shared.py
from scripts.bronze._shared import (
    get_logger,
    add_common_test_flags,
    fetch_json,
    fetch_text,
    parquet_write,
    write_csv_mirror,
    utc_now,
)

LOG = get_logger("bronze.team_sheets")

SQUIGGLE_GAMES_URL = "https://api.squiggle.com.au/?q=games"
AFL_LINEUPS_URL = "https://www.afl.com.au/matches/team-lineups"


def fetch_upcoming_games(season: int, days_ahead: int = 14, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Squiggle: upcoming/uncompleted games for a season.
    Filters to a lookahead window to keep things modest.
    """
    data = fetch_json(SQUIGGLE_GAMES_URL, params={"year": season, "complete": 0})
    games = data.get("games", [])
    if not games:
        return pl.DataFrame()

    df = pl.DataFrame(games)

    # Coerce Squiggle 'date' string to datetime if present
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
            .str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S", strict=False)
            .alias("scheduled_local")
        )

    if days_ahead is not None:
        now = dt.datetime.now()
        horizon = now + dt.timedelta(days=days_ahead)
        df = df.filter(pl.col("scheduled_local").is_not_null() & (pl.col("scheduled_local") < pl.lit(horizon)))

    if limit:
        df = df.head(limit)

    keep = [c for c in ["id", "round", "year", "hteam", "ateam", "venue", "scheduled_local"] if c in df.columns]
    return df.select(keep) if keep else pl.DataFrame()


def scrape_team_lineups_html() -> str:
    """Get the AFL team line-ups page HTML."""
    return fetch_text(AFL_LINEUPS_URL)


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
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, Any]] = []

    # Heuristic role labels we tend to see on afl.com.au
    role_labels = {
        "onfield": re.compile(r"Backs|Half[- ]Backs|Centres|Half[- ]Forwards|Forwards|Followers", re.I),
        "interchange": re.compile(r"Interchange", re.I),
        "sub": re.compile(r"(Substitute|Sub)\b", re.I),
        "emergency": re.compile(r"Emergenc", re.I),
    }

    # Find likely team panels
    team_panels = soup.find_all(["section", "article", "div"], class_=re.compile(r"team|club|panel|squad", re.I))
    if not team_panels:
        return rows  # nothing announced yet or different markup

    def extract_players(container) -> List[str]:
        players: List[str] = []
        for el in container.find_all(["li", "p", "div"]):
            txt = el.get_text(" ", strip=True)
            # crude filters to skip headings
            if not txt or re.search(r"Backs|Interchange|Emergenc|Sub", txt, re.I):
                continue
            # keep short-ish lines that look like names
            if len(txt.split()) <= 6:
                players.append(txt)
        # de-dup preserve order
        seen, out = set(), []
        for p in players:
            if p not in seen:
                out.append(p)
                seen.add(p)
        return out

    for panel in team_panels:
        # Try to get team name from a header or logo alt
        h = panel.find(["h2", "h3", "h4"])
        team_name = (h.get_text(strip=True) if h else "").strip()
        if not team_name:
            img = panel.find("img", alt=True)
            if img:
                team_name = img["alt"].strip()
        if not team_name:
            continue

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
    Best-effort: join scraped team names to Squiggle games by matching hteam/ateam (case-insensitive).
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


def build_placeholders(games: pl.DataFrame) -> pl.DataFrame:
    """
    If no teams are announced, emit one placeholder row per round to keep Bronze partitions stable.
    """
    if games.is_empty():
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

    season = int(games["year"].item())
    rows = []
    for rnd in sorted([int(r) for r in games["round"].unique()]):
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
    return pl.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Bronze ingest: AFL team line-ups (team sheets).")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--days-ahead", type=int, default=14, help="Fixture lookahead window (days)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing Parquet partitions")
    ap.add_argument("--csv-mirror", action="store_true", help="Also write a flat CSV mirror for eyeballing")
    # standard test flags from your _shared.py
    add_common_test_flags(ap)

    args = ap.parse_args()
    LOG.info("Fetching upcoming games: season=%s days_ahead=%s limit=%s", args.season, args.days_ahead, args.limit)

    games = fetch_upcoming_games(args.season, days_ahead=args.days_ahead, limit=args.limit)
    if games.is_empty():
        LOG.warning("No upcoming games found.")
        return

    LOG.info("Scraping AFL team line-ups page …")
    html = scrape_team_lineups_html()
    team_rows = parse_team_lineups(html)

    if not team_rows:
        LOG.warning("No team line-ups detected (likely not announced yet). Writing placeholders per round.")
        df = build_placeholders(games)
    else:
        LOG.info("Parsed %d player rows from AFL team line-ups. Reconciling to fixtures …", len(team_rows))
        df = reconcile_to_games(team_rows, games)
        if df.is_empty():
            LOG.warning("Parsed team-sheet rows could not be reconciled to fixtures. Writing placeholders per round.")
            df = build_placeholders(games)

    # Write Parquet partitioned by season, round using your duckdb-backed parquet_write
    out_root = "bronze/teamsheets"
    parquet_write(df, out_root, partition_cols=["season", "round"], overwrite=args.overwrite)
    LOG.info("Wrote Parquet partitions -> %s (partitioned by season, round)", out_root)

    # Optional CSV mirror of the full tidy view (not partitioned, for quick inspection)
    if args.csv_mirror:
        write_csv_mirror(df, subdir="teamsheets", stem=f"teamsheets_{args.season}")

    LOG.info("Done.")


if __name__ == "__main__":
    main()
