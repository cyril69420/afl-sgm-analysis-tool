#!/usr/bin/env python
"""
Build Silver core dims/facts from Bronze fixtures.

Outputs (under --silver-dir, default 'silver/'):
  - silver/dim_team.parquet
  - silver/dim_venue.parquet
  - silver/f_fixture.parquet

Assumptions:
  - Bronze fixtures written under bronze/fixtures/season=YYYY/round=*/...parquet
  - Columns include: game_key, home_team, away_team, venue, scheduled_time_utc (lenient mapping below)
  - Optional canonicalisation via config/team_aliases.csv (alias,team_name_canon)
"""
from __future__ import annotations

import argparse
import hashlib
import logging
from pathlib import Path

import duckdb
import pandas as pd

# ---------- helpers ----------

def _setup_logger(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )


def _stable_id(prefix: str, text: str, n: int = 12) -> str:
    h = hashlib.md5(text.strip().lower().encode("utf-8")).hexdigest()[:n]
    return f"{prefix}_{h}"


def _read_alias_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path).rename(
                columns={c: c.strip().lower() for c in pd.read_csv(path, nrows=0).columns}
            )
            # Expect columns: alias, team_name_canon
            have = {c for c in df.columns}
            if not {"alias", "team_name_canon"} <= have:
                logging.warning("team_aliases missing required columns; using passthrough.")
                return pd.DataFrame(columns=["alias", "team_name_canon"])
            df["alias"] = df["alias"].astype(str).str.strip()
            df["team_name_canon"] = df["team_name_canon"].astype(str).str.strip()
            return df[["alias", "team_name_canon"]]
        except Exception as e:
            logging.warning("Failed reading team alias CSV (%s): %s", path, e)
    return pd.DataFrame(columns=["alias", "team_name_canon"])


# ---------- core build ----------

def build(silver_dir: Path, bronze_dir: Path, config_dir: Path) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8;")

    fixtures_glob = str(bronze_dir / "fixtures/**/*.parquet").replace("\\", "/")
    logging.info("Scanning fixtures from %s", fixtures_glob)

    # Flexible column normalisation for fixture variants
    q = f"""
    WITH raw AS (
      SELECT *
      FROM read_parquet('{fixtures_glob}', hive_partitioning=1)
    ),
    fx AS (
      SELECT
        COALESCE(game_key, match_key, fixture_key)                      AS game_key,
        COALESCE(home_team, home, home_name)                            AS home_team,
        COALESCE(away_team, away, away_name)                            AS away_team,
        COALESCE(venue, venue_name, stadium, ground)                    AS venue_name,
        TRY_CAST(COALESCE(scheduled_time_utc, scheduled_utc, kickoff_utc, start_time_utc) AS TIMESTAMP) AS scheduled_time_utc,
        TRY_CAST(season AS INTEGER)                                     AS season,
        TRY_CAST(round AS INTEGER)                                      AS round
      FROM raw
    )
    SELECT * FROM fx
    WHERE game_key IS NOT NULL AND home_team IS NOT NULL AND away_team IS NOT NULL
    """
    fixtures = con.execute(q).fetch_df()
    if fixtures.empty:
        raise SystemExit("No fixtures discovered in bronze/fixtures. Did Bronze run successfully?")

    # Canonicalise team names via config/team_aliases.csv (optional)
    team_alias_df = _read_alias_csv(config_dir / "team_aliases.csv")
    if not team_alias_df.empty:
        # Map home/away through alias table
        alias_map = dict(zip(team_alias_df["alias"].str.lower(), team_alias_df["team_name_canon"]))
        fixtures["home_team_canon"] = fixtures["home_team"].astype(str).map(
            lambda s: alias_map.get(s.strip().lower(), s.strip()))
        fixtures["away_team_canon"] = fixtures["away_team"].astype(str).map(
            lambda s: alias_map.get(s.strip().lower(), s.strip()))
    else:
        fixtures["home_team_canon"] = fixtures["home_team"].astype(str).str.strip()
        fixtures["away_team_canon"] = fixtures["away_team"].astype(str).str.strip()

    # Venue canon (cheap normalisation)
    fixtures["venue_name_canon"] = fixtures["venue_name"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    # Build dims (stable IDs by md5 of canon name)
    teams = pd.DataFrame(
        sorted(set(fixtures["home_team_canon"]) | set(fixtures["away_team_canon"])),
        columns=["team_name"],
    )
    teams["team_id"] = teams["team_name"].map(lambda s: _stable_id("tm", s))

    venues = pd.DataFrame(sorted(set(fixtures["venue_name_canon"])), columns=["venue_name"])
    venues["venue_id"] = venues["venue_name"].map(lambda s: _stable_id("vn", s))

    # Join IDs into fixtures → f_fixture
    fixt = fixtures.merge(
        teams, left_on="home_team_canon", right_on="team_name", how="left"
    ).rename(columns={"team_id": "home_team_id"}).drop(columns=["team_name"])
    fixt = fixt.merge(
        teams, left_on="away_team_canon", right_on="team_name", how="left"
    ).rename(columns={"team_id": "away_team_id"}).drop(columns=["team_name"])
    fixt = fixt.merge(
        venues, left_on="venue_name_canon", right_on="venue_name", how="left"
    ).rename(columns={"venue_id": "venue_id"}).drop(columns=["venue_name"])

    f_fixture = fixt[[
        "game_key",
        "season", "round",
        "home_team_id", "away_team_id", "venue_id",
        "scheduled_time_utc",
        "home_team_canon", "away_team_canon", "venue_name_canon"
    ]].rename(columns={
        "home_team_canon": "home_team_name",
        "away_team_canon": "away_team_name",
        "venue_name_canon": "venue_name"
    }).sort_values(["season", "round", "scheduled_time_utc", "game_key"])

    # Write outputs
    silver_dir.mkdir(parents=True, exist_ok=True)
    teams[["team_id", "team_name"]].to_parquet(silver_dir / "dim_team.parquet", index=False)
    venues[["venue_id", "venue_name"]].to_parquet(silver_dir / "dim_venue.parquet", index=False)
    f_fixture.to_parquet(silver_dir / "f_fixture.parquet", index=False)

    logging.info("Wrote: %s, %s, %s",
                 silver_dir / "dim_team.parquet",
                 silver_dir / "dim_venue.parquet",
                 silver_dir / "f_fixture.parquet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", default="bronze", type=Path)
    ap.add_argument("--silver-dir", default="silver", type=Path)
    ap.add_argument("--config-dir", default="config", type=Path)
    ap.add_argument("--log-level", default="INFO")
    _ = ap.parse_args()

    _setup_logger(_.log_level)
    build(_.silver_dir, _.bronze_dir, _.config_dir)


if __name__ == "__main__":
    main()
