# scripts/silver/build_silver.py
from __future__ import annotations
import argparse, glob, json, os, sys
from datetime import datetime
from zoneinfo import ZoneInfo
import polars as pl

from scripts.silver.utils import team_id as mk_team_id, venue_id as mk_venue_id, match_id as mk_match_id

# ---------- Helpers ----------
def now_utc_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def read_alias_map(path_csv: str) -> dict[str, str]:
    if not os.path.exists(path_csv):
        return {}
    df = pl.read_csv(path_csv)
    out = {}
    for row in df.iter_rows(named=True):
        out[str(row["alias"]).strip().lower()] = str(row["team_name_canon" if "team_name_canon" in row else "player_name_canon"]).strip().lower()
    return out

def canonize(name: str, alias_map: dict[str, str]) -> str:
    if name is None:
        return ""
    key = str(name).strip().lower()
    return alias_map.get(key, key).replace(" ", "_")

def scan_any(paths: list[str]) -> pl.DataFrame:
    frames = []
    for p in paths:
        for fp in glob.glob(p):
            if fp.endswith(".parquet"):
                frames.append(pl.read_parquet(fp))
            elif fp.endswith(".csv"):
                frames.append(pl.read_csv(fp))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")

def pick_first(*cols):
    for c in cols:
        if c is not None:
            return c
    return None

# ---------- Main ----------
def build(args):
    os.makedirs(args.silver_dir, exist_ok=True)
    created_ts = now_utc_str()

    team_alias = read_alias_map(args.team_aliases)
    # --- 1) Collect fixtures
    fixtures = scan_any([
        os.path.join(args.bronze_dir, "fixtures", "*.parquet"),
        os.path.join(args.bronze_dir, "fixtures", "*.csv"),
    ])

    if fixtures.is_empty():
        print("[build_silver] No fixtures in bronze/fixtures. Aborting.", file=sys.stderr)
        sys.exit(2)

    # Map likely column variants -> standard
    # Expected: home_team, away_team, scheduled_utc, season, round, venue (variants supported)
    colmap = {
        "home_team": [ "home_team", "home", "home_name" ],
        "away_team": [ "away_team", "away", "away_name" ],
        "scheduled_utc": [ "scheduled_utc", "kickoff_utc", "start_time_utc" ],
        "season": [ "season", "year" ],
        "round": [ "round", "rnd" ],
        "venue": [ "venue", "venue_name", "stadium" ],
        "source_tz": [ "source_tz" ],
        "status": [ "status" ],
        "discovered_at_utc": [ "discovered_at_utc", "first_seen_utc" ],
    }

    def getcol(df, keys):
        for k in keys:
            if k in df.columns:
                return pl.col(k)
        return pl.lit(None).alias(keys[0])

    fixtures_std = fixtures.select([
        getcol(fixtures, colmap["home_team"]).alias("home_team"),
        getcol(fixtures, colmap["away_team"]).alias("away_team"),
        getcol(fixtures, colmap["scheduled_utc"]).alias("scheduled_utc"),
        getcol(fixtures, colmap["season"]).cast(pl.Int64).alias("season"),
        getcol(fixtures, colmap["round"]).cast(pl.Int64).alias("round"),
        getcol(fixtures, colmap["venue"]).alias("venue"),
        getcol(fixtures, colmap["source_tz"]).alias("source_tz"),
        getcol(fixtures, colmap["status"]).alias("status"),
        getcol(fixtures, colmap["discovered_at_utc"]).alias("discovered_at_utc"),
    ])

    # Canonicalise team/venue strings
    fixtures_std = fixtures_std.with_columns([
        pl.col("home_team").cast(pl.Utf8).str.strip().alias("home_team"),
        pl.col("away_team").cast(pl.Utf8).str.strip().alias("away_team"),
        pl.col("venue").cast(pl.Utf8).str.strip().alias("venue"),
        pl.col("scheduled_utc").cast(pl.Utf8),
    ])

    # Build dim_team candidates by union of teams seen in fixtures (+ optional odds/player_stats later)
    team_names = (
        pl.concat([
            fixtures_std.select(pl.col("home_team").alias("t")),
            fixtures_std.select(pl.col("away_team").alias("t"))
        ])
        .drop_nulls()
        .with_columns(pl.col("t").str.to_lowercase())
        .unique()
        .filter(pl.col("t") != "")
        .to_series()
        .to_list()
    )
    teams_canon = [canonize(t, team_alias) for t in team_names]
    dim_team = pl.DataFrame({
        "team_name_canon": teams_canon,
    }).with_columns([
        pl.col("team_name_canon"),
        pl.col("team_name_canon").map_elements(lambda s: json.dumps([s]), return_dtype=pl.String).alias("aliases_json"),
        pl.col("team_name_canon").map_elements(mk_team_id).alias("team_id"),
        pl.lit(created_ts).alias("created_utc"),
        pl.lit(created_ts).alias("updated_utc"),
    ]).select(["team_id", "team_name_canon", "aliases_json", "created_utc", "updated_utc"])

    # dim_venue (minimal; you can enrich later)
    venues = (
        fixtures_std.select(pl.col("venue"))
        .drop_nulls()
        .unique()
        .filter(pl.col("venue") != "")
        .to_series().to_list()
    )
    dim_venue = pl.DataFrame({
        "venue_name": venues
    }).with_columns([
        pl.col("venue_name"),
        pl.col("venue_name").map_elements(mk_venue_id).alias("venue_id"),
    ]).with_columns([
        pl.lit(None, dtype=pl.Float64).alias("lat"),
        pl.lit(None, dtype=pl.Float64).alias("lon"),
        pl.lit(None, dtype=pl.Utf8).alias("tz"),
    ]).select(["venue_id", "venue_name", "lat", "lon", "tz"])

    # Map canonical team ids onto fixtures and compute match_id
    # Canonise strings for join
    canon_home = [canonize(x or "", team_alias) for x in fixtures_std.get_column("home_team").to_list()]
    canon_away = [canonize(x or "", team_alias) for x in fixtures_std.get_column("away_team").to_list()]
    home_ids = [mk_team_id(x) if x else None for x in canon_home]
    away_ids = [mk_team_id(x) if x else None for x in canon_away]

    venue_ids = [mk_venue_id(v or "") if v else None for v in fixtures_std.get_column("venue").to_list()]

    # Compose f_fixture
    f_fixture = fixtures_std.with_columns([
        pl.Series("home_team_id", home_ids),
        pl.Series("away_team_id", away_ids),
        pl.Series("venue_id", venue_ids),
    ])

    # Build match_id
    # NOTE: we leave scheduled_utc exactly as provided (string). If you prefer, parse to ts first.
    match_ids = []
    for r in f_fixture.iter_rows(named=True):
        match_ids.append(mk_match_id(
            int(r["season"]) if r["season"] is not None else 0,
            int(r["round"]) if r["round"] is not None else 0,
            r["home_team_id"] or "",
            r["away_team_id"] or "",
            r["scheduled_utc"] or "",
            r["venue_id"] or "",
        ))
    f_fixture = f_fixture.with_columns(pl.Series("match_id", match_ids))

    # Add date_local (if tz known later; for now keep null)
    f_fixture = f_fixture.with_columns([
        pl.lit(None, dtype=pl.Date).alias("date_local"),
    ])

    # Reorder columns
    f_fixture = f_fixture.select([
        "match_id", "season", "round", "scheduled_utc", "date_local",
        "venue_id", "home_team_id", "away_team_id",
        "status", "source_tz", "discovered_at_utc"
    ])

    # --- Write Parquet
    os.makedirs(args.silver_dir, exist_ok=True)
    dim_team.write_parquet(os.path.join(args.silver_dir, "dim_team.parquet"))
    dim_venue.write_parquet(os.path.join(args.silver_dir, "dim_venue.parquet"))
    f_fixture.write_parquet(os.path.join(args.silver_dir, "f_fixture.parquet"))

    # Optional CSV mirrors for easy eyeballing
    if args.csv_mirror:
        dim_team.write_csv(os.path.join(args.silver_dir, "dim_team.csv"))
        dim_venue.write_csv(os.path.join(args.silver_dir, "dim_venue.csv"))
        f_fixture.write_csv(os.path.join(args.silver_dir, "f_fixture.csv"))

    print(f"[OK] Wrote Silver: dim_team ({dim_team.height}), dim_venue ({dim_venue.height}), f_fixture ({f_fixture.height})")

def main(argv=None):
    p = argparse.ArgumentParser(description="Build minimal Silver (dims + fixtures)")
    p.add_argument("--bronze-dir", default="bronze", help="Path to bronze data directory")
    p.add_argument("--silver-dir", default="silver", help="Output directory for Silver tables")
    p.add_argument("--team-aliases", default="config/team_aliases.csv", help="Team alias map CSV")
    p.add_argument("--csv-mirror", action="store_true", help="Also write CSV next to Parquet")
    args = p.parse_args(argv)
    build(args)

if __name__ == "__main__":
    main()
