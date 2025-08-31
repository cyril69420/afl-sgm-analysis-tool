# scripts/silver/silver_build_odds_snapshot.py
from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# -----------------------
# Normalisation helpers
# -----------------------

_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^a-z0-9]+")

def norm_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip().lower().replace("’", "'")
    s = s.replace("&", " and ").replace("@", " at ")
    s = _NONALNUM.sub(" ", s)
    return _WS.sub(" ", s).strip()

def norm_url(u: Optional[str]) -> str:
    if not u:
        return ""
    u = re.sub(r"[?#].*$", "", str(u).strip())
    u = re.sub(r"^https?://(www\.)?", "", u, flags=re.I)
    return u.rstrip("/").lower()

def q(s: str) -> str:
    return s.replace("'", "''")

# -----------------------
# CLI
# -----------------------

def parse_args():
    p = argparse.ArgumentParser(
        "Build silver/f_odds_snapshot using URL-seeded H2H pairs (with robust fallback + diagnostics)."
    )
    p.add_argument("--bronze-dir", default="bronze")
    p.add_argument("--silver-dir", default="silver")
    p.add_argument("--season", type=int, default=None)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--csv-mirror", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--debug-dir", default=None, help="If set, write CSV diagnostics here (e.g., silver_debug)")
    p.add_argument("--hide-examples", action="store_true", help="Drop example.com odds rows and example fixtures.")
    # NEW: log-level
    p.add_argument("--log-level",
                   default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Python logging level for this script.")
    return p.parse_args()

# -----------------------
# Main
# -----------------------

def main():
    args = parse_args()

    # Configure logging per --log-level (standard library argparse+logging pattern). :contentReference[oaicite:1]{index=1}
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.info("Starting silver_build_odds_snapshot (log level=%s)", args.log_level.upper())

    bron = Path(args.bronze_dir)
    silv = Path(args.silver_dir)
    silv.mkdir(parents=True, exist_ok=True)

    # Core silver deps
    f_fixture_path = silv / "f_fixture.parquet"
    dim_team_path  = silv / "dim_team.parquet"
    if not f_fixture_path.exists():
        logging.error("%s not found. Run silver_build_core first.", f_fixture_path)
        sys.exit(2)
    if not dim_team_path.exists():
        logging.error("%s not found. Run silver_build_core first.", dim_team_path)
        sys.exit(2)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    logging.debug("DuckDB connected and threads set to 4")

    # Fixtures + teams
    con.execute(f"CREATE OR REPLACE VIEW fixture AS SELECT * FROM read_parquet('{q(f_fixture_path.as_posix())}')")
    con.execute(f"CREATE OR REPLACE VIEW dim_team AS SELECT * FROM read_parquet('{q(dim_team_path.as_posix())}')")

    # Team name lexicon (normalized) — lower() FIRST, then regex with global 'g'. :contentReference[oaicite:2]{index=2}
    con.execute(r"""
        CREATE OR REPLACE VIEW dim_team_norm AS
        SELECT
          team_id,
          team_name,
          regexp_replace(regexp_replace(lower(team_name), '[^a-z0-9]+', ' ', 'g'), '\s+', ' ', 'g') AS team_norm
        FROM dim_team
    """)

    # Normalised fixture names (use the same normalization as above)
    con.execute(r"""
        CREATE OR REPLACE VIEW fixture_norm_base AS
        SELECT
          f.*,
          h.team_norm AS home_norm,
          a.team_norm AS away_norm
        FROM fixture f
        JOIN dim_team_norm h ON f.home_team_id = h.team_id
        JOIN dim_team_norm a ON f.away_team_id = a.team_id
    """)

    # Optional: drop example fixtures (team names containing 'example')
    con.execute(f"""
        CREATE OR REPLACE VIEW fixture_norm AS
        SELECT *
        FROM fixture_norm_base
        {"WHERE home_norm NOT LIKE '%example%' AND away_norm NOT LIKE '%example%'" if args.hide_examples else ""}
    """)

    # Alias map: user's config + auto-add canon team names as aliases
    alias_csv = Path("config") / "team_aliases.csv"
    if alias_csv.exists():
        alias_df = pd.read_csv(alias_csv)
    else:
        alias_df = pd.DataFrame(columns=["alias", "team_name_canon"])
    # Add canon names (helps if CSV misses one)
    team_df = con.execute("SELECT team_name FROM dim_team").fetchdf()
    canon_rows = [{"alias": t, "team_name_canon": t} for t in team_df["team_name"].tolist()]
    alias_df = pd.concat([alias_df, pd.DataFrame(canon_rows)], ignore_index=True).drop_duplicates()
    alias_df["alias_norm"] = alias_df["alias"].map(norm_text)
    alias_df["team_norm"]  = alias_df["team_name_canon"].map(norm_text)

    # Harmonize alias → dim_team canon by token containment
    dim_lex = con.execute("SELECT DISTINCT team_norm FROM dim_team_norm").fetchdf()["team_norm"].tolist()
    dim_lex_set = set(dim_lex)
    def snap_to_dim(team_norm_alias: str) -> str:
        if team_norm_alias in dim_lex_set:
            return team_norm_alias
        alias_tok = f" {team_norm_alias} "
        for d in dim_lex:
            d_tok = f" {d} "
            if alias_tok in d_tok or f" {d} " in alias_tok:
                return d
        return team_norm_alias
    alias_df["team_norm"] = alias_df["team_norm"].map(snap_to_dim)
    con.register("alias_map", alias_df)

    # ---- Read bronze odds (Hive partition columns via folder names). :contentReference[oaicite:3]{index=3}
    snaps_glob = (bron / "odds" / "snapshots" / "season=*" / "round=*" / "bookmaker=*" / "*").as_posix()
    hist_glob  = (bron / "odds" / "history"   / "season=*" / "round=*" / "bookmaker=*" / "*").as_posix()

    def load_odds(glob_path: str, source_kind: str) -> pd.DataFrame:
        qread = f"""
            SELECT
              season::BIGINT AS season,
              round::VARCHAR AS round,
              COALESCE(lower(bookmaker),'') AS bookmaker,
              event_url,
              market_group, market_name, selection,
              line, decimal_odds, captured_at_utc, raw_payload, hash_key
            FROM read_parquet('{q(glob_path)}', hive_partitioning=1)
        """
        df = con.execute(qread).fetchdf()
        if df.empty:
            return df
        df["event_url_norm"]   = df["event_url"].map(norm_url)
        df["selection_norm"]   = df["selection"].map(norm_text) if "selection" in df.columns else ""
        df["mg_norm"]          = df["market_group"].map(norm_text) if "market_group" in df.columns else ""
        df["mn_norm"]          = df["market_name"].map(norm_text) if "market_name" in df.columns else ""
        df["bookmaker_norm"]   = df["bookmaker"].map(norm_text)
        df["source_kind"]      = source_kind
        return df

    df_live = load_odds(snaps_glob, "live")
    df_hist = load_odds(hist_glob,  "history")
    df_all  = pd.concat([df_live, df_hist], ignore_index=True)
    if df_all.empty:
        logging.error("No odds rows found under bronze/odds/{snapshots,history}/...")
        sys.exit(3)
    if args.limit:
        df_all = df_all.head(args.limit)

    # Optional: drop example odds URLs
    if args.hide_examples:
        before = len(df_all)
        df_all = df_all[~df_all["event_url"].astype(str).str.contains("example.com", na=False)].copy()
        logging.info("[hide-examples] Dropped %d odds rows from example.com", before - len(df_all))

    # Require URL for URL-seeded mapping
    df_all = df_all[df_all["event_url_norm"] != ""].copy()
    con.register("odds_all", df_all)
    logging.info("Loaded odds rows: %d", len(df_all))

    # ---- Seed pairs (URL-level) using your exact rules
    con.execute("""
        CREATE OR REPLACE VIEW pb_seed AS
        SELECT o.season, o.round, o.event_url_norm, a.team_norm
        FROM odds_all o
        JOIN alias_map a ON o.selection_norm = a.alias_norm
        WHERE o.bookmaker_norm = 'pointsbet'
          AND o.mg_norm = 'game lines'
          AND o.mn_norm = 'match result'
          AND a.team_norm <> 'draw'
    """)

    con.execute("""
        CREATE OR REPLACE VIEW sb_seed AS
        SELECT o.season, o.round, o.event_url_norm, a.team_norm
        FROM odds_all o
        JOIN alias_map a ON o.selection_norm = a.alias_norm
        WHERE o.bookmaker_norm = 'sportsbet'
          AND o.mg_norm = 'head to head markets'
          AND o.mn_norm = 'head to head'
          AND a.team_norm <> 'draw'
    """)

    con.execute("""
        CREATE OR REPLACE VIEW seed_pairs_url AS
        SELECT season, round, event_url_norm, list_distinct(list(team_norm)) AS teams2
        FROM (
          SELECT * FROM pb_seed
          UNION ALL
          SELECT * FROM sb_seed
        )
        GROUP BY season, round, event_url_norm
        HAVING len(list_distinct(list(team_norm))) = 2
    """)  # lists are 1-based in DuckDB. :contentReference[oaicite:4]{index=4}

    # ---- STRICT map URL → fixture on (season, round, team pair)
    con.execute("""
        CREATE OR REPLACE VIEW ev_to_fixture_strict AS
        SELECT DISTINCT
          s.season, s.round, s.event_url_norm, f.game_key
        FROM seed_pairs_url s
        JOIN fixture_norm f
          ON s.season = f.season
         AND s.round  = f.round
         AND (
              (s.teams2[1] = f.home_norm AND s.teams2[2] = f.away_norm) OR
              (s.teams2[1] = f.away_norm AND s.teams2[2] = f.home_norm)
         )
    """)

    # ---- LOOSE fallback: (season, team pair) when fixture.round is NULL / mismatched
    con.execute("""
        CREATE OR REPLACE VIEW ev_to_fixture_loose AS
        WITH cand AS (
          SELECT
            s.season, s.round, s.event_url_norm, f.game_key, f.scheduled_time_utc,
            (s.teams2[1] = f.home_norm AND s.teams2[2] = f.away_norm) OR
            (s.teams2[1] = f.away_norm AND s.teams2[2] = f.home_norm) AS pair_ok
          FROM seed_pairs_url s
          JOIN fixture_norm f
            ON s.season = f.season
          WHERE pair_ok
        ),
        ranked AS (
          SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY season, round, event_url_norm ORDER BY scheduled_time_utc DESC NULLS LAST) AS rn
          FROM cand
        )
        SELECT season, round, event_url_norm, game_key
        FROM ranked
        WHERE rn = 1
    """)

    con.execute("""
        CREATE OR REPLACE VIEW ev_to_fixture AS
        SELECT * FROM ev_to_fixture_strict
        UNION ALL
        SELECT l.*
        FROM ev_to_fixture_loose l
        LEFT JOIN ev_to_fixture_strict s
          ON l.season = s.season AND l.round = s.round AND l.event_url_norm = s.event_url_norm
        WHERE s.event_url_norm IS NULL
    """)

    # ---- Diagnostics (use --verbose to print tables)
    if args.verbose:
        print("\n[Strict resolved URLs]")
        print(con.execute("""
            SELECT COUNT(DISTINCT event_url_norm) AS urls_resolved_strict,
                   COUNT(DISTINCT game_key)      AS fixtures_covered_strict
            FROM ev_to_fixture_strict
        """).fetchdf())

        print("\n[Loose resolved URLs]")
        print(con.execute("""
            SELECT COUNT(DISTINCT event_url_norm) AS urls_resolved_loose,
                   COUNT(DISTINCT game_key)      AS fixtures_covered_loose
            FROM ev_to_fixture_loose
        """).fetchdf())

    # ---- Attach game_key to ALL odds on the URL
    season_clause = f"WHERE o.season = {int(args.season)}" if args.season else ""
    con.execute(f"""
        CREATE OR REPLACE VIEW odds_enriched AS
        SELECT o.*, m.game_key
        FROM odds_all o
        JOIN ev_to_fixture m
          ON o.event_url_norm = m.event_url_norm
         AND o.season = m.season
         AND o.round  = m.round
        {season_clause}
    """)

    # ---- Final fact (with robust bookmaker fill)
    con.execute("""
        CREATE OR REPLACE VIEW f_odds_snapshot AS
        SELECT
          game_key,
          CASE
            WHEN length(trim(bookmaker)) > 0 THEN lower(bookmaker)
            WHEN event_url_norm LIKE '%sportsbet.com.au%' THEN 'sportsbet'
            WHEN event_url_norm LIKE '%pointsbet.com.au%' THEN 'pointsbet'
            ELSE COALESCE(NULLIF(bookmaker_norm,''), '')
          END                                           AS bookmaker,
          COALESCE(market_group,'')                     AS market_group,
          COALESCE(market_name,'')                      AS market_name,
          COALESCE(selection,'')                        AS selection,
          TRY_CAST(line AS DOUBLE)                      AS line,
          TRY_CAST(decimal_odds AS DOUBLE)              AS decimal_odds,
          TRY_CAST(captured_at_utc AS TIMESTAMP)        AS captured_at_utc,
          COALESCE(source_kind,'')                      AS source_kind,
          season, round
        FROM odds_enriched
    """)

    out_dir = silv / "f_odds_snapshot.parquet"
    if args.overwrite and out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)

    con.execute(f"""
        COPY (SELECT * FROM f_odds_snapshot)
        TO '{q(out_dir.as_posix())}'
        (FORMAT PARQUET, PARTITION_BY (season, round))
    """)
    logging.info("Wrote %s", out_dir.as_posix())

    if args.csv_mirror:
        csv_dir = Path("silver_csv_mirror") / "f_odds_snapshot"
        csv_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT * FROM f_odds_snapshot)
            TO '{q((csv_dir / "f_odds_snapshot.csv").as_posix())}' WITH (HEADER, DELIMITER ',')
        """)
        logging.info("CSV mirror: %s", (csv_dir / "f_odds_snapshot.csv").as_posix())

    logging.info("Done.")

if __name__ == "__main__":
    main()
