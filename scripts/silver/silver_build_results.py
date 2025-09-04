# scripts/silver/silver_build_results.py
from __future__ import annotations
import argparse, json, logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Iterable
import duckdb
import pandas as pd

log = logging.getLogger("silver.results")
def setup_logging(level: str) -> None:
    level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

# ---------- normalisation helpers ----------
def norm(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    t = "".join(ch for ch in s.lower().strip() if ch.isalnum() or ch.isspace())
    t = " ".join(t.split())
    t = t.replace("gws", "giants").replace("greater western sydney", "giants")
    t = t.replace("the mcg", "mcg").replace("m.c.g.", "mcg")
    return t

# explicit long->base mappings for AFL names
LONG_TO_BASE = {
    "adelaide crows": "adelaide",
    "brisbane lions": "brisbane",
    "carlton blues": "carlton",
    "collingwood magpies": "collingwood",
    "essendon bombers": "essendon",
    "fremantle dockers": "fremantle",
    "geelong cats": "geelong",
    "hawthorn hawks": "hawthorn",
    "melbourne demons": "melbourne",
    "north melbourne kangaroos": "north melbourne",
    "port adelaide power": "port adelaide",
    "richmond tigers": "richmond",
    "st kilda saints": "st kilda",
    "sydney swans": "sydney",
    "west coast eagles": "west coast",
    "gold coast suns": "gold coast",
    "gws giants": "giants",
    "giants giants": "giants",
    "greater western sydney giants": "giants",
    "western bulldogs": "western bulldogs",  # keep whole; "western" alone is too ambiguous
}

def slim_name(n: Optional[str]) -> Optional[str]:
    if n is None: return None
    b = norm(n)
    if not b: return b
    # apply long->base collapse where possible
    for long_form, base in LONG_TO_BASE.items():
        if long_form in b:
            b = base
            break
    # de-dup repeated words like "giants giants"
    toks = []
    for t in b.split():
        if not toks or toks[-1] != t:
            toks.append(t)
    b = " ".join(toks)
    return b

# ---------- IO helpers ----------
def write_partitioned(df: pd.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("df", df)
    sql = f"""
        COPY df TO '{out_dir.as_posix()}'
        (FORMAT 'parquet',
         PARTITION_BY ({", ".join(partition_cols)}),
         OVERWRITE {str(overwrite).lower()},
         WRITE_PARTITION_COLUMNS false)
    """
    con.execute(sql)

def write_csv_mirror(df: pd.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool) -> None:
    for key, g in df.groupby(partition_cols):
        key = key if isinstance(key, tuple) else (key,)
        part_dir = out_dir.joinpath(*[f"{c}={v}" for c, v in zip(partition_cols, key)])
        part_dir.mkdir(parents=True, exist_ok=True)
        p = part_dir / "part.csv"
        if p.exists() and not overwrite: continue
        g.to_csv(p, index=False)

# ---------- Bronze discovery ----------
def discover_bronze_result_files(season: int, bronze_dir: Path) -> List[Path]:
    season_dir = bronze_dir / "results" / f"season={season}"
    c = list(season_dir.glob("round=*/*.parquet"))
    single = season_dir / "results.parquet"
    if single.exists(): c.append(single)
    if not c: c = list(season_dir.glob("**/*.parquet"))
    return sorted(set(p.resolve() for p in c))

def discover_bronze_result_csv(season: int, csv_root: Path) -> List[Path]:
    season_dir = csv_root / f"season={season}"
    c = list(season_dir.glob("round=*/part.csv"))
    single = season_dir / "results.csv"
    if single.exists(): c.append(single)
    if not c: c = list(season_dir.glob("**/*.csv"))
    return sorted(set(p.resolve() for p in c))

def read_bronze_results_any(con: duckdb.DuckDBPyConnection, season: int, bronze_dir: Path) -> pd.DataFrame:
    pq = discover_bronze_result_files(season, bronze_dir)
    if pq:
        log.info("Discovered %d Bronze Parquet file(s) for season=%s", len(pq), season)
        for p in pq[:5]: log.info("  • %s", p.as_posix())
        files = ", ".join(f"'{p.as_posix()}'" for p in pq)
        return con.execute(f"SELECT * FROM read_parquet([{files}])").fetchdf()
    csv_files = discover_bronze_result_csv(season, Path("bronze_csv_mirror") / "results")
    if csv_files:
        log.warning("No Bronze Parquet found; falling back to CSV mirror (%d file(s))", len(csv_files))
        for p in csv_files[:5]: log.info("  • %s", p.as_posix())
        files = ", ".join(f"'{p.as_posix()}'" for p in csv_files)
        return con.execute(f"SELECT * FROM read_csv_auto([{files}])").fetchdf()
    raise FileNotFoundError(f"No Bronze results for season={season} under { (bronze_dir / 'results' / f'season={season}').as_posix() }")

# ---------- Fixture schema detection ----------
def detect_fixture_columns(con: duckdb.DuckDBPyConnection, fixture_path: Path) -> Tuple[str, str, str, str, str, bool]:
    info = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{fixture_path.as_posix()}') LIMIT 0").fetchdf()
    log.info("f_fixture schema:\n%s", info.to_string(index=False))
    cols = [c.lower() for c in info["column_name"].tolist()]
    round_col = "round" if "round" in cols else ("round_code" if "round_code" in cols else cols[0])
    sched_candidates = ["scheduled_utc", "start_time_utc", "scheduled_time_utc", "scheduled_time", "scheduled"]
    scheduled_col = next((c for c in sched_candidates if c in cols), None)
    if not scheduled_col:
        raise RuntimeError("f_fixture missing scheduled time column (tried: scheduled_utc/start_time_utc/scheduled_time_utc/...)")
    home_id_col = "home_team_id" if "home_team_id" in cols else None
    away_id_col = "away_team_id" if "away_team_id" in cols else None
    if not home_id_col or not away_id_col:
        raise RuntimeError("f_fixture missing home_team_id/away_team_id")
    game_key_col = "game_key" if "game_key" in cols else ("match_id" if "match_id" in cols else None)
    if not game_key_col:
        raise RuntimeError("f_fixture missing game_key/match_id")
    have_names = ("home_team_name" in cols) and ("away_team_name" in cols)
    return round_col, scheduled_col, home_id_col, away_id_col, game_key_col, have_names

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Silver: build canonical f_result from Bronze results + f_fixture")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--csv-mirror", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--debug-dir", default=".debug_afl")
    ap.add_argument("--bronze-dir", default="bronze")
    ap.add_argument("--silver-dir", default="silver")
    ap.add_argument("--time-window-days", type=int, default=3, help="Time window for time-based joins")
    args = ap.parse_args()
    setup_logging(args.log_level)

    season = args.season
    bronze_dir = Path(args.bronze_dir)
    silver_dir = Path(args.silver_dir)
    debug_dir = Path(args.debug_dir); debug_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # Bronze
    log.info("Reading Bronze results (season=%s) with flexible discovery…", season)
    br_df = read_bronze_results_any(con, season, bronze_dir)
    log.info("Loaded Bronze results rows: %d", len(br_df))
    log.info("Bronze sample:\n%s", br_df.head(5).to_string(index=False))

    br = br_df.copy().reset_index(drop=True)
    br["row_id"] = br.index.astype("int64")
    for col in ["home","away"]:
        if col not in br.columns: raise RuntimeError(f"Bronze results missing '{col}' column")
        br[col] = br[col].astype("string")
    br["home_norm"] = br["home"].map(norm)
    br["away_norm"] = br["away"].map(norm)
    br["home_slim"] = br["home"].map(slim_name)
    br["away_slim"] = br["away"].map(slim_name)

    # Fixture
    fxt_path = silver_dir / "f_fixture.parquet"
    rnd_col, sched_col, fx_home_col, fx_away_col, key_col, have_names = detect_fixture_columns(con, fxt_path)
    fx_df = con.execute(f"""
        SELECT
            season,
            CAST({rnd_col} AS VARCHAR) AS round_str,
            CAST({fx_home_col} AS VARCHAR) AS home_team_id,
            CAST({fx_away_col} AS VARCHAR) AS away_team_id,
            {sched_col} AS scheduled_utc,
            CAST({key_col} AS VARCHAR) AS game_key,
            venue_id
            {", home_team_name, away_team_name" if have_names else ""}
        FROM read_parquet('{fxt_path.as_posix()}')
        WHERE season = {season}
    """).fetchdf()
    if have_names:
        for c in ["home_team_name","away_team_name"]:
            fx_df[c] = fx_df[c].astype("string")
        fx_df["home_norm"] = fx_df["home_team_name"].map(norm)
        fx_df["away_norm"] = fx_df["away_team_name"].map(norm)
        fx_df["home_slim"] = fx_df["home_team_name"].map(slim_name)
        fx_df["away_slim"] = fx_df["away_team_name"].map(slim_name)
    log.info("Loaded Fixtures rows for season=%s: %d", season, len(fx_df))
    if len(fx_df):
        log.info("Fixture rounds present: %s", fx_df["round_str"].value_counts().to_dict())

    # Register
    con.register("br", br); con.register("fx", fx_df)
    W = args.time_window_days

    # ---- Join SQLs ----
    # 1) Exact by Team IDs + Round
    sql_exact_id = f"""
        WITH br1 AS (
            SELECT row_id, season, CAST(round AS VARCHAR) AS round_str,
                   CAST(home_norm AS VARCHAR) AS home_norm, CAST(away_norm AS VARCHAR) AS away_norm,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   CAST(NULL AS VARCHAR) AS home_team_id, CAST(NULL AS VARCHAR) AS away_team_id,
                   start_time_utc, home_score, away_score, venue, source_kind, raw_payload
            FROM br
        ), fx1 AS (
            SELECT season, round_str,
                   CAST(home_team_id AS VARCHAR) AS home_team_id, CAST(away_team_id AS VARCHAR) AS away_team_id,
                   scheduled_utc, game_key, venue_id
            FROM fx
        )
        SELECT br1.*, fx1.game_key, fx1.venue_id, fx1.scheduled_utc, 'exact_id' AS join_mode, 1 AS _prio
        FROM br1 JOIN fx1
          ON br1.season = fx1.season
         AND br1.round_str = fx1.round_str
         AND br1.home_norm = br1.home_norm  -- no-op; placeholder to keep schema same
         AND 1=0  -- disable until we have team_id mapping in f_fixture + Bronze
    """  # kept as placeholder; ID-based mapping can be re-enabled later

    # 2) Exact by Names (basic)
    sql_exact_name = """
        WITH br1 AS (
            SELECT row_id, season, CAST(round AS VARCHAR) AS round_str,
                   CAST(home_norm AS VARCHAR) AS home_norm, CAST(away_norm AS VARCHAR) AS away_norm,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   start_time_utc, home_score, away_score, venue, source_kind, raw_payload
            FROM br
        ), fx1 AS (
            SELECT season, round_str,
                   CAST(home_norm AS VARCHAR) AS home_norm, CAST(away_norm AS VARCHAR) AS away_norm,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   scheduled_utc, game_key, venue_id
            FROM fx
        )
        SELECT br1.*, fx1.game_key, fx1.venue_id, fx1.scheduled_utc, 'exact_name' AS join_mode, 2 AS _prio
        FROM br1 JOIN fx1
         ON br1.season = fx1.season
        AND br1.round_str = fx1.round_str
        AND LEAST(br1.home_norm, br1.away_norm) = LEAST(fx1.home_norm, fx1.away_norm)
        AND GREATEST(br1.home_norm, br1.away_norm) = GREATEST(fx1.home_norm, fx1.away_norm)
    """

    # 3) Time-window by Names (basic)
    sql_time_name = f"""
        WITH br1 AS (
            SELECT row_id, season, CAST(round AS VARCHAR) AS round_str,
                   CAST(home_norm AS VARCHAR) AS home_norm, CAST(away_norm AS VARCHAR) AS away_norm,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   start_time_utc, home_score, away_score, venue, source_kind, raw_payload
            FROM br
            WHERE start_time_utc IS NOT NULL
        ), fx1 AS (
            SELECT season, round_str,
                   CAST(home_norm AS VARCHAR) AS home_norm, CAST(away_norm AS VARCHAR) AS away_norm,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   scheduled_utc, game_key, venue_id
            FROM fx
            WHERE scheduled_utc IS NOT NULL
        )
        SELECT br1.*, fx1.game_key, fx1.venue_id, fx1.scheduled_utc, 'time_name' AS join_mode, 3 AS _prio
        FROM br1 JOIN fx1
         ON br1.season = fx1.season
        AND LEAST(br1.home_norm, br1.away_norm) = LEAST(fx1.home_norm, fx1.away_norm)
        AND GREATEST(br1.home_norm, br1.away_norm) = GREATEST(fx1.home_norm, fx1.away_norm)
        AND abs(epoch(CAST(br1.start_time_utc AS TIMESTAMPTZ)) - epoch(CAST(fx1.scheduled_utc AS TIMESTAMPTZ))) <= {W}*24*60*60
    """

    # 4) Exact by Names (slim)
    sql_exact_name_slim = """
        WITH br1 AS (
            SELECT row_id, season, CAST(round AS VARCHAR) AS round_str,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   start_time_utc, home_score, away_score, venue, source_kind, raw_payload
            FROM br
        ), fx1 AS (
            SELECT season, round_str,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   scheduled_utc, game_key, venue_id
            FROM fx
        )
        SELECT br1.*, fx1.game_key, fx1.venue_id, fx1.scheduled_utc, 'exact_name_slim' AS join_mode, 4 AS _prio
        FROM br1 JOIN fx1
         ON br1.season = fx1.season
        AND br1.round_str = fx1.round_str
        AND LEAST(br1.home_slim, br1.away_slim) = LEAST(fx1.home_slim, fx1.away_slim)
        AND GREATEST(br1.home_slim, br1.away_slim) = GREATEST(fx1.home_slim, fx1.away_slim)
    """

    # 5) Time-window by Names (slim)
    sql_time_name_slim = f"""
        WITH br1 AS (
            SELECT row_id, season, CAST(round AS VARCHAR) AS round_str,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   start_time_utc, home_score, away_score, venue, source_kind, raw_payload
            FROM br
            WHERE start_time_utc IS NOT NULL
        ), fx1 AS (
            SELECT season, round_str,
                   CAST(home_slim AS VARCHAR) AS home_slim, CAST(away_slim AS VARCHAR) AS away_slim,
                   scheduled_utc, game_key, venue_id
            FROM fx
            WHERE scheduled_utc IS NOT NULL
        )
        SELECT br1.*, fx1.game_key, fx1.venue_id, fx1.scheduled_utc, 'time_name_slim' AS join_mode, 5 AS _prio
        FROM br1 JOIN fx1
         ON br1.season = fx1.season
        AND LEAST(br1.home_slim, br1.away_slim) = LEAST(fx1.home_slim, fx1.away_slim)
        AND GREATEST(br1.home_slim, br1.away_slim) = GREATEST(fx1.away_slim, fx1.away_slim)
        AND abs(epoch(CAST(br1.start_time_utc AS TIMESTAMPTZ)) - epoch(CAST(fx1.scheduled_utc AS TIMESTAMPTZ))) <= {W}*24*60*60
    """

    # Execute
    exact_id = con.execute(sql_exact_id).fetchdf()
    exact_nm = con.execute(sql_exact_name).fetchdf() if have_names else pd.DataFrame()
    time_nm  = con.execute(sql_time_name).fetchdf()  if have_names else pd.DataFrame()
    exact_ns = con.execute(sql_exact_name_slim).fetchdf() if have_names else pd.DataFrame()
    time_ns  = con.execute(sql_time_name_slim).fetchdf()  if have_names else pd.DataFrame()

    cand = pd.concat([exact_id, exact_nm, time_nm, exact_ns, time_ns], ignore_index=True)
    if cand.empty:
        log.warning("No matches found between Bronze results and fixtures for season %s", season)

    if not cand.empty:
        cand = cand.sort_values(["row_id", "_prio"]).drop_duplicates(subset=["row_id"], keep="first")

    rows_in = len(br)
    rows_matched = 0 if cand.empty else len(cand)
    unique_fixtures = 0 if cand.empty else cand["game_key"].nunique()
    log.info("Mapping coverage: rows_in=%d, rows_matched=%d, unique_fixtures_covered=%d", rows_in, rows_matched, unique_fixtures)
    if rows_matched:
        log.info("Join modes: %s", cand["join_mode"].value_counts().to_dict())
    else:
        nm_path = debug_dir / f"results_unmatched_to_fixture_{season}.json"
        br[["season","round","home","away","home_norm","away_norm","home_slim","away_slim","start_time_utc"]].to_json(nm_path, orient="records", indent=2, date_format="iso")
        log.warning("0 matches. Wrote unmatched rows -> %s", nm_path.as_posix())
        log.warning("Tip: your f_fixture only has %d rows for %s; consider rebuilding fixtures for the season.", len(fx_df), season)
        return

    # Build f_result
    def _winner(hid: Optional[str], aid: Optional[str], hs, as_):
        if pd.isna(hs) or pd.isna(as_): return None
        if hs > as_: return str(hid) if hid is not None else None
        if as_ > hs: return str(aid) if aid is not None else None
        return None

    out = cand.copy()
    out["home_team_id"] = None  # IDs unknown in name joins; will get from fixture in a future pass if needed
    out["away_team_id"] = None
    out["margin"] = out["home_score"].astype("Int64") - out["away_score"].astype("Int64")
    out["winner_team_id"] = [ _winner(h, a, hs, as_) for h,a,hs,as_ in zip(out["home_team_id"], out["away_team_id"], out["home_score"], out["away_score"]) ]
    out["status"] = "final"
    out["payload_json"] = out["raw_payload"]

    keep_cols = [
        "game_key","season","round_str","venue_id",
        "home_team_id","away_team_id",
        "home_score","away_score","margin",
        "winner_team_id","status","source_kind","payload_json","join_mode"
    ]
    out = out.rename(columns={"round_str":"round"})[keep_cols].copy()

    # Write
    out_dir = silver_dir / "f_result.parquet"
    write_partitioned(out, out_dir, ["season","round"], overwrite=args.overwrite)
    log.info("Wrote Silver f_result to %s (partitioned by season/round)", out_dir.as_posix())
    if args.csv_mirror:
        write_csv_mirror(out, Path("silver_csv_mirror") / "f_result", ["season","round"], overwrite=args.overwrite)
    summary = {
        "season": season,
        "rows_in": rows_in,
        "rows_matched": rows_matched,
        "unique_fixtures": int(unique_fixtures),
        "join_modes": {k:int(v) for k,v in (out["join_mode"].value_counts().to_dict() if rows_matched else {}).items()},
        "output_partitions": out.groupby(["season","round"]).size().reset_index(name="rows").to_dict(orient="records"),
    }
    (Path(args.debug_dir) / f"f_result_build_summary_{season}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("Partition summary: %s", summary["output_partitions"])

if __name__ == "__main__":
    setup_logging("INFO")
    main()
