#!/usr/bin/env python
"""
Build Silver core dims + facts from:
  • Bronze fixtures (upcoming)
  • Bronze results (historical finals)

Outputs under --silver-dir (default 'silver/'):
  - dim_team.parquet
  - dim_venue.parquet
  - f_game_upcoming.parquet      (upcoming/scheduled; partitioned by season/round)
  - f_fixture.parquet            (compat: upcoming-only with readable names; single file)
  - f_game_final.parquet         (historical/final results; partitioned by season/round)
  - f_result.parquet             (compat: same as f_game_final; partitioned)
  - v_game_all.parquet           (union of upcoming+final; partitioned)
"""

from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

# Deterministic IDs
from scripts.silver.utils import (
    team_id as util_team_id,
    venue_id as util_venue_id,
    sha1_key as util_sha1,
)

LOG = logging.getLogger("silver.core")


# -------------------- logging --------------------
def setup_logging(level: str) -> None:
    level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# -------------------- helpers --------------------
def _discover_parquet_files(root: Path, pattern: str) -> List[Path]:
    files = sorted(p.resolve() for p in root.glob(pattern) if p.is_file())
    return files

def _read_alias_csv(path: Path) -> Dict[str, str]:
    """Return {alias_lower -> canonical_team_name}"""
    if not path.exists():
        LOG.info("No team alias file found at %s (OK).", path.as_posix())
        return {}
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip().lower() for c in df.columns]
        need = {"alias", "team_name_canon"}
        if not need.issubset(df.columns):
            LOG.warning("team_aliases.csv missing columns %s; found %s", need, list(df.columns))
            return {}
        df["alias"] = df["alias"].astype(str).str.strip()
        df["team_name_canon"] = df["team_name_canon"].astype(str).str.strip()
        m = dict(zip(df["alias"].str.lower(), df["team_name_canon"]))
        LOG.info("Loaded %d team alias rows from %s", len(m), path.as_posix())
        return m
    except Exception as e:
        LOG.warning("Failed reading %s: %s", path.as_posix(), e)
        return {}

def _canon_team(name: Optional[str], alias_map: Dict[str, str]) -> Optional[str]:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None
    raw = str(name).strip()
    return alias_map.get(raw.lower(), raw)

def _canon_venue(name: Optional[str]) -> Optional[str]:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None
    return " ".join(str(name).split())

def _to_utc_iso(ts) -> Optional[str]:
    """Return ISO string in UTC with trailing Z (or None)."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    try:
        t = pd.to_datetime(ts, utc=True)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return t.isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def _make_game_key(
    season: Optional[int],
    round_str: str,
    home_team_id: Optional[str],
    away_team_id: Optional[str],
    tstamp_iso_utc: Optional[str],
    venue_id: Optional[str],
) -> str:
    """Stable across streams; finals-safe (round is a string)."""
    return util_sha1(
        [
            "match",
            "" if season is None else str(int(season)),
            str(round_str),
            home_team_id or "",
            away_team_id or "",
            tstamp_iso_utc or "",
            venue_id or "",
        ]
    )

def _write_partitioned(df: pd.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool) -> None:
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


# ---- column coalescing in pandas (safe even if some columns don't exist) ----
def coalesce_columns(df: pd.DataFrame, candidates: List[str], out_col: str) -> pd.Series:
    """Create df[out_col] as first non-null from the listed candidate columns that exist."""
    s = pd.Series([None] * len(df), dtype="object")
    for c in candidates:
        if c in df.columns:
            s = s.where(s.notna(), df[c])
    df[out_col] = s
    return df[out_col]

def parse_timestamp_utc(df: pd.DataFrame, candidates: List[str], out_col: str) -> pd.Series:
    s = pd.Series([pd.NaT] * len(df), dtype="datetime64[ns, UTC]")
    for c in candidates:
        if c in df.columns:
            v = pd.to_datetime(df[c], errors="coerce", utc=True)
            s = s.where(~s.isna(), v)
    df[out_col] = s
    return df[out_col]


# -------------------- main build --------------------
def build(
    silver_dir: Path,
    bronze_dir: Path,
    config_dir: Path,
    season: Optional[int],
    overwrite: bool,
    csv_mirror: bool,
) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8;")

    # ---------- Fixtures (upcoming) ----------
    fixture_files = _discover_parquet_files(bronze_dir / "fixtures", "**/*.parquet")
    LOG.info("Scanning Bronze fixtures from %s → %d file(s)",
             (bronze_dir / "fixtures").as_posix(), len(fixture_files))

    if fixture_files:
        files_sql = ", ".join(f"'{p.as_posix()}'" for p in fixture_files)
        # union_by_name TRUE to allow missing columns; hive_partitioning TRUE to pull partition keys if present
        fx_df = con.execute(
            f"SELECT * FROM read_parquet([{files_sql}], union_by_name=true, hive_partitioning=1)"
        ).fetchdf()
        LOG.info("Loaded fixtures raw rows: %d", len(fx_df))
        LOG.info("Fixture columns discovered: %s", sorted(fx_df.columns.tolist()))
    else:
        fx_df = pd.DataFrame()
        LOG.warning("No Bronze fixtures parquet files found under %s", (bronze_dir / "fixtures").as_posix())

    if season is not None and not fx_df.empty and "season" in fx_df.columns:
        before = len(fx_df)
        fx_df = fx_df.loc[fx_df["season"].astype("Int64") == season].copy()
        LOG.info("Filtered fixtures to season=%s: %d → %d rows", season, before, len(fx_df))

    if not fx_df.empty:
        # Coalesce expected columns safely
        coalesce_columns(fx_df, ["home_team", "home", "home_name"], "home_team")
        coalesce_columns(fx_df, ["away_team", "away", "away_name"], "away_team")
        coalesce_columns(fx_df, ["venue", "venue_name", "stadium", "ground"], "venue_name")
        parse_timestamp_utc(fx_df, ["scheduled_time_utc", "scheduled_utc", "kickoff_utc", "start_time_utc"], "scheduled_time_utc")
        # round/season
        if "round" in fx_df.columns:
            fx_df["round"] = fx_df["round"].astype("string")
        else:
            fx_df["round"] = pd.Series([None] * len(fx_df), dtype="string")
        if "season" in fx_df.columns:
            fx_df["season"] = pd.to_numeric(fx_df["season"], errors="coerce").astype("Int64")
        else:
            fx_df["season"] = pd.Series([pd.NA] * len(fx_df), dtype="Int64")

        # Canonicalise names
        alias_map = _read_alias_csv(config_dir / "team_aliases.csv")
        fx_df["home_team_canon"] = fx_df["home_team"].map(lambda s: _canon_team(s, alias_map))
        fx_df["away_team_canon"] = fx_df["away_team"].map(lambda s: _canon_team(s, alias_map))
        fx_df["venue_name_canon"] = fx_df["venue_name"].map(_canon_venue)
        LOG.info("Fixtures canonicalised. Nulls: home=%d away=%d venue=%d",
                 int(fx_df["home_team_canon"].isna().sum()),
                 int(fx_df["away_team_canon"].isna().sum()),
                 int(fx_df["venue_name_canon"].isna().sum()))
        LOG.info("Fixture rounds present (top 10): %s",
                 fx_df["round"].value_counts().head(10).to_dict())
        LOG.info("Fixture sample:\n%s", fx_df[["season","round","home_team","away_team","venue_name","scheduled_time_utc"]].head(5).to_string(index=False))
    else:
        alias_map = _read_alias_csv(config_dir / "team_aliases.csv")
        fx_df = pd.DataFrame(columns=["home_team_canon","away_team_canon","venue_name_canon","scheduled_time_utc","season","round"])

    # ---------- Results (historical) ----------
    results_root = bronze_dir / "results"
    if season is not None:
        rs_files = _discover_parquet_files(results_root, f"season={season}/**/*.parquet")
    else:
        rs_files = _discover_parquet_files(results_root, "**/*.parquet")

    LOG.info("Scanning Bronze results from %s → %d file(s)", results_root.as_posix(), len(rs_files))

    if rs_files:
        files_sql = ", ".join(f"'{p.as_posix()}'" for p in rs_files)
        rs_df = con.execute(
            f"SELECT * FROM read_parquet([{files_sql}], union_by_name=true, hive_partitioning=1)"
        ).fetchdf()
        LOG.info("Loaded results raw rows: %d", len(rs_df))
        LOG.info("Results columns discovered: %s", sorted(rs_df.columns.tolist()))
    else:
        rs_df = pd.DataFrame()
        LOG.warning("No Bronze results parquet files found. Did you run bronze_ingest_results.py?")

    if season is not None and not rs_df.empty and "season" in rs_df.columns:
        before = len(rs_df)
        rs_df = rs_df.loc[pd.to_numeric(rs_df["season"], errors="coerce").astype("Int64") == season].copy()
        LOG.info("Filtered results to season=%s: %d → %d rows", season, before, len(rs_df))

    if not rs_df.empty:
        # Coalesce/rename expected fields
        # Ensure required columns exist even if empty
        for col in ["home","away","venue","start_time_utc","round","source_kind","raw_payload",
                    "home_score","away_score","home_goals","home_behinds","away_goals","away_behinds"]:
            if col not in rs_df.columns:
                rs_df[col] = pd.NA

        # Canonicalise
        rs_df["round"] = rs_df["round"].astype("string")
        rs_df["season"] = pd.to_numeric(rs_df["season"], errors="coerce").astype("Int64")
        rs_df["start_time_utc"] = pd.to_datetime(rs_df["start_time_utc"], errors="coerce", utc=True)
        rs_df["home_team_canon"] = rs_df["home"].map(lambda s: _canon_team(s, alias_map))
        rs_df["away_team_canon"] = rs_df["away"].map(lambda s: _canon_team(s, alias_map))
        rs_df["venue_name_canon"] = rs_df["venue"].map(_canon_venue)

        LOG.info("Results canonicalised. Nulls: home=%d away=%d venue=%d",
                 int(rs_df["home_team_canon"].isna().sum()),
                 int(rs_df["away_team_canon"].isna().sum()),
                 int(rs_df["venue_name_canon"].isna().sum()))
        LOG.info("Results sample:\n%s", rs_df[["season","round","home","away","venue","start_time_utc","home_score","away_score"]].head(5).to_string(index=False))
    else:
        rs_df = pd.DataFrame(columns=[
            "home_team_canon","away_team_canon","venue_name_canon","start_time_utc",
            "season","round","home_score","away_score","home_goals","home_behinds","away_goals","away_behinds",
            "source_kind","raw_payload"
        ])

    # ---------- Build dims (union of names seen) ----------
    team_names = set(fx_df["home_team_canon"].dropna()) | set(fx_df["away_team_canon"].dropna()) | \
                 set(rs_df["home_team_canon"].dropna())  | set(rs_df["away_team_canon"].dropna())
    venue_names = set(fx_df["venue_name_canon"].dropna()) | set(rs_df["venue_name_canon"].dropna())

    dim_team = pd.DataFrame(sorted(team_names), columns=["team_name"])
    dim_team["team_id"] = dim_team["team_name"].map(util_team_id)
    dim_venue = pd.DataFrame(sorted(venue_names), columns=["venue_name"])
    dim_venue["venue_id"] = dim_venue["venue_name"].map(util_venue_id)

    silver_dir.mkdir(parents=True, exist_ok=True)
    dim_team[["team_id","team_name"]].to_parquet(silver_dir / "dim_team.parquet", index=False)
    dim_venue[["venue_id","venue_name"]].to_parquet(silver_dir / "dim_venue.parquet", index=False)
    LOG.info("Wrote dims → %s, %s", (silver_dir / "dim_team.parquet").as_posix(), (silver_dir / "dim_venue.parquet").as_posix())
    LOG.info("dim_team size=%d | dim_venue size=%d", len(dim_team), len(dim_venue))

    # ---------- UPCOMING stream → f_game_upcoming + f_fixture ----------
    if not fx_df.empty:
        fx = fx_df.copy()

        # Attach IDs
        fx = fx.merge(dim_team, left_on="home_team_canon", right_on="team_name", how="left").rename(columns={"team_id":"home_team_id"}).drop(columns=["team_name"])
        fx = fx.merge(dim_team, left_on="away_team_canon", right_on="team_name", how="left").rename(columns={"team_id":"away_team_id"}).drop(columns=["team_name"])
        fx = fx.merge(dim_venue, left_on="venue_name_canon", right_on="venue_name",  how="left")

        # Coverage debug
        miss = fx["home_team_id"].isna() | fx["away_team_id"].isna()
        if miss.any():
            LOG.warning("Upcoming: %d rows with unmapped team IDs (showing up to 10):\n%s",
                        int(miss.sum()),
                        fx.loc[miss, ["season","round","home_team","away_team","home_team_canon","away_team_canon"]]
                          .head(10).to_string(index=False))

        # Keys
        ts_iso = fx["scheduled_time_utc"].map(_to_utc_iso)
        fx["game_key"] = [
            _make_game_key(s, r, h, a, t, v)
            for s, r, h, a, t, v in zip(fx["season"], fx["round"], fx["home_team_id"], fx["away_team_id"], ts_iso, fx["venue_id"])
        ]

        f_game_upcoming = fx[[
            "game_key","season","round","venue_id","home_team_id","away_team_id","scheduled_time_utc"
        ]].copy()
        f_game_upcoming["status"] = "scheduled"
        f_game_upcoming["source_kind"] = "fixtures"
        f_game_upcoming["payload_json"] = None

        _write_partitioned(f_game_upcoming, silver_dir / "f_game_upcoming.parquet", ["season","round"], overwrite)
        LOG.info("Wrote upcoming → %s (rows=%d)", (silver_dir / "f_game_upcoming.parquet").as_posix(), len(f_game_upcoming))

        # Back-compat f_fixture (single file)
        f_fixture = fx[[
            "game_key","season","round","home_team_id","away_team_id","venue_id",
            "scheduled_time_utc","home_team_canon","away_team_canon","venue_name_canon"
        ]].rename(columns={
            "home_team_canon":"home_team_name",
            "away_team_canon":"away_team_name",
            "venue_name_canon":"venue_name",
        }).sort_values(["season","round","scheduled_time_utc","game_key"])
        f_fixture.to_parquet(silver_dir / "f_fixture.parquet", index=False)
        LOG.info("Wrote compat fixture → %s (rows=%d)", (silver_dir / "f_fixture.parquet").as_posix(), len(f_fixture))

        if csv_mirror:
            out_csv = Path("silver_csv_mirror") / "f_game_upcoming"
            for (s, r), g in f_game_upcoming.groupby(["season","round"]):
                p = out_csv / f"season={s}" / f"round={r}" / "part.csv"
                p.parent.mkdir(parents=True, exist_ok=True)
                g.to_csv(p, index=False)
            LOG.info("CSV mirror → %s", out_csv.as_posix())
    else:
        LOG.warning("Upcoming stream: nothing to write (no fixtures rows).")

    # ---------- FINAL stream → f_game_final + f_result ----------
    if not rs_df.empty:
        rs = rs_df.copy()

        rs = rs.merge(dim_team, left_on="home_team_canon", right_on="team_name", how="left").rename(columns={"team_id":"home_team_id"}).drop(columns=["team_name"])
        rs = rs.merge(dim_team, left_on="away_team_canon", right_on="team_name", how="left").rename(columns={"team_id":"away_team_id"}).drop(columns=["team_name"])
        rs = rs.merge(dim_venue, left_on="venue_name_canon", right_on="venue_name",  how="left")

        missf = rs["home_team_id"].isna() | rs["away_team_id"].isna()
        if missf.any():
            LOG.warning("Finals: %d rows with unmapped team IDs (showing up to 10):\n%s",
                        int(missf.sum()),
                        rs.loc[missf, ["season","round","home","away","home_team_canon","away_team_canon"]]
                          .head(10).to_string(index=False))

        # numeric scores + derived
        rs["home_score"] = pd.to_numeric(rs.get("home_score"), errors="coerce").astype("Int64")
        rs["away_score"] = pd.to_numeric(rs.get("away_score"), errors="coerce").astype("Int64")
        rs["margin"] = rs["home_score"] - rs["away_score"]

        def _winner(hid, aid, hs, as_):
            if pd.isna(hs) or pd.isna(as_): return None
            if hs > as_: return hid
            if as_ > hs: return aid
            return None

        rs["winner_team_id"] = [
            _winner(h, a, hs, as_) for h,a,hs,as_ in zip(rs["home_team_id"], rs["away_team_id"], rs["home_score"], rs["away_score"])
        ]

        # Keys
        ts_iso_res = rs["start_time_utc"].map(_to_utc_iso)
        rs["game_key"] = [
            _make_game_key(s, r, h, a, t, v)
            for s, r, h, a, t, v in zip(rs["season"], rs["round"], rs["home_team_id"], rs["away_team_id"], ts_iso_res, rs["venue_id"])
        ]

        keep_cols = [
            "game_key","season","round","venue_id",
            "home_team_id","away_team_id",
            "home_score","away_score","margin","winner_team_id","start_time_utc",
        ]
        for extra in ["home_goals","home_behinds","away_goals","away_behinds"]:
            if extra in rs.columns: keep_cols.append(extra)

        f_game_final = rs[keep_cols].copy()
        f_game_final["status"] = "final"
        f_game_final["source_kind"] = rs.get("source_kind", "squiggle")
        f_game_final["payload_json"] = rs.get("raw_payload")

        _write_partitioned(f_game_final, silver_dir / "f_game_final.parquet", ["season","round"], overwrite)
        LOG.info("Wrote finals → %s (rows=%d)", (silver_dir / "f_game_final.parquet").as_posix(), len(f_game_final))

        # compat mirror
        _write_partitioned(f_game_final, silver_dir / "f_result.parquet", ["season","round"], overwrite)
        LOG.info("Wrote compat result → %s (rows=%d)", (silver_dir / "f_result.parquet").as_posix(), len(f_game_final))

        if csv_mirror:
            out_csv = Path("silver_csv_mirror") / "f_game_final"
            for (s, r), g in f_game_final.groupby(["season","round"]):
                p = out_csv / f"season={s}" / f"round={r}" / "part.csv"
                p.parent.mkdir(parents=True, exist_ok=True)
                g.to_csv(p, index=False)
            LOG.info("CSV mirror → %s", out_csv.as_posix())
    else:
        LOG.warning("Final stream: nothing to write (no results rows).")

    # ---------- Union convenience → v_game_all ----------
    def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        out = df.copy()
        for c in cols:
            if c not in out.columns:
                out[c] = pd.NA
        return out[cols]

    common_cols = [
        "game_key","season","round","venue_id","home_team_id","away_team_id",
        "scheduled_time_utc","start_time_utc","home_score","away_score","margin","winner_team_id",
        "status","source_kind","payload_json"
    ]

    # Load written parts (robust even if in-memory dfs are empty)
    up_paths = list((silver_dir / "f_game_upcoming.parquet").glob("**/*.parquet"))
    fn_paths = list((silver_dir / "f_game_final.parquet").glob("**/*.parquet"))

    up_df = pd.DataFrame()
    fn_df = pd.DataFrame()
    if up_paths:
        files_sql = ", ".join(f"'{p.as_posix()}'" for p in up_paths)
        up_df = duckdb.connect().execute(f"SELECT * FROM read_parquet([{files_sql}])").fetchdf()
    if fn_paths:
        files_sql = ", ".join(f"'{p.as_posix()}'" for p in fn_paths)
        fn_df = duckdb.connect().execute(f"SELECT * FROM read_parquet([{files_sql}])").fetchdf()

    up_df = _ensure_cols(up_df, common_cols)
    fn_df = _ensure_cols(fn_df, common_cols)

    v_game_all = pd.concat([up_df, fn_df], ignore_index=True)
    if not v_game_all.empty:
        _write_partitioned(v_game_all, silver_dir / "v_game_all.parquet", ["season","round"], overwrite)
        LOG.info("Wrote union → %s (rows=%d)", (silver_dir / "v_game_all.parquet").as_posix(), len(v_game_all))
    else:
        LOG.warning("v_game_all: nothing to write (no upcoming and no final rows).")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", type=Path, default=Path("bronze"))
    ap.add_argument("--silver-dir", type=Path, default=Path("silver"))
    ap.add_argument("--config-dir", type=Path, default=Path("config"))
    ap.add_argument("--season", type=int, default=None, help="Optional filter for a single season")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--csv-mirror", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    setup_logging(args.log_level)
    LOG.info("Starting Silver core build… season=%s overwrite=%s csv-mirror=%s", args.season, args.overwrite, args.csv_mirror)
    build(args.silver_dir, args.bronze_dir, args.config_dir, args.season, args.overwrite, args.csv_mirror)
    LOG.info("Silver core build complete.")

if __name__ == "__main__":
    main()
