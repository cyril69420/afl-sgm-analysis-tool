# scripts/bronze/bronze_ingest_results.py
# Purpose: Fetch completed match results and write Parquet (Hive partitioned by default).
# Sources:
#   - Default: Squiggle (?q=games;year=YYYY;complete=100) + teams map (?q=teams)
#   - Optional: AFL Tables via R/fitzRoy for deep backfill (requires Rscript & fitzRoy)
# CLI:
#   --season INT (required)
#   --limit INT
#   --overwrite
#   --csv-mirror
#   --single-file               # NEW: write one Parquet per season instead of Hive partitions
#   --log-level {DEBUG,INFO,WARNING,ERROR}
#   --debug-dump-dir PATH
#   --source {squiggle,afltables}

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import requests
from dateutil import parser as dtparse
from dateutil import tz

# ------------------------
# Logging
# ------------------------
def setup_logging(level: str) -> None:
    level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

log = logging.getLogger("bronze.results")

# ------------------------
# IO Helpers
# ------------------------
def write_partitioned_parquet(
    df: pd.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("df", df)
    part_cols = ", ".join(partition_cols)
    sql = f"""
        COPY df TO '{out_dir.as_posix()}'
        (FORMAT 'parquet',
         PARTITION_BY ({part_cols}),
         OVERWRITE {str(overwrite).lower()},
         WRITE_PARTITION_COLUMNS false)
    """
    con.execute(sql)

def write_single_parquet(df: pd.DataFrame, out_path: Path, overwrite: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("df", df)
    sql = f"""
        COPY df TO '{out_path.as_posix()}'
        (FORMAT 'parquet',
         OVERWRITE {str(overwrite).lower()},
         WRITE_PARTITION_COLUMNS false)
    """
    con.execute(sql)

def write_csv_mirror_partitioned(
    df: pd.DataFrame, csv_root: Path, partition_cols: List[str], overwrite: bool
) -> None:
    for key_vals, g in df.groupby(partition_cols):
        if not isinstance(key_vals, tuple):
            key_vals = (key_vals,)
        subdir = csv_root.joinpath(*[f"{c}={v}" for c, v in zip(partition_cols, key_vals)])
        subdir.mkdir(parents=True, exist_ok=True)
        p = subdir / "part.csv"
        if p.exists() and not overwrite:
            continue
        g.to_csv(p, index=False)

def write_csv_single(df: pd.DataFrame, out_path: Path, overwrite: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not overwrite:
        return
    df.to_csv(out_path, index=False)

# ------------------------
# HTTP / parsing
# ------------------------
SQUIGGLE_API = "https://api.squiggle.com.au/"

def _http_json(url: str, timeout: float = 25.0) -> Dict[str, Any]:
    t0 = time.time()
    r = requests.get(url, headers={"User-Agent": "afl-sgm/bronze-results"}, timeout=timeout)
    ms = (time.time() - t0) * 1000.0
    log.info("HTTP %s %s %.0f ms", r.status_code, url, ms)
    r.raise_for_status()
    return r.json()

def parse_iso_utc(s: Optional[str], tz_offset: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    dt = dtparse.isoparse(s)
    if dt.tzinfo is None:
        if tz_offset:
            try:
                dt = dt.replace(tzinfo=tz.gettz(tz_offset))
            except Exception:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _to_int_or_none(v) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None

def fetch_squiggle_team_map(year: int) -> Dict[int, str]:
    url = f"{SQUIGGLE_API}?q=teams;year={year};format=json"
    js = _http_json(url)
    teams = js.get("teams") or js.get("team") or []
    id_to_name: Dict[int, str] = {}
    for t in teams:
        tid = t.get("id") or t.get("teamid") or t.get("team")
        try:
            tid = int(tid)
        except (TypeError, ValueError):
            continue
        name = t.get("name") or t.get("teamname") or t.get("abbr") or str(tid)
        id_to_name[tid] = str(name)
    if not id_to_name:
        log.warning("Squiggle teams mapping empty; will fall back to raw names.")
    return id_to_name

def fetch_squiggle_results(year: int, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    url = f"{SQUIGGLE_API}?q=games;year={year};complete=100;format=json"
    js = _http_json(url)
    games = js.get("games", [])
    if limit is not None:
        games = games[: int(limit)]
    return games

def _resolve_team_name(game: Dict[str, Any], side: str, id_to_name: Dict[int, str]) -> str:
    """
    side: 'h' or 'a'
    Prefer numeric IDs (hteamid/ateamid) -> canonical map; fallback to string fields.
    """
    id_fields = [f"{side}teamid", f"{side}_team_id"]
    name_fields = [f"{side}teamname", f"{side}team", f"{side}_team_name", f"{side}_team"]

    for f in id_fields:
        v = game.get(f)
        if v is None:
            continue
        try:
            tid = int(v)
            return id_to_name.get(tid, str(tid))
        except (TypeError, ValueError):
            pass

    for f in name_fields:
        v = game.get(f)
        if v:
            return str(v)

    return ""

def bronze_rows_from_squiggle(year: int, limit: Optional[int]) -> pd.DataFrame:
    id_to_name = fetch_squiggle_team_map(year)
    games = fetch_squiggle_results(year, limit=limit)

    rows: List[Dict[str, Any]] = []
    for g in games:
        gid = g.get("id") or g.get("gameid")
        year_ = _to_int_or_none(g.get("year")) or year
        round_ = g.get("round")
        tz_off = g.get("tz") or ""
        start_utc = parse_iso_utc(g.get("date"), tz_off)

        home = _resolve_team_name(g, "h", id_to_name)
        away = _resolve_team_name(g, "a", id_to_name)

        # Scores + NEW detail
        home_score = _to_int_or_none(g.get("hscore"))
        away_score = _to_int_or_none(g.get("ascore"))
        home_goals = _to_int_or_none(g.get("hgoals"))
        home_behinds = _to_int_or_none(g.get("hbehinds"))
        away_goals = _to_int_or_none(g.get("agoals"))
        away_behinds = _to_int_or_none(g.get("abehinds"))

        rows.append(
            {
                "season": int(year_),
                "round": str(round_) if round_ is not None else None,
                "home": home,
                "away": away,
                "home_score": home_score,
                "away_score": away_score,
                "home_goals": home_goals,
                "home_behinds": home_behinds,
                "away_goals": away_goals,
                "away_behinds": away_behinds,
                "venue": g.get("venue"),
                "start_time_utc": start_utc,
                "match_id_source": str(gid) if gid is not None else None,
                "source_kind": "squiggle",
                "raw_payload": json.dumps(g, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)

    # Light coercions (canonicalisation happens in Silver)
    for col in ["home", "away", "venue", "round"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Debug
    if not df.empty:
        counts = df.groupby(["season", "round"], dropna=False).size().reset_index(name="rows")
        log.info("Counts per (season,round):\n%s", counts.head(50).to_string(index=False))
        pct_missing = (df["home_score"].isna() | df["away_score"].isna()).mean() * 100.0
        log.info("%% rows missing a score: %.2f%%", pct_missing)
        log.info("Sample first rows:\n%s", df.head(3).to_string(index=False))
        log.info("Sample last rows:\n%s", df.tail(3).to_string(index=False))

    return df

# ------------------------
# AFL Tables via fitzRoy (optional)
# ------------------------
def bronze_rows_from_afltables(year: int, limit: Optional[int]) -> pd.DataFrame:
    import shutil
    from subprocess import PIPE, CalledProcessError, run

    if shutil.which("Rscript") is None:
        raise RuntimeError("Rscript not found in PATH. Install R + fitzRoy, or use --source squiggle.")

    r_code = f"""
    suppressMessages(library(fitzRoy))
    suppressMessages(library(jsonlite))
    df <- fetch_results_afltables(season={year}, comp="AFLM")
    if (!is.null({str(limit is not None).lower()})) {{
      df <- head(df, {int(limit or 0)})
    }}
    cat(toJSON(df, dataframe="rows", na="null", auto_unbox=TRUE))
    """
    try:
        t0 = time.time()
        p = run(["Rscript", "-e", r_code], check=True, stdout=PIPE, stderr=PIPE, text=True)
        ms = (time.time() - t0) * 1000.0
        log.info("Rscript fitzRoy latency: %.0f ms", ms)
        data = json.loads(p.stdout)
    except CalledProcessError as e:
        log.error("Rscript error: %s", e.stderr)
        raise

    rows: List[Dict[str, Any]] = []
    for g in data:
        dt_raw = g.get("Date")
        start_utc: Optional[datetime] = None
        if dt_raw:
            try:
                dt = dtparse.parse(dt_raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz.gettz("Australia/Melbourne"))
                start_utc = dt.astimezone(timezone.utc)
            except Exception:
                start_utc = None

        rows.append(
            {
                "season": _to_int_or_none(g.get("Season")),
                "round": str(g.get("Round")) if g.get("Round") is not None else None,
                "home": g.get("Home.Team"),
                "away": g.get("Away.Team"),
                "home_score": _to_int_or_none(g.get("Home.Points")),
                "away_score": _to_int_or_none(g.get("Away.Points")),
                # fitzRoy rows may not include goals/behinds split; keeping scores only for this source
                "home_goals": None,
                "home_behinds": None,
                "away_goals": None,
                "away_behinds": None,
                "venue": g.get("Venue"),
                "start_time_utc": start_utc,
                "match_id_source": str(g.get("Match.Id") or ""),
                "source_kind": "afltables",
                "raw_payload": json.dumps(g, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        counts = df.groupby(["season", "round"], dropna=False).size().reset_index(name="rows")
        log.info("AFL Tables rows: %d | Counts per (season,round):\n%s", len(df), counts.head(50).to_string(index=False))
    return df

# ------------------------
# Main
# ------------------------
def main():
    ap = argparse.ArgumentParser(description="Bronze: ingest completed match results")
    ap.add_argument("--season", type=int, required=True, help="Season (year)")
    ap.add_argument("--limit", type=int, default=None, help="Cap rows for quick tests")
    ap.add_argument("--overwrite", action="store_true", help="Allow overwrite of outputs")
    ap.add_argument("--csv-mirror", action="store_true", help="Write CSV mirror (partitioned by default)")
    ap.add_argument("--single-file", action="store_true", help="Write a single Parquet per season instead of Hive partitions")
    ap.add_argument("--log-level", default="INFO", help="Logging level")
    ap.add_argument("--debug-dump-dir", default=".debug_afl", help="Directory for small debug dumps")
    ap.add_argument("--source", default="squiggle", choices=["squiggle", "afltables"], help="Results source")
    args = ap.parse_args()

    setup_logging(args.log_level)

    # Read
    if args.source == "squiggle":
        df = bronze_rows_from_squiggle(args.season, args.limit)
    else:
        df = bronze_rows_from_afltables(args.season, args.limit)

    if df.empty:
        log.warning("No rows to write for season=%s", args.season)
        return

    # Paths
    bronze_root = Path("bronze") / "results"
    csv_root = Path("bronze_csv_mirror") / "results"
    debug_dir = Path(args.debug_dump_dir)
    debug_dir.mkdir(parents=True, exist_ok=True)

    if args.single_file:
        # One Parquet per season
        out_parquet = bronze_root / f"season={args.season}" / "results.parquet"
        write_single_parquet(df, out_parquet, overwrite=args.overwrite)
        log.info("Wrote single parquet -> %s", out_parquet.as_posix())

        if args.csv_mirror:
            out_csv = csv_root / f"season={args.season}" / "results.csv"
            write_csv_single(df, out_csv, overwrite=args.overwrite)
            log.info("Wrote single CSV -> %s", out_csv.as_posix())
    else:
        # Hive-partitioned by (season, round)
        write_partitioned_parquet(df, bronze_root, ["season", "round"], overwrite=args.overwrite)
        log.info("Wrote partitioned parquet under %s (by season/round)", bronze_root.as_posix())
        if args.csv_mirror:
            write_csv_mirror_partitioned(df, csv_root, ["season", "round"], overwrite=args.overwrite)
            log.info("Wrote partitioned CSV mirror under %s (by season/round)", csv_root.as_posix())

    # Tiny debug sample of first 10 rows
    sample_path = debug_dir / f"results_{args.season}_sample.json"
    sample = df.head(10).to_dict(orient="records")
    sample_path.write_text(json.dumps(sample, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log.info("Debug sample -> %s", sample_path.as_posix())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Fatal: %s", e)
        sys.exit(2)
