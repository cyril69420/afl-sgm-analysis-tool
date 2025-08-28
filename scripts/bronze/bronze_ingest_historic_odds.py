#!/usr/bin/env python3
"""
Bronze: ingest historical AFL odds for 2024 & 2025 from Australia Sports Betting (ASB) workbook.

Fixes in this version:
- Robust fixture join: handles timezone-aware Polars datetimes (e.g. Australia/Perth) without using strptime on non-strings.
- Correct logging formatter (uses %(asctime)s with datefmt).
- Same Medallion + deterministic writes.

ASB dataset reference (columns incl. Date/Home/Away, and 2013+ close prices for H2H/Line/Totals).  See site:
https://www.aussportsbetting.com/data/historical-afl-results-and-odds-data/
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb
import polars as pl

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
DEFAULT_BRONZE_DIR = REPO / "bronze"
DEFAULT_CSV_MIRROR_DIR = REPO / "bronze_csv_mirror"
CACHE_DIR = REPO / ".cache" / "asb"
ASB_URL = "https://www.aussportsbetting.com/historical_data/afl.xlsx"

# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze: ingest historic AFL odds (ASB workbook).")
    p.add_argument("--season", type=int, required=True, choices=[2024, 2025])
    p.add_argument("--round", type=str, default=None, help="Optional round filter (e.g. 'R12' or 'finals_w1').")
    p.add_argument("--bronze-dir", type=str, default=str(DEFAULT_BRONZE_DIR))
    p.add_argument("--csv-mirror", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit-games", type=int, default=None)
    p.add_argument("--log-level", type=str, default="INFO")
    return p.parse_args()

# ---------- logging ----------

def get_logger(name: str, level: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ))
        logger.addHandler(h)
    return logger

# ---------- utils ----------

def _duck() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _quote_list_for_duckdb(paths: Iterable[str]) -> str:
    esc = []
    for p in paths:
        esc.append("'" + str(p).replace("'", "''") + "'")
    return "[" + ", ".join(esc) + "]"

def _download_asb_workbook(path: Path, log: logging.Logger) -> Optional[Path]:
    import urllib.request
    try:
        _ensure_dir(path.parent)
        with urllib.request.urlopen(ASB_URL, timeout=90) as resp:
            path.write_bytes(resp.read())
        log.info("Downloaded ASB workbook → %s", path)
        return path
    except Exception as e:
        log.error("Failed to download ASB workbook: %s", e)
        return None

# ---------- Excel header autodetect ----------

def _read_excel_header_autodetect(xlsx_path: Path):
    import pandas as pd
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")

    def detect_on_sheet(name: str):
        raw = pd.read_excel(xlsx_path, sheet_name=name, header=None, engine="openpyxl")
        max_scan = min(80, len(raw))
        for i in range(max_scan):
            vals = [str(v).strip().lower() for v in raw.iloc[i].tolist()]
            if "date" in vals and any("home" in v for v in vals) and any("away" in v for v in vals):
                return pd.read_excel(xlsx_path, sheet_name=name, header=i, engine="openpyxl")
        return None

    df = detect_on_sheet(xl.sheet_names[0])
    if df is not None:
        return df
    for name in xl.sheet_names[1:]:
        df = detect_on_sheet(name)
        if df is not None:
            return df
    raise RuntimeError("Could not locate header row with Date/Home/Away columns in ASB workbook.")

# ---------- normalization helpers ----------

def _norm(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", str(s).strip().lower())
    return re.sub(r"_+", "_", s).strip("_")

TEAM_CANON = {
    "gws": "GWS Giants",
    "greater_western_sydney": "GWS Giants",
    "west_coast": "West Coast",
    "west_coast_eagles": "West Coast",
    "brisbane": "Brisbane Lions",
    "kangaroos": "North Melbourne",
    "bulldogs": "Western Bulldogs",
    "western_bulldogs": "Western Bulldogs",
    "st_kilda": "St Kilda",
}

def _canon_team(s: str) -> str:
    key = _norm(s)
    return TEAM_CANON.get(key, s.strip())

def _find_col(cols: List[str], *needles: str) -> Optional[str]:
    needles = tuple(_norm(x) for x in needles)
    for c in cols:
        cN = _norm(c)
        if all(n in cN for n in needles):
            return c
    return None

def _mk_hash(bookmaker: str, event_url: str, market_group: str, market_name: str,
             selection: str, line: Optional[float], dec: Optional[float]) -> str:
    s = "|".join([
        bookmaker or "", event_url or "", market_group or "", market_name or "",
        selection or "", "" if line is None else str(line), "" if dec is None else str(dec)
    ])
    return sha256(s.encode("utf-8")).hexdigest()

# ---------- parse ASB workbook ----------

def _parse_asb_rows(pdf, season: int, log: logging.Logger) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if pdf is None or pdf.empty:
        return rows

    cols = list(pdf.columns)
    c_date = _find_col(cols, "date")
    c_home = _find_col(cols, "home", "team") or _find_col(cols, "home")
    c_away = _find_col(cols, "away", "team") or _find_col(cols, "away")
    if not (c_date and c_home and c_away):
        log.error("ASB workbook missing expected columns (date/home/away). Found: %s", cols)
        return rows

    # H2H closing
    c_h2h_home_close = _find_col(cols, "h2h", "home", "closing") or _find_col(cols, "home", "closing", "odds") or _find_col(cols, "home", "odds")
    c_h2h_away_close = _find_col(cols, "h2h", "away", "closing") or _find_col(cols, "away", "closing", "odds") or _find_col(cols, "away", "odds")

    # Line closing
    c_line_pts_close = _find_col(cols, "line", "closing") or _find_col(cols, "handicap", "closing")
    c_line_home_close = _find_col(cols, "line", "home", "closing") or _find_col(cols, "home", "line", "closing")
    c_line_away_close = _find_col(cols, "line", "away", "closing") or _find_col(cols, "away", "line", "closing")

    # Totals closing
    c_tot_pts_close = _find_col(cols, "total", "closing") or _find_col(cols, "over_under", "closing")
    c_over_close = _find_col(cols, "over", "closing") or _find_col(cols, "over", "odds", "closing")
    c_under_close = _find_col(cols, "under", "closing") or _find_col(cols, "under", "odds", "closing")

    for _, r in pdf.iterrows():
        d = r.get(c_date)
        if d is None:
            continue
        if isinstance(d, dt.datetime):
            d_parsed = d
        else:
            d_parsed = None
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
                try:
                    d_parsed = dt.datetime.strptime(str(d), fmt)
                    break
                except Exception:
                    pass
            if d_parsed is None:
                try:
                    d_parsed = dt.datetime.fromisoformat(str(d))
                except Exception:
                    continue
        if d_parsed.year != season:
            continue

        home = _canon_team(str(r.get(c_home) or "").strip())
        away = _canon_team(str(r.get(c_away) or "").strip())
        if not home or not away:
            continue

        event_id = "asb:{season}:{home}_vs_{away}:{date}".format(
            season=season, home=home, away=away, date=d_parsed.date().isoformat()
        )
        captured = dt.datetime(d_parsed.year, d_parsed.month, d_parsed.day, 23, 59, 0, tzinfo=dt.timezone.utc).isoformat()

        # H2H close
        h_home = r.get(c_h2h_home_close) if c_h2h_home_close else None
        h_away = r.get(c_h2h_away_close) if c_h2h_away_close else None
        if h_home is not None and h_away is not None:
            for sel, price in (("Home", h_home), ("Away", h_away)):
                try:
                    dec = float(price)
                except Exception:
                    dec = None
                rows.append({
                    "captured_at_utc": captured,
                    "bookmaker": "market_consensus",
                    "event_url": event_id,
                    "market_group": "H2H",
                    "market_name": "Match",
                    "selection": sel,
                    "line": None,
                    "decimal_odds": dec,
                    "raw_payload": None,
                    "season": season,
                    "round": None,
                    "hash_key": _mk_hash("market_consensus", event_id, "H2H", "Match", sel, None, dec),
                    "source_kind": "asb_excel",
                })

        # Line close
        ln = r.get(c_line_pts_close) if c_line_pts_close else None
        lh = r.get(c_line_home_close) if c_line_home_close else None
        la = r.get(c_line_away_close) if c_line_away_close else None
        try:
            ln_val = float(ln) if ln is not None and str(ln) != "" else None
        except Exception:
            ln_val = None
        if ln_val is not None and (lh is not None or la is not None):
            if lh is not None:
                try:
                    dec = float(lh)
                except Exception:
                    dec = None
                rows.append({
                    "captured_at_utc": captured,
                    "bookmaker": "market_consensus",
                    "event_url": event_id,
                    "market_group": "Line",
                    "market_name": "Point Spread",
                    "selection": "Home",
                    "line": -ln_val,
                    "decimal_odds": dec,
                    "raw_payload": None,
                    "season": season,
                    "round": None,
                    "hash_key": _mk_hash("market_consensus", event_id, "Line", "Point Spread", "Home", -ln_val, dec),
                    "source_kind": "asb_excel",
                })
            if la is not None:
                try:
                    dec = float(la)
                except Exception:
                    dec = None
                rows.append({
                    "captured_at_utc": captured,
                    "bookmaker": "market_consensus",
                    "event_url": event_id,
                    "market_group": "Line",
                    "market_name": "Point Spread",
                    "selection": "Away",
                    "line": ln_val,
                    "decimal_odds": dec,
                    "raw_payload": None,
                    "season": season,
                    "round": None,
                    "hash_key": _mk_hash("market_consensus", event_id, "Line", "Point Spread", "Away", ln_val, dec),
                    "source_kind": "asb_excel",
                })

        # Totals close
        tp = r.get(c_tot_pts_close) if c_tot_pts_close else None
        to = r.get(c_over_close) if c_over_close else None
        tu = r.get(c_under_close) if c_under_close else None
        try:
            tp_val = float(tp) if tp is not None and str(tp) != "" else None
        except Exception:
            tp_val = None
        if tp_val is not None and (to is not None or tu is not None):
            if to is not None:
                try:
                    dec = float(to)
                except Exception:
                    dec = None
                rows.append({
                    "captured_at_utc": captured,
                    "bookmaker": "market_consensus",
                    "event_url": event_id,
                    "market_group": "Totals",
                    "market_name": "Game Total",
                    "selection": "Over",
                    "line": tp_val,
                    "decimal_odds": dec,
                    "raw_payload": None,
                    "season": season,
                    "round": None,
                    "hash_key": _mk_hash("market_consensus", event_id, "Totals", "Game Total", "Over", tp_val, dec),
                    "source_kind": "asb_excel",
                })
            if tu is not None:
                try:
                    dec = float(tu)
                except Exception:
                    dec = None
                rows.append({
                    "captured_at_utc": captured,
                    "bookmaker": "market_consensus",
                    "event_url": event_id,
                    "market_group": "Totals",
                    "market_name": "Game Total",
                    "selection": "Under",
                    "line": tp_val,
                    "decimal_odds": dec,
                    "raw_payload": None,
                    "season": season,
                    "round": None,
                    "hash_key": _mk_hash("market_consensus", event_id, "Totals", "Game Total", "Under", tp_val, dec),
                    "source_kind": "asb_excel",
                })

    return rows

# ---------- attach round from fixtures ----------

def _attach_round(rows: List[Dict[str, Any]], fixtures: pl.DataFrame, season: int) -> List[Dict[str, Any]]:
    if not rows or fixtures.is_empty():
        return rows

    # Your fixtures columns (per earlier dump):
    # ["game_key","home","away","venue","scheduled_time_utc","source_tz","discovered_utc","bookmaker_event_url","round","season"]

    base = fixtures.select([
        pl.col("season"),
        pl.col("round"),
        pl.col("home").alias("home_team"),
        pl.col("away").alias("away_team"),
        pl.col("scheduled_time_utc").alias("kick_ts"),
    ])

    # Try timezone-safe datetime path first; fallback to string parsing only if needed.
    try:
        fx = base.with_columns([
            # If kick_ts has a timezone (e.g., Australia/Perth), normalize to UTC and drop tz, then take the date.
            pl.col("kick_ts")
              .dt.convert_time_zone("UTC")     # works when tz-aware; no-op if naive
              .dt.replace_time_zone(None)
              .dt.date()
              .alias("kick_date")
        ])
    except Exception:
        # Fallback: cast to string then parse to datetime; then date.
        # Note: str.strptime expects String columns. (Polars docs) :contentReference[oaicite:2]{index=2}
        fx = base.with_columns([
            pl.col("kick_ts").cast(pl.Utf8).str.strptime(pl.Datetime, strict=False, exact=False).dt.date().alias("kick_date")
        ])

    fx = fx.select(["season", "round", "home_team", "away_team", "kick_date"])

    index: Dict[tuple, List[tuple]] = {}
    for r in fx.iter_rows(named=True):
        index.setdefault((int(r["season"]), r["home_team"], r["away_team"]), []).append((r["round"], r["kick_date"]))

    out: List[Dict[str, Any]] = []
    for row in rows:
        # event_url is "asb:{season}:{Home}_vs_{Away}:{YYYY-MM-DD}"
        try:
            parts = row["event_url"].split(":")
            teams = parts[2]
            match_date = parts[3]
            home, away = teams.split("_vs_")
        except Exception:
            out.append(row)
            continue

        candidates = index.get((season, home, away), [])
        if candidates:
            try:
                rdate = dt.date.fromisoformat(match_date)
            except Exception:
                rdate = None
            if rdate:
                best_round = min(
                    candidates,
                    key=lambda x: abs((x[1] - rdate).days) if x[1] else 9999
                )[0]
            else:
                best_round = candidates[0][0]
            row["round"] = best_round

        out.append(row)

    return out

# ---------- write helpers ----------

def _write_partitioned_parquet(df: pl.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = _duck()
    con.register("df", df.to_arrow())
    opts = "FORMAT PARQUET, PARTITION_BY (" + ", ".join(partition_cols) + ")"
    if overwrite:
        opts += ", OVERWRITE_OR_IGNORE"
    target = str(out_dir).replace("'", "''")
    con.execute("COPY df TO '" + target + "' (" + opts + ")")
    con.unregister("df")

# ---------- main ----------

def main() -> int:
    args = parse_args()
    log = get_logger("odds_history", args.log_level)

    bronze_dir = Path(args.bronze_dir)
    _ensure_dir(CACHE_DIR)
    xlsx_path = CACHE_DIR / "afl.xlsx"
    if not xlsx_path.exists():
        if not _download_asb_workbook(xlsx_path, log):
            log.error("Could not download ASB workbook; aborting.")
            return 2

    # Fixtures (for round join)
    fix_files = [p.as_posix() for p in (bronze_dir / "fixtures").rglob("*.parquet")]
    fixtures = pl.DataFrame([])
    if fix_files:
        con = _duck()
        file_list_sql = _quote_list_for_duckdb(fix_files)
        fpdf = con.execute("SELECT * FROM read_parquet(" + file_list_sql + ", hive_partitioning=1)").df()
        if not fpdf.empty:
            fixtures = pl.from_pandas(fpdf).filter(pl.col("season") == args.season)
            if args.round:
                fixtures = fixtures.filter(pl.col("round") == args.round)

    # Read workbook (no fastexcel; pandas+openpyxl)
    try:
        import pandas as pd  # noqa: F401
        import openpyxl  # noqa: F401
    except Exception as e:
        raise RuntimeError("Requires `pandas` and `openpyxl` (`pip install pandas openpyxl`).") from e

    pdf = _read_excel_header_autodetect(xlsx_path)
    rows = _parse_asb_rows(pdf, args.season, log)
    if args.limit_games:
        rows = rows[: args.limit_games]

    out_frames: List[pl.DataFrame] = []
    if rows:
        rows = _attach_round(rows, fixtures, args.season)
        hist = pl.DataFrame(rows).unique(subset=[
            "bookmaker","event_url","market_group","market_name","selection","line","captured_at_utc"
        ])
        out_frames.append(hist)
    else:
        log.warning("ASB workbook produced no rows for %s; will seed from live snapshots if available.", args.season)
        snap_files = [p.as_posix() for p in (bronze_dir / "odds" / "snapshots").rglob("*.parquet")]
        if snap_files:
            con = _duck()
            ssql = _quote_list_for_duckdb(snap_files)
            seed = con.execute(
                "SELECT captured_at_utc, bookmaker, event_url, market_group, market_name, "
                "selection, CAST(line AS DOUBLE) AS line, CAST(decimal_odds AS DOUBLE) AS decimal_odds, "
                "NULL AS raw_payload, CAST(season AS BIGINT) AS season, CAST(round AS VARCHAR) AS round, "
                "hash_key, 'seed_from_live' AS source_kind "
                "FROM read_parquet(" + ssql + ", hive_partitioning=1) "
                "WHERE season = " + str(args.season)
            ).df()
            if not seed.empty:
                out_frames.append(pl.from_pandas(seed).unique(subset=[
                    "bookmaker","event_url","market_group","market_name","selection","line","captured_at_utc"
                ]))

    if not out_frames:
        log.warning("No history rows to write.")
        return 0

    out = pl.concat(out_frames, how="vertical")

    if args.dry_run:
        log.info("DRY RUN: history rows=%d, cols=%s", out.height, out.columns)
        if out.height:
            log.info("Sample:\n%s", out.head(12))
        return 0

    out_dir = bronze_dir / "odds" / "history"
    _write_partitioned_parquet(out, out_dir, partition_cols=["season","round","bookmaker"], overwrite=args.overwrite)
    if args.csv_mirror:
        csv_dir = DEFAULT_CSV_MIRROR_DIR / "odds_history"
        _ensure_dir(csv_dir)
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out.write_csv(csv_dir / ("odds_history_" + ts + ".csv"))

    log.info("Wrote odds history → %s", out_dir)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
