# -*- coding: utf-8 -*-
r"""
bronze_backfill_odds_oddsportal.py — AFL 2020+ backfill via oddsportal full_scraper (non‑interactive)

Fixes
- Prevents Windows path escapes breaking the module docstring by using a *raw* string.
- Drives upstream `op.py` non‑interactively (pipes the AFL selection) and enforces a timeout.
- Finds `AFL.json` **recursively** under `output/**` (the scraper saves to per‑sport subfolders).

What it does
- Edits `config/sports.json` to enable AFL only and set the year window.
- Runs `op.py` with sensible defaults and feeds the AFL index via stdin.
- Parses the generated JSON to Bronze long‑form odds (H2H/line/total) for seasons ≥ 2020 by default.

Env
  ODDSPORTAL_ROOT       Path to the scraper repo root (the `full_scraper` directory).
  CHROMEDRIVER         Optional path to chromedriver (its parent folder is added to PATH).
  ODDSPORTAL_TIMEOUT_S Optional whole‑process timeout in seconds (default 1800).

Usage
  export ODDSPORTAL_ROOT="/c/Users/Ethan/odds-portal-scraper/full_scraper"
  python scripts/bronze_backfill_odds_oddsportal.py --start 2020 --cpus 1 --wait 5

Output
  bronze/historical_odds/snapshot_ts=YYYYmmddTHHMMSSZ/historical_odds_oddsportal.csv
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

THIS = Path(__file__).resolve()
ROOT = THIS.parents[1]
BRONZE = ROOT / "bronze"
OUT_ROOT = BRONZE / "historical_odds"

# ---------------- helpers ----------------

def _find_repo_root() -> Path:
    env = os.getenv("ODDSPORTAL_ROOT", "").strip()
    if env:
        p = Path(env)
        if p.exists():
            return p
    # common hints
    hints = [
        Path.home() / "odds-portal-scraper" / "full_scraper",
        Path.home() / "oddsporter",
        (ROOT.parent / "odds-portal-scraper" / "full_scraper").resolve(),
        (ROOT.parent / "oddsporter").resolve(),
    ]
    for h in hints:
        if h.exists():
            return h
    raise SystemExit("[error] ODDSPORTAL_ROOT not set and repo not found — clone odds-portal-scraper/full_scraper and set ODDSPORTAL_ROOT")


def _sports_json_path(repo: Path) -> Path:
    return repo / "config" / "sports.json"


def _op_script(repo: Path) -> Path:
    return repo / "op.py"


def _is_afl_entry(obj: Dict) -> bool:
    text = json.dumps(obj).lower()
    return ("afl" in text) or ("australian" in text and "football" in text)


def _toggle_afl_only_and_get_index(sports_json: Path, start: int, end: int) -> int:
    """Enable AFL only, set year window where fields exist, and return the 1‑based
    index that `op.py` will prompt for (so we can auto‑select it)."""
    try:
        raw = sports_json.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception as e:
        raise SystemExit(f"[error] failed to read {sports_json}: {e}")

    items: List[Dict] = data if isinstance(data, list) else data.get("sports", [])
    if not isinstance(items, list) or not items:
        raise SystemExit(f"[error] {sports_json} has no sports entries")

    afl_positions: List[int] = []
    for i, item in enumerate(items):
        is_afl = _is_afl_entry(item)
        for key in ("enabled","active","scrape","selected"):
            if key in item:
                item[key] = bool(is_afl)
        for yrkey in ("startYear","endYear","yearStart","yearEnd","seasonStart","seasonEnd"):
            if yrkey in item:
                item[yrkey] = int(start if 'start' in yrkey.lower() else end)
        if is_afl:
            afl_positions.append(i)

    if not afl_positions:
        raise SystemExit("[error] could not find an AFL entry in sports.json — please add/enable AFL")

    # Persist modifications
    new_blob = items if isinstance(data, list) else {"sports": items}
    sports_json.write_text(json.dumps(new_blob, indent=2), encoding="utf-8")

    # `op.py` prints menu as 1..N in the current list order
    return afl_positions[0] + 1


def _run_scraper(repo: Path, selection_idx: int, timeout_s: int, num_cpus: int = 1, wait_on_load: int = 5) -> None:
    py = os.environ.get("PYTHON", sys.executable)
    op = _op_script(repo)
    if not op.exists():
        raise SystemExit(f"[error] cannot find op.py at {op}")

    env = os.environ.copy()
    if os.getenv("CHROMEDRIVER"):
        env["PATH"] = f"{Path(os.getenv('CHROMEDRIVER')).parent};{env['PATH']}"

    cmd = [py, str(op), "--number-of-cpus", str(num_cpus), "--wait-time-on-page-load", str(wait_on_load)]

    proc = subprocess.Popen(
        cmd, cwd=str(repo),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1, env=env,
    )
    try:
        if proc.stdin:
            proc.stdin.write(f"{selection_idx}\n")   # ← this line was broken
            proc.stdin.flush()
        start = time.time()
        while True:
            if proc.poll() is not None:
                break
            if proc.stdout:
                line = proc.stdout.readline()
                if line:
                    print(line.rstrip())
            if proc.stderr:
                err = proc.stderr.readline()
                if err:
                    print(err.rstrip())
            if time.time() - start > timeout_s:
                proc.kill()
                raise SystemExit(f"[error] oddsportal scraper timed out after {timeout_s}s — try a smaller year window or increase ODDSPORTAL_TIMEOUT_S")
            time.sleep(0.05)
    finally:
        try:
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass

    if proc.returncode != 0:
        out = proc.stdout.read() if proc.stdout else ""
        err = proc.stderr.read() if proc.stderr else ""
        raise SystemExit(f"[error] oddsportal scraper failed (exit {proc.returncode})\n--- STDOUT ---\n{out}\n--- STDERR ---\n{err}")


def _find_output_json(repo: Path) -> Path:
    out_dir = repo / "output"
    if not out_dir.exists():
        raise SystemExit(f"[error] no output directory at {out_dir}")
    # canonical name
    cands = list(out_dir.rglob("AFL.json"))
    if not cands:
        # fallback: any json with 'afl' in filename
        cands = [p for p in out_dir.rglob("*.json") if "afl" in p.name.lower()]
    if not cands:
        raise SystemExit(f"[error] AFL JSON not found under {out_dir}; open logs/ for details and ensure AFL was enabled")
    cands.sort(key=lambda p: len(str(p)))
    return cands[0]


# ---------------- parsing ----------------

def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace("$", "").strip())
    except Exception:
        return None


def _emit_row(rows: List[Dict[str,object]], meta: Dict[str,object], market: str, odds_type: str, side: str, price, line=None):
    fv = _to_float(price)
    if fv is None:
        return
    rows.append({
        **meta,
        "market_code": market,
        "odds_type": "closing" if odds_type == "close" else odds_type,
        "selection_name": side,
        "price_decimal": fv,
        "line": _to_float(line) if line is not None else None,
    })


def _normalise_json(afl_json: Path, start_year: int, end_year: int) -> pd.DataFrame:
    data = json.loads(afl_json.read_text(encoding="utf-8"))
    rows: List[Dict[str,object]] = []

    games = data if isinstance(data, list) else data.get("games") or data.get("matches") or []
    # Try the nested structure used by full_scraper: {league: {seasons: [ {name, games:[...]}, ... ]}}
    if not games and isinstance(data, dict) and "league" in data:
        seasons_obj = data["league"].get("seasons", [])
        seasons = list(seasons_obj.values()) if isinstance(seasons_obj, dict) else seasons_obj
        for season in seasons or []:
            season_name = season.get("name")
            for g in season.get("games", []) or []:
                g["__season_name"] = season_name
            games.extend(season.get("games", []) or [])

    for g in games:
        dt = g.get("date") or g.get("datetime") or g.get("matchDate") or g.get("kickoff") or g.get("game_datetime")
        try:
            y = int(str(dt)[:4])
        except Exception:
            y = None
        if y is None or y < start_year or y > end_year:
            continue
        date_local = str(dt)[:10] if dt else None
        home = (g.get("home") or g.get("homeTeam") or g.get("teamHome") or {}).get("name") if isinstance(g.get("home"), dict) else (g.get("home") or g.get("homeTeam") or g.get("teamHome") or g.get("team_home"))
        away = (g.get("away") or g.get("awayTeam") or g.get("teamAway") or {}).get("name") if isinstance(g.get("away"), dict) else (g.get("away") or g.get("awayTeam") or g.get("teamAway") or g.get("team_away"))
        venue = g.get("venue")
        meta = {
            "ts": datetime.utcnow().isoformat(),
            "source": "oddsportal",
            "season": y,
            "round": g.get("round") or g.get("stage") or g.get("__season_name"),
            "date_local": date_local,
            "home_team": str(home or "").strip(),
            "away_team": str(away or "").strip(),
            "venue": venue,
        }
        mkts = g.get("markets") or g.get("odds") or {}
        # H2H
        for key in ("h2h","moneyline","1x2","match_winner"):
            m = mkts.get(key) if isinstance(mkts, dict) else None
            if m:
                for odt in ("opening","min","max","closing","close"):
                    if odt in m:
                        sub = m[odt]
                        _emit_row(rows, meta, "h2h", odt, "home", sub.get("home") or sub.get("1") or sub.get("home_odds"))
                        _emit_row(rows, meta, "h2h", odt, "away", sub.get("away") or sub.get("2") or sub.get("away_odds"))
                _emit_row(rows, meta, "h2h", "closing", "home", m.get("home") or m.get("1"))
                _emit_row(rows, meta, "h2h", "closing", "away", m.get("away") or m.get("2"))
        # Handicap / line
        for key in ("spread","handicap","line"):
            m = mkts.get(key) if isinstance(mkts, dict) else None
            if m:
                line_val = m.get("line") or m.get("handicap")
                for odt in ("opening","min","max","closing","close"):
                    if odt in m:
                        sub = m[odt]
                        _emit_row(rows, meta, "line", odt, "home", sub.get("home"), line=line_val)
                        _emit_row(rows, meta, "line", odt, "away", sub.get("away"), line=line_val)
                _emit_row(rows, meta, "line", "closing", "home", m.get("home"), line=line_val)
                _emit_row(rows, meta, "line", "closing", "away", m.get("away"), line=line_val)
        # Totals / O-U
        for key in ("totals","total","over_under","ou"):
            m = mkts.get(key) if isinstance(mkts, dict) else None
            if m:
                total_val = m.get("total") or m.get("line")
                for odt in ("opening","min","max","closing","close"):
                    if odt in m:
                        sub = m[odt]
                        _emit_row(rows, meta, "total", odt, "over", sub.get("over"), line=total_val)
                        _emit_row(rows, meta, "total", odt, "under", sub.get("under"), line=total_val)
                _emit_row(rows, meta, "total", "closing", "over", m.get("over"), line=total_val)
                _emit_row(rows, meta, "total", "closing", "under", m.get("under"), line=total_val)

    if not rows:
        return pd.DataFrame()

    cols = [
        "ts","source","season","round","date_local","home_team","away_team","venue",
        "market_code","odds_type","selection_name","price_decimal","line"
    ]
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols]
    # keep >= 2020 only
    df = df[pd.to_numeric(df["season"], errors="coerce").fillna(0).astype(int) >= 2020]
    # drop non-numeric prices
    df = df[pd.to_numeric(df["price_decimal"], errors="coerce").notna()]
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=2020)
    ap.add_argument("--end", type=int, default=datetime.utcnow().year)
    ap.add_argument("--cpus", type=int, default=1, help="parallel CPUs for scraper (default: 1)")
    ap.add_argument("--wait", type=int, default=5, help="wait seconds on page load (default 5)")
    args = ap.parse_args()

    repo = _find_repo_root()
    sjson = _sports_json_path(repo)
    idx = 1
    if sjson.exists():
        idx = _toggle_afl_only_and_get_index(sjson, args.start, args.end)

    timeout_s = int(os.getenv("ODDSPORTAL_TIMEOUT_S", "1800"))
    _run_scraper(repo, selection_idx=idx, timeout_s=timeout_s, num_cpus=args.cpus, wait_on_load=args.wait)

    afl_json = _find_output_json(repo)
    print(f"[oddsportal] found JSON: {afl_json}")
    df = _normalise_json(afl_json, args.start, args.end)
    if df.empty:
        raise SystemExit("[error] oddsportal JSON parsed but produced no rows — check seasons and repo logs/")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = OUT_ROOT / f"snapshot_ts={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "historical_odds_oddsportal.csv"
    df.to_csv(out_csv, index=False)
    print(str(out_csv))


if __name__ == "__main__":
    main()
