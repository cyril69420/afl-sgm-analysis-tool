#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import inspect
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import polars as pl
import requests
from playwright.async_api import async_playwright  # async API

from scripts.bronze._shared import (
    add_common_test_flags,
    maybe_limit_df,
    echo_df_info,
    parquet_write,
    ensure_dir,
    load_yaml,
    utc_now,
    write_csv_mirror,
)

from schemas.bronze import BronzeEventUrl

LOG = logging.getLogger("bronze.discover")

# ---------- helpers ----------

def _parse_rounds(value: Optional[str]) -> List[str]:
    if not value or value.lower() == "all":
        return []
    out: List[str] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            s, e = part.split("-", 1)
            out.extend([str(i) for i in range(int(s), int(e) + 1)])
        else:
            out.append(part)
    return out


def _load_fixtures_bronze(project_root: Path, season: int, rounds: List[str]) -> Optional[pl.DataFrame]:
    root = project_root / "bronze" / "fixtures"
    if not root.exists():
        return None
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW fixtures AS SELECT * FROM read_parquet('{root.as_posix()}/**/*.parquet')")
        q = "SELECT * FROM fixtures WHERE season = ?"
        params: List = [season]
        if rounds:
            q += f" AND round IN ({','.join(['?'] * len(rounds))})"
            params.extend(rounds)
        df = con.execute(q, params).df()
        if df.empty:
            return None
        return pl.from_pandas(df)
    except Exception:
        return None


def _fetch_squiggle_games(season: int) -> List[Dict]:
    # Public Squiggle fixtures API
    r = requests.get(f"https://api.squiggle.com.au/?q=games;year={season}", timeout=60)
    r.raise_for_status()
    js = r.json()
    return js.get("games") or js.get("Games") or []  # API returns "games"


def _fixtures_from_squiggle(season: int, rounds: List[str]) -> pl.DataFrame:
    games = _fetch_squiggle_games(season)
    rows = []
    for g in games:
        home = g.get("hteam") or g.get("homeTeam")
        away = g.get("ateam") or g.get("awayTeam")
        rnd = str(g.get("round") or "unknown")
        tz = g.get("tz") or "Australia/Melbourne"
        if not (home and away):
            continue
        rows.append({"season": season, "round": rnd, "home": home, "away": away, "source_tz": tz})
    df = pl.DataFrame(rows)
    if rounds:
        df = df.filter(pl.col("round").cast(pl.Utf8).is_in(rounds))
    return df


def _load_fixtures(project_root: Path, season: int, rounds: List[str]) -> pl.DataFrame:
    df = _load_fixtures_bronze(project_root, season, rounds)
    if df is not None:
        return df.select("season", "round", "home", "away", "source_tz")
    return _fixtures_from_squiggle(season, rounds)


def _load_team_alias_map(project_root: Path) -> Dict[str, str]:
    alias_csv = project_root / "config" / "team_aliases.csv"
    m: Dict[str, str] = {}
    if alias_csv.exists():
        df = pl.read_csv(alias_csv)
        alias_col = df.columns[0]
        canon_col = df.columns[1]
        for r in df.iter_rows(named=True):
            alias = str(r[alias_col]).strip()
            canon = str(r[canon_col]).strip()
            if alias and canon:
                m[alias] = canon
                m[canon] = canon
    return m


@dataclass
class Resolver:
    name: str
    call: object  # async callable: resolve(game, alias_map, ctx)


def _load_module_by_path(modname: str, path: Path):
    spec = importlib.util.spec_from_file_location(modname, path.as_posix())
    if not spec or not spec.loader:
        return None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def _find_resolve_callable(mod) -> Optional[object]:
    # Prefer class *Resolver with async classmethod resolve(...)
    for attr in dir(mod):
        obj = getattr(mod, attr)
        if inspect.isclass(obj) and attr.lower().endswith("resolver") and hasattr(obj, "resolve"):
            return getattr(obj, "resolve")
    # Fallback: module-level resolve(...)
    if hasattr(mod, "resolve"):
        return getattr(mod, "resolve")
    return None


def _load_resolver(project_root: Path, name: str) -> Optional[Resolver]:
    # 1) scripts.bookmakers.<name>
    try:
        mod = importlib.import_module(f"scripts.bookmakers.{name}")
        call = _find_resolve_callable(mod)
        if call:
            return Resolver(name=name, call=call)
    except Exception:
        pass
    # 2) scripts.bronze.<name>
    try:
        mod = importlib.import_module(f"scripts.bronze.{name}")
        call = _find_resolve_callable(mod)
        if call:
            return Resolver(name=name, call=call)
    except Exception:
        pass
    # 3) scripts/bookmakers/<name>.py
    cand = project_root / "scripts" / "bookmakers" / f"{name}.py"
    if cand.exists():
        mod = _load_module_by_path(f"scripts.bookmakers.{name}", cand)
        call = _find_resolve_callable(mod)
        if call:
            return Resolver(name=name, call=call)
    # 4) scripts/bronze/<name>.py
    cand = project_root / "scripts" / "bronze" / f"{name}.py"
    if cand.exists():
        mod = _load_module_by_path(f"scripts.bronze.{name}", cand)
        call = _find_resolve_callable(mod)
        if call:
            return Resolver(name=name, call=call)
    return None


async def _discover_urls_async(
    project_root: Path,
    season: int,
    rounds: List[str],
    bookmakers: List[str],
    limit: Optional[int],
    headless: bool,
) -> pl.DataFrame:
    fixtures = _load_fixtures(project_root, season, rounds)
    if fixtures.is_empty():
        LOG.warning("No fixtures available (bronze or Squiggle). Cannot resolve URLs per game.")
        return pl.DataFrame([])

    games = fixtures.select(["season", "round", "home", "away"]).to_dicts()
    if limit and limit > 0:
        games = games[:limit]

    alias_map = _load_team_alias_map(project_root)

    resolvers: List[Resolver] = []
    for bk in bookmakers:
        r = _load_resolver(project_root, bk)
        if not r:
            LOG.warning("No resolver found for bookmaker=%s (module or file). Skipping.", bk)
            continue
        resolvers.append(r)

    if not resolvers:
        return pl.DataFrame([])

    out_rows: List[Dict] = []
    run_now = utc_now()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        ctx = await browser.new_context()
        try:
            for g in games:
                for r in resolvers:
                    try:
                        # Your resolvers are async classmethods: SportsbetResolver/PointsbetResolver.resolve(...)
                        res = await r.call(g, alias_map, ctx)
                    except Exception as e:
                        LOG.debug("Resolver %s failed for %s vs %s: %s", r.name, g["home"], g["away"], e)
                        res = None
                    if not res or not res.get("event_url"):
                        continue
                    out_rows.append({
                        "bookmaker": r.name,
                        "season": season,
                        "round": str(g["round"]),
                        "event_url": res["event_url"],
                        "competition": "afl",
                        "first_seen_utc": run_now,
                        "last_seen_utc": run_now,
                        "status": "upcoming",
                        "discovery_run_id": f"auto-{run_now.strftime('%Y%m%d%H%M%S')}",
                        "match_quality": res.get("match_quality"),
                        "found_at": res.get("found_at"),
                        "notes": res.get("notes"),
                    })
        finally:
            await ctx.close()
            await browser.close()

    if not out_rows:
        return pl.DataFrame([])
    df = pl.DataFrame(out_rows).unique(subset=["bookmaker", "event_url"], keep="first")
    return df


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Discover AFL event URLs via bookmaker resolvers")
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--season", type=int, help="Season (e.g. 2025)")
    ap.add_argument("--rounds", type=str, default=None, help="Filter rounds: '1,2' or '1-5' or 'all'")
    ap.add_argument("--bookmakers", type=str, default="sportsbet,pointsbet", help="Comma-separated names")
    ap.add_argument("--headless", action="store_true", help="Run browser headless")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite partitioned Parquet on re-run")
    ap.add_argument("--csv-mirror", action="store_true", help="Write CSV mirror under bronze_csv_mirror/event_urls/")
    ap.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARN/ERROR")
    add_common_test_flags(ap)  # --dry-run --limit
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | bronze | %(message)s",
    )

    root = Path(args.root)
    settings = load_yaml(root / "config" / "settings.yaml") or {}
    season = args.season or (settings.get("seasons") or [datetime.now().year])[0]
    rounds = _parse_rounds(args.rounds) or _parse_rounds(settings.get("default_rounds", "all"))
    bookmakers = [b.strip() for b in (args.bookmakers or "").split(",") if b.strip()]

    df = asyncio.run(_discover_urls_async(
        project_root=root,
        season=season,
        rounds=rounds,
        bookmakers=bookmakers,
        limit=args.limit,
        headless=args.headless,
    ))

    if df.is_empty():
        LOG.warning("No event URLs discovered.")
        return 0

    df = maybe_limit_df(df, args.limit)
    echo_df_info(df, note="event_urls (post-limit)")

    if args.dry_run:
        return 0

    out_root = root / "bronze" / "event_urls"
    parquet_write(df, out_root, partition_cols=["season", "round", "bookmaker"], overwrite=args.overwrite)
    LOG.info("Wrote %d rows → %s", df.height, out_root.as_posix())

    if args.csv_mirror:
        write_csv_mirror(df, "event_urls", f"event_urls_{season}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
