# scripts/bronze/bronze_backfill_team_sheets.py
from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import polars as pl
import requests

# ---- import ONLY helpers that exist in scripts/bronze/_shared.py ----
from scripts.bronze._shared import (
    get_logger,
    add_common_test_flags,
    ensure_dir,
    fetch_json,
    parquet_write,
    write_csv_mirror,
    session_with_retries,
)

AFL_V2 = "https://aflapi.afl.com.au/afl/v2"
MATCH_CENTRE_URL = "https://www.afl.com.au/afl/matches/{provider_id}"

LOG = get_logger("bronze.team_sheets_backfill")


# -----------------------------
# Utilities
# -----------------------------
def _http_get(sess: requests.Session, url: str, *, timeout: Optional[float], trace: bool, params: Dict[str, Any] | None = None) -> requests.Response:
    if trace:
        LOG.debug("HTTP GET %s | params=%s | timeout=%s", url, params, timeout)
    r = sess.get(url, params=params, timeout=timeout or 20, headers={"User-Agent": sess.headers.get("User-Agent", "Mozilla/5.0")})
    r.raise_for_status()
    return r


def _dump_json(obj: Any, outdir: Optional[Path], stem: str) -> None:
    if not outdir:
        return
    ensure_dir(outdir)
    p = outdir / f"{re.sub(r'[^a-zA-Z0-9_.-]+','_',stem)}.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    LOG.debug("Wrote debug JSON: %s", p)


def _extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    """
    Extract __NEXT_DATA__ JSON. Uses BeautifulSoup if available, otherwise
    falls back to a regex to avoid adding a hard dependency.
    """
    try:
        from bs4 import BeautifulSoup  # optional
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag and tag.string:
            return json.loads(tag.string)
    except Exception:
        pass

    # Regex fallback
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', html, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _walk(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for i in obj:
            yield from _walk(i)


# -----------------------------
# AFL API discovery
# -----------------------------
_PREMIERSHIP = re.compile(r"premiership", re.I)
_REJECT = re.compile(r"(community|marsh|aami|all[-\s]?stars|practice|pre[-\s]?season)", re.I)

def _competition_id_men() -> int:
    """Pick the AFL men's competition from /competitions (exclude Legacy variants)."""
    data = fetch_json(f"{AFL_V2}/competitions", params={"pageSize": 500})
    comps = [c for c in data.get("competitions", []) if not re.search("Legacy", c.get("name",""), re.I)]
    # prefer code AFL/AFLM
    for c in comps:
        if c.get("code") in {"AFL", "AFLM"}:
            return int(c["id"])
    # fallback to first
    if comps:
        return int(comps[0]["id"])
    raise RuntimeError("Could not resolve competition id from /competitions")

def _comp_season_id_premiership(year: int) -> int:
    comp_id = _competition_id_men()
    data = fetch_json(f"{AFL_V2}/competitions/{comp_id}/compseasons", params={"pageSize": 500})
    seasons = data.get("compSeasons", [])
    def year_of(cs: dict) -> Optional[int]:
        s = cs.get("season") or {}
        if isinstance(s, dict) and "year" in s:
            try:
                return int(s["year"])
            except Exception:
                pass
        m = re.search(r"(20\d{2})", cs.get("name", "") or "")
        return int(m.group(1)) if m else None
    matches = [cs for cs in seasons
               if year_of(cs) == year and _PREMIERSHIP.search(cs.get("name","") or "")
               and not _REJECT.search(cs.get("name","") or "")]
    if not matches:
        names = [cs.get("name") for cs in seasons if year_of(cs) == year]
        raise RuntimeError(f"Could not find Premiership compSeasonId for {year}. Found names={names}")
    # Choose highest id defensively
    return int(sorted(int(cs["id"]) for cs in matches)[-1])

def _rounds_with_matches(comp_season_id: int) -> List[int]:
    rounds: List[int] = []
    for rnd in range(1, 61):
        data = fetch_json(f"{AFL_V2}/matches", params={"compSeasonId": comp_season_id, "roundNumber": rnd, "pageSize": 50})
        if data.get("matches"):
            rounds.append(rnd)
    return rounds

def _matches_for_round(comp_season_id: int, rnd: int) -> List[Dict[str, Any]]:
    data = fetch_json(f"{AFL_V2}/matches", params={"compSeasonId": comp_season_id, "roundNumber": rnd, "pageSize": 50})
    return data.get("matches", [])


# -----------------------------
# Match Centre lineup parsing
# -----------------------------
def _candidate_lineups(node: Dict[str, Any]) -> Optional[Any]:
    for key in ("teamLineUps", "teamLineups", "lineups", "lineUps", "selectedTeams", "teams", "teamLists", "squads"):
        if key in node and node[key] is not None:
            return node[key]
    return None

def _player_name(p: Dict[str, Any]) -> Optional[str]:
    for k in ("displayName", "name", "fullName"):
        v = p.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    fn = p.get("firstName") or p.get("givenName")
    ln = p.get("lastName") or p.get("surname")
    if fn or ln:
        return " ".join([x for x in (fn, ln) if x]).strip() or None
    return None

def _coerce_player_row(team_side: str, team_name: Optional[str], p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_side": team_side,
        "team_name": team_name,
        "player_name": _player_name(p),
        "guernsey": p.get("jumperNumber") or p.get("guernsey") or p.get("number"),
        "position": p.get("position") or p.get("namedPosition") or p.get("pos"),
        "status": p.get("status") or p.get("selectionStatus") or p.get("selStatus"),
    }

def _rows_from_lineups_struct(struct: Any, home_name: Optional[str], away_name: Optional[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    def extract_team(side: str, team_obj: Dict[str, Any], default_name: Optional[str]):
        if not isinstance(team_obj, dict):
            return
        players = None
        for k in ("players", "lineup", "squad", "selected", "named"):
            if isinstance(team_obj.get(k), list):
                players = team_obj[k]
                break
        if players is None and isinstance(team_obj.get("team"), dict):
            for k in ("players", "lineup", "squad"):
                val = team_obj["team"].get(k)
                if isinstance(val, list):
                    players = val
                    break
        tname = team_obj.get("displayName") or team_obj.get("name") or team_obj.get("teamName") or default_name
        if players:
            for p in players:
                rows.append(_coerce_player_row(side, tname, p))

    if isinstance(struct, dict) and ("homeTeam" in struct or "awayTeam" in struct):
        extract_team("home", struct.get("homeTeam", {}), home_name)
        extract_team("away", struct.get("awayTeam", {}), away_name)
    elif isinstance(struct, list) and len(struct) >= 2:
        extract_team("home", struct[0], home_name)
        extract_team("away", struct[1], away_name)
    return rows

def _extract_lineups_from_next(next_json: Dict[str, Any]) -> Optional[Any]:
    for node in _walk(next_json):
        if isinstance(node, dict):
            cand = _candidate_lineups(node)
            if cand is not None:
                return cand
    return None

def _match_home_away_names(next_json: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    home = away = None
    for node in _walk(next_json):
        if isinstance(node, dict) and ("homeTeam" in node and "awayTeam" in node):
            ht = node.get("homeTeam") or {}
            at = node.get("awayTeam") or {}
            home = ht.get("displayName") or ht.get("name") or home
            away = at.get("displayName") or at.get("name") or away
            if home or away:
                break
    return home, away

def _scrape_match_centre(sess: requests.Session, provider_id: int, *, timeout: Optional[float], trace: bool, debug_dump_dir: Optional[Path]) -> List[Dict[str, Any]]:
    url = MATCH_CENTRE_URL.format(provider_id=provider_id)
    r = _http_get(sess, url, timeout=timeout, trace=trace)
    html = r.text
    next_json = _extract_next_data(html)
    if debug_dump_dir and next_json:
        _dump_json(next_json, debug_dump_dir, f"next_match_{provider_id}")
    if not next_json:
        LOG.debug("No __NEXT_DATA__ found for match provider_id=%s", provider_id)
        return []

    home_name, away_name = _match_home_away_names(next_json)
    struct = _extract_lineups_from_next(next_json)
    if struct is None:
        LOG.debug("No team list structure found in __NEXT_DATA__ for provider_id=%s", provider_id)
        return []

    return _rows_from_lineups_struct(struct, home_name, away_name)


# -----------------------------
# Output helpers
# -----------------------------
BRONZE_SUBDIR = "teamsheets"

SCHEMA = pl.Schema(
    {
        "season": pl.Int64,
        "round": pl.Int64,
        "match_provider_id": pl.Int64,
        "team_side": pl.Utf8,
        "team_name": pl.Utf8,
        "player_name": pl.Utf8,
        "guernsey": pl.Utf8,
        "position": pl.Utf8,
        "status": pl.Utf8,
        "source": pl.Utf8,
    }
)

def _schema_df() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)

def _rows_to_df(rows: List[Dict[str, Any]], season: int, rnd: int, provider_id: int) -> pl.DataFrame:
    if not rows:
        return _schema_df().head(0)
    df = pl.from_dicts(rows)
    df = df.with_columns(
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.lit(rnd, dtype=pl.Int64).alias("round"),
        pl.lit(int(provider_id), dtype=pl.Int64).alias("match_provider_id"),
        pl.lit("afl.com.au_match_centre_next").alias("source"),
    )
    return df.select(list(SCHEMA.keys()))

def _write_outputs(df: pl.DataFrame, *, season: int, overwrite: bool, csv_mirror: bool, dry_run: bool) -> None:
    if dry_run:
        LOG.info("DRY RUN: would write %d rows to bronze/%s partitioned by (season, round).", df.height, BRONZE_SUBDIR)
        if csv_mirror:
            LOG.info("DRY RUN: would also write CSV mirror to bronze_csv_mirror/%s/%s.csv", BRONZE_SUBDIR, f"teamsheets_hist_{season}")
        return

    out_root = Path("bronze") / BRONZE_SUBDIR
    if df.is_empty():
        LOG.warning("No lineup rows extracted. Writing schema-only Parquet for discoverability.")
        schema_path = out_root / "_schema" / f"teamsheets_schema_{season}.parquet"
        ensure_dir(schema_path.parent)
        _schema_df().write_parquet(schema_path)
    else:
        parquet_write(df, out_root, partition_cols=["season", "round"], overwrite=overwrite)

    if csv_mirror:
        write_csv_mirror(df, subdir=BRONZE_SUBDIR, stem=f"teamsheets_hist_{season}")


# -----------------------------
# CLI
# -----------------------------
def _parse_rounds_arg(arg: str, available: List[int]) -> List[int]:
    if arg.lower() == "all":
        return sorted(available)
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", arg)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return [r for r in available if a <= r <= b]
    if arg.isdigit():
        r = int(arg)
        return [r] if r in available else []
    return []

def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill AFL Premiership team sheets (scrapes afl.com.au Match Centre __NEXT_DATA__).")
    ap.add_argument("--season", type=int, required=True, help="Season year, e.g. 2025")
    ap.add_argument("--rounds", type=str, default="all", help="Rounds to fetch: 'all', '8', or '1-10'")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing parquet partitions")
    ap.add_argument("--csv-mirror", action="store_true", help="Also write a CSV mirror for the season")
    ap.add_argument("--http-timeout", type=float, default=None, help="HTTP timeout (seconds)")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    ap.add_argument("--debug-dump-dir", type=str, default=None, help="Dir to dump raw __NEXT_DATA__ JSON for parser tuning")
    ap.add_argument("--trace-urls", action="store_true", help="Log each requested URL at DEBUG level")
    add_common_test_flags(ap)
    args = ap.parse_args()

    LOG.setLevel(args.log_level.upper())
    debug_dir = Path(args.debug_dump_dir) if args.debug_dump_dir else None

    sess = session_with_retries(timeout=int(args.http_timeout or 20))

    LOG.info("Resolving Toyota AFL Premiership compSeasonId for season=%s …", args.season)
    comp_season_id = _comp_season_id_premiership(args.season)
    LOG.info("compSeasonId (Premiership) = %s", comp_season_id)

    LOG.info("Discovering rounds with matches …")
    rounds = _rounds_with_matches(comp_season_id)
    if not rounds:
        LOG.warning("No rounds found — writing schema only.")
        _write_outputs(_schema_df(), season=args.season, overwrite=args.overwrite, csv_mirror=args.csv_mirror, dry_run=args.dry_run)
        return

    target_rounds = _parse_rounds_arg(args.rounds, rounds)
    if not target_rounds:
        LOG.warning("No target rounds resolved from %r among %s", args.rounds, rounds)
        _write_outputs(_schema_df(), season=args.season, overwrite=args.overwrite, csv_mirror=args.csv_mirror, dry_run=args.dry_run)
        return

    LOG.info("Target rounds: %s", target_rounds)

    all_frames: List[pl.DataFrame] = []
    for rnd in target_rounds:
        matches = _matches_for_round(comp_season_id, rnd)
        _dump_json(matches, debug_dir, f"matches_round_{rnd}")
        for m in matches:
            prov = int(m.get("providerId") or m.get("id"))
            home = (((m.get("home") or {}).get("team")) or {}).get("name") or m.get("home", {}).get("name")
            away = (((m.get("away") or {}).get("team")) or {}).get("name") or m.get("away", {}).get("name")
            LOG.info("Round %s: %s vs %s (provider_id=%s) — extracting line-ups", rnd, home, away, prov)
            try:
                rows = _scrape_match_centre(sess, prov, timeout=args.http_timeout, trace=args.trace_urls, debug_dump_dir=debug_dir)
            except requests.RequestException as e:
                LOG.warning("HTTP error for provider_id=%s: %s", prov, e)
                rows = []
            except Exception as e:
                LOG.warning("Parsing error for provider_id=%s: %s", prov, e)
                rows = []

            if rows:
                all_frames.append(_rows_to_df(rows, args.season, rnd, prov))
            else:
                LOG.debug("No lineups found for provider_id=%s (round %s).", prov, rnd)

    out_df = pl.concat(all_frames, how="vertical", rechunk=True) if all_frames else _schema_df().head(0)
    _write_outputs(out_df, season=args.season, overwrite=args.overwrite, csv_mirror=args.csv_mirror, dry_run=args.dry_run)
    LOG.info("Done. Rows: %s", 0 if out_df.is_empty() else out_df.height)


if __name__ == "__main__":
    main()
