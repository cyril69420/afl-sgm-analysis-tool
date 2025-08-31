#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bronze backfill for AFL Team Sheets (23s).

- Primary: Resolve compSeasonId via AFL v2 API and enumerate matches by round.
- Fallback: If AFL API is blocked (403/401) or fails, enumerate matches from
  the public fixture page on afl.com.au (parse __NEXT_DATA__).

- For each match with a numeric id:
    * Fetch Match Centre HTML, parse __NEXT_DATA__, mine lineups robustly.
    * NEVER cast providerId to int (keep as string).
- Always write Parquet partitioned by (season, round); write schema-only if empty.
- Optional season CSV mirror.

Only imports helpers that exist in scripts/bronze/_shared.py:
    get_logger, add_common_test_flags, ensure_dir, fetch_json,
    parquet_write, write_csv_mirror, session_with_retries
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

# Allowed helpers only
from ._shared import (
    get_logger,
    add_common_test_flags,
    ensure_dir,
    fetch_json,
    parquet_write,
    write_csv_mirror,
    session_with_retries,
)

# ---------------------------
# Constants & utilities
# ---------------------------

AFL_API_BASE = "https://api.afl.com.au/afl/v2"
MATCH_CENTRE_PRIMARY = "https://www.afl.com.au/afl/matches/{match_id}"
MATCH_CENTRE_ALT = "https://www.afl.com.au/match-centre/{match_id}"  # optional fallback
FIXTURE_ROUND_PAGE = "https://www.afl.com.au/fixture?Season={season}&Round={round}"

COMP_NAME_KEYWORDS = ["toyota", "premiership"]  # fuzzy match
DATASET_DIR_DEFAULT = os.path.join("bronze", "teamsheets")

CANDIDATE_LINEUP_KEYS = {
    "teamLineUps",
    "teamLineups",
    "lineUps",
    "lineups",
    "selectedTeams",
    "teams",
    "teamLists",
    "squads",
    "team_list",
    "teamlist",
}

PLAYER_NAME_KEYS = ["name", "fullName", "displayName", "playerName"]
PLAYER_FIRST_LAST = [("firstName", "lastName"), ("givenName", "surname"), ("givenName", "familyName")]

PLAYER_NUMBER_KEYS = ["guernsey", "guernseyNumber", "jumperNumber", "number", "jerseyNumber"]
PLAYER_POSITION_KEYS = ["position", "startingPosition", "selectedPosition", "role"]
PLAYER_STATUS_KEYS = ["status", "selectionStatus", "selection", "availability"]

NEXT_DATA_RE = re.compile(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s or None


def _is_intlike(x: Any) -> bool:
    try:
        int(str(x))
        return True
    except Exception:
        return False


# ---------------------------
# AFL API lookups (with graceful failure)
# ---------------------------

def resolve_premiership_comp_id(session, logger, http_timeout: int) -> Optional[int]:
    """
    Find the "Toyota AFL Premiership" competition id via /competitions.
    """
    url = f"{AFL_API_BASE}/competitions"
    logger.info("Resolving Toyota AFL Premiership competition id …")
    try:
        comps = fetch_json(url, session, timeout=http_timeout)
    except requests.HTTPError as e:
        logger.warning("AFL API competitions returned %s; will fallback to site fixture for match discovery.", e.response.status_code if e.response else "HTTPError")
        return None
    except Exception as e:
        logger.warning("AFL API competitions failed (%s); will fallback to site fixture for match discovery.", e)
        return None

    best = None
    if isinstance(comps, list):
        for c in comps:
            name = _norm(c.get("name"))
            if not name:
                continue
            lower = name.lower()
            if all(k in lower for k in COMP_NAME_KEYWORDS):
                best = c
                break
    if best is None:
        if isinstance(comps, list) and comps:
            best = comps[0]
        else:
            return None
    cid = best.get("id")
    if not _is_intlike(cid):
        logger.warning("Competition id not numeric; got=%r. Will fallback to site fixture for match discovery.", cid)
        return None
    return int(cid)


def resolve_comp_season_id(session, logger, comp_id: int, season: int, http_timeout: int) -> Optional[int]:
    """
    Find compSeasonId for the given season from /competitions/{id}/compseasons.
    """
    if comp_id is None:
        return None
    url = f"{AFL_API_BASE}/competitions/{comp_id}/compseasons"
    logger.info("Resolving compSeasonId for season=%s …", season)
    try:
        data = fetch_json(url, session, timeout=http_timeout)
    except requests.HTTPError as e:
        logger.warning("AFL API compseasons returned %s; will fallback to site fixture for match discovery.", e.response.status_code if e.response else "HTTPError")
        return None
    except Exception as e:
        logger.warning("AFL API compseasons failed (%s); will fallback to site fixture for match discovery.", e)
        return None

    if isinstance(data, list):
        for row in data:
            yr = row.get("year") or row.get("season")
            if str(yr) == str(season):
                csid = row.get("id")
                if _is_intlike(csid):
                    return int(csid)
                logger.warning("Found comp season for %s but id not numeric: %r; fallback to site fixture.", season, csid)
    logger.warning("No compSeasonId found for season=%s via API; fallback to site fixture.", season)
    return None


def fetch_matches_for_round_api(session, logger, comp_season_id: int, round_number: int, http_timeout: int) -> List[Dict[str, Any]]:
    """
    API path: /matches?compSeasonId=...&roundNumber=N
    """
    url = f"{AFL_API_BASE}/matches?compSeasonId={comp_season_id}&roundNumber={round_number}"
    try:
        matches = fetch_json(url, session, timeout=http_timeout)
    except requests.HTTPError as e:
        logger.warning("AFL API matches returned %s for round=%s; will fallback to site fixture.", e.response.status_code if e.response else "HTTPError", round_number)
        return []
    except Exception as e:
        logger.warning("AFL API matches failed for round=%s (%s); will fallback to site fixture.", round_number, e)
        return []
    if not isinstance(matches, list):
        logger.warning("Unexpected matches payload (round=%s): %s; fallback to site fixture.", round_number, type(matches).__name__)
        return []
    return matches


def fetch_all_matches_api(session, logger, comp_season_id: int, http_timeout: int) -> List[Dict[str, Any]]:
    """
    API path: /matches?compSeasonId=...
    Used only to infer the set of valid roundNumbers when --rounds=all.
    """
    url = f"{AFL_API_BASE}/matches?compSeasonId={comp_season_id}"
    try:
        matches = fetch_json(url, session, timeout=http_timeout)
    except Exception:
        return []
    return matches if isinstance(matches, list) else []


# ---------------------------
# Public site fixture fallback (no API)
# ---------------------------

def fetch_next_data_html(session, url: str, http_timeout: int) -> Optional[Dict[str, Any]]:
    resp = session.get(url, timeout=http_timeout)
    if resp.status_code != 200 or not resp.text:
        return None
    m = NEXT_DATA_RE.search(resp.text)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def walk_collect_dicts(node: Any) -> Iterable[Dict[str, Any]]:
    """Yield all dict nodes in a nested JSON-like structure."""
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from walk_collect_dicts(v)
    elif isinstance(node, list):
        for it in node:
            yield from walk_collect_dicts(it)


def fixture_matches_from_next(next_data: Dict[str, Any], target_round: int) -> List[Dict[str, Any]]:
    """
    Extract match dicts (with numeric id) for a given round from fixture __NEXT_DATA__.
    We look for dicts having an intlike 'id' and fields indicating a match (home/away/teams),
    and roundNumber matching target_round (directly or nested).
    """
    out: List[Dict[str, Any]] = []
    for d in walk_collect_dicts(next_data):
        mid = d.get("id")
        # Heuristics: looks like a match object?
        looks_like_match = (
            ("homeTeam" in d) or ("awayTeam" in d) or ("teams" in d) or ("match" in d) or ("venue" in d)
        )
        if not looks_like_match:
            continue
        # Work out round number from a few common shapes
        rnum = d.get("roundNumber")
        if not _is_intlike(rnum):
            rnum = (d.get("round") or {}).get("roundNumber") if isinstance(d.get("round"), dict) else None
        if _is_intlike(rnum) and int(rnum) != int(target_round):
            continue
        if not _is_intlike(mid):
            # Some shapes nest id deeper (e.g., d["match"]["id"])
            alt = (d.get("match") or {}).get("id")
            if _is_intlike(alt):
                mid = alt
            else:
                continue
        provider_id = d.get("providerId") or (d.get("match") or {}).get("providerId")
        out.append({"id": int(mid), "providerId": provider_id, "roundNumber": int(rnum) if _is_intlike(rnum) else target_round})
    # de-dup by id
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for m in out:
        if m["id"] in seen:
            continue
        seen.add(m["id"])
        uniq.append(m)
    return uniq


def fetch_matches_for_round_site(session, logger, season: int, round_number: int, http_timeout: int, trace_urls: bool) -> List[Dict[str, Any]]:
    """
    Fallback: enumerate matches by scraping the public fixture page for the round.
    """
    url = FIXTURE_ROUND_PAGE.format(season=season, round=round_number)
    if trace_urls:
        logger.info("Fetch fixture page: %s", url)
    next_data = fetch_next_data_html(session, url, http_timeout)
    if not next_data:
        logger.debug("No __NEXT_DATA__ on fixture page for round=%s; returning empty.", round_number)
        return []
    matches = fixture_matches_from_next(next_data, round_number)
    if not matches:
        logger.debug("Fixture page yielded no matches for round=%s.", round_number)
    return matches


# ---------------------------
# Match Centre parsing (lineups)
# ---------------------------

def fetch_match_next_data(session, logger, match_id: int, http_timeout: int, trace_urls: bool, fallback_alt_routes: bool) -> Optional[Dict[str, Any]]:
    urls = [MATCH_CENTRE_PRIMARY.format(match_id=match_id)]
    if fallback_alt_routes:
        urls.append(MATCH_CENTRE_ALT.format(match_id=match_id))
    for url in urls:
        if trace_urls:
            logger.info("Fetch match centre: %s", url)
        nd = fetch_next_data_html(session, url, http_timeout)
        if nd:
            return nd
    return None


def _collect_candidate_containers(obj: Any) -> List[Tuple[Any, Optional[str], Optional[str]]]:
    out: List[Tuple[Any, Optional[str], Optional[str]]] = []

    def walk(node: Any, parent_key: Optional[str] = None, parent_side: Optional[str] = None, parent_team_name: Optional[str] = None):
        if isinstance(node, dict):
            for hk in ("home", "away", "homeTeam", "awayTeam"):
                if hk in node:
                    walk(node[hk], hk.lower().replace("team", ""), None)

            lower_keys = {str(k).lower() for k in node.keys()}
            team_name_hint = _norm(node.get("teamName") or node.get("name") or (node.get("team") or {}).get("name") if isinstance(node.get("team"), dict) else None)

            if lower_keys & {k.lower() for k in CANDIDATE_LINEUP_KEYS}:
                for key, val in node.items():
                    if str(key).lower() in {k.lower() for k in CANDIDATE_LINEUP_KEYS}:
                        out.append((val, parent_key, team_name_hint))

            for players_key in ["players", "lineup", "lineUp", "squad"]:
                if isinstance(node.get(players_key), list):
                    out.append((node, parent_key, team_name_hint))

            for k, v in node.items():
                walk(v, k, parent_side, team_name_hint or parent_team_name)

        elif isinstance(node, list):
            for item in node:
                walk(item, parent_key, parent_side, parent_team_name)

    walk(obj, None, None, None)
    return out


def _stringify_player_name(p: Dict[str, Any]) -> Optional[str]:
    base = p.get("player") if isinstance(p.get("player"), dict) else p
    for k in PLAYER_NAME_KEYS:
        v = _norm(base.get(k))
        if v:
            return v
    for a, b in PLAYER_FIRST_LAST:
        if _norm(base.get(a)) or _norm(base.get(b)):
            first = _norm(base.get(a)) or ""
            last = _norm(base.get(b)) or ""
            both = f"{first} {last}".strip()
            return both or None
    if "player" in p and isinstance(p["player"], dict):
        return _stringify_player_name(p["player"])
    return None


def _extract_value_from_keys(d: Dict[str, Any], candidate_keys: Iterable[str]) -> Optional[str]:
    for k in candidate_keys:
        v = d.get(k)
        v = _norm(v if v is not None else d.get(k.lower()) if isinstance(k, str) else None)
        if v:
            return v
    return None


def _bool_flags_to_status(d: Dict[str, Any]) -> Optional[str]:
    flags = []
    for k in ["isOut", "out", "omitted"]:
        if bool(d.get(k)):
            flags.append("out")
    for k in ["isEmergency", "emergency"]:
        if bool(d.get(k)):
            flags.append("emergency")
    for k in ["isSub", "sub"]:
        if bool(d.get(k)):
            flags.append("sub")
    if flags:
        return ",".join(sorted(set(flags)))
    return None


def extract_lineups_from_next(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    root_candidates = []
    try:
        props = next_data.get("props", {})
        page_props = props.get("pageProps", {})
        root_candidates.append(page_props)
        dehydrated = page_props.get("dehydratedState", {})
        root_candidates.append(dehydrated)
        queries = (dehydrated or {}).get("queries") or []
        for q in queries:
            data = (((q or {}).get("state") or {}).get("data")) or {}
            if data:
                root_candidates.append(data)
    except Exception:
        pass
    root_candidates.append(next_data)

    for root in root_candidates:
        containers = _collect_candidate_containers(root)
        for container, side_hint, team_name_hint in containers:
            if isinstance(container, dict):
                players = None
                for pk in ["players", "lineup", "lineUp", "squad"]:
                    if isinstance(container.get(pk), list):
                        players = container.get(pk)
                        break
                if not players and any(k in container for k in CANDIDATE_LINEUP_KEYS):
                    for v in container.values():
                        if isinstance(v, list) and v and isinstance(v[0], dict):
                            players = v
                            break

                if players:
                    team_name = _norm(
                        container.get("teamName")
                        or container.get("name")
                        or ((container.get("team") or {}).get("name") if isinstance(container.get("team"), dict) else None)
                        or team_name_hint
                    )
                    team_side = _norm(
                        container.get("teamSide")
                        or container.get("side")
                        or container.get("homeAway")
                        or side_hint
                    )
                    for p in players:
                        player_name = _stringify_player_name(p)
                        if not player_name:
                            continue
                        guernsey = _extract_value_from_keys(p, PLAYER_NUMBER_KEYS) or _extract_value_from_keys(p.get("player", {}) if isinstance(p.get("player"), dict) else {}, PLAYER_NUMBER_KEYS)
                        position = _extract_value_from_keys(p, PLAYER_POSITION_KEYS)
                        status = _extract_value_from_keys(p, PLAYER_STATUS_KEYS) or _bool_flags_to_status(p)
                        rows.append(dict(
                            team_side=team_side,
                            team_name=team_name,
                            player_name=player_name,
                            guernsey=_norm(guernsey),
                            position=_norm(position),
                            status=_norm(status),
                        ))

            elif isinstance(container, list):
                if container and isinstance(container[0], dict) and ("team" in container[0] or "players" in container[0] or "lineup" in container[0] or "lineUp" in container[0]):
                    for t in container:
                        team_name = _norm(
                            t.get("teamName")
                            or t.get("name")
                            or ((t.get("team") or {}).get("name") if isinstance(t.get("team"), dict) else None)
                            or team_name_hint
                        )
                        team_side = _norm(t.get("teamSide") or t.get("side") or t.get("homeAway") or side_hint)
                        players = None
                        for pk in ["players", "lineup", "lineUp", "squad"]:
                            if isinstance(t.get(pk), list):
                                players = t.get(pk)
                                break
                        if not players:
                            continue
                        for p in players:
                            player_name = _stringify_player_name(p)
                            if not player_name:
                                continue
                            guernsey = _extract_value_from_keys(p, PLAYER_NUMBER_KEYS) or _extract_value_from_keys(p.get("player", {}) if isinstance(p.get("player"), dict) else {}, PLAYER_NUMBER_KEYS)
                            position = _extract_value_from_keys(p, PLAYER_POSITION_KEYS)
                            status = _extract_value_from_keys(p, PLAYER_STATUS_KEYS) or _bool_flags_to_status(p)
                            rows.append(dict(
                                team_side=team_side,
                                team_name=team_name,
                                player_name=player_name,
                                guernsey=_norm(guernsey),
                                position=_norm(position),
                                status=_norm(status),
                            ))
                else:
                    for p in container:
                        if not isinstance(p, dict):
                            continue
                        player_name = _stringify_player_name(p)
                        if not player_name:
                            continue
                        guernsey = _extract_value_from_keys(p, PLAYER_NUMBER_KEYS) or _extract_value_from_keys(p.get("player", {}) if isinstance(p.get("player"), dict) else {}, PLAYER_NUMBER_KEYS)
                        position = _extract_value_from_keys(p, PLAYER_POSITION_KEYS)
                        status = _extract_value_from_keys(p, PLAYER_STATUS_KEYS) or _bool_flags_to_status(p)
                        rows.append(dict(
                            team_side=_norm(side_hint),
                            team_name=_norm(team_name_hint),
                            player_name=player_name,
                            guernsey=_norm(guernsey),
                            position=_norm(position),
                            status=_norm(status),
                        ))

    if rows:
        seen = set()
        uniq = []
        for r in rows:
            key = (
                (r.get("team_side") or "").lower(),
                (r.get("team_name") or "").lower(),
                (r.get("player_name") or "").lower(),
                r.get("guernsey") or "",
                (r.get("position") or "").lower(),
                (r.get("status") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            uniq.append(r)
        rows = uniq

    return rows


# ---------------------------
# IO helpers (defensive)
# ---------------------------

def _parquet_write_safe(logger, df: pd.DataFrame, out_dir: str, partition_cols: List[str], overwrite: bool) -> None:
    ensure_dir(out_dir)
    try:
        parquet_write(df=df, dataset_dir=out_dir, partition_cols=partition_cols, overwrite=overwrite)
        return
    except TypeError:
        pass
    try:
        parquet_write(df, out_dir, partition_cols=partition_cols, overwrite=overwrite)
        return
    except TypeError:
        pass
    try        :
        mode = "overwrite" if overwrite else "append"
        parquet_write(df=df, root=out_dir, partition_cols=partition_cols, mode=mode)
        return
    except TypeError:
        pass
    parquet_write(df, out_dir)


def _csv_mirror_safe(logger, df: pd.DataFrame, out_dir: str, season: int, filename: Optional[str] = None) -> None:
    ensure_dir(out_dir)
    filename = filename or f"teamsheets_{season}.csv"
    try:
        write_csv_mirror(df=df, csv_dir=out_dir, season=season, filename=filename)
        return
    except TypeError:
        pass
    try:
        write_csv_mirror(out_dir, filename, df)
        return
    except TypeError:
        pass
    try:
        write_csv_mirror(out_dir, df, season)
        return
    except TypeError:
        pass
    try:
        df.to_csv(os.path.join(out_dir, filename), index=False)
        logger.warning("write_csv_mirror signature mismatch; wrote CSV directly to %s", os.path.join(out_dir, filename))
    except Exception as exc:
        logger.error("Failed to write CSV mirror: %s", exc)


# ---------------------------
# Rounds parsing & main
# ---------------------------

def parse_rounds_arg(rounds_arg: str, all_matches: List[Dict[str, Any]]) -> List[int]:
    if rounds_arg.lower() == "all":
        rset = set()
        for m in all_matches:
            rn = m.get("roundNumber") or (m.get("round") or {}).get("roundNumber")
            if _is_intlike(rn):
                rset.add(int(rn))
        return sorted(rset) if rset else []
    if re.fullmatch(r"\d+", rounds_arg):
        return [int(rounds_arg)]
    m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", rounds_arg)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        lo, hi = min(a, b), max(a, b)
        return list(range(lo, hi + 1))
    raise ValueError(f"Invalid --rounds argument: {rounds_arg}")


def build_schema_only_df(season: int, round_num: Optional[int] = None) -> pd.DataFrame:
    cols = dict(
        season=pd.Series(dtype="int64"),
        round=pd.Series(dtype="int64"),
        match_id=pd.Series(dtype="Int64"),
        match_provider_id=pd.Series(dtype="string"),
        team_side=pd.Series(dtype="string"),
        team_name=pd.Series(dtype="string"),
        player_name=pd.Series(dtype="string"),
        guernsey=pd.Series(dtype="string"),
        position=pd.Series(dtype="string"),
        status=pd.Series(dtype="string"),
        source=pd.Series(dtype="string"),
    )
    return pd.DataFrame(cols)


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Backfill AFL Team Sheets (23s) into bronze/teamsheets")
    p.add_argument("--season", type=int, required=True, help="Season year, e.g., 2025")
    p.add_argument("--rounds", default="all", help="Rounds: all | N | A-B (default: all)")
    p.add_argument("--dataset-dir", default=DATASET_DIR_DEFAULT, help="Output dataset root (parquet)")
    p.add_argument("--csv-mirror", action="store_true", help="Also write a CSV mirror for the whole season")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing partitions")
    p.add_argument("--http-timeout", type=int, default=20, help="HTTP timeout (seconds)")
    p.add_argument("--comp-season-id", type=int, default=None, help="Optional compSeasonId override (skips discovery)")
    p.add_argument("--debug-dump-dir", default=None, help="If set, dump round matches and __NEXT_DATA__ here")
    p.add_argument("--trace-urls", action="store_true", help="Log every URL fetched")
    p.add_argument("--fallback-alt-routes", action="store_true", help="Try alternate Match Centre routes if primary fails")
    p.add_argument("--log-level", default="INFO", help="Logging level")
    add_common_test_flags(p)

    args = p.parse_args(argv)
    logger = get_logger("bronze.team_sheets_backfill", level=args.log_level)

    season = int(args.season)
    dataset_dir = args.dataset_dir
    http_timeout = int(args.http_timeout)
    debug_dir = args.debug_dump_dir
    trace_urls = bool(args.trace_urls)

    if debug_dir:
        ensure_dir(debug_dir)

    session = session_with_retries()
    # Harden headers to reduce chance of 403 on public endpoints
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-AU,en;q=0.9",
        "Origin": "https://www.afl.com.au",
        "Referer": "https://www.afl.com.au/",
        "Connection": "keep-alive",
    })

    # Resolve compSeasonId via API if possible
    comp_season_id = args.comp_season_id
    if comp_season_id is None:
        comp_id = resolve_premiership_comp_id(session, logger, http_timeout)
        comp_season_id = resolve_comp_season_id(session, logger, comp_id, season, http_timeout) if comp_id else None

    # Determine rounds
    rounds: List[int] = []
    if args.rounds.lower() == "all":
        if comp_season_id is not None:
            all_matches_payload = fetch_all_matches_api(session, logger, comp_season_id, http_timeout)
            rounds = parse_rounds_arg("all", all_matches_payload)
        if not rounds:
            # Fall back to broad default; site fallback will handle empty rounds
            rounds = list(range(1, 31))
    else:
        rounds = parse_rounds_arg(args.rounds, [])

    logger.info("Target rounds: %s", rounds)

    season_rows: List[Dict[str, Any]] = []

    for rnd in rounds:
        # Prefer API if we have compSeasonId
        if comp_season_id is not None:
            matches = fetch_matches_for_round_api(session, logger, comp_season_id, rnd, http_timeout)
        else:
            matches = []

        # Fallback: site fixture page
        if not matches:
            matches = fetch_matches_for_round_site(session, logger, season, rnd, http_timeout, trace_urls)

        if debug_dir:
            path = os.path.join(debug_dir, f"matches_round_{rnd}.json")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(matches, f, ensure_ascii=False, indent=2)
                logger.info("Wrote debug JSON: %s", path)
            except Exception:
                pass

        round_rows: List[Dict[str, Any]] = []

        for m in matches:
            match_provider_id = _norm(m.get("providerId") or (m.get("match") or {}).get("providerId"))
            raw_numeric_id = m.get("id") if "id" in m else (m.get("match") or {}).get("id")

            numeric_match_id: Optional[int] = None
            if _is_intlike(raw_numeric_id):
                numeric_match_id = int(raw_numeric_id)
            else:
                for k in ["matchId", "match_id", "externalId", "external_id", "aflMatchId"]:
                    rv = m.get(k) or (m.get("match") or {}).get(k)
                    if _is_intlike(rv):
                        numeric_match_id = int(rv)
                        break

            if numeric_match_id is None:
                logger.debug("Skip match without numeric id (round=%s, providerId=%r).", rnd, match_provider_id)
                continue

            # Fetch and parse match centre
            next_data = fetch_match_next_data(
                session=session,
                logger=logger,
                match_id=numeric_match_id,
                http_timeout=http_timeout,
                trace_urls=trace_urls,
                fallback_alt_routes=args.fallback_alt_routes,
            )
            if next_data is None:
                logger.debug("No __NEXT_DATA__ for match_id=%s (round=%s); skipping.", numeric_match_id, rnd)
                continue

            if debug_dir:
                nd_path = os.path.join(debug_dir, f"nextdata_match_{numeric_match_id}.json")
                try:
                    with open(nd_path, "w", encoding="utf-8") as f:
                        json.dump(next_data, f, ensure_ascii=False, indent=2)
                    logger.debug("Wrote __NEXT_DATA__ to %s", nd_path)
                except Exception:
                    pass

            player_rows = extract_lineups_from_next(next_data)
            if not player_rows:
                logger.debug("No team lineups parsed for match_id=%s (round=%s).", numeric_match_id, rnd)
                continue

            for r in player_rows:
                round_rows.append(dict(
                    season=season,
                    round=int(rnd),
                    match_id=numeric_match_id,
                    match_provider_id=match_provider_id or "",
                    team_side=_norm(r.get("team_side")),
                    team_name=_norm(r.get("team_name")),
                    player_name=_norm(r.get("player_name")),
                    guernsey=_norm(r.get("guernsey")),
                    position=_norm(r.get("position")),
                    status=_norm(r.get("status")),
                    source="match_centre",
                ))
            logger.info("Parsed %d lineup rows for match_id=%s (round=%s).", len(player_rows), numeric_match_id, rnd)

        # Write this round (or schema-only)
        if round_rows:
            df_round = pd.DataFrame(round_rows)
        else:
            logger.debug("Round %s yielded no rows; writing schema-only parquet partition.", rnd)
            df_round = build_schema_only_df(season, rnd)

        if "match_id" in df_round.columns:
            df_round["match_id"] = df_round["match_id"].astype("Int64")
        for col in ["match_provider_id", "team_side", "team_name", "player_name", "guernsey", "position", "status", "source"]:
            if col in df_round.columns:
                df_round[col] = df_round[col].astype("string")

        _parquet_write_safe(
            logger=logger,
            df=df_round,
            out_dir=dataset_dir,
            partition_cols=["season", "round"],
            overwrite=bool(args.overwrite),
        )

        if not df_round.empty:
            season_rows.extend(df_round.to_dict("records"))

    # Optional season CSV mirror
    if args.csv_mirror:
        df_season = pd.DataFrame(season_rows) if season_rows else build_schema_only_df(season)
        if "match_id" in df_season.columns:
            df_season["match_id"] = df_season["match_id"].astype("Int64")
        for col in ["match_provider_id", "team_side", "team_name", "player_name", "guernsey", "position", "status", "source"]:
            if col in df_season.columns:
                df_season[col] = df_season[col].astype("string")
        _csv_mirror_safe(
            logger=logger,
            df=df_season,
            out_dir=os.path.join(dataset_dir, "_csv_mirror"),
            season=season,
            filename=f"teamsheets_{season}.csv",
        )

    logger.info("Done.")


if __name__ == "__main__":
    main()
