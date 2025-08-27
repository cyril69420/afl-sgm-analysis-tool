# scripts/silver/utils.py
from __future__ import annotations
import hashlib
import re
from typing import Iterable

def _norm(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def sha1_key(parts: Iterable[str]) -> str:
    key = "||".join(_norm(p) for p in parts if p is not None)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def team_id(team_name_canon: str) -> str:
    return sha1_key(["team", team_name_canon])

def player_id(player_name_canon: str, dob: str | None = None) -> str:
    return sha1_key(["player", player_name_canon, dob or ""])

def venue_id(venue_name: str) -> str:
    return sha1_key(["venue", venue_name])

def match_id(season: int, round_: int, home_team_id: str, away_team_id: str,
             scheduled_utc: str, venue_id_: str) -> str:
    return sha1_key(["match", str(season), str(round_), home_team_id, away_team_id, scheduled_utc, venue_id_])
