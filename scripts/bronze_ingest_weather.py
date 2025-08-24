# -*- coding: utf-8 -*-
"""
bronze_ingest_weather.py — Drop‑in v1.1 (Visual Crossing → Open‑Meteo fallback + venue synonyms)

What this does
- Reads latest fixtures from bronze/games/*/games.csv
- Resolves venue → (lat,lon) using a curated alias map + optional config/venues_map.csv
- Pulls hourly weather for a window around kickoff (default ±6h)
  • Primary: Visual Crossing Timeline API (if VISUAL_CROSSING_API_KEY is a real key)
  • Fallback: Open‑Meteo (forecast + archive as needed)
- Writes bronze/weather/snapshot_ts=YYYYmmddTHHMMSSZ/weather.csv

Notes
- If your VC key is unset or a placeholder (e.g. "YOUR_KEY"), we auto‑fallback to Open‑Meteo.
- You can extend/override venue coordinates via config/venues_map.csv with columns:
    alias,canonical,lat,lon
- Timestamps are ISO8601 in UTC to align with your bronze fixtures.

Usage
  python scripts/bronze_ingest_weather.py --from-days -1 --to-days 14 --window-hours 6

Env
  VISUAL_CROSSING_API_KEY  (optional)

Output columns
  ts, game_id, venue_name, lat, lon, datetime_utc,
  temp_c, feelslike_c, humidity_pct, precip_mm, precip_prob_pct,
  windspeed_kph, windgust_kph, winddir_deg, pressure_hpa, cloudcover_pct,
  conditions, provider
"""
from __future__ import annotations

import csv
import io
import os
import sys
import math
import json
import time
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

THIS = Path(__file__).resolve()
ROOT = THIS.parents[1]
BRONZE = ROOT / "bronze"
GAMES = BRONZE / "games"
WEATHER_ROOT = BRONZE / "weather"
CONFIG = ROOT / "config"
VENUES_CSV = CONFIG / "venues_map.csv"

# ---------------- Venue map & synonyms -----------------

# Canonical venue coordinates (lowercased canonical name → (lat, lon))
# Sources: public stadium coords (club/venue pages). Keep approximate stadium centroid.
VENUE_COORDS: Dict[str, Tuple[float, float]] = {
    # VIC
    "melbourne cricket ground": (-37.8199, 144.9834),
    "mcg": (-37.8199, 144.9834),
    "marvel stadium": (-37.8163, 144.9475),
    "docklands stadium": (-37.8163, 144.9475),
    "princes park": (-37.7833, 144.9617),
    "mars stadium": (-37.5517, 143.8065),
    "gmhba stadium": (-38.1549, 144.3548),
    "kardinia park": (-38.1549, 144.3548),
    # NSW/ACT
    "giants stadium": (-33.8463, 151.0674),  # sydney showground, olympic park
    "sydney cricket ground": (-33.8916, 151.2248),
    "scg": (-33.8916, 151.2248),
    "manuka oval": (-35.3188, 149.1322),
    # QLD
    "brisbane cricket ground": (-27.4861, 153.0390),  # gabba
    "the gabba": (-27.4861, 153.0390),
    "heritage bank stadium": (-28.0025, 153.3669),  # carrara / metricon / people first
    # SA
    "adelaide oval": (-34.9156, 138.5961),
    # WA
    "optus stadium": (-31.9511, 115.8892),
    "perth stadium": (-31.9511, 115.8892),
    # TAS/NT
    "blundstone arena": (-42.8806, 147.3702),  # bellerive
    "utas stadium": (-41.4388, 147.1372),  # york park
    "traeger park": (-23.7003, 133.8800),
    "tio stadium": (-12.3929, 130.8935),
    "cazaly's stadium": (-16.9347, 145.7434),
}

# Common fixture name variants → canonical key
ALIASES: Dict[str, str] = {
    # MCG variants
    "m.c.g.": "melbourne cricket ground",
    "mcg": "melbourne cricket ground",
    # Docklands / Marvel
    "docklands": "marvel stadium",
    "docklands stadium": "marvel stadium",
    "etihad stadium": "marvel stadium",
    # Sydney Showground / GIANTS
    "sydney showground": "giants stadium",
    "sydney showground stadium": "giants stadium",
    "engie stadium": "giants stadium",
    # Gabba / Carrara
    "gabba": "brisbane cricket ground",
    "carrara": "heritage bank stadium",
    "metricon stadium": "heritage bank stadium",
    "people first stadium": "heritage bank stadium",
    # Other common shortenings
    "scg": "sydney cricket ground",
    "optus": "optus stadium",
}


def load_custom_venues() -> None:
    if VENUES_CSV.exists():
        try:
            df = pd.read_csv(VENUES_CSV)
            for _, r in df.iterrows():
                alias = str(r.get("alias", "")).strip().lower()
                canon = str(r.get("canonical", "")).strip().lower() or alias
                try:
                    lat = float(r.get("lat"))
                    lon = float(r.get("lon"))
                except Exception:
                    continue
                if alias:
                    ALIASES[alias] = canon
                VENUE_COORDS[canon] = (lat, lon)
        except Exception:
            pass


# ---------------- Fixtures helpers -----------------

FIXTURE_TIME_KEYS = [
    "start_time_utc", "kickoff_utc", "utc_kickoff", "match_time_utc", "utc_start",
]
VENUE_KEYS = ["venue", "venue_name", "ground", "stadium"]


def latest_games_csv() -> Path:
    snaps = sorted(GAMES.glob("snapshot_ts=*/games.csv"))
    if not snaps:
        raise SystemExit("[error] no fixtures under bronze/games/ — run bronze_ingest_games.py")
    return snaps[-1]


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k in cols:
            return cols[k]
    return None


# ---------------- Provider: Visual Crossing -----------------

VC_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}/{start}/{end}"
VC_ELEMENTS = "datetimeEpoch,temp,feelslike,humidity,precip,preciptype,precipprob,windspeed,windgust,winddir,pressure,cloudcover,conditions"


def query_visual_crossing(lat: float, lon: float, start_iso: str, end_iso: str, key: str) -> List[Dict[str, object]]:
    url = VC_BASE.format(lat=lat, lon=lon, start=start_iso, end=end_iso)
    params = {
        "key": key,
        "unitGroup": "metric",
        "include": "hours,obs,fcst",
        "elements": VC_ELEMENTS,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    hours = []
    # VC returns days -> hours; also may include currentConditions.
    for day in data.get("days", []):
        for h in day.get("hours", []):
            hours.append(h)
    out = []
    for h in hours:
        ts = int(h.get("datetimeEpoch"))
        out.append({
            "datetime_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "temp_c": h.get("temp"),
            "feelslike_c": h.get("feelslike"),
            "humidity_pct": h.get("humidity"),
            "precip_mm": h.get("precip"),
            "precip_prob_pct": h.get("precipprob"),
            "windspeed_kph": h.get("windspeed"),
            "windgust_kph": h.get("windgust"),
            "winddir_deg": h.get("winddir"),
            "pressure_hpa": h.get("pressure"),
            "cloudcover_pct": h.get("cloudcover"),
            "conditions": h.get("conditions"),
        })
    return out


# ---------------- Provider: Open‑Meteo -----------------

OM_FORECAST = "https://api.open-meteo.com/v1/forecast"
OM_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OM_HOURLY = [
    "temperature_2m","apparent_temperature","relative_humidity_2m",
    "precipitation","rain","showers","snowfall","weathercode",
    "windspeed_10m","windgusts_10m","winddirection_10m",
    "surface_pressure","cloudcover",
]


def query_open_meteo(lat: float, lon: float, start: datetime, end: datetime) -> List[Dict[str, object]]:
    now = datetime.now(timezone.utc)
    out: List[Dict[str, object]] = []

    def call(endpoint: str, params: Dict[str, object]) -> Dict[str, object]:
        r = requests.get(endpoint, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def to_rows(payload: Dict[str, object]) -> List[Dict[str, object]]:
        times = payload.get("hourly", {}).get("time", [])
        rows = []
        for i, t in enumerate(times):
            row = {"datetime_utc": datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()}
            # map fields if present
            def g(k):
                arr = payload.get("hourly", {}).get(k)
                return arr[i] if isinstance(arr, list) and i < len(arr) else None
            row.update({
                "temp_c": g("temperature_2m"),
                "feelslike_c": g("apparent_temperature"),
                "humidity_pct": g("relative_humidity_2m"),
                "precip_mm": g("precipitation"),
                "precip_prob_pct": None,  # OM no prob in archive; forecast has precipitation_probability but optional
                "windspeed_kph": g("windspeed_10m"),
                "windgust_kph": g("windgusts_10m"),
                "winddir_deg": g("winddirection_10m"),
                "pressure_hpa": g("surface_pressure"),
                "cloudcover_pct": g("cloudcover"),
                "conditions": g("weathercode"),
            })
            rows.append(row)
        return rows

    # Split ranges: past (archive) vs future (forecast). Allow a 24h buffer.
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    cutoff = now - timedelta(hours=12)

    # 1) Past: use archive if any portion is strictly before cutoff
    if start < cutoff:
        a_start = start.date().isoformat()
        a_end = min(end, cutoff).date().isoformat()
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": a_start,
            "end_date": a_end,
            "timezone": "UTC",
            "hourly": ",".join(OM_HOURLY),
        }
        payload = call(OM_ARCHIVE, params)
        out += to_rows(payload)

    # 2) Future/near‑present: use forecast
    if end > cutoff:
        # "past_days" allows recent history in forecast payload
        forecast_days = 16
        past_days = min(7, max(0, (cutoff.date() - start.date()).days))
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(OM_HOURLY + ["precipitation_probability"]),
            "forecast_days": forecast_days,
            "past_days": past_days,
            "timezone": "UTC",
        }
        payload = call(OM_FORECAST, params)
        rows = to_rows(payload)
        # overwrite precip_prob if available
        if payload.get("hourly", {}).get("precipitation_probability"):
            probs = payload["hourly"]["precipitation_probability"]
            for i in range(min(len(rows), len(probs))):
                rows[i]["precip_prob_pct"] = probs[i]
        out += rows

    # Trim to [start,end]
    def within(r):
        t = datetime.fromisoformat(r["datetime_utc"]).astimezone(timezone.utc)
        return start <= t <= end
    out = [r for r in out if within(r)]
    # De‑duplicate by timestamp
    seen = set()
    uniq = []
    for r in out:
        k = r["datetime_utc"]
        if k not in seen:
            uniq.append(r)
            seen.add(k)
    return uniq


# ---------------- Main -----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-days", type=int, default=-1, help="relative days from now (UTC) for earliest fixture")
    ap.add_argument("--to-days", type=int, default=14, help="relative days from now (UTC) for latest fixture")
    ap.add_argument("--window-hours", type=int, default=6, help="hours before/after kickoff to fetch")
    args = ap.parse_args()

    load_custom_venues()

    games_csv = latest_games_csv()
    df = pd.read_csv(games_csv)
    # normalise columns
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    tkey = pick_col(df, FIXTURE_TIME_KEYS)
    vkey = pick_col(df, VENUE_KEYS)

    if not (tkey and vkey and "game_id" in df.columns):
        raise SystemExit("[error] games.csv missing required columns (game_id, venue, kickoff_utc)")

    # filter fixtures window
    now = datetime.now(timezone.utc)
    start = now + timedelta(days=args.from_days)
    end = now + timedelta(days=args.to_days)

    def parse_ts(s: str) -> Optional[datetime]:
        s = str(s).strip()
        if not s:
            return None
        # support both '2025-08-23T10:29:07+00:00' and '2025-08-23 10:29:07'
        try:
            if 'T' in s:
                return datetime.fromisoformat(s.replace('Z','+00:00')).astimezone(timezone.utc)
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            try:
                return pd.to_datetime(s, utc=True).to_pydatetime().replace(tzinfo=timezone.utc)
            except Exception:
                return None

    rows = []
    for _, r in df.iterrows():
        gid = str(r.get("game_id"))
        venue_raw = str(r.get(vkey) or "").strip()
        kickoff = parse_ts(r.get(tkey))
        if not (gid and venue_raw and kickoff):
            continue
        if not (start <= kickoff <= end):
            continue
        # resolve venue
        key = venue_raw.strip().lower()
        canon = ALIASES.get(key, key)
        coords = VENUE_COORDS.get(canon)
        if not coords:
            print(f"[warn] no lat/lon for venue: {venue_raw}")
            continue
        lat, lon = coords
        t0 = (kickoff - timedelta(hours=args.window_hours)).astimezone(timezone.utc)
        t1 = (kickoff + timedelta(hours=args.window_hours)).astimezone(timezone.utc)

        # provider selection
        vc_key = os.getenv("VISUAL_CROSSING_API_KEY")
        use_vc = bool(vc_key) and str(vc_key).upper() not in {"YOUR_KEY", "REPLACE_ME"}
        try:
            if use_vc:
                hours = query_visual_crossing(lat, lon, t0.isoformat(), t1.isoformat(), vc_key)
                provider = "visual_crossing"
            else:
                hours = query_open_meteo(lat, lon, t0, t1)
                provider = "open_meteo"
        except Exception as e:
            # fallback if VC fails → OM
            if use_vc:
                try:
                    hours = query_open_meteo(lat, lon, t0, t1)
                    provider = "open_meteo"
                except Exception as e2:
                    print(f"[warn] weather fetch failed for {venue_raw} ({lat},{lon}) -> {e2}")
                    continue
            else:
                print(f"[warn] weather fetch failed for {venue_raw} ({lat},{lon}) -> {e}")
                continue

        for h in hours:
            rows.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "game_id": gid,
                "venue_name": venue_raw,
                "lat": lat,
                "lon": lon,
                **h,
                "provider": provider,
            })

    if not rows:
        raise SystemExit("[error] no weather rows collected — check fixtures window and provider keys")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = WEATHER_ROOT / f"snapshot_ts={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "weather.csv"

    cols = [
        "ts","game_id","venue_name","lat","lon","datetime_utc",
        "temp_c","feelslike_c","humidity_pct","precip_mm","precip_prob_pct",
        "windspeed_kph","windgust_kph","winddir_deg","pressure_hpa","cloudcover_pct",
        "conditions","provider",
    ]

    df_out = pd.DataFrame(rows)
    # ensure all columns present
    for c in cols:
        if c not in df_out.columns:
            df_out[c] = pd.NA
    df_out = df_out[cols].sort_values(["game_id","datetime_utc"]).reset_index(drop=True)
    df_out.to_csv(out_csv, index=False)
    print(str(out_csv))


if __name__ == "__main__":
    main()
