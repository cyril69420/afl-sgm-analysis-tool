# -*- coding: utf-8 -*-
"""
bronze_ingest_weather.py — Bronze weather ingester

Pulls hour-level weather around each fixture's kickoff and writes a bronze snapshot:
  bronze/weather/snapshot_ts=YYYYmmddTHHMMSSZ/weather.csv

Provider strategy
- If VISUAL_CROSSING_API_KEY is set, use Visual Crossing Timeline API
  (single endpoint covers historical + forecast and returns hourly blocks).
- Else, fall back to Open‑Meteo:
  - For dates within the next ~15 days, query the forecast API.
  - For past dates (>=2–5 day delay), query the historical archive API.

Inputs
- Latest fixtures CSV from bronze/games/snapshot_ts=*/games.csv
  Required columns (case-insensitive, first match wins):
    • game_id
    • venue (or venue_name)
    • kickoff_utc / start_time_utc / utc_start / start_utc (ISO8601)

Optional config
- config/venues_map.csv → columns: venue, latitude, longitude, tz
  (script also includes a built‑in default map for common AFL venues)

Env
- VISUAL_CROSSING_API_KEY   (if provided, VC is used as primary provider)

Usage
  python scripts/bronze_ingest_weather.py [--from-days -1] [--to-days 14] \
      [--window-hours 6] [--save-hours 1]

Notes
- We always write hourly rows keyed by (game_id, venue, hour_utc).
- On VC we use datetimeEpoch for unambiguous UTC recovery.
- On Open‑Meteo we request timezone=UTC so the timestamps come out in UTC.
"""
from __future__ import annotations

import csv
import os
import sys
import json
import time
import math
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Iterable, Tuple, Optional

import requests
import pandas as pd

THIS = Path(__file__).resolve()
ROOT = THIS.parents[1]
BRONZE = ROOT / "bronze"
GAMES_ROOT = BRONZE / "games"
WEATHER_ROOT = BRONZE / "weather"
CONFIG = ROOT / "config"
VENUES_CSV = CONFIG / "venues_map.csv"

# ---- Helpers -----------------------------------------------------------------

VENUE_MAP_BUILTIN: Dict[str, Tuple[float, float, str]] = {
    # name → (lat, lon, tz)
    # Coordinates gathered from public sources; override in config/venues_map.csv if needed.
    "melbourne cricket ground": (-37.819954, 144.983398, "Australia/Melbourne"),
    "mcg": (-37.819954, 144.983398, "Australia/Melbourne"),
    "marvel stadium": (-37.816528, 144.947266, "Australia/Melbourne"),
    "docklands stadium": (-37.816528, 144.947266, "Australia/Melbourne"),
    "optus stadium": (-31.95111, 115.88917, "Australia/Perth"),
    "perth stadium": (-31.95111, 115.88917, "Australia/Perth"),
    "adelaide oval": (-34.91558, 138.59619, "Australia/Adelaide"),
    "sydney cricket ground": (-33.891525, 151.224121, "Australia/Sydney"),
    "scg": (-33.891525, 151.224121, "Australia/Sydney"),
    "the gabba": (-27.486099, 153.03743, "Australia/Brisbane"),
    "brisbane cricket ground": (-27.486099, 153.03743, "Australia/Brisbane"),
    "giants stadium": (-33.83917, 151.06733, "Australia/Sydney"),  # Sydney Showground
    "sydney showground stadium": (-33.83917, 151.06733, "Australia/Sydney"),
    "gmhba stadium": (-38.15805, 144.35458, "Australia/Melbourne"),
    "kardinia park": (-38.15805, 144.35458, "Australia/Melbourne"),
    "blundstone arena": (-42.8730, 147.3708, "Australia/Hobart"),
    "bellerive oval": (-42.8730, 147.3708, "Australia/Hobart"),
    "utas stadium": (-41.4207, 147.136, "Australia/Hobart"),  # York Park approx
    "york park": (-41.4207, 147.136, "Australia/Hobart"),
    "heritage bank stadium": (-28.0030, 153.3650, "Australia/Brisbane"),  # Carrara
    "metricon stadium": (-28.0030, 153.3650, "Australia/Brisbane"),
    "mars stadium": (-37.5570, 143.8380, "Australia/Melbourne"),  # Ballarat approx
    "tio stadium": (-12.406, 130.879, "Australia/Darwin"),
    "tio traeger park": (-23.70, 133.87, "Australia/Darwin"),
    "manuka oval": (-35.318, 149.129, "Australia/Sydney"),
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "SGM-AFL/bronze_ingest_weather (+https://example.com) Python/requests",
    "Accept": "application/json, text/plain,*/*",
})


def latest_games_csv() -> Path:
    snaps = sorted(GAMES_ROOT.glob("snapshot_ts=*/games.csv"))
    if not snaps:
        raise SystemExit("[error] no fixtures found under bronze/games/")
    return snaps[-1]


def load_venue_map() -> Dict[str, Tuple[float, float, str]]:
    m = dict(VENUE_MAP_BUILTIN)
    if VENUES_CSV.exists():
        df = pd.read_csv(VENUES_CSV)
        for _, r in df.iterrows():
            name = str(r.get("venue", "")).strip().lower()
            lat = r.get("latitude")
            lon = r.get("longitude")
            tz = str(r.get("tz") or "").strip() or "UTC"
            if name and pd.notna(lat) and pd.notna(lon):
                m[name] = (float(lat), float(lon), tz)
    return m


def parse_kickoff(row: Dict[str, str]) -> Optional[datetime]:
    for k in ("kickoff_utc", "start_time_utc", "utc_start", "start_utc", "commence_time_utc"):
        v = row.get(k) or row.get(k.upper()) or row.get(k.capitalize())
        if v:
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                pass
    return None


def venue_key(v: str) -> str:
    return (v or "").strip().lower()


# ------------------------- Providers -----------------------------------------

def query_visual_crossing(lat: float, lon: float, start: datetime, end: datetime) -> List[Dict[str, object]]:
    key = os.getenv("VISUAL_CROSSING_API_KEY")
    if not key:
        raise RuntimeError("VISUAL_CROSSING_API_KEY not set")
    # VC wants local-date strings; we can pass ISO with time for single-day, or use epoch range + include=hours
    # We'll request [start-1h, end+1h] in case of rounding and extract by epoch.
    loc = f"{lat},{lon}"
    url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{loc}/{start.strftime('%Y-%m-%dT%H:%M:%S')}/{end.strftime('%Y-%m-%dT%H:%M:%S')}"
    )
    params = {
        "key": key,
        "unitGroup": "metric",
        "include": "hours,obs,fcst",
        # request a compact set of elements to keep payloads small
        "elements": ",".join([
            "datetimeEpoch","temp","feelslike","humidity","precip","preciptype",
            "precipprob","windspeed","windgust","winddir","pressure","cloudcover","conditions"
        ]),
    }
    r = SESSION.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    out: List[Dict[str, object]] = []
    for day in data.get("days", []):
        for h in day.get("hours", []):
            epoch = int(h.get("datetimeEpoch"))
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            out.append({
                "provider": "visual_crossing",
                "datetime_utc": dt.isoformat(),
                "temp_c": h.get("temp"),
                "feelslike_c": h.get("feelslike"),
                "humidity_pct": h.get("humidity"),
                "precip_mm": h.get("precip"),
                "precipprob_pct": h.get("precipprob"),
                "preciptype": ",".join(h.get("preciptype", []) if isinstance(h.get("preciptype"), list) else (h.get("preciptype") or [])) if h.get("preciptype") else None,
                "wind_kph": h.get("windspeed"),
                "wind_gust_kph": h.get("windgust"),
                "wind_dir_deg": h.get("winddir"),
                "pressure_hpa": h.get("pressure"),
                "cloudcover_pct": h.get("cloudcover"),
                "conditions": h.get("conditions"),
            })
    return out


def query_open_meteo(lat: float, lon: float, start: datetime, end: datetime) -> List[Dict[str, object]]:
    # Try forecast API if window includes near-future; otherwise archive API
    out: List[Dict[str, object]] = []
    start_date = start.date().isoformat()
    end_date = end.date().isoformat()
    hourly = [
        "temperature_2m","relative_humidity_2m","precipitation","precipitation_probability",
        "wind_speed_10m","wind_gusts_10m","wind_direction_10m","surface_pressure","cloud_cover"
    ]
    common = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly),
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
    }
    # archive endpoint (better for past); forecast endpoint (for next ~16 days)
    base_archive = "https://archive-api.open-meteo.com/v1/archive"
    base_forecast = "https://api.open-meteo.com/v1/forecast"

    def _req(base):
        r = SESSION.get(base, params=common, timeout=30)
        r.raise_for_status()
        j = r.json()
        times = j.get("hourly", {}).get("time", [])
        def _g(k):
            return j.get("hourly", {}).get(k, [None]*len(times))
        for i, t in enumerate(times):
            out.append({
                "provider": "open_meteo",
                "datetime_utc": t + "+00:00",
                "temp_c": _g("temperature_2m")[i],
                "feelslike_c": None,
                "humidity_pct": _g("relative_humidity_2m")[i],
                "precip_mm": _g("precipitation")[i],
                "precipprob_pct": _g("precipitation_probability")[i],
                "preciptype": None,
                "wind_kph": _g("wind_speed_10m")[i],
                "wind_gust_kph": _g("wind_gusts_10m")[i],
                "wind_dir_deg": _g("wind_direction_10m")[i],
                "pressure_hpa": _g("surface_pressure")[i],
                "cloudcover_pct": _g("cloud_cover")[i],
                "conditions": None,
            })

    # If any hour is within next 14 days, prefer forecast for entire span; otherwise archive
    if end - datetime.now(timezone.utc) < timedelta(days=14):
        _req(base_forecast)
    else:
        _req(base_archive)
    return out


# ---- Main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-days", type=int, default=-1, help="lower bound relative to now (UTC) for fixtures")
    ap.add_argument("--to-days", type=int, default=14, help="upper bound relative to now (UTC) for fixtures")
    ap.add_argument("--window-hours", type=int, default=6, help="hours around kickoff to fetch (± window)")
    ap.add_argument("--save-hours", type=int, default=1, help="round and keep 1h buckets")
    args = ap.parse_args()

    games_csv = latest_games_csv()
    df_games = pd.read_csv(games_csv)
    df_games.columns = [c.strip().lower() for c in df_games.columns]

    vkey = None
    for k in ("venue","venue_name"):
        if k in df_games.columns:
            vkey = k
            break
    if not vkey:
        raise SystemExit("[error] fixtures missing venue column")

    tkey = None
    for k in ("kickoff_utc","start_time_utc","utc_start","start_utc","commence_time_utc"):
        if k in df_games.columns:
            tkey = k
            break
    if not tkey:
        raise SystemExit("[error] fixtures missing kickoff time column (UTC)")

    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    from_cut = now + timedelta(days=args.from_days)
    to_cut = now + timedelta(days=args.to_days)

    venues = load_venue_map()

    rows_out: List[Dict[str, object]] = []

    for _, r in df_games.iterrows():
        gid = str(r.get("game_id"))
        if not gid or gid == "nan":
            continue
        venue_name = str(r.get(vkey) or "").strip()
        ko = parse_kickoff({tkey: r.get(tkey)})
        if not venue_name or not ko:
            continue
        if not (from_cut <= ko <= to_cut):
            continue

        vk = venue_key(venue_name)
        latlon = venues.get(vk)
        if not latlon:
            # try a looser match by removing sponsorship words
            simple = vk.replace(" stadium"," ").replace(" oval"," ").replace(" arena"," ").strip()
            latlon = venues.get(simple)
        if not latlon:
            print(f"[warn] no lat/lon for venue: {venue_name}")
            continue

        lat, lon, tz = latlon
        start = ko - timedelta(hours=args.window_hours)
        end = ko + timedelta(hours=args.window_hours)

        try:
            if os.getenv("VISUAL_CROSSING_API_KEY"):
                hours = query_visual_crossing(lat, lon, start, end)
            else:
                hours = query_open_meteo(lat, lon, start, end)
        except Exception as e:
            print(f"[warn] weather fetch failed for {venue_name} ({lat},{lon}) -> {e}")
            continue

        for h in hours:
            # Keep only within [start,end]
            dt = datetime.fromisoformat(str(h["datetime_utc"]))
            if not (start <= dt <= end):
                continue
            rows_out.append({
                "ts": now.isoformat(),
                "game_id": gid,
                "venue": venue_name,
                "latitude": lat,
                "longitude": lon,
                "datetime_utc": dt.isoformat(),
                **h
            })

    if not rows_out:
        raise SystemExit("[error] no weather rows collected — check fixtures window and provider keys")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = WEATHER_ROOT / f"snapshot_ts={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "weather.csv"

    cols = [
        "ts","game_id","venue","latitude","longitude","datetime_utc",
        "provider","temp_c","feelslike_c","humidity_pct","precip_mm","precipprob_pct","preciptype",
        "wind_kph","wind_gust_kph","wind_dir_deg","pressure_hpa","cloudcover_pct","conditions"
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in sorted(rows_out, key=lambda x: (x["game_id"], x["datetime_utc"])):
            w.writerow({k: row.get(k) for k in cols})

    print(str(out_csv))


if __name__ == "__main__":
    main()
