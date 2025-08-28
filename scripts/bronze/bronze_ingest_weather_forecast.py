#!/usr/bin/env python
"""
Bronze: ingest hourly weather forecasts from Open-Meteo (no API key).

- Reads fixtures strictly under bronze/fixtures/season={season}/round=*/
- Geocodes venues via Open-Meteo Geocoding API (with AFL-specific aliasing & fallbacks)
- Pulls hourly forecast (up to 16 days) and filters to ±6h around kickoff
- Writes hive-partitioned parquet bronze/weather/forecast/season=…/round=…/venue=…

Columns:
  provider, venue, lat, lon, tz, captured_at_utc, forecast_time_utc,
  season, round, game_key, temp_c, wind_speed_ms, wind_gust_ms,
  rain_prob (0..1), pressure_hpa, cloud_pct, raw_payload (dict)
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Any

import requests

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None  # type: ignore

try:
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover
    pl = None  # type: ignore


# ---------------------------
# AFL venue aliasing & known coords (fallbacks)
# ---------------------------

def _norm(name: str) -> str:
    s = (name or "").strip()
    s = s.casefold()
    if s.startswith("the "):
        s = s[4:]
    return " ".join(s.split())  # collapse whitespace


# --- AFL venues used in 2025 (premiership season) ---

VENUE_ALIASES: dict[str, str] = {
    # Melbourne / VIC
    "mcg": "Melbourne Cricket Ground",
    "the mcg": "Melbourne Cricket Ground",
    "melbourne cricket ground": "Melbourne Cricket Ground",

    "marvel": "Marvel Stadium",
    "docklands": "Marvel Stadium",
    "etihad stadium": "Marvel Stadium",
    "telstra dome": "Marvel Stadium",
    "docklands stadium": "Marvel Stadium",
    "marvel stadium": "Marvel Stadium",

    "gmhba": "GMHBA Stadium",
    "kardinia park": "GMHBA Stadium",
    "g.m.h.b.a stadium": "GMHBA Stadium",

    "mars stadium": "Mars Stadium",
    "eureka stadium": "Mars Stadium",

    # Adelaide / SA
    "adelaide oval": "Adelaide Oval",

    "norwood oval": "Norwood Oval",
    "coopers stadium (norwood)": "Norwood Oval",  # avoid clash with A-League Hindmarsh
    "coopers stadium - norwood": "Norwood Oval",

    "barossa park": "Lyndoch Recreation Park",
    "lyndoch recreation park": "Lyndoch Recreation Park",

    # Sydney / NSW & ACT
    "scg": "Sydney Cricket Ground",
    "the scg": "Sydney Cricket Ground",
    "sydney cricket ground": "Sydney Cricket Ground",

     # Sydney (Showground)
    "engie stadium": "Sydney Showground Stadium",
    "Engie stadium": "Sydney Showground Stadium",
    "ENGIE stadium": "Sydney Showground Stadium",
    "giants stadium": "Sydney Showground Stadium",
    "sydney showground": "Sydney Showground Stadium",
    "sydney showground stadium": "Sydney Showground Stadium",

    "manuka oval": "Manuka Oval",

    # Queensland
    "the gabba": "The Gabba",
    "gabba": "The Gabba",
    "brisbane cricket ground": "The Gabba",

    "people first stadium": "People First Stadium",
    "heritage bank stadium": "People First Stadium",
    "metricon stadium": "People First Stadium",
    "carrara stadium": "People First Stadium",

    # Western Australia
    "optus stadium": "Optus Stadium",
    "perth stadium": "Optus Stadium",

    # Tasmania
    "utas stadium": "University of Tasmania Stadium",
    "york park": "University of Tasmania Stadium",
    "university of tasmania stadium": "University of Tasmania Stadium",

    "blundstone arena": "Blundstone Arena",
    "bellerive oval": "Blundstone Arena",
    "ninja stadium": "Blundstone Arena",  # branding seen in NMFC Hobart comms (2025)

    # Northern Territory
    "tio stadium": "TIO Stadium",
    "marrara oval": "TIO Stadium",

    "tio traeger park": "TIO Traeger Park",
    "traeger park": "TIO Traeger Park",
}

KNOWN_VENUES: dict[str, dict[str, object]] = {
    # VIC
    "Melbourne Cricket Ground": {
        "lat": -37.819967, "lon": 144.983449, "tz": "Australia/Melbourne"
    },  # :contentReference[oaicite:0]{index=0}
    "Marvel Stadium": {
        "lat": -37.816528, "lon": 144.947266, "tz": "Australia/Melbourne"
    },  # :contentReference[oaicite:1]{index=1}
    "GMHBA Stadium": {
        "lat": -38.158050, "lon": 144.354580, "tz": "Australia/Melbourne"
    },  # :contentReference[oaicite:2]{index=2}
    "Mars Stadium": {
        "lat": -37.539440, "lon": 143.848060, "tz": "Australia/Melbourne"
    },  # :contentReference[oaicite:3]{index=3}

    # SA
    "Adelaide Oval": {
        "lat": -34.909330, "lon": 138.591000, "tz": "Australia/Adelaide"
    },  # :contentReference[oaicite:4]{index=4}
    "Norwood Oval": {
        "lat": -34.919720, "lon": 138.630560, "tz": "Australia/Adelaide"
    },  # :contentReference[oaicite:5]{index=5}
    "Lyndoch Recreation Park": {
        "lat": -34.598246, "lon": 138.885585, "tz": "Australia/Adelaide"
    },  # :contentReference[oaicite:6]{index=6}

    # NSW / ACT
    "Sydney Cricket Ground": {
        "lat": -33.891525, "lon": 151.224121, "tz": "Australia/Sydney"
    },  # :contentReference[oaicite:7]{index=7}
    "Sydney Showground Stadium": {
        "lat": -33.845560, "lon": 151.068060, "tz": "Australia/Sydney"
    },  # :contentReference[oaicite:8]{index=8}
    "Manuka Oval": {
        "lat": -35.317500, "lon": 149.134200, "tz": "Australia/Sydney"
    },  # :contentReference[oaicite:9]{index=9}

    # QLD
    "The Gabba": {
        "lat": -27.486099, "lon": 153.037430, "tz": "Australia/Brisbane"
    },  # :contentReference[oaicite:10]{index=10}
    "People First Stadium": {
        "lat": -28.009000, "lon": 153.366000, "tz": "Australia/Brisbane"
    },  # :contentReference[oaicite:11]{index=11}

    # WA
    "Optus Stadium": {
        "lat": -31.951110, "lon": 115.889170, "tz": "Australia/Perth"
    },  # :contentReference[oaicite:12]{index=12}

    # TAS
    "University of Tasmania Stadium": {
        "lat": -41.424100, "lon": 147.137400, "tz": "Australia/Hobart"
    },  # :contentReference[oaicite:13]{index=13}
    "Blundstone Arena": {
        "lat": -42.873000, "lon": 147.370800, "tz": "Australia/Hobart"
    },  # :contentReference[oaicite:14]{index=14}

    # NT
    "TIO Stadium": {
        "lat": -12.392800, "lon": 130.885700, "tz": "Australia/Darwin"
    },  # :contentReference[oaicite:15]{index=15}
    "TIO Traeger Park": {
        "lat": -23.705830, "lon": 133.872330, "tz": "Australia/Darwin"
    },  # :contentReference[oaicite:16]{index=16}
}


# canonical name (normalized) -> lat/lon/tz fallback
KNOWN_VENUES = {
    _norm("ENGIE Stadium"):             {"lat": -33.845560, "lon": 151.068060, "tz": "Australia/Sydney"},
    _norm("Melbourne Cricket Ground"):  {"lat": -37.819954, "lon": 144.983398, "tz": "Australia/Melbourne"},
    _norm("Optus Stadium"):             {"lat": -31.951110, "lon": 115.889170, "tz": "Australia/Perth"},
    _norm("Adelaide Oval"):             {"lat": -34.915580, "lon": 138.596190, "tz": "Australia/Adelaide"},
    _norm("Marvel Stadium"):            {"lat": -37.816528, "lon": 144.947266, "tz": "Australia/Melbourne"},
    _norm("GMHBA Stadium"):             {"lat": -38.158050, "lon": 144.354580, "tz": "Australia/Melbourne"},
    _norm("Sydney Cricket Ground"):     {"lat": -33.891525, "lon": 151.224121, "tz": "Australia/Sydney"},
    _norm("Manuka Oval"):               {"lat": -35.317500, "lon": 149.134200, "tz": "Australia/Sydney"},
    _norm("University of Tasmania Stadium"): {"lat": -41.424100, "lon": 147.137400, "tz": "Australia/Hobart"},
    _norm("Blundstone Arena"):          {"lat": -42.873000, "lon": 147.370800, "tz": "Australia/Hobart"},
    _norm("TIO Stadium"):               {"lat": -12.392800, "lon": 130.885700, "tz": "Australia/Darwin"},
    _norm("TIO Traeger Park"):          {"lat": -23.705830, "lon": 133.872330, "tz": "Australia/Darwin"},
    _norm("Norwood Oval"):              {"lat": -34.919720, "lon": 138.630560, "tz": "Australia/Adelaide"},
    _norm("Lyndoch Recreation Park"):   {"lat": -34.598246, "lon": 138.885585, "tz": "Australia/Adelaide"},
    _norm("Mars Stadium"):              {"lat": -37.539440, "lon": 143.848060, "tz": "Australia/Melbourne"},
    _norm("People First Stadium"):      {"lat": -28.009000, "lon": 153.366000, "tz": "Australia/Brisbane"},
}

# ---------------------------
# Model
# ---------------------------
@dataclass
class BronzeWeatherRow:
    provider: str
    venue: str
    lat: float
    lon: float
    tz: str
    captured_at_utc: str
    forecast_time_utc: str
    season: int
    round: str
    game_key: str
    temp_c: Optional[float]
    wind_speed_ms: Optional[float]
    wind_gust_ms: Optional[float]
    rain_prob: Optional[float]       # unit interval 0..1
    pressure_hpa: Optional[float]
    cloud_pct: Optional[float]
    raw_payload: dict


# ---------------------------
# Logging
# ---------------------------
def _setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


# ---------------------------
# Paths/utilities
# ---------------------------
def _repo_root() -> Path:
    # <repo>/scripts/bronze/this_file.py -> <repo>
    return Path(__file__).resolve().parents[2]


def _season_fixture_files(bronze_dir: Path, season: int) -> List[Path]:
    # Only read hive partitions to avoid mismatches
    base = bronze_dir / "fixtures" / f"season={season}"
    patterns = [
        base.glob("round=*/**/*.parquet"),
        base.glob("round=*/*.parquet"),
        base.glob("round=*/data_*.parquet"),
    ]
    files: List[Path] = []
    for it in patterns:
        files.extend([p for p in it if p.is_file()])
    files = sorted(set(files))
    if not files:
        raise FileNotFoundError(f"No fixtures parquet found under {base}")
    return files


def _read_fixtures(bronze_dir: Path, season: int) -> "pl.DataFrame":
    if duckdb is None:
        raise RuntimeError("duckdb is required")
    if pl is None:
        raise RuntimeError("polars is required")

    files = _season_fixture_files(bronze_dir, season)
    # Build a safe DuckDB list literal using single quotes
    file_list_sql = json.dumps([str(f) for f in files]).replace('"', "'")
    con = duckdb.connect()
    pdf = con.execute(
        f"SELECT * FROM read_parquet({file_list_sql}, hive_partitioning=1)"
    ).fetch_df()
    return pl.from_pandas(pdf)


def _to_utc_dt(value: Any) -> Optional[datetime]:
    """Best-effort conversion to timezone-aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# ---------------------------
# Open-Meteo integration
# ---------------------------
def _canonical_query_name(raw_name: str) -> str:
    n = _norm(raw_name)
    # map aliases to canonical
    if n in VENUE_ALIASES:
        return VENUE_ALIASES[n]
    # otherwise try as-is (capitalize words for nicer caching)
    return raw_name


def _geocode_venue(name: str, session: requests.Session) -> Optional[dict]:
    """
    Resolve venue name to lat/lon/tz. Strategy:
    1) Normalize and alias (e.g., 'the mcg' -> 'Melbourne Cricket Ground')
    2) Open-Meteo geocoder with AU filter
    3) Retry without countrycode
    4) Fallback to KNOWN_VENUES table
    """
    q = _canonical_query_name(name)
    # 2) with AU filter
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": q, "count": 1, "language": "en", "format": "json", "countrycode": "AU"}
    try:
        r = session.get(url, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        results = js.get("results") or []
        if results:
            top = results[0]
            return {
                "name": top.get("name") or q,
                "lat": float(top["latitude"]),
                "lon": float(top["longitude"]),
                "tz": top.get("timezone") or "UTC",
            }
    except Exception:
        pass

    # 3) retry without countrycode
    try:
        r = session.get(url, params={"name": q, "count": 1, "language": "en", "format": "json"}, timeout=20)
        r.raise_for_status()
        js = r.json()
        results = js.get("results") or []
        if results:
            top = results[0]
            return {
                "name": top.get("name") or q,
                "lat": float(top["latitude"]),
                "lon": float(top["longitude"]),
                "tz": top.get("timezone") or "UTC",
            }
    except Exception:
        pass

    # 4) fallback to known coords
    cn = _norm(q)
    if cn in KNOWN_VENUES:
        kv = KNOWN_VENUES[cn]
        return {"name": q, "lat": kv["lat"], "lon": kv["lon"], "tz": kv["tz"]}

    logging.getLogger("weather_forecast").warning("Geocoding failed for %s (q=%s)", name, q)
    return None


def _fetch_forecast(lat: float, lon: float, session: requests.Session) -> dict:
    """
    Open-Meteo hourly forecast: request up to 16 days ahead.
    IMPORTANT: Do NOT combine start/end with forecast_days (400 error).
    """
    # https://open-meteo.com/en/docs
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m",
            "precipitation_probability",
            "pressure_msl",
            "cloud_cover",
            "wind_speed_10m",
            "wind_gusts_10m",
        ]),
        "windspeed_unit": "ms",
        "timezone": "UTC",
        "forecast_days": 16,  # do not set start_date/end_date together with this
    }
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _safe(seq: list, idx: int) -> Optional[float]:
    try:
        v = seq[idx]
        return None if v is None else float(v)
    except Exception:
        return None


def _pct_to_unit(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x) / 100.0
    except Exception:
        return None


def _rows_from_forecast(js: dict,
                        venue: str, season: int, round_str: str, game_key: str,
                        capture_ts: datetime, window_start: datetime, window_end: datetime,
                        lat: float, lon: float, tz: str) -> Iterable[BronzeWeatherRow]:
    hourly = (js or {}).get("hourly") or {}
    times = hourly.get("time") or []
    temp = hourly.get("temperature_2m") or []
    pprob = hourly.get("precipitation_probability") or []
    wspd = hourly.get("wind_speed_10m") or []
    wgst = hourly.get("wind_gusts_10m") or []
    pres = hourly.get("pressure_msl") or []
    cld = hourly.get("cloud_cover") or []

    for i, t in enumerate(times):
        # Open-Meteo returns ISO8601 in UTC; normalize defensively
        ft = datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc)
        if not (window_start <= ft <= window_end):
            continue

        yield BronzeWeatherRow(
            provider="open-meteo",
            venue=venue,
            lat=lat,
            lon=lon,
            tz=tz,
            captured_at_utc=capture_ts.isoformat(),
            forecast_time_utc=ft.isoformat(),
            season=season,
            round=str(round_str),
            game_key=game_key,
            temp_c=_safe(temp, i),
            wind_speed_ms=_safe(wspd, i),
            wind_gust_ms=_safe(wgst, i),
            rain_prob=_pct_to_unit(_safe(pprob, i)),
            pressure_hpa=_safe(pres, i),
            cloud_pct=_safe(cld, i),
            raw_payload={"elevation": js.get("elevation"), "generationtime_ms": js.get("generationtime_ms")},
        )


# ---------------------------
# Writes
# ---------------------------
def _write_parquet(rows: List[BronzeWeatherRow], out_dir: Path, overwrite: bool) -> None:
    if not rows:
        return
    if duckdb is None:
        raise RuntimeError("duckdb is required")
    out_dir.mkdir(parents=True, exist_ok=True)

    import pandas as pd
    pdf = pd.DataFrame([asdict(r) for r in rows])

    con = duckdb.connect()
    con.register("rows", pdf)
    opts = "FORMAT PARQUET, PARTITION_BY (season, round, venue)"
    if overwrite:
        opts += ", OVERWRITE_OR_IGNORE"
    tgt = str(out_dir).replace("'", "''")
    con.execute(f"COPY rows TO '{tgt}' ({opts})")
    con.unregister("rows")


def _write_csv(rows: List[BronzeWeatherRow], mirror_dir: Path, season: int) -> None:
    if not rows:
        return
    mirror_dir.mkdir(parents=True, exist_ok=True)
    out = mirror_dir / f"weather_forecast_{season}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


# ---------------------------
# Main
# ---------------------------
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Bronze: ingest weather forecasts from Open-Meteo.")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--round", type=str, default=None, help="Optional: R1, R2, finals_w1, etc.")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--csv-mirror", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of fixtures processed")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)

    _setup_logging(args.log_level)
    log = logging.getLogger("weather_forecast")

    root = _repo_root()
    bronze_dir = root / "bronze"

    # Read fixtures (hive partitions only)
    fixtures = _read_fixtures(bronze_dir, args.season)
    if args.round:
        fixtures = fixtures.filter(pl.col("round") == str(args.round))  # type: ignore

    # Build time horizon (we still filter in-code to game window)
    now_utc = datetime.now(timezone.utc)
    start_window = now_utc - timedelta(days=1)
    end_window = now_utc + timedelta(days=16)  # Open-Meteo supports up to 16 days of forecast

    records = list(fixtures.iter_rows(named=True))  # type: ignore
    if args.limit:
        records = records[: args.limit]
    log.info("Loaded %d fixture rows (pre-window filter)", len(records))

    session = requests.Session()
    session.headers.update({"User-Agent": "afl-sgm-weather/1.1 (+github.com/cyril69420/afl-sgm-analysis-tool)"})

    # simple venue geocode cache
    cache_path = root / ".cache" / "geocode_venues.json"
    cache: Dict[str, dict] = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    rows: List[BronzeWeatherRow] = []
    for rec in records:
        venue_raw = rec["venue"]
        round_str = rec["round"]
        game_key = rec["game_key"]
        kick_ts = _to_utc_dt(rec.get("scheduled_time_utc"))
        if not kick_ts:
            log.warning("Skipping %s (unable to parse scheduled_time_utc)", game_key)
            continue
        # only process fixtures inside our forecast horizon
        if not (start_window <= kick_ts <= end_window):
            continue

        cache_key = _canonical_query_name(venue_raw)
        geo = cache.get(cache_key) or _geocode_venue(venue_raw, session)
        if not geo:
            log.warning("Skipping %s (no geocode for venue=%r)", game_key, venue_raw)
            continue
        cache[cache_key] = geo
        cache_path.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")

        lat, lon, tz = geo["lat"], geo["lon"], geo.get("tz", "UTC")

        # Pull forecast (full 16 days), then filter to ±6h around KO
        js = _fetch_forecast(lat, lon, session)
        window_start = (kick_ts - timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)
        window_end = (kick_ts + timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)

        for r in _rows_from_forecast(
            js=js,
            venue=cache_key,
            season=int(args.season),
            round_str=str(round_str),
            game_key=game_key,
            capture_ts=now_utc,
            window_start=window_start,
            window_end=window_end,
            lat=lat,
            lon=lon,
            tz=tz,
        ):
            rows.append(r)

    log.info("Prepared %d forecast rows", len(rows))

    if args.dry_run:
        log.info("DRY RUN complete; no writes.")
        if rows:
            log.info("Sample: %s", rows[:3])
        return 0

    out_dir = bronze_dir / "weather" / "forecast"
    _write_parquet(rows, out_dir, overwrite=args.overwrite)
    log.info("Wrote weather forecast → %s", out_dir)

    if args.csv_mirror:
        _write_csv(rows, root / "bronze_csv_mirror" / "weather" / "forecast", args.season)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
