# scripts/bronze_ingest_games.py
import csv, json, hashlib, re
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

try:
    import pytz
except Exception:
    pytz = None  # optional

ROOT = Path(__file__).resolve().parents[1]
OUT_ROOT = ROOT / "bronze" / "games"
YEARS = (2024, 2025)  # change as needed

import time
import urllib.error

API = "https://api.squiggle.com.au/?q=games;year={year};format=json"

def fetch_games(year: int):
    req = urllib.request.Request(
        API.format(year=year),
        headers={"User-Agent": "sgm-pipeline/1.0 (+https://github.com/your-org/your-repo)"}
    )
    last_err = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read().decode("utf-8"))
            games = data.get("games", [])
            if not isinstance(games, list):
                raise ValueError("Squiggle response missing 'games' list")
            return games
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError) as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch Squiggle games for {year}: {last_err}")
# --- end patch ---
def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")

def _safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

def mk_game_id(g: dict) -> str:
    """
    Robust, repeatable ID:
      Prefer: {year}_ID{id}
      Fallback: {year}_{slug(home)}_{slug(away)}_{unixtime or ms}
      Final fallback: sha1 of {year|home|away|kickoff_utc}
    """
    year = g.get("year") or g.get("season") or ""
    gid = _safe_int(g.get("id"), None)
    if gid is not None:
        return f"{year}_ID{gid}"

    # try other keys some clients expose
    gid = _safe_int(g.get("gameid"), None)
    if gid is not None:
        return f"{year}_ID{gid}"

    home = g.get("h") or g.get("hteam") or ""
    away = g.get("a") or g.get("ateam") or ""
    ut = g.get("unixtime")
    if isinstance(ut, (int, float)):
        return f"{year}_{_slug(home)}_{_slug(away)}_{int(ut)}"
    ms = g.get("date")
    if isinstance(ms, (int, float)):
        return f"{year}_{_slug(home)}_{_slug(away)}_{int(ms)}"

    # last resort: hash
    key = f"{year}|{home}|{away}|{to_utc_iso(g)}"
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
    return f"{year}_{_slug(home)}_{_slug(away)}_{h}"

def to_utc_iso(g: dict) -> str:
    """Return ISO8601 UTC from available fields."""
    ut = g.get("unixtime")
    if isinstance(ut, (int, float)) and ut > 0:
        return datetime.fromtimestamp(int(ut), tz=timezone.utc).isoformat()

    ms = g.get("date")
    if isinstance(ms, (int, float)) and ms > 0:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()

    lt = g.get("localtime")
    tzname = g.get("tz")
    if isinstance(lt, str) and lt:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt_naive = datetime.strptime(lt, fmt)
                break
            except Exception:
                dt_naive = None
        if dt_naive:
            if pytz and tzname:
                try:
                    tz = pytz.timezone(tzname)
                    dt_local = tz.localize(dt_naive)
                    return dt_local.astimezone(timezone.utc).isoformat()
                except Exception:
                    pass
            return dt_naive.replace(tzinfo=timezone.utc).isoformat()
    return ""

def safe_round_raw(g: dict) -> str:
    """Return whatever 'round' info exists (string or number) for transparency."""
    for k in ("round", "roundname", "roundname_short"):
        v = g.get(k)
        if v not in (None, ""):
            return str(v)
    return ""

def to_row(g: dict) -> dict:
    kickoff_utc = to_utc_iso(g)
    home = g.get("h") or g.get("hteam") or ""
    away = g.get("a") or g.get("ateam") or ""
    status = (
        "final" if g.get("complete") == 100
        else ("scheduled" if not g.get("ascore") and not g.get("hscore") else "live")
    )
    return {
        "game_id": mk_game_id(g),
        "season": g.get("year") or g.get("season") or "",
        "round_raw": safe_round_raw(g),   # keep raw round label; don’t assume numeric
        "home": home,
        "away": away,
        "venue": g.get("venue", ""),
        "kickoff_utc": kickoff_utc,
        "kickoff_local_awst": "",   # populate in Silver if needed
        "status": status,
        "home_score": g.get("hscore", ""),
        "away_score": g.get("ascore", ""),
        "source": "squiggle",
    }

def main():
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = OUT_ROOT / f"snapshot_ts={stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "games.csv"

    games = []
    for y in YEARS:
        games.extend(fetch_games(y))

    rows = [to_row(g) for g in games]
    headers = [
        "game_id","season","round_raw","home","away","venue",
        "kickoff_utc","kickoff_local_awst","status",
        "home_score","away_score","source"
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(out_csv)

if __name__ == "__main__":
    main()
