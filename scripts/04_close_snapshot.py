# scripts/04_close_snapshot.py
import csv, os, sys, subprocess, shlex
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pytz
except ImportError:
    pytz = None

ROOT = Path(__file__).resolve().parents[1]

def parse_time(s):
    s = s.replace("Z","")
    for fmt in ["%Y-%m-%d %H:%M","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M","%Y-%m-%dT%H:%M:%S"]:
        try: 
            return datetime.strptime(s, fmt)
        except: 
            pass
    raise ValueError(f"Unrecognized time: {s}")

def main():
    # Prefer SILVER games times (your pipeline already produces silver/games.csv)
    games_csv = ROOT/"silver"/"games.csv"
    if not games_csv.exists():
        print(f"[close] missing {games_csv}", file=sys.stderr); return

    tz = pytz.timezone("Australia/Perth") if pytz else None
    now = datetime.now(tz) if tz else datetime.now()

    with games_csv.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            gid = r.get("game_id", "")
            bounce = r.get("kickoff_local_awst") or r.get("kickoff_utc") or ""
            if not gid or not bounce: 
                continue
            try:
                bt = parse_time(bounce)
                if tz and not bt.tzinfo: bt = tz.localize(bt)
            except: 
                continue
            target = bt - timedelta(minutes=5)
            if abs((now - target).total_seconds()) <= 120:
                # Within ±2min → refresh discovery then snapshot odds
                print(f"[close] refreshing URLs + taking closing snapshot for {gid}")
                subprocess.run([sys.executable, str(ROOT/"scripts"/"bookmakers"/"bronze_discover_event_urls.py")], check=False)
                subprocess.run([sys.executable, str(ROOT/"scripts"/"bronze_ingest_odds.py")], check=False)

if __name__ == "__main__":
    main()
