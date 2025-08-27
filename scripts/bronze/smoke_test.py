import importlib.util as imps
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

# List of (module_path, extra_args)
TARGETS = [
    ("scripts.bronze.bronze_discover_event_urls", []),
    ("scripts.bronze.bronze_ingest_games",        ["--season", "2025"]),
    ("scripts.bronze.bronze_ingest_odds_live",    []),
    ("scripts.bronze.bronze_ingest_historic_odds", []),
    ("scripts.bronze.bronze_ingest_weather_forecast", []),
    ("scripts.bronze.bronze_ingest_weather_history",  []),
    ("scripts.bronze.bronze_ingest_player_stats", ["--season", "2025"]),  # if present
]

def exists(mod: str) -> bool:
    return imps.find_spec(mod) is not None

def run_one(mod: str, extra):
    cmd = [sys.executable, "-m", mod, "--dry-run", "--limit", "5", *extra]
    print("•", " ".join(cmd))
    return subprocess.run(cmd, cwd=ROOT).returncode

def main():
    rc_total = 0
    for mod, extra in TARGETS:
        if not exists(mod):
            print(f"SKIP (not found): {mod}")
            continue
        rc = run_one(mod, extra)
        status = "OK" if rc == 0 else f"FAIL rc={rc}"
        print(f" -> {mod}: {status}")
        rc_total |= (rc != 0)
    sys.exit(1 if rc_total else 0)

if __name__ == "__main__":
    main()
