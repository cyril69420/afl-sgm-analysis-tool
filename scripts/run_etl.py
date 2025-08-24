import subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

STEPS = [
    # Bronze: inputs & discovery
    ["bronze_ingest_games.py"],
    ["bronze_discover_event_urls.py"],
    ["bronze_ingest_odds.py"],

    # Bronze: (optional) others, if you’re also pulling now
    # ["bronze_ingest_lineups.py"],
    # ["bronze_ingest_player_stats.py"],
    # ["bronze_ingest_weather.py"],

    # Silver
    ["02_bronze_to_silver.py"],

    # Gold
    ["build_pred_totals.py"],
    ["build_pred_match.py"],
    ["sgm_generator_v2.py"],
    ["qa_silver.py"],
]

def run(step):
    print(">>", step[0])
    r = subprocess.run([PY, str(ROOT/"scripts"/step[0])], cwd=ROOT)
    if r.returncode != 0:
        raise SystemExit(f"Failed: {step[0]}")

def main():
    for s in STEPS:
        run(s)
    print("DONE.")

if __name__ == "__main__":
    main()
