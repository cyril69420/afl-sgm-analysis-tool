# AFL SGM Analysis Tool

Pipelines and utilities to collect and model AFL (Australian Football League) data for **same-game multi (SGM)** analysis. The project follows a Lakehouse **Medallion** flow:

- **Bronze** — raw ingests (event URLs, fixtures, odds snapshots, weather, player stats) written as scraped, minimally altered.
- **Silver** — canonical, **ID-stable**, time-aware, schema-cleaned tables safe for modelling and feature generation.
- **Gold** — modelling features, EV/edge, and SGM construction using bias-free joins (e.g., latest pre-kickoff odds).

> Run everything end-to-end via the orchestrator, or call individual scripts as needed. (See **Quickstart**)
  
---

## Repository Structure

afl-sgm-analysis-tool/
bronze/ # data lake outputs written by Bronze scripts
event_urls/ fixtures/ odds/{snapshots,history}/ weather/{forecast,history}/
silver/ # Silver tables (dims + facts)
gold/ # Gold features / SGM outputs
config/
team_aliases.csv # canonicalisation for team names (Phase-1)
player_aliases.csv # canonicalisation for player names (growing)
schemas/ # pydantic schemas (bronze rows)
scripts/
bronze/
_shared.py # shared flags/logging/atomic IO/http + parquet_write()
bronze_discover_event_urls.py
bronze_ingest_games.py # fixtures
bronze_ingest_odds_live.py
bronze_ingest_historic_odds.py
bronze_ingest_weather_forecast.py
bronze_ingest_weather_history.py
bronze_ingest_player_stats.py
smoke_test.py # runs all Bronze modules with --dry-run --limit
silver/
utils.py # deterministic ID helpers (team/venue/match/player)
build_silver.py # Phase-1: dims + fixtures (recursive scan)
gold/
build_gold.py # placeholder: SGM/EV (later phases)
pipeline/
run_all.py # Orchestrate Bronze→Silver→Gold
Makefile
requirements.txt
.env.example

yaml
Copy code

---

## Requirements

- Python **3.10+** (3.11 OK)  
- `pip install -r requirements.txt`  
- If you use any Playwright-based scrapers, install a browser:
  ```bash
  python -m playwright install chromium
Windows users: prefer Git Bash or PowerShell. Paths in commands use forward slashes.

Quickstart
0) Environment
bash
Copy code
python -m venv .venv
# Windows PowerShell:   .venv\Scripts\Activate.ps1
# Git Bash/Linux/Mac:   source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill any required API keys if applicable
1) Bronze — smoke test (no writes)
Ensures every Bronze module runs & returns rows without side effects.

bash
Copy code
python -m scripts.bronze.smoke_test
2) Bronze — sample writes (deterministic)
All Bronze scripts accept:

--dry-run — log shape/columns only, no file writes

--limit N — cap rows for quick tests

--overwrite — allow re-running into non-empty partition dirs (DuckDB partitioned Parquet)

Example (2025 small slice):

bash
Copy code
# Discover URLs + fixtures
python -m scripts.bronze.bronze_discover_event_urls --season 2025 --bookmakers betfair,tab,pointsbet,sportsbet --limit 12
python -m scripts.bronze.bronze_ingest_games         --season 2025 --limit 12 --overwrite

# Odds + Weather (overwrite keeps Bronze deterministic on re-runs)
python -m scripts.bronze.bronze_ingest_odds_live     --season 2025 --limit 24 --overwrite
python -m scripts.bronze.bronze_ingest_historic_odds --season 2025 --limit 24 --overwrite
python -m scripts.bronze.bronze_ingest_weather_forecast --season 2025 --limit 50 --overwrite
python -m scripts.bronze.bronze_ingest_weather_history  --season 2025 --limit 50 --overwrite

# Player stats (read-mostly; overwrite typically not needed)
python -m scripts.bronze.bronze_ingest_player_stats  --season 2025 --season 2024 --limit 200
Why --overwrite? DuckDB’s partitioned Parquet writer refuses to write into non-empty dirs by default. Overwrite lets re-runs replace small samples cleanly (fixtures must be deterministic).

3) Silver — build dims + fixtures
Phase-1 build_silver.py:

Recursively scans bronze/fixtures/** (Hive-style partitions like season=…/round=…/…)

Normalises column variants (e.g., scheduled_time_utc → scheduled_utc)

Canonicalises names via config/*_aliases.csv

Generates stable IDs using scripts/silver/utils.py

Writes:

silver/dim_team.parquet

silver/dim_venue.parquet

silver/f_fixture.parquet

--csv-mirror also writes .csv for quick eyeballing

bash
Copy code
python -m scripts.silver.build_silver --bronze-dir bronze --silver-dir silver --csv-mirror
4) Orchestrator — end-to-end
Run Bronze → Silver → (Gold later) with one command:

bash
Copy code
python scripts/pipeline/run_all.py --season 2025
This populates bronze/, silver/, and (when enabled) gold/…. 
GitHub

Canonicalisation (Phase-1)
Add/maintain alias maps under config/:

config/team_aliases.csv — columns: alias,team_name_canon

config/player_aliases.csv — columns: alias,player_name_canon[,dob]

These files do not auto-populate; seed them manually and grow over time (new variants, typos, abbreviations). Silver consumes them to keep IDs and joins stable.

Determinism & Reproducibility
Bronze: use --dry-run + --limit for smoke; --overwrite for clean reruns into existing partitions. Odds/history rows are idempotent by hash; fixtures are deterministic via overwrite.

Silver: IDs come from canonical inputs; re-running with unchanged inputs yields identical IDs. Validate with a quick DuckDB check:

bash
Copy code
python - <<'PY'
import duckdb
con=duckdb.connect()
print("f_fixture rows:",
      con.execute("SELECT COUNT(*) FROM read_parquet('silver/f_fixture.parquet')").fetchone()[0])
print(con.execute("""
WITH f AS (SELECT * FROM read_parquet('silver/f_fixture.parquet'))
SELECT COUNT(*) AS rows, COUNT(DISTINCT match_id) AS distinct_ids FROM f
""").fetchdf())
PY
Make targets (convenience)
make
Copy code
.PHONY: bronze-smoke bronze-sample silver-min
bronze-smoke:
	python -m scripts.bronze.smoke_test

bronze-sample:
	python -m scripts.bronze.bronze_discover_event_urls --season 2025 --limit 12 ;\
	python -m scripts.bronze.bronze_ingest_games --season 2025 --limit 12 --overwrite ;\
	python -m scripts.bronze.bronze_ingest_odds_live --season 2025 --limit 24 --overwrite ;\
	python -m scripts.bronze.bronze_ingest_historic_odds --season 2025 --limit 24 --overwrite ;\
	python -m scripts.bronze.bronze_ingest_weather_forecast --season 2025 --limit 50 --overwrite ;\
	python -m scripts.bronze.bronze_ingest_weather_history --season 2025 --limit 50 --overwrite

silver-min:
	python -m scripts.silver.build_silver --bronze-dir bronze --silver-dir silver --csv-mirror
Troubleshooting
“Directory … is not empty! Enable OVERWRITE” — Re-run with --overwrite on the Bronze script that failed.

Silver says “No fixtures in bronze/fixtures” but Bronze wrote rows — Ensure fixtures were written under bronze/fixtures/season=…/round=…/… and you’re on the updated build_silver.py that recursively scans.

Windows globbing — prefer forward slashes in globs (bronze/fixtures/**/*.parquet) — DuckDB handles them fine cross-platform.

Polars warnings about map_elements — the updated Silver builder already declares return_dtype and avoids Python UDFs where possible.

Contributing
Create a feature branch (e.g., bronze_layer_data_increase).

Keep Bronze parser changes surgical; log all commands in RUN.md.

Open a PR to main once Bronze smoke + Silver build pass locally.

