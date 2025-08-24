# AFL SGM Analysis Tool

This repository contains an end‑to‑end pipeline for sourcing, cleaning and modelling
AFL (Australian Football League) data for **same game multi** (SGM) analysis.

## What’s new in `feat/sgm‑enhancements`

The `feat/sgm‑enhancements` branch replaces the stubbed ingestion logic with
fully functional scrapers and processing steps. Major improvements include:

* **Real bookmaker scraping** – Playwright scripts navigate Sportsbet and
  PointsBet to discover upcoming and historic AFL event URLs and scrape
  detailed markets (match result, line, totals and player props). The
  selectors used for each site live in `config/bookmakers.yaml` and can be
  customised without changing code. Scrapers respect polite throttling and
  retry transient network errors.
* **Fixture enrichment** – Fixtures are scraped from bookmaker event pages and
  enriched with the venue’s latitude, longitude and timezone using
  `config/venues.yaml`. Kick‑off times are normalised to UTC via
  `zoneinfo`. A deterministic `game_key` allows idempotent upserts.
* **Weather integration** – Both forecast and observed weather are fetched
  from [Open‑Meteo](https://open‑meteo.com) using the venue coordinates. The
  ingestion scripts normalise units (temperature in °C, wind speed in m/s,
  rain probability as a fraction) and compute the lead time between model
  run and valid time. See `scripts/bronze/bronze_ingest_weather_*.py` for
  details.
* **Enhanced Silver layer** – The Silver builder now derives `is_upcoming`
  and `seen_span_s` for event URLs, joins season and round onto the odds
  table, normalises text casing and attaches venue metadata to the
  fixtures. Weather data are aligned to hourly bins around the scheduled
  kick‑off.
* **Richer SGMs** – The Gold layer supports additional markets (spread, total
  points, alternate totals and player props) and calibrates implied
  probabilities by removing bookmaker margin. A simple correlation factor
  can be specified per market group to dampen optimistic independence
  assumptions. The staking algorithm still respects a $5 bank per round and
  allocates stakes in \$0.50 increments.
* **Quality assurance** – Comprehensive [Great Expectations](https://great
  expectations.io/) suites validate the Bronze, Silver and Gold outputs.
  Run `make qa` to execute all checks. The QA scripts live under
  `scripts/bronze/qa_bronze.py`, `scripts/silver/qa_silver.py` and
  `scripts/gold/qa_gold.py`.

Refer to the top‑level `scripts/pipeline/run_all.py` for orchestrating a
complete Bronze→Silver→Gold run. Each ingestion script also exposes a
`--help` flag describing available arguments (season, rounds, bookmakers,
weather provider, headless mode etc.).

## Running the pipeline

1. **Create a Python environment**

   This project requires Python 3.10+. Create and activate a virtual
   environment and install dependencies:

   ```bash
   python3.10 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m playwright install chromium
   ```

2. **Configure environment variables**

   Copy `.env.example` to `.env` and fill in any provider keys (e.g.
   weather API keys). All secrets are read from the environment at runtime.

3. **Run the pipeline**

   The orchestrator will discover events, scrape fixtures and odds,
   enrich with weather, build Silver tables, construct SGMs and generate a
   report:

   ```bash
   python scripts/pipeline/run_all.py --season 2025 --rounds all \
     --bookmakers sportsbet,pointsbet --include-weather
   ```

   Outputs are written to the `bronze/`, `silver/` and `gold/` directories
   within the project root. The human‑readable report lives at
   `gold/sgm/report.md`.

4. **Run quality checks**

   To validate the outputs, run the QA suite:

   ```bash
   make qa
   ```

## Development notes

* All timestamps are stored as timezone‑aware UTC datetimes. Local times are
  converted using `zoneinfo` and preserved in the `source_tz` column when
  appropriate.
* The ingestion scripts are idempotent and append‑only. Re‑running with the
  same inputs will not create duplicate records.
* Configuration is YAML‑driven – new bookmakers or venues can be added
  without code changes.
* The `.gitattributes` file ensures consistent line endings on Windows and
  UNIX systems.

## Licence

This project is provided for educational purposes only and does not
constitute betting advice. Use responsibly and adhere to the terms of
service for any data sources utilised.