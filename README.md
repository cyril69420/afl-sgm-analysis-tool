# AFL SGM Analysis Tool

This repository contains a set of ingestion pipelines and utilities for
collecting AFL (Australian Football League) data suitable for multi‑leg
same game multis (SGM) analysis. The project is structured into three
tiers of a modern data lake: **Bronze** (raw ingestion), **Silver**
(cleansed and enriched) and **Gold** (analytics ready). This README
documents the primary entrypoints for the Bronze layer and explains how
to configure and run the pipelines.

## Repository Structure

```
afl-sgm-analysis-tool/
  config/              # YAML configuration files
    bookmakers.yaml    # Per‑bookmaker scraping selectors
    settings.yaml      # Global ingestion settings
  scripts/
    bronze/            # Bronze ingestion scripts and helpers
      _shared.py       # Shared helpers for all Bronze scripts
      _duck.py         # DuckDB convenience functions
      bronze_discover_event_urls.py
      bronze_ingest_games.py
      bronze_ingest_odds_live.py
      bronze_ingest_historic_odds.py
      bronze_ingest_weather_forecast.py
      bronze_ingest_weather_history.py
      qa_bronze.py     # Minimal Great Expectations suite
    silver/
      build_silver.py  # Construct the Silver layer (clean tables)
      qa_silver.py     # Placeholder for Silver QA
    gold/
      build_gold.py    # Construct SGMs and compute EV/stakes
    pipeline/
      run_all.py       # Orchestrate Bronze→Silver→Gold end-to-end
  schemas/
    bronze.py          # Pydantic models defining Bronze rows
  bronze/              # Data lake output partitioned by subject
    event_urls/
    fixtures/
    odds/
      snapshots/
      history/
    weather/
      forecast/
      history/
    _rejects/
    .checkpoints/
  silver/              # Silver data (clean tables)
  gold/                # Gold data (analytics ready)
  misc/
    powerquery_template.m
  .gitattributes
  requirements.txt
  Makefile
  .env.example
```

## Getting Started

1. **Install dependencies**

   Create and activate a Python 3.10 environment, then install
   requirements:

   ```bash
   python3.10 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   # install the Playwright browsers
   python -m playwright install chromium
   ```

2. **Configure environment**

   Copy `.env.example` to `.env` and fill in any required API keys
   (e.g. for your chosen weather provider).

3. **Run the pipeline**

   The easiest way to run everything (Bronze discovery, Silver build and
   Gold SGM generation) is via the orchestrator:

   ```bash
   # Runs Bronze→Silver→Gold for the 2025 season
   python scripts/pipeline/run_all.py --season 2025
   ```

   This will populate `bronze/`, `silver/` and `gold/sgm/` with
   Parquet and CSV files. You can inspect the results in
   `gold/sgm/sgm_ranked.csv` and read the human‑readable report in
   `gold/sgm/report.md`.

   Individual steps can still be invoked via the Makefile. For
   example, to rerun only the Silver build:

   ```bash
   make silver
   make gold
   ```

   Each ingestion script supports `--help` for more options (season,
   rounds, bookmaker selection, checkpoint directory etc.).

## Development Notes

* All timestamps are normalised to UTC before being written to disk. A
  `source_tz` column is preserved on fixture rows to capture the
  original timezone of scheduled kickoffs.
* The Bronze layer is append‑only and idempotent. Re‑running the
  ingestion scripts with the same inputs will not create duplicate
  records.
* Data validation is enforced via Pydantic models in
  `schemas/bronze.py` and a lightweight Great Expectations suite
  (`qa_bronze.py`). Run `make qa-bronze` to execute the checks.
* The `.gitattributes` file standardises line endings across operating
  systems and prevents CRLF noise in diffs.

## Licence

This project is provided for educational purposes and does not include
any betting advice. Use at your own risk and adhere to the terms of
service for any data sources.
