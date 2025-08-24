# Top‑level Makefile for the AFL SGM analysis tool.
#
# This Makefile defines a set of convenience targets for running
# the ingestion pipeline, building Silver and Gold layers and executing
# quality assurance suites. You can override the default season by
# passing SEASON=<year> on the command line, e.g. `make bronze SEASON=2025`.

PYTHON ?= python
SEASON ?= 2025
BOOKMAKERS ?= sportsbet,pointsbet

.PHONY: bronze bronze-discover bronze-fixtures bronze-odds-live bronze-odds-hist bronze-weather-forecast bronze-weather-history silver gold pipeline qa qa-bronze qa-silver qa-gold

# Run all Bronze ingestion steps (event discovery, fixtures, odds, weather)
bronze: bronze-discover bronze-fixtures bronze-odds-live bronze-odds-hist bronze-weather-forecast bronze-weather-history

bronze-discover:
	$(PYTHON) scripts/bronze/bronze_discover_event_urls.py --season $(SEASON) --bookmakers $(BOOKMAKERS)

bronze-fixtures:
	$(PYTHON) scripts/bronze/bronze_ingest_games.py --season $(SEASON)

bronze-odds-live:
	$(PYTHON) scripts/bronze/bronze_ingest_odds_live.py --season $(SEASON)

bronze-odds-hist:
	$(PYTHON) scripts/bronze/bronze_ingest_historic_odds.py --season $(SEASON)

bronze-weather-forecast:
	$(PYTHON) scripts/bronze/bronze_ingest_weather_forecast.py --season $(SEASON)

bronze-weather-history:
	$(PYTHON) scripts/bronze/bronze_ingest_weather_history.py --season $(SEASON)

# Build the Silver layer
silver:
	$(PYTHON) scripts/silver/build_silver.py --season $(SEASON) --root . --include-weather

# Build the Gold layer (SGMs)
gold:
	$(PYTHON) scripts/gold/build_gold.py --season $(SEASON) --root .

# Execute the full pipeline
pipeline:
	$(PYTHON) scripts/pipeline/run_all.py --season $(SEASON) --rounds all --bookmakers $(BOOKMAKERS) --include-weather

# Quality assurance targets
qa: qa-bronze qa-silver qa-gold

qa-bronze:
	$(PYTHON) scripts/bronze/qa_bronze.py .

qa-silver:
	$(PYTHON) scripts/silver/qa_silver.py .

qa-gold:
	$(PYTHON) scripts/gold/qa_gold.py .