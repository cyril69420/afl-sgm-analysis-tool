# Top-level Makefile for the AFL SGM analysis tool.
# This Makefile defines a few convenience targets for running the
# ingestion scripts. Feel free to extend with additional tasks.

PYTHON=python

.PHONY: bronze-discover bronze-fixtures bronze-odds-live bronze-odds-hist bronze-weather-forecast bronze-weather-history qa-bronze qa-silver

bronze-discover:
	$(PYTHON) scripts/bronze/bronze_discover_event_urls.py --season 2025 --bookmakers sportsbet,pointsbet

bronze-fixtures:
	$(PYTHON) scripts/bronze/bronze_ingest_games.py --season 2025

bronze-odds-live:
	$(PYTHON) scripts/bronze/bronze_ingest_odds_live.py --season 2025

bronze-odds-hist:
	$(PYTHON) scripts/bronze/bronze_ingest_historic_odds.py --season 2025

bronze-weather-forecast:
	$(PYTHON) scripts/bronze/bronze_ingest_weather_forecast.py --season 2025

bronze-weather-history:
	$(PYTHON) scripts/bronze/bronze_ingest_weather_history.py --season 2024

qa-bronze:
	$(PYTHON) scripts/bronze/qa_bronze.py

qa-silver:
	$(PYTHON) scripts/silver/qa_silver.py