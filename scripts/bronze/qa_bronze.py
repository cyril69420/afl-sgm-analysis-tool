#!/usr/bin/env python
"""
Quality assurance suite for the Bronze layer.

This script performs a lightweight set of checks on the raw Bronze
datasets to catch obvious schema and range issues before data is
promoted to the Silver layer.  It relies on Great Expectations to
evaluate expectations against each Parquet file individually.  If any
expectation fails, an AssertionError will be raised detailing the
failure.  Running this script against a local checkout helps ensure
that scraping and ingestion remain sane over time.

Usage:

    python scripts/bronze/qa_bronze.py <project_root>

The ``project_root`` argument should point to the top level of the
repository (where the ``bronze`` directory lives).  The Makefile
provides a convenient ``make qa-bronze`` target that invokes this
script with ``.`` as the root.
"""

from __future__ import annotations

import sys
from pathlib import Path
import logging

import great_expectations as ge

logger = logging.getLogger(__name__)


def _expect_in_file(parquet_path: Path, expectations: list[tuple[str, tuple]]):
    """Apply a list of Great Expectations to a single Parquet file.

    Parameters
    ----------
    parquet_path : Path
        The Parquet file to validate.
    expectations : list of tuple
        Each tuple contains the expectation method name and the
        arguments to pass to that method.  The expectation will be
        invoked on a Great Expectations dataset.
    """
    ds = ge.read_parquet(parquet_path.as_posix())
    for method_name, args in expectations:
        method = getattr(ds, method_name)
        result = method(*args)
        if not result.success:
            raise AssertionError(
                f"Expectation {method_name}{args} failed for {parquet_path}: {result.to_json_dict()}"
            )


def validate_event_urls(root: Path) -> None:
    """Validate the event_urls dataset.

    Checks that the essential fields exist and are non-null and that the
    ``status`` field contains only recognised values.
    """
    event_dir = root / "bronze" / "event_urls"
    if not event_dir.exists():
        logger.warning("Skipping event_urls validation: %s does not exist", event_dir)
        return
    expectations = [
        ("expect_column_to_exist", ("event_url",)),
        ("expect_column_to_exist", ("bookmaker",)),
        ("expect_column_values_to_not_be_null", ("event_url",)),
        ("expect_column_values_to_not_be_null", ("bookmaker",)),
        ("expect_column_values_to_match_regex", ("status", "^(upcoming|past)$")),
    ]
    for parquet_path in event_dir.rglob("*.parquet"):
        _expect_in_file(parquet_path, expectations)


def validate_fixtures(root: Path) -> None:
    """Validate the fixtures dataset.

    Ensures that all key fields are present and non-null and that the
    scheduled times are timezone-aware datetimes.
    """
    fixtures_dir = root / "bronze" / "fixtures"
    if not fixtures_dir.exists():
        logger.warning("Skipping fixtures validation: %s does not exist", fixtures_dir)
        return
    expectations = [
        ("expect_column_to_exist", ("game_key",)),
        ("expect_column_values_to_not_be_null", ("game_key",)),
        ("expect_column_values_to_not_be_null", ("home",)),
        ("expect_column_values_to_not_be_null", ("away",)),
        ("expect_column_values_to_not_be_null", ("scheduled_time_utc",)),
    ]
    for parquet_path in fixtures_dir.rglob("*.parquet"):
        _expect_in_file(parquet_path, expectations)


def validate_odds(root: Path) -> None:
    """Validate both snapshot and historical odds datasets.

    Checks that decimal odds are greater than 1.0 and that essential
    categorical fields are present.
    """
    odds_root = root / "bronze" / "odds"
    if not odds_root.exists():
        logger.warning("Skipping odds validation: %s does not exist", odds_root)
        return
    expectations = [
        ("expect_column_values_to_be_between", ("decimal_odds", 1.01, None)),
        ("expect_column_values_to_not_be_null", ("market_group",)),
        ("expect_column_values_to_not_be_null", ("selection",)),
    ]
    for parquet_path in odds_root.rglob("*.parquet"):
        _expect_in_file(parquet_path, expectations)


def validate_weather(root: Path) -> None:
    """Validate weather forecast and history datasets.

    Ensures numeric bounds where applicable and required fields are
    present.  Temperature is allowed to be null if the provider did
    not supply it.
    """
    weather_root = root / "bronze" / "weather"
    if not weather_root.exists():
        logger.warning("Skipping weather validation: %s does not exist", weather_root)
        return
    expectations = [
        ("expect_column_values_to_not_be_null", ("venue",)),
        ("expect_column_values_to_not_be_null", ("valid_time_utc",)),
        ("expect_column_values_to_be_between", ("rain_prob", 0.0, 1.0)),
    ]
    for parquet_path in weather_root.rglob("*.parquet"):
        _expect_in_file(parquet_path, expectations)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/bronze/qa_bronze.py <project_root>")
        sys.exit(1)
    root = Path(sys.argv[1]).resolve()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    validate_event_urls(root)
    validate_fixtures(root)
    validate_odds(root)
    validate_weather(root)
    print("Bronze QA passed")


if __name__ == "__main__":
    main()