#!/usr/bin/env python
"""
Quality assurance suite for the Silver layer.

This script uses Great Expectations to validate that the Silver
datasets meet basic schema and business rules.  It focuses on
structural properties rather than content correctness.  Failures will
raise an AssertionError and print a summary of expectations.

Run via:

    python scripts/silver/qa_silver.py <project_root>

"""

from __future__ import annotations

import sys
from pathlib import Path
import logging

import polars as pl
import great_expectations as ge

logger = logging.getLogger(__name__)


def validate_event_urls(path: Path) -> None:
    df = ge.read_parquet(path.as_posix())
    ge_df = df.expect_column_to_exist("event_url")
    ge_df = df.expect_column_to_exist("bookmaker")
    ge_df = df.expect_column_values_to_not_be_null("event_url")
    ge_df = df.expect_column_values_to_match_regex("status", "^(upcoming|past)$")
    ge_df = df.expect_column_values_to_be_of_type("is_upcoming", "boolean")
    ge_df = df.expect_column_values_to_be_between("seen_span_s", min_value=0)
    assert ge_df.success, f"Event URLs validation failed: {ge_df.to_json_dict()}"


def validate_fixtures(path: Path) -> None:
    df = ge.read_parquet(path.as_posix())
    ge_df = df.expect_column_values_to_not_be_null("game_key")
    ge_df = df.expect_column_values_to_not_be_null("home")
    ge_df = df.expect_column_values_to_not_be_null("away")
    ge_df = df.expect_column_values_to_match_regex("tz", r"^\w+/\w+")
    ge_df = df.expect_column_values_to_be_of_type("scheduled_time_local", "datetime64[ns]")
    assert ge_df.success, f"Fixtures validation failed: {ge_df.to_json_dict()}"


def validate_odds(path: Path) -> None:
    df = ge.read_parquet(path.as_posix())
    ge_df = df.expect_column_values_to_not_be_null("event_url")
    ge_df = df.expect_column_values_to_not_be_null("market_group")
    ge_df = df.expect_column_values_to_be_between("decimal_odds", min_value=1.01)
    ge_df = df.expect_column_values_to_be_between("cal_prob", min_value=0, max_value=1)
    assert ge_df.success, f"Odds validation failed: {ge_df.to_json_dict()}"


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/silver/qa_silver.py <project_root>")
        sys.exit(1)
    root = Path(sys.argv[1]).resolve()
    silver_root = root / "silver"
    event_urls_path = silver_root / "event_urls.parquet"
    fixtures_path = silver_root / "fixtures.parquet"
    odds_path = silver_root / "odds.parquet"
    if not event_urls_path.exists():
        raise FileNotFoundError(f"Missing {event_urls_path}")
    if not fixtures_path.exists():
        raise FileNotFoundError(f"Missing {fixtures_path}")
    if not odds_path.exists():
        raise FileNotFoundError(f"Missing {odds_path}")
    validate_event_urls(event_urls_path)
    validate_fixtures(fixtures_path)
    validate_odds(odds_path)
    print("Silver QA passed")


if __name__ == "__main__":
    main()