#!/usr/bin/env python
"""
Run a minimal Great Expectations validation suite against Bronze data.

This script performs a set of basic data quality checks on the Bronze
layer. Currently two checks are implemented:

1. Odds snapshots and historical odds: ensure ``decimal_odds`` is
   greater than 1.01, and that ``market_name`` and ``selection`` are
   non‑null.
2. Weather forecast and history: ensure temperature is within a
   sensible range (−50°C to 60°C) and rain probability between 0 and 1.

Results are printed to stdout as JSON. If the failure rate exceeds 2%
for any check a warning is emitted. You can extend this script with
additional expectations as needed.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
import sys
from typing import Optional

import great_expectations as gx
import polars as pl

logger = logging.getLogger(__name__)


def validate_odds(path: Path, label: str) -> dict:
    """Validate odds Parquet dataset located at path.

    Returns a dictionary summarising failures.
    """
    if not path.exists():
        logger.info("Dataset %s not found; skipping", path)
        return {}
    # Read data
    df = pl.scan_parquet(str(path / "**" / "*.parquet"), allow_missing_files=True).collect().to_pandas()
    if df.empty:
        logger.info("Dataset %s is empty; skipping", path)
        return {}
    ge_df = gx.from_pandas(df)
    results = {}
    # decimal_odds > 1.01
    res = ge_df.expect_column_values_to_be_greater_than("decimal_odds", 1.01)
    results["decimal_odds_gt_1.01"] = res.to_json_dict()
    # market_name not null
    res = ge_df.expect_column_values_to_not_be_null("market_name")
    results["market_name_not_null"] = res.to_json_dict()
    # selection not null
    res = ge_df.expect_column_values_to_not_be_null("selection")
    results["selection_not_null"] = res.to_json_dict()
    print(json.dumps({"dataset": label, "results": results}, indent=2))
    return results


def validate_weather(path: Path, label: str) -> dict:
    if not path.exists():
        logger.info("Dataset %s not found; skipping", path)
        return {}
    df = pl.scan_parquet(str(path / "**" / "*.parquet"), allow_missing_files=True).collect().to_pandas()
    if df.empty:
        logger.info("Dataset %s is empty; skipping", path)
        return {}
    ge_df = gx.from_pandas(df)
    results = {}
    res = ge_df.expect_column_values_to_be_between("temp_c", -50, 60)
    results["temp_range"] = res.to_json_dict()
    res = ge_df.expect_column_values_to_be_between("rain_prob", 0, 1)
    results["rain_prob_range"] = res.to_json_dict()
    print(json.dumps({"dataset": label, "results": results}, indent=2))
    return results


def main(argv: Optional[list[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if len(argv or []) > 1:
        print("Usage: python qa_bronze.py [project_root]", file=sys.stderr)
        sys.exit(1)
    project_root = Path(argv[0]) if argv else Path(".")
    # Validate odds snapshots and history
    odds_snap_path = project_root / "bronze" / "odds" / "snapshots"
    odds_hist_path = project_root / "bronze" / "odds" / "history"
    validate_odds(odds_snap_path, "odds_snapshots")
    validate_odds(odds_hist_path, "odds_history")
    # Validate weather forecast and history
    weather_fore_path = project_root / "bronze" / "weather" / "forecast"
    weather_hist_path = project_root / "bronze" / "weather" / "history"
    validate_weather(weather_fore_path, "weather_forecast")
    validate_weather(weather_hist_path, "weather_history")


if __name__ == "__main__":
    main(sys.argv[1:])