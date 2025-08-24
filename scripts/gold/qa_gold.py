#!/usr/bin/env python
"""
Quality assurance for the Gold layer.

This script validates the ranked Same Game Multi (SGM) output
produced by ``build_gold.py``.  It leverages Great Expectations to
ensure that probabilities, EVs and stakes are within sensible bounds
and that required columns exist.  If any check fails, an
AssertionError is raised summarising the offending expectation.

Usage:

    python scripts/gold/qa_gold.py <project_root>

When run via the Makefile target ``make qa-gold`` the current
directory is passed as the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
import logging

import great_expectations as ge


logger = logging.getLogger(__name__)


def validate_sgm(root: Path) -> None:
    """Validate the SGM ranked output.

    Applies a set of expectations to the ``sgm_ranked.parquet`` file to
    check stake sizing and probability ranges.  If the file does not
    exist the function logs a warning and returns.
    """
    sgm_path = root / "gold" / "sgm" / "sgm_ranked.parquet"
    if not sgm_path.exists():
        logger.warning("Skipping Gold QA: %s does not exist", sgm_path)
        return
    ds = ge.read_parquet(sgm_path.as_posix())
    # Stake should be non-negative and not exceed the maximum bank (assume <= 5)
    result = ds.expect_column_values_to_be_between("stake", 0.0, 5.0)
    if not result.success:
        raise AssertionError(f"Stake range check failed: {result.to_json_dict()}")
    # EV should be strictly positive
    result = ds.expect_column_values_to_be_between("ev", 0.0, None, strictly=True)
    if not result.success:
        raise AssertionError(f"EV positivity check failed: {result.to_json_dict()}")
    # Combined probability between 0 and 1
    result = ds.expect_column_values_to_be_between("combo_prob", 0.0, 1.0)
    if not result.success:
        raise AssertionError(f"Combo prob range check failed: {result.to_json_dict()}")
    # Combined price should exceed 1.0
    result = ds.expect_column_values_to_be_between("combo_price", 1.01, None)
    if not result.success:
        raise AssertionError(f"Combo price check failed: {result.to_json_dict()}")
    # Required columns should exist
    required_columns = [
        "game_key",
        "leg1_group",
        "leg2_group",
        "combo_price",
        "combo_prob",
        "ev",
        "stake",
    ]
    for col in required_columns:
        result = ds.expect_column_to_exist(col)
        if not result.success:
            raise AssertionError(f"Missing required column {col}: {result.to_json_dict()}")
    print("Gold QA passed")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/gold/qa_gold.py <project_root>")
        sys.exit(1)
    root = Path(sys.argv[1]).resolve()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    validate_sgm(root)


if __name__ == "__main__":
    main()