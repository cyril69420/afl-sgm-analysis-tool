#!/usr/bin/env python
"""
scripts.gold.build_gold
========================

CLI for generating SGM recommendations for a specific AFL round.  This
script glues together the gold layer components.  It performs the
following steps:

1. Dynamically adds the project root to ``sys.path`` so that the ``gold``
   package can be imported as a top‑level module.
2. Constructs candidate SGMs using hard‑coded market and model inputs.
3. Allocates a bankroll across the SGMs subject to an optional breakeven
   constraint.
4. Writes a debug CSV for audit, a machine‑readable recommendations CSV
   and a Markdown report.

Example::

    python -m scripts.gold.build_gold --round "Finals Week 1" --bankroll 5.00 --breakeven

"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import List

def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build Gold SGM recommendations")
    parser.add_argument("--round", dest="round_name", required=True, help="Name of the AFL round")
    parser.add_argument("--bankroll", type=float, default=5.0, help="Total bankroll to allocate ($)")
    parser.add_argument("--breakeven", action="store_true", help="Ensure non‑negative expected profit")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    # Add project root to sys.path for module resolution
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    # Import gold layer modules after adjusting sys.path
    from gold.sgm_constructor import construct_candidate_sgms
    from gold.bankroll import allocate_bankroll
    from gold.report import generate_report

    logging.info("Constructing candidate SGMs...")
    sgms = construct_candidate_sgms()
    logging.info("Constructed %d candidate SGMs", len(sgms))
    logging.info("Allocating bankroll: $%.2f with breakeven=%s", args.bankroll, args.breakeven)
    sgms, summary = allocate_bankroll(sgms, bankroll=args.bankroll, min_unit=0.5, breakeven=args.breakeven)
    logging.info(
        "Total stake allocated: $%.2f, expected profit: $%.2f, expected ROI: %.2f%%",
        summary["total_stake"],
        summary["expected_profit"],
        summary["expected_roi"] * 100,
    )
    # Determine directories within the repo
    gold_dir = root / "gold"
    debug_dir = gold_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    # Write debug CSV for audit
    debug_csv = debug_dir / "sgms.csv"
    with open(debug_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = list(sgms[0].keys()) if sgms else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
        for s in sgms:
            writer.writerow({k: ("; ".join(v) if k == "legs" else v) for k, v in s.items()})
    logging.info("Wrote debug SGM table to %s", debug_csv)
    # Generate report and recommendation CSV
    rec_csv = gold_dir / "sgm_recommendations.csv"
    report_md = gold_dir / "REPORT.md"
    generate_report(sgms, summary, csv_path=str(rec_csv), md_path=str(report_md), round_name=args.round_name)
    logging.info("Wrote recommendations to %s and report to %s", rec_csv, report_md)


if __name__ == "__main__":
    main()
