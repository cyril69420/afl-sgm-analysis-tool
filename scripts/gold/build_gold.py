#!/usr/bin/env python
"""
Construct the Gold layer of the AFL SGM analysis tool.

This script reads the Silver odds and fixtures tables and produces a
set of hypothetical Same Game Multis (SGMs) for each event. It
supports a basic leg architecture (match result and total points) and
computes the combined implied probability, price and expected value
under a naïve independence assumption. A simple staking strategy
allocates up to $5 per round in 50¢ increments proportional to EV.
"""

from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
from typing import List

import polars as pl

def implied_prob(decimal_odds: float) -> float:
    return 1.0 / decimal_odds if decimal_odds and decimal_odds > 0 else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Gold layer (SGMs) from Silver data")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season to build")
    parser.add_argument("--max-bank", type=float, default=5.0, help="Maximum bank per round ($)")
    parser.add_argument("--stake-step", type=float, default=0.50, help="Stake increment ($)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(message)s")
    root = Path(args.root).resolve()
    silver_root = root / "silver"
    odds_path = silver_root / "odds.parquet"
    fixtures_path = silver_root / "fixtures.parquet"
    if not odds_path.exists() or not fixtures_path.exists():
        logging.error("Silver inputs missing; run build_silver first")
        return
    odds = pl.read_parquet(odds_path.as_posix())
    fixtures = pl.read_parquet(fixtures_path.as_posix())
    # Keep only odds for events present in fixtures
    odds = odds.join(fixtures.select(["game_key", "season", "round", "home", "away", "bookmaker_event_url"]).rename({"bookmaker_event_url": "event_url"}), on="event_url", how="inner")
    # Leg archetypes: match result and totals
    match_filter = (
        pl.col("market_group").str.contains("match|head|result") &
        pl.col("market_name").str.contains("head|result")
    )
    totals_filter = (
        pl.col("market_group").str.contains("total|points|over|under") &
        pl.col("market_name").str.contains("over|under") &
        pl.col("line").is_not_null()
    )
    match_odds = odds.filter(match_filter).select([
        "event_url", "bookmaker", "market_group", "market_name", "selection", "decimal_odds", "captured_at_utc", "season", "round", "game_key"
    ])
    totals_odds = odds.filter(totals_filter).select([
        "event_url", "bookmaker", "market_group", "market_name", "selection", "line", "decimal_odds", "captured_at_utc", "season", "round", "game_key"
    ])
    if match_odds.height == 0 or totals_odds.height == 0:
        logging.info("No suitable odds for SGM construction; aborting")
        return
    # Most recent price per selection per event
    match_latest = (
        match_odds.sort(["event_url", "selection", "captured_at_utc"]).group_by(["event_url", "selection"]).tail(1)
        .with_columns(pl.col("decimal_odds").map_elements(implied_prob).alias("prob"))
    )
    totals_latest = (
        totals_odds.sort(["event_url", "selection", "line", "captured_at_utc"]).group_by(["event_url", "selection", "line"]).tail(1)
        .with_columns(pl.col("decimal_odds").map_elements(implied_prob).alias("prob"))
    )
    # Join to create 2-leg SGMs within the same event
    sgm = match_latest.join(totals_latest, on=["event_url"], how="inner", suffix="_tot")
    if sgm.height == 0:
        logging.info("No cross-product of match result and totals available")
        return
    # Compute combined probability and price under independence
    sgm = sgm.with_columns([
        (pl.col("prob") * pl.col("prob_tot")).alias("combo_prob"),
        (pl.col("decimal_odds") * pl.col("decimal_odds_tot")).alias("combo_price"),
    ])
    sgm = sgm.with_columns((pl.col("combo_prob") * pl.col("combo_price") - 1.0).alias("ev"))
    # Filter positive EV
    sgm = sgm.filter(pl.col("ev") > 0)
    if sgm.height == 0:
        logging.info("No positive EV SGMs found")
        return
    sgm = sgm.sort("ev", descending=True)
    # Stake allocation
    total_ev = float(sgm["ev"].sum()) if sgm.height > 0 else 0.0
    stakes: List[float] = []
    for ev in sgm["ev"]:
        frac = float(ev) / total_ev if total_ev > 0 else 0.0
        raw_stake = frac * args.max_bank
        # Round to nearest stake step
        stake_units = round(raw_stake / args.stake_step)
        stake = stake_units * args.stake_step
        stakes.append(stake)
    # Ensure total stake <= max_bank
    over = max(0.0, sum(stakes) - args.max_bank)
    if over > 0:
        # Reduce stakes starting from largest until within budget
        paired = sorted([(s, i) for i, s in enumerate(stakes)], reverse=True)
        k = 0
        while over > 1e-9 and k < len(paired):
            s, i = paired[k]
            if stakes[i] >= args.stake_step:
                stakes[i] -= args.stake_step
                over -= args.stake_step
            else:
                k += 1
    sgm = sgm.with_columns(pl.Series("stake", stakes))
    # Prepare final output columns
    sgm = sgm.select([
        "event_url",
        "season",
        "round",
        "game_key",
        "bookmaker",
        "selection",
        "decimal_odds",
        "prob",
        pl.col("selection_tot").alias("tot_selection"),
        pl.col("line").alias("tot_line"),
        pl.col("decimal_odds_tot").alias("tot_odds"),
        pl.col("prob_tot").alias("tot_prob"),
        "combo_price",
        "combo_prob",
        "ev",
        "stake"
    ])
    # Write outputs
    gold_dir = root / "gold" / "sgm"
    gold_dir.mkdir(parents=True, exist_ok=True)
    sgm.write_parquet((gold_dir / "sgm_ranked.parquet").as_posix(), compression="zstd")
    sgm.write_csv((gold_dir / "sgm_ranked.csv").as_posix())
    # Create a simple human-readable report
    lines = ["# Top Same Game Multis\n"]
    lines.append("| Game | Legs | Combined Price | Implied Prob | EV | Stake |")
    lines.append("|------|-----|---------------|-------------|----|------|")
    # Build table
    for row in sgm.rows():
        game = row["game_key"]
        legs = f"{row['selection']} & {row['tot_selection']} (O/U {row['tot_line']})"
        combo_price = f"{row['combo_price']:.2f}"
        combo_prob = f"{row['combo_prob']:.3f}"
        ev = f"{row['ev']:.3f}"
        stake = f"${row['stake']:.2f}"
        lines.append(f"| {game} | {legs} | {combo_price} | {combo_prob} | {ev} | {stake} |")
    lines.append("\nAssumption: Leg probabilities are treated as independent; correlation effects are ignored in this version. Staking is proportional to EV and rounded to $0.50 increments (max $5 per round).\n")
    report_path = gold_dir / "report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logging.info("Wrote SGM ranked data (%d rows) and report to %s", sgm.height, gold_dir)


if __name__ == "__main__":
    main()