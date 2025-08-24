#!/usr/bin/env python
"""
Construct the Gold layer with enriched Same Game Multis (SGMs).

This script reads the Silver odds and fixtures tables and produces a
ranking of hypothetical SGMs.  Compared to the baseline naive
implementation it introduces the following enhancements:

  * Supports additional market groups including line bets and player
    props in addition to match result and totals markets.
  * Calibrates implied probabilities by removing bookmaker margin on a
    per‑market basis using the sum of inverted odds.
  * Applies a simple correlation adjustment between legs to account
    for non‑independence.  Correlation coefficients are specified per
    pair of market groups (e.g. match×totals, match×line).
  * Maintains an idempotent staking strategy allocating up to a
    ``max_bank`` per round in steps of ``stake_step``.

Outputs include a Parquet and CSV file of ranked SGMs along with a
Markdown report summarising the top combinations.
"""

from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl


def implied_prob(decimal_odds: float) -> float:
    return 1.0 / decimal_odds if decimal_odds and decimal_odds > 0 else 0.0


def calibrate_probs(df: pl.DataFrame) -> pl.DataFrame:
    """Remove bookmaker margin by normalising implied probabilities.

    For each event and market group (market_name and line), the implied
    probabilities of all selections are summed.  Each probability is
    then divided by this margin to produce ``cal_prob``.  If the sum
    is zero or missing, the original probability is retained.
    """
    # Group key columns: event_url, market_group, market_name, line
    margin_df = (
        df.group_by(["event_url", "market_group", "market_name", "line"])
        .agg(pl.sum("prob").alias("margin"))
    )
    df = df.join(margin_df, on=["event_url", "market_group", "market_name", "line"], how="left")
    df = df.with_columns(
        pl.when(pl.col("margin") > 0)
        .then(pl.col("prob") / pl.col("margin"))
        .otherwise(pl.col("prob"))
        .alias("cal_prob")
    )
    return df


def correlation_adjusted_prob(p1: float, p2: float, rho: float) -> float:
    """Combine two probabilities with a correlation adjustment.

    A simple formula based on the Gaussian copula approximation is used:

        p_combo = p1 * p2 + rho * sqrt(p1*(1-p1) * p2*(1-p2))

    The result is capped to [0,1].
    """
    base = p1 * p2
    adjustment = rho * math.sqrt(max(p1 * (1 - p1) * p2 * (1 - p2), 0.0))
    p = base + adjustment
    if p < 0:
        p = 0.0
    if p > 1:
        p = 1.0
    return p


def build_sgms(
    odds: pl.DataFrame,
    correlation_map: Dict[Tuple[str, str], float],
    max_bank: float,
    stake_step: float,
) -> pl.DataFrame:
    """Construct Same Game Multis from calibrated odds.

    The function creates two‑leg SGMs by joining markets of different
    groups within the same event.  For each pair of market groups the
    corresponding correlation coefficient is applied.  Positive EV
    combinations are sorted and stake is allocated proportionally to
    EV with rounding to ``stake_step``.  Stakes are capped such that
    the total per round does not exceed ``max_bank``.
    """
    # Prepare subsets per group
    subsets: Dict[str, pl.DataFrame] = {}
    for group in odds["market_group"].unique().to_list():
        subsets[group] = odds.filter(pl.col("market_group") == group)
    combo_records = []
    # Generate pairwise combinations per event
    market_groups = list(subsets.keys())
    for i in range(len(market_groups)):
        for j in range(i + 1, len(market_groups)):
            g1 = market_groups[i]
            g2 = market_groups[j]
            rho = correlation_map.get((g1, g2), correlation_map.get((g2, g1), 0.0))
            df1 = subsets[g1]
            df2 = subsets[g2]
            # Inner join on event_url
            joined = df1.join(df2, on=["event_url"], how="inner", suffix="_2")
            if joined.height == 0:
                continue
            # Compute combo probability and price
            joined = joined.with_columns([
                (pl.col("cal_prob") * pl.col("cal_prob_2"))
                .map_elements(lambda x: x)  # placeholder to force evaluation
            ])
            # Use correlation adjustment via row-wise mapping
            def adjust(row) -> float:
                return correlation_adjusted_prob(row["cal_prob"], row["cal_prob_2"], rho)
            joined = joined.with_columns(
                pl.struct(["cal_prob", "cal_prob_2"]).map_elements(
                    lambda s: correlation_adjusted_prob(s["cal_prob"], s["cal_prob_2"], rho)
                ).alias("combo_prob")
            )
            joined = joined.with_columns(
                (pl.col("decimal_odds") * pl.col("decimal_odds_2")).alias("combo_price")
            )
            joined = joined.with_columns((pl.col("combo_prob") * pl.col("combo_price") - 1.0).alias("ev"))
            # Keep necessary fields and rename for clarity
            subset_cols = [
                "event_url",
                "season",
                "round",
                "game_key",
                "bookmaker",
                "selection",
                "decimal_odds",
                "cal_prob",
                "selection_2",
                "decimal_odds_2",
                "cal_prob_2",
                "combo_price",
                "combo_prob",
                "ev",
            ]
            joined = joined.select(subset_cols)
            joined = joined.rename({
                "selection": f"{g1}_selection",
                "decimal_odds": f"{g1}_odds",
                "cal_prob": f"{g1}_prob",
                "selection_2": f"{g2}_selection",
                "decimal_odds_2": f"{g2}_odds",
                "cal_prob_2": f"{g2}_prob",
            })
            joined = joined.with_columns([
                pl.lit(g1).alias("leg1_group"),
                pl.lit(g2).alias("leg2_group"),
            ])
            combo_records.append(joined)
    if not combo_records:
        return pl.DataFrame()
    sgm = pl.concat(combo_records, how="vertical")
    # Filter positive EV
    sgm = sgm.filter(pl.col("ev") > 0)
    if sgm.height == 0:
        return sgm
    # Sort by EV descending
    sgm = sgm.sort("ev", descending=True)
    # Stake allocation per round: compute total EV per round to distribute
    # First, compute relative stakes for each row within its round
    # We'll do this in Python for simplicity
    sgm_pd = sgm.to_pandas()
    # For each round assign stake
    stakes = [0.0] * len(sgm_pd)
    for (rnd, _), group in sgm_pd.groupby(["round", "season"]):
        total_ev = group["ev"].sum()
        if total_ev <= 0:
            continue
        # Compute raw stake per row
        raw_stakes = (group["ev"] / total_ev) * max_bank
        # Round to nearest stake_step
        rounded = (raw_stakes / stake_step).round().astype(int) * stake_step
        # Enforce cap per group
        over = rounded.sum() - max_bank
        if over > 1e-6:
            # Reduce from largest stakes
            order = rounded.sort_values(ascending=False).index
            i = 0
            while over > 1e-6 and i < len(order):
                idx = order[i]
                if rounded[idx] >= stake_step:
                    rounded[idx] -= stake_step
                    over -= stake_step
                else:
                    i += 1
        stakes[group.index] = rounded
    sgm_pd["stake"] = stakes
    sgm = pl.from_pandas(sgm_pd)
    return sgm


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Gold layer (SGMs) from Silver data")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season to build")
    parser.add_argument("--max-bank", type=float, default=5.0, help="Maximum bank per round ($)")
    parser.add_argument("--stake-step", type=float, default=0.50, help="Stake increment ($)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(message)s",
    )
    root = Path(args.root).resolve()
    silver_root = root / "silver"
    odds_path = silver_root / "odds.parquet"
    fixtures_path = silver_root / "fixtures.parquet"
    if not odds_path.exists() or not fixtures_path.exists():
        logging.error("Silver inputs missing; run build_silver first")
        return
    odds = pl.read_parquet(odds_path.as_posix())
    fixtures = pl.read_parquet(fixtures_path.as_posix())
    # Filter by season and join fixture metadata (game_key, home, away)
    odds = odds.filter(pl.col("season") == args.season)
    if odds.height == 0:
        logging.info("No odds found for season %s", args.season)
        return
    # Keep only needed columns
    odds = odds.select([
        "event_url",
        "season",
        "round",
        "game_key",
        "bookmaker",
        "market_group",
        "market_name",
        "selection",
        "line",
        "decimal_odds",
        "captured_at_utc",
        "source_kind",
    ])
    # Compute implied probability and calibrate per market
    odds = odds.with_columns(pl.col("decimal_odds").map_elements(implied_prob).alias("prob"))
    odds = calibrate_probs(odds)
    # Filter to valid market groups we know how to combine
    valid_groups = ["match", "totals", "line", "player"]
    odds = odds.filter(pl.col("market_group").is_in(valid_groups))
    if odds.height == 0:
        logging.info("No supported market groups available; aborting")
        return
    # Define correlation coefficients for each pair of groups
    correlation_map: Dict[Tuple[str, str], float] = {
        ("match", "totals"): 0.05,
        ("match", "line"): 0.1,
        ("match", "player"): 0.0,
        ("totals", "player"): 0.0,
        ("line", "player"): 0.0,
        ("totals", "line"): 0.02,
    }
    sgm = build_sgms(odds, correlation_map, args.max_bank, args.stake_step)
    if sgm.height == 0:
        logging.info("No positive EV SGMs found")
        return
    # Write outputs
    gold_dir = root / "gold" / "sgm"
    gold_dir.mkdir(parents=True, exist_ok=True)
    sgm.write_parquet((gold_dir / "sgm_ranked.parquet").as_posix(), compression="zstd")
    sgm.write_csv((gold_dir / "sgm_ranked.csv").as_posix())
    # Create simple report
    lines: List[str] = ["# Top Same Game Multis\n"]
    lines.append("| Game | Leg1 Group | Leg1 | Leg2 Group | Leg2 | Combo Price | Combo Prob | EV | Stake |")
    lines.append("|------|------------|-----|------------|-----|------------|-----------|----|------|")
    for row in sgm.rows():
        game = row["game_key"]
        leg1 = f"{row['leg1_group']}: {row[row['leg1_group'] + '_selection']}"
        leg2 = f"{row['leg2_group']}: {row[row['leg2_group'] + '_selection']}"
        combo_price = f"{row['combo_price']:.2f}"
        combo_prob = f"{row['combo_prob']:.3f}"
        ev = f"{row['ev']:.3f}"
        stake = f"${row['stake']:.2f}"
        lines.append(
            f"| {game} | {row['leg1_group']} | {row[row['leg1_group'] + '_selection']} | {row['leg2_group']} | {row[row['leg2_group'] + '_selection']} | {combo_price} | {combo_prob} | {ev} | {stake} |"
        )
    lines.append(
        "\nAssumptions: Probabilities are calibrated to remove bookmaker margins. "
        "Correlation between legs is accounted for via simple coefficients. "
        "Stakes are proportional to EV and rounded to the nearest stake step "
        f"(${args.stake_step:.2f}), with a maximum of ${args.max_bank:.2f} per round.\n"
    )
    report_path = gold_dir / "report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logging.info("Wrote SGM ranked data (%d rows) and report to %s", sgm.height, gold_dir)


if __name__ == "__main__":
    main()