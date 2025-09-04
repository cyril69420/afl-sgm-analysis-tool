#!/usr/bin/env python
"""
Construct fair (vig-free) probabilities for the latest odds of each market.
"""
from __future__ import annotations
import argparse, logging
from pathlib import Path
import polars as pl
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="Compute fair probabilities (devig) from latest odds")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g. 2025)")
    parser.add_argument("--log-level", default="INFO")
    # Accept unused flags for compatibility with pipeline
    parser.add_argument("--overwrite", action="store_true", help="overwrite outputs if exists")
    parser.add_argument("--csv-mirror", action="store_true", help="(unused)")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s | %(levelname)s | %(message)s")
    root = Path(args.root).resolve()

    # Load silver data
    silver_dir = root / "silver"
    odds_path = silver_dir / "f_odds_snapshot.parquet"
    fixture_path = silver_dir / "f_fixture.parquet"
    if not odds_path.exists() or not fixture_path.exists():
        logging.error("Missing silver odds or fixture data. Run silver layer first.")
        return
    odds = pl.read_parquet(str(odds_path))
    fixtures = pl.read_parquet(str(fixture_path))
    # Filter to target season fixtures
    fixtures = fixtures.filter(pl.col("season") == args.season).select(["match_id", "scheduled_utc"])
    odds = odds.join(fixtures, on="match_id", how="inner")
    # Sportsbet only
    odds = odds.filter(pl.col("bookmaker") == "sportsbet")
    # Only consider odds captured before the match start (to avoid in-play or post-start odds if any)
    if "snapshot_ts_utc" in odds.columns:
        odds = odds.filter(pl.col("snapshot_ts_utc") <= pl.col("scheduled_utc"))
    # Get latest snapshot per outcome (match_id + market_type + selection specifics)
    if "selection_ref" in odds.columns:
        group_cols = ["match_id", "market_type", "selection_ref", "line"]
        if "selection_entity" in odds.columns:
            group_cols.append("selection_entity")
    else:
        # If no selection_ref (older schema), group by selection name text
        group_cols = ["match_id", "market_type", "selection", "line"]
    odds_latest = (odds.sort(["match_id","market_type","selection_ref","line","snapshot_ts_utc"])
                      .group_by(group_cols).tail(1))
    if odds_latest.height == 0:
        logging.error("No odds data after filtering (season=%s)", args.season)
        return

    # Convert to pandas for easier grouping for vig removal
    df = odds_latest.to_pandas()
    # Compute fair probabilities
    fair_rows = []
    # Define grouping key function to identify a market (all outcomes that sum to 100%)
    def market_group_key(row):
        mtype = row["market_type"]; mid = row["match_id"]
        if mtype == "match_result":
            # Group all outcomes (home/away/(draw)) together
            return (mid, mtype)
        elif pd.notnull(row.get("line")):
            if str(mtype).startswith("player"):
                # include player identifier (player id or name) and line
                pid = row.get("selection_ref") or row.get("selection")  # use player id if present else name
                return (mid, mtype, pid, float(row["line"]))
            else:
                # totals or other market with a numeric line
                return (mid, mtype, float(row["line"]))
        else:
            # Markets without a numeric line (e.g., maybe "anytime goalscorer")
            key = row.get("selection_ref") or row.get("selection") or ""
            return (mid, mtype, key)
    grouped = df.groupby(df.apply(market_group_key, axis=1))
    for key, group in grouped:
        # Sum implied probabilities (if not present, compute 1/price)
        if "implied_p" in group.columns:
            total_imp = group["implied_p"].sum()
            group_probs = group.copy()
        else:
            group_probs = group.copy()
            group_probs["implied_p"] = 1.0 / group_probs["price"]
            total_imp = group_probs["implied_p"].sum()
        if total_imp <= 0:
            continue  # skip if something is off
        group_probs["fair_prob"] = group_probs["implied_p"] / total_imp
        fair_rows.append(group_probs)
    if not fair_rows:
        logging.error("No groups formed for devigging.")
        return
    fair_df = pd.concat(fair_rows, ignore_index=True)
    # Save output
    gold_dir = root / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / "odds_latest_fair.parquet"
    fair_df.to_parquet(out_path, index=False)
    if args.csv_mirror:
        fair_df.to_csv(gold_dir / "odds_latest_fair.csv", index=False)
    logging.info("Wrote fair odds for %d outcomes to %s", len(fair_df), out_path)

if __name__ == "__main__":
    main()
