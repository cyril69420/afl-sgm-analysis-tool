#!/usr/bin/env python
"""
Allocate stakes to each recommended SGM based on EV.
"""
from __future__ import annotations
import argparse, logging
from pathlib import Path
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="Allocate stakes to positive-EV SGMs")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season year")
    parser.add_argument("--max-bank", type=float, default=5.0, help="Max total stake per round ($)")
    parser.add_argument("--stake-step", type=float, default=0.50, help="Stake increment ($)")
    parser.add_argument("--log-level", default="INFO")
    # accept pipeline flags to avoid errors
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--csv-mirror", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s | %(levelname)s | %(message)s")
    root = Path(args.root).resolve()
    candidates_path = root / "gold" / "sgm_candidates.parquet"
    if not candidates_path.exists():
        logging.error("SGM candidates file not found. Run gold_build_sgms first.")
        return
    df = pd.read_parquet(candidates_path)
    if df.empty:
        logging.info("No SGM candidates to allocate stakes for.")
        return

    total_ev = df["ev"].sum()
    stakes = []
    for ev in df["ev"]:
        frac = ev / total_ev if total_ev > 0 else 0.0
        raw_stake = frac * args.max_bank
        stake_units = round(raw_stake / args.stake_step)  # round to nearest increment
        stake = stake_units * args.stake_step
        stakes.append(stake)
    # Adjust if total exceeds budget
    total_stake = sum(stakes)
    if total_stake > args.max_bank:
        over = total_stake - args.max_bank
        # Trim from largest stakes until within budget
        paired = sorted([(s,i) for i,s in enumerate(stakes)], reverse=True)
        k = 0
        while over > 1e-9 and k < len(paired):
            s, i = paired[k]
            if stakes[i] >= args.stake_step:
                stakes[i] -= args.stake_step
                over -= args.stake_step
            else:
                k += 1
    df["stake"] = stakes

    # Save outputs: Parquet, CSV, and Markdown report
    out_parquet = root / "gold" / "sgm_recommendations.parquet"
    df.to_parquet(out_parquet, index=False)
    df.to_csv(root / "gold" / "sgm_recommendations.csv", index=False)
    # Generate markdown report
    lines = ["# Top Same Game Multi Recommendations\n"]
    lines.append("| Game | Legs | Combined Price | Implied Prob | EV | Stake |")
    lines.append("|------|------|----------------|--------------|----|-------|")
    for _, row in df.iterrows():
        game = row.get("game", f"Match {row['match_id']}")
        legs_desc = f"{row['leg1']} + {row['leg2']}"
        combo_price = f"{row['combined_price']:.2f}"
        combo_prob = f"{row['combo_prob']:.3f}"
        ev_str = f"{row['ev']:.3f}"
        stake_str = f"${row['stake']:.2f}"
        lines.append(f"| {game} | {legs_desc} | {combo_price} | {combo_prob} | {ev_str} | {stake_str} |")
    lines.append("\n*Assuming leg independence. Stakes allocated ≈proportional to EV (total ${:.2f} bank, {} unit).*"
                 .format(args.max_bank, f"${args.stake_step:.2f}"))
    report_path = root / "gold" / "sgm_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    logging.info("Wrote recommendations to %s and %s", out_parquet, report_path)

if __name__ == "__main__":
    main()
