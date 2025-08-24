# 03_models_to_gold.py
import argparse, os, pandas as pd, numpy as np
from datetime import datetime

def simple_match_model(odds_latest: pd.DataFrame) -> pd.DataFrame:
    df = odds_latest.copy()
    mask = df["market_id"].notna()  # placeholder; swap for market_code == 'Match Result' when you add markets table
    # For now, average implied_prob_unvig by game and selection_name
    avg = df.groupby(["game_id","selection_id","selection_name_std"], as_index=False)["implied_prob_unvig"].mean()
    avg.rename(columns={"implied_prob_unvig":"model_prob"}, inplace=True)
    return avg

def simple_totals_model(odds_latest: pd.DataFrame) -> pd.DataFrame:
    df = odds_latest.copy()
    # crude midpoint: pick the line with closest to 0.5 probability between Over and Under
    # Here we don't yet know which selection is Over/Under; keep mean line as placeholder
    g = df.groupby("game_id", as_index=False)["line"].mean()
    g.rename(columns={"line":"mean_total"}, inplace=True)
    g["std_total"] = 25.0
    return g

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--run_id")
    args = ap.parse_args()

    root = args.root
    run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    silver_dir = os.path.join(root, "silver")
    gold_dir = os.path.join(root, "gold")
    os.makedirs(gold_dir, exist_ok=True)

    odds_latest = pd.read_csv(os.path.join(silver_dir, "odds_latest.csv"))
    pred_match = simple_match_model(odds_latest)
    pred_match["model_name"] = "baseline_h2h_avg"
    pred_match["model_version"] = "0.1"
    pred_match["train_window_desc"] = "n/a"
    pred_match["run_id"] = run_id
    pred_match["features_hash"] = "n/a"
    pred_match.to_csv(os.path.join(gold_dir, "pred_match.csv"), index=False)

    pred_totals = simple_totals_model(odds_latest)
    pred_totals["model_name"] = "baseline_totals_midpoint"
    pred_totals["model_version"] = "0.1"
    pred_totals["train_window_desc"] = "n/a"
    pred_totals["run_id"] = run_id
    pred_totals["features_hash"] = "n/a"
    pred_totals.to_csv(os.path.join(gold_dir, "pred_totals.csv"), index=False)

    print(f"[done] wrote gold/pred_match.csv ({len(pred_match)}) and gold/pred_totals.csv ({len(pred_totals)}) with run_id={run_id}")

if __name__ == "__main__":
    main()
