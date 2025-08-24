# scripts/backtest_v0.py
import pandas as pd
import numpy as np
from pathlib import Path

DATA = Path("."); BRONZE = DATA/"bronze"; GOLD = DATA/"gold"; GOLD.mkdir(exist_ok=True, parents=True)

def load_inputs():
    # bronze snapshots: one file per run with timestamp; we filter to closing odds per game/market
    snap = pd.read_parquet(BRONZE/"odds_snapshots.parquet") 
    # cols: ts, game_id, market, selection, price_decimal, line, bookmaker
    sgm = pd.read_csv(GOLD/"sgm_candidates.csv")
    outcomes = pd.read_parquet(BRONZE/"outcomes.parquet") 
    # cols: game_id, home_score, away_score, winner, total_points, player_stats (if used later)
    return snap, sgm, outcomes

def resolve_leg_hit(row, outcomes_row):
    # Minimal v0 rules (extend as you add props):
    mk = row["market"].upper()
    sel = row["selection"].upper()
    if mk == "H2H":
        return int(sel == outcomes_row["winner"].upper())
    if mk.startswith("TOTAL OVER"):
        line = float(row.get("line", np.nan))
        return int(outcomes_row["total_points"] > line)
    if mk.startswith("TOTAL UNDER"):
        line = float(row.get("line", np.nan))
        return int(outcomes_row["total_points"] < line)
    if mk == "LINE":
        # assume selection like "HOME -12.5" or "AWAY +9.5"
        # implement your project’s exact encoding here
        return np.nan  # fill later
    return np.nan

def build_closing_book(snap):
    # define closing per game/market/selection as last ts before start_time (already filtered in bronze)
    snap = snap.sort_values("ts").groupby(["game_id","market","selection"], as_index=False).last()
    return snap

def main():
    snap, sgm, outcomes = load_inputs()
    closing = build_closing_book(snap)

    # join the leg prices back onto SGM legs by exploding
    legs = sgm.copy()
    legs["leg_list"] = legs["legs"].str.split(" \+ ")
    legs = legs.explode("leg_list")
    legs[["market","selection"]] = legs["leg_list"].str.split(":", n=1, expand=True)

    legs = legs.merge(closing[["game_id","market","selection","price_decimal","line"]],
                      on=["game_id","market","selection"], how="left")
    legs = legs.merge(outcomes[["game_id","winner","total_points"]], on="game_id", how="left")

    legs["leg_hit"] = legs.apply(lambda r: resolve_leg_hit(r, r), axis=1)
    # aggregate to SGM outcome
    sgm_hit = (legs.groupby(["game_id","legs"])
                    .agg(n_legs=("leg_hit","count"),
                         any_nan=("leg_hit", lambda s: int(s.isna().any())),
                         all_hit=("leg_hit", "min"),
                         combined_price=("combined_price","first"),
                         implied_prob=("implied_prob","first"),
                         expected_value=("expected_value","first"))
                    .reset_index())
    sgm_hit = sgm_hit[sgm_hit["any_nan"]==0].copy()
    sgm_hit["payout"] = np.where(sgm_hit["all_hit"]==1, sgm_hit["combined_price"], 0.0)
    sgm_hit["profit"] = sgm_hit["payout"] - 1.0  # unit stake

    # calibration bins
    sgm_hit["prob_bin"] = (sgm_hit["implied_prob"]*10).clip(0,9).astype(int)
    cal = (sgm_hit.groupby("prob_bin")
                 .agg(n=("all_hit","size"),
                      avg_prob=("implied_prob","mean"),
                      hit_rate=("all_hit","mean"),
                      roi=("profit","mean"))
                 .reset_index())

    sgm_hit.to_csv(GOLD/"backtest_trades_v0.csv", index=False)
    cal.to_csv(GOLD/"backtest_results.csv", index=False)
    print(f"Wrote backtest tables to {GOLD}")

if __name__ == "__main__":
    main()
