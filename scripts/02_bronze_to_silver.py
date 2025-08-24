# 02_bronze_to_silver.py
import argparse, os, glob, pandas as pd, numpy as np

def load_latest_snapshot(bronze_odds_root, snapshot_ts=None):
    if snapshot_ts:
        path = os.path.join(bronze_odds_root, f"snapshot_ts={snapshot_ts}")
        if not os.path.isdir(path):
            raise SystemExit(f"[error] snapshot_ts not found: {path}")
        return path
    snaps = sorted([d for d in glob.glob(os.path.join(bronze_odds_root, "snapshot_ts=*")) if os.path.isdir(d)])
    if not snaps:
        raise SystemExit("[error] no bronze odds snapshots found")
    return snaps[-1]

def unvig_group(df):
    df = df.copy()
    df["implied_prob"] = 1.0 / df["price"].astype(float)
    overround = df["implied_prob"].sum()
    df["market_overround"] = overround
    df["implied_prob_unvig"] = df["implied_prob"] / overround if overround > 0 else np.nan
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--snapshot_ts")
    args = ap.parse_args()

    root = args.root
    bronze_odds_root = os.path.join(root, "bronze", "odds")
    snap_dir = load_latest_snapshot(bronze_odds_root, args.snapshot_ts)
    print(f"[info] using bronze odds snapshot: {snap_dir}")

    odds_files = glob.glob(os.path.join(snap_dir, "*.csv"))
    if not odds_files:
        raise SystemExit("[error] no odds CSV files in snapshot")
    bronze = pd.concat([pd.read_csv(f) for f in odds_files], ignore_index=True)

    config_dir = os.path.join(root, "config")
    markets_map = os.path.join(config_dir, "markets_map.csv")
    if os.path.exists(markets_map):
        mm = pd.read_csv(markets_map)
        bronze = bronze.merge(mm, how="left", left_on="market_name", right_on="market_name_raw")
        bronze["market_code"] = bronze["market_code"].fillna(bronze["market_name"])
        bronze["selection_name_std"] = bronze["selection_name"]
    else:
        bronze["market_code"] = bronze["market_name"]
        bronze["selection_name_std"] = bronze["selection_name"]

    games_csv = os.path.join(root, "silver", "games.csv")
    if os.path.exists(games_csv):
        games = pd.read_csv(games_csv)
    else:
        games = pd.DataFrame(columns=["game_id","season","round","home","away","venue","kickoff_utc","kickoff_local_awst"])

    if "game_ref" in bronze.columns:
        unique_refs = bronze["game_ref"].dropna().unique().tolist()
        ref_to_id = {ref: i+1 for i,ref in enumerate(unique_refs)}
        bronze["game_id"] = bronze["game_ref"].map(ref_to_id)
    else:
        bronze["game_id"] = np.nan

    group_cols = ["bookmaker","game_id","market_code","line"]
    silver_odds = bronze.groupby(group_cols, dropna=False, group_keys=False).apply(unvig_group)

    out_cols = ["game_id","market_code","selection_name_std","line","price","implied_prob","market_overround","implied_prob_unvig","bookmaker"]
    silver_odds_latest = silver_odds[out_cols].copy()
    import os
    silver_odds_latest["latest_snapshot_ts"] = os.path.basename(snap_dir).split("snapshot_ts=")[-1]

    silver_odds_latest["market_id"] = (silver_odds_latest["market_code"].astype(str)).astype("category").cat.codes + 1
    silver_odds_latest["selection_id"] = (silver_odds_latest["selection_name_std"].astype(str)).astype("category").cat.codes + 1

    schema_cols = ["game_id","market_id","selection_id","selection_name_std","line","price","implied_prob","market_overround","implied_prob_unvig","bookmaker","latest_snapshot_ts"]
    silver_odds_latest = silver_odds_latest[schema_cols]

    silver_dir = os.path.join(root, "silver")
    os.makedirs(silver_dir, exist_ok=True)
    silver_odds_latest.to_csv(os.path.join(silver_dir, "odds_latest.csv"), index=False)
    print(f"[done] wrote silver/odds_latest.csv with {len(silver_odds_latest)} rows")

if __name__ == "__main__":
    main()
