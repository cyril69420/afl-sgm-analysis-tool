# 02_excel_to_silver.py
# Usage (Windows CMD/PowerShell):
#   python "C:\Users\Ethan\SGM Agent Project\scripts\02_excel_to_silver.py" --root "C:\Users\Ethan\SGM Agent Project" --excel "C:\Users\Ethan\SGM Agent Project\manual_ingest\Data_deposit_210825-1649.xlsx"
#
# Reads your Excel workbook and writes normalized CSVs to silver/ (games, odds_latest, lineups, weather).

import argparse, os
import pandas as pd
import numpy as np

def to_csv(df, path, cols):
    df2 = df.reindex(columns=cols)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df2.to_csv(path, index=False)
    print(f"[write] {path} ({len(df2)} rows)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--excel", required=True)
    args = ap.parse_args()

    root = args.root
    excel = args.excel
    silver = os.path.join(root, "silver")
    os.makedirs(silver, exist_ok=True)

    xl = pd.ExcelFile(excel)

    # 1) games
    games_cols = ["game_id","season","round","home","away","venue","kickoff_utc","kickoff_local_awst"]
    games = pd.DataFrame(columns=games_cols)
    if "Upcoming_AFL_Games" in xl.sheet_names:
        g = xl.parse("Upcoming_AFL_Games")
        games = pd.DataFrame({
            "game_id": g.get("game_id"),
            "season": g.get("year"),
            "round": g.get("round"),
            "home": g.get("home"),
            "away": g.get("away"),
            "venue": g.get("venue"),
            "kickoff_utc": g.get("start_time_utc"),
            "kickoff_local_awst": g.get("start_time_awst") if "start_time_awst" in g else pd.NaT
        })
    to_csv(games, os.path.join(silver, "games.csv"), games_cols)

    # 2) odds_latest
    odds_cols = ["game_id","market_id","selection_id","selection_name_std","line","price",
                 "implied_prob","market_overround","implied_prob_unvig","bookmaker","latest_snapshot_ts"]
    odds_latest = pd.DataFrame(columns=odds_cols)
    if "afl_round_all_markets" in xl.sheet_names:
        m = xl.parse("afl_round_all_markets")
        # Standardize fields
        m["bookmaker"] = m.get("bookmaker")
        m["price"] = pd.to_numeric(m.get("odds"), errors="coerce")
        m["market_code"] = m.get("market_name")
        m["selection_name_std"] = m.get("selection_name")
        m["line"] = pd.to_numeric(m.get("line"), errors="coerce")
        # Try to map your event url/id to game_id if present, else fallback
        if "game_id" in m.columns:
            m["game_id"] = m["game_id"]
        else:
            # Fallback: crude join via home-away in market block if present; else NaN
            m["game_id"] = np.nan

        # group to compute overround per (bookmaker, game_id, market_code, line)
        grp = m.groupby(["bookmaker","game_id","market_code","line"], dropna=False)
        rows = []
        for (bk, gid, mkt, line), sub in grp:
            sub = sub.copy()
            sub["implied_prob"] = 1.0 / sub["price"].astype(float)
            over = sub["implied_prob"].sum(skipna=True)
            sub["market_overround"] = over if over > 0 else np.nan
            sub["implied_prob_unvig"] = sub["implied_prob"] / over if over and over > 0 else np.nan
            sub["latest_snapshot_ts"] = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
            # create deterministic ids for now
            sub["market_id"] = sub["market_code"].astype(str).astype("category").cat.codes + 1
            sub["selection_id"] = sub["selection_name_std"].astype(str).astype("category").cat.codes + 1
            rows.append(sub[["game_id","market_id","selection_id","selection_name_std","line","price",
                             "implied_prob","market_overround","implied_prob_unvig","bookmaker","latest_snapshot_ts"]])
        if rows:
            odds_latest = pd.concat(rows, ignore_index=True)
    to_csv(odds_latest, os.path.join(silver, "odds_latest.csv"), odds_cols)

    # 3) lineups
    lineups_cols = ["game_id","team","player_id","player_name_std","status","source","latest_snapshot_ts"]
    lineups = pd.DataFrame(columns=lineups_cols)
    for sheet in ["afl_lineups_round_24", "afl_lineups", "lineups"]:
        if sheet in xl.sheet_names:
            L = xl.parse(sheet)
            lineups = pd.DataFrame({
                "game_id": L.get("game_id"),
                "team": L.get("team"),
                "player_id": L.get("player_id") if "player_id" in L else pd.NA,
                "player_name_std": L.get("player") if "player" in L else L.get("player_name"),
                "status": L.get("status") if "status" in L else "in",
                "source": sheet,
                "latest_snapshot_ts": pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
            })
            break
    to_csv(lineups, os.path.join(silver, "lineups.csv"), lineups_cols)

    # 4) weather
    weather_cols = ["game_id","temp_c","rain_prob","rain_mm","wind_kmh","gust_kmh","msl_hpa","cloud_pct","latest_snapshot_ts"]
    weather = pd.DataFrame(columns=weather_cols)
    if "Weather_For_Upcoming" in xl.sheet_names:
        W = xl.parse("Weather_For_Upcoming")
        weather = pd.DataFrame({
            "game_id": W.get("game_id"),
            "temp_c": W.get("temperature"),
            "rain_prob": W.get("rain_prob") if "rain_prob" in W else W.get("rain_probability"),
            "rain_mm": W.get("rain_mm") if "rain_mm" in W else pd.NA,
            "wind_kmh": W.get("wind_kmh") if "wind_kmh" in W else W.get("wind"),
            "gust_kmh": W.get("gust_kmh") if "gust_kmh" in W else pd.NA,
            "msl_hpa": W.get("pressure") if "pressure" in W else pd.NA,
            "cloud_pct": W.get("cloud") if "cloud" in W else pd.NA,
            "latest_snapshot_ts": pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
        })
    to_csv(weather, os.path.join(silver, "weather.csv"), weather_cols)

if __name__ == "__main__":
    main()
