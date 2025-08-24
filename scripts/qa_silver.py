import os, pandas as pd, numpy as np, sys
root = r"C:\Users\Ethan\SGM Agent Project"
silver = os.path.join(root, "silver")
gold   = os.path.join(root, "gold")
os.makedirs(gold, exist_ok=True)

issues = []

# 1) Load essentials
odds = pd.read_csv(os.path.join(silver, "odds_latest.csv"))
games = pd.read_csv(os.path.join(silver, "games.csv")) if os.path.exists(os.path.join(silver, "games.csv")) else pd.DataFrame()
weather = pd.read_csv(os.path.join(silver, "weather.csv")) if os.path.exists(os.path.join(silver, "weather.csv")) else pd.DataFrame()
lineups = pd.read_csv(os.path.join(silver, "lineups.csv")) if os.path.exists(os.path.join(silver, "lineups.csv")) else pd.DataFrame()

# 2) Overround sanity by market group
grp = odds.groupby(["bookmaker","game_id","market_id","line"], dropna=False)["implied_prob"].sum().reset_index(name="overround")
# Loose bands (tune later)
bad = grp[(grp["overround"] < 1.01) | (grp["overround"] > 1.20)]
for _, r in bad.iterrows():
    issues.append({"type":"overround_out_of_range","bookmaker":r["bookmaker"],"game_id":r["game_id"],"market_id":r["market_id"],"line":r["line"],"overround":r["overround"]})

# 3) Missing game_id joins
if not games.empty:
    missing_games = set(odds["game_id"].dropna().unique()) - set(games["game_id"].dropna().unique())
    for gid in sorted(missing_games):
        issues.append({"type":"missing_game_in_games","game_id":gid})

# 4) Weather coverage
if not weather.empty and not games.empty:
    missing_weather = set(games["game_id"].dropna().unique()) - set(weather["game_id"].dropna().unique())
    for gid in sorted(missing_weather):
        issues.append({"type":"missing_weather","game_id":gid})

# 5) Lineup player_id mapping
if not lineups.empty:
    unmapped = lineups[lineups["player_id"].isna() | (lineups["player_id"]=="")]
    for _, r in unmapped.iterrows():
        issues.append({"type":"unmapped_player","game_id":r.get("game_id"),"team":r.get("team"),"player_name_std":r.get("player_name_std")})

qa = pd.DataFrame(issues)
qa.to_csv(os.path.join(gold, "qa_anomalies.csv"), index=False)
print(f"Wrote {len(qa)} QA issues to gold/qa_anomalies.csv")
