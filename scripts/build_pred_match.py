# scripts/build_pred_match.py
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegressionCV
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib

DATA = Path(".")
SILVER = DATA / "silver"
GOLD = DATA / "gold"
MODELS = DATA / "models"
MODELS.mkdir(exist_ok=True, parents=True)
GOLD.mkdir(exist_ok=True, parents=True)

K = 28.0              # Elo K-factor (tune in backtest)
HGA = 30.0            # Baseline home-ground advantage Elo pts (tune)
VENUE_HGA_BONUS = 12  # Extra Elo if 'true' home venue (tune)

def load_hist():
    # cols: date, season, round, home, away, home_score, away_score, venue, is_true_home
    return pd.read_parquet(SILVER/"matches_hist.parquet").sort_values("date")

def init_elos(teams):
    return pd.Series(1500.0, index=sorted(teams))

def expected_from_elo(diff):
    # logistic map to win probability
    return 1.0 / (1.0 + 10 ** (-diff/400.0))

def roll_elo(hist):
    teams = pd.unique(hist[["home","away"]].values.ravel("K"))
    elo = init_elos(teams)
    rows = []
    for r in hist.itertuples():
        hga = HGA + (VENUE_HGA_BONUS if getattr(r,"is_true_home",False) else 0.0)
        diff = (elo[r.home] + hga) - elo[r.away]
        p_home = expected_from_elo(diff)
        home_win = 1 if r.home_score > r.away_score else 0
        # update
        elo[r.home] += K * (home_win - p_home)
        elo[r.away] -= K * (home_win - p_home)
        rows.append({"date":r.date,"home":r.home,"away":r.away,"venue":r.venue,"elo_home_pre":elo[r.home],"elo_away_pre":elo[r.away],"p_home_elo":p_home,"home_win":home_win,"elo_diff":diff})
    return pd.DataFrame(rows)

def train_logistic(features):
    X = features[["elo_diff","rest_diff","travel_km_diff","is_true_home"]].fillna(0.0)
    y = features["home_win"].values
    pipe = Pipeline([("sc", StandardScaler()), ("lr", LogisticRegressionCV(cv=5, max_iter=200, scoring="neg_log_loss"))])
    pipe.fit(X,y)
    # Reliability via isotonic/platt using CalibratedClassifierCV on same data (fast v0)
    clf = CalibratedClassifierCV(pipe, cv="prefit", method="isotonic")
    clf.fit(X,y)
    joblib.dump(clf, MODELS/"logit_on_elo.pkl")
    return clf

def add_context(df):
    # Optional: rest/travel (if available in silver). Otherwise set zeros.
    for col in ["rest_diff","travel_km_diff","is_true_home"]:
        if col not in df.columns: df[col]=0.0
    return df

def predict_upcoming(clf, elo_table):
    upc = pd.read_parquet(SILVER/"upcoming_games.parquet")
    # map last known Elo for each team
    latest_elo = (elo_table
                  .sort_values("date")
                  .groupby("home")[["elo_home_pre"]].last().rename(columns={"elo_home_pre":"elo"}).rename_axis("team")
                  .reset_index())
    latest_elo = pd.concat([
        latest_elo,
        elo_table.sort_values("date").groupby("away")[["elo_away_pre"]].last().rename(columns={"elo_away_pre":"elo"}).rename_axis("team").reset_index()
    ]).groupby("team")["elo"].max().to_frame().reset_index()

    upc = upc.merge(latest_elo.rename(columns={"team":"home","elo":"elo_home"}), on="home", how="left")
    upc = upc.merge(latest_elo.rename(columns={"team":"away","elo":"elo_away"}), on="away", how="left")
    upc["elo_diff"] = (upc["elo_home"] + HGA) - upc["elo_away"]
    upc = add_context(upc)

    X = upc[["elo_diff","rest_diff","travel_km_diff","is_true_home"]].fillna(0.0)
    p_home = clf.predict_proba(X)[:,1]
    upc["p_home_win"] = p_home
    upc["p_away_win"] = 1 - p_home
    upc["fair_price_home"] = 1.0 / np.clip(upc["p_home_win"], 1e-6, 1-1e-6)
    upc["fair_price_away"] = 1.0 / np.clip(upc["p_away_win"], 1e-6, 1-1e-6)

    out_cols = ["game_id","start_time","home","away","elo_home","elo_away","elo_diff","p_home_win","p_away_win","fair_price_home","fair_price_away"]
    upc[out_cols].to_csv(GOLD/"pred_match.csv", index=False)
    return upc[out_cols]

def main():
    hist = load_hist()
    elo_table = roll_elo(hist)
    with_context = elo_table.merge(hist[["date","home","away","is_true_home"]], on=["date","home","away"], how="left")
    with_context["rest_diff"] = 0.0  # placeholder until rest/travel available
    with_context["travel_km_diff"] = 0.0
    clf = train_logistic(with_context)
    out = predict_upcoming(clf, with_context)
    print(f"Wrote {GOLD/'pred_match.csv'} with {len(out)} rows")

if __name__ == "__main__":
    main()
