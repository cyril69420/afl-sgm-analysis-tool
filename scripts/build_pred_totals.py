# scripts/build_pred_totals.py
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib

DATA = Path(".")
SILVER = DATA / "silver"
GOLD = DATA / "gold"
MODELS = DATA / "models"
MODELS.mkdir(exist_ok=True, parents=True)
GOLD.mkdir(exist_ok=True, parents=True)

def load_data():
    hist = pd.read_parquet(SILVER / "matches_hist.parquet")  # cols: date, season, round, venue, home, away, home_score, away_score, is_day, temp_c, wind_kph, rain_prob, pressure_hpa
    upc = pd.read_parquet(SILVER / "upcoming_games.parquet") # cols: game_id, start_time, home, away, venue
    wx = pd.read_parquet(SILVER / "weather_features.parquet")# cols: game_id, temp_c, wind_kph, rain_prob, pressure_hpa, is_day
    return hist, upc, wx

def add_targets(hist: pd.DataFrame):
    hist = hist.copy()
    hist["total"] = hist["home_score"] + hist["away_score"]
    # "era" normalisation: centre per season to remove league-wide shifts
    hist["total_norm"] = hist.groupby("season")["total"].transform(lambda s: s - s.mean())
    return hist

def train_totals_model(hist):
    # Features: venue fixed effects + weather + day/night + round-in-season harmonic
    use = hist.dropna(subset=["venue","temp_c","wind_kph","rain_prob","pressure_hpa","is_day","total_norm"]).copy()
    use["sin_round"] = np.sin(2*np.pi*use["round"]/24.0)
    use["cos_round"] = np.cos(2*np.pi*use["round"]/24.0)

    num = ["temp_c","wind_kph","rain_prob","pressure_hpa","sin_round","cos_round"]
    cat = ["venue","is_day"]
    y = use["total_norm"].values

    pre = ColumnTransformer([
        ("num", StandardScaler(), num),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat)
    ])
    model = Pipeline([
        ("pre", pre),
        ("ridge", RidgeCV(alphas=np.logspace(-3,3,25)))
    ])
    model.fit(use[num+cat], y)
    joblib.dump(model, MODELS / "totals_ridge.pkl")

    # Venue baselines (historical, de-normalised): μ_total per venue in raw space
    venue_base = hist.groupby("venue")["total"].mean().rename("base_total_venue").reset_index()
    return model, venue_base

def predict_upcoming(model, venue_base, upc, wx, hist):
    df = upc.merge(wx, on="game_id", how="left")
    df["is_day"] = df["is_day"].fillna(True)
    # Era shift: add back current-season mean minus hist-season mean
    this_season = hist["season"].max()
    era_shift = hist.loc[hist["season"]==this_season, "total"].mean() - hist["total"].mean()
    df["round"] = df["start_time"].dt.isocalendar().week.clip(1,24).astype(int)
    df["sin_round"] = np.sin(2*np.pi*df["round"]/24.0)
    df["cos_round"] = np.cos(2*np.pi*df["round"]/24.0)

    X = df[["venue","is_day","temp_c","wind_kph","rain_prob","pressure_hpa","sin_round","cos_round"]]
    pred_norm = model.predict(X)
    # Map venue baseline
    df = df.merge(venue_base, on="venue", how="left")
    df["base_total_venue"] = df["base_total_venue"].fillna(hist["total"].mean())
    # Weather delta is model prediction (in normalized space) adjusted to raw via era shift
    df["wx_delta"] = pred_norm
    df["pred_total_mean"] = df["base_total_venue"] + df["wx_delta"] + era_shift
    # A pragmatic uncertainty (tunable): sd grows with wind/rain
    df["pred_total_sd"] = 14 + 0.08*df["wind_kph"].fillna(0) + 6*df["rain_prob"].fillna(0)

    out_cols = ["game_id","start_time","home","away","venue","base_total_venue","wx_delta","pred_total_mean","pred_total_sd"]
    return df[out_cols]

def main():
    hist, upc, wx = load_data()
    hist = add_targets(hist)
    model, venue_base = train_totals_model(hist)
    out = predict_upcoming(model, venue_base, upc, wx, hist)
    out.to_csv(GOLD / "pred_totals.csv", index=False)
    print(f"Wrote {GOLD/'pred_totals.csv'} with {len(out)} rows")

if __name__ == "__main__":
    main()
