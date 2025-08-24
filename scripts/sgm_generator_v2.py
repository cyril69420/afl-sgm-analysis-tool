# scripts/sgm_generator_v2.py
import pandas as pd
import numpy as np
from pathlib import Path
from itertools import combinations
from math import exp

DATA = Path("."); GOLD = DATA/"gold"; CONFIG = DATA/"config"
GOLD.mkdir(exist_ok=True, parents=True)

LAMBDA = 0.60   # correlation haircut intensity (tune via backtesting)
MAX_LEGS = 6    # supports up to 6 legs

def load_inputs():
    pm = pd.read_csv(GOLD/"pred_match.csv")   # p_home_win, etc.
    pt = pd.read_csv(GOLD/"pred_totals.csv")  # pred_total_mean, sd
    odds = pd.read_csv(GOLD/"market_books.csv")  # gold view of latest odds: game_id, market, selection, price_decimal, line, leg_type
    # odds must include leg_type (H2H, TOTAL_OVER, LINE_COVER, PLAYER_* etc.)
    return pm, pt, odds

def implied_prob(price):
    return 1.0 / price

def pair_weight(a, b, caps):
    row = caps.get((a,b))
    if row is not None: return row
    row = caps.get((b,a))
    return row if row is not None else 0.0

def build_caps():
    df = pd.read_csv(CONFIG/"sgm_pairwise_caps.csv", comment="#")
    caps = { (r.leg_type_a, r.leg_type_b): float(r.weight) for r in df.itertuples() }
    return caps

def cap_factor(legs, caps):
    # sum positive pair weights across legs; exponential haircut
    s = 0.0
    for a,b in combinations(legs, 2):
        s += max(0.0, pair_weight(a["leg_type"], b["leg_type"], caps))
    return exp(LAMBDA * s)

def gen_same_game_multis(pm, pt, odds):
    # Minimal example: construct SGMs using {H2H}x{TOTAL}x{LINE} subsets per game, up to MAX_LEGS
    out_rows = []
    caps = build_caps()
    for gid, gdf in odds.groupby("game_id"):
        # choose a small candidate set per market group (you can expand this)
        pool = []
        # take top-1 price per leg_type per game_id per side (expand later)
        for leg_type, sdf in gdf.groupby("leg_type"):
            pool.extend(sdf.sort_values("price_decimal", ascending=False).head(2).to_dict("records"))  # 2 per type to keep combinatorics sane

        # build combinations 2..MAX_LEGS
        recs = []
        for k in range(2, min(MAX_LEGS, len(pool)) + 1):
            for combo in combinations(pool, k):
                leg_types = [c["leg_type"] for c in combo]
                # Skip obviously conflicting legs (e.g., H2H both sides)
                if any(leg_types.count(t) > 2 for t in set(leg_types)): 
                    continue

                price_prod = np.prod([c["price_decimal"] for c in combo])
                p_prod = np.prod([implied_prob(c["price_decimal"]) for c in combo])

                cf = cap_factor(combo, caps)
                eff_price = price_prod / cf
                implied = min(1.0, p_prod * cf)   # conservative: raise P(all) by same factor
                ev = eff_price * implied - 1.0

                recs.append({
                    "game_id": gid,
                    "legs": " + ".join([f'{c["market"]}:{c["selection"]}' for c in combo]),
                    "leg_types": ",".join(leg_types),
                    "n_legs": k,
                    "combined_price_naive": price_prod,
                    "cap_factor": cf,
                    "combined_price": eff_price,
                    "implied_prob": implied,
                    "expected_value": ev
                })
        out_rows.extend(recs)

    sgm = pd.DataFrame(out_rows).sort_values(["expected_value","combined_price"], ascending=[False, False])
    sgm.to_csv(GOLD/"sgm_candidates.csv", index=False)
    return sgm

def main():
    pm, pt, odds = load_inputs()
    sgm = gen_same_game_multis(pm, pt, odds)
    print(f"Wrote {GOLD/'sgm_candidates.csv'} with {len(sgm)} candidates")

if __name__ == "__main__":
    main()
