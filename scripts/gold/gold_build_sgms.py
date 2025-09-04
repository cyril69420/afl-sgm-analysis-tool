#!/usr/bin/env python
"""
Generate candidate SGM combinations and calculate their EV.
"""
from __future__ import annotations
import argparse, logging, math
from pathlib import Path
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="Build SGM combos from fair odds and player stats")
    parser.add_argument("--root", default=".", help="Project root directory")
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g. 2025)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s | %(levelname)s | %(message)s")
    root = Path(args.root).resolve()

    # Load inputs
    fair_odds_path = root / "gold" / "odds_latest_fair.parquet"
    player_stats_path = root / "silver" / "f_player_game_stats.parquet"
    fixture_path = root / "silver" / "f_fixture.parquet"
    team_dim_path = root / "silver" / "dim_team.parquet"
    player_dim_path = root / "silver" / "dim_player.parquet"
    if not fair_odds_path.exists():
        logging.error("Fair odds file not found. Run gold_devig_latest first.")
        return
    fair_df = pd.read_parquet(fair_odds_path)
    # Filter to relevant market types
    allowed_types = {"match_result", "total_points", "player_disposals", "player_goals"}
    fair_df = fair_df[fair_df["market_type"].isin(allowed_types)]
    if fair_df.empty:
        logging.error("No odds data for allowed markets after filtering.")
        return

    # Load player stats and prepare stats dictionary
    if not player_stats_path.exists():
        logging.error("Player stats data not found (expected f_player_game_stats.parquet).")
        return
    stats_df = pd.read_parquet(player_stats_path)
    # Consider stats from the target season and previous season for form
    if "season" in stats_df.columns:
        stats_df = stats_df[stats_df["season"].isin([args.season, args.season - 1])]
    # Ensure disposals column exists
    if "disposals" not in stats_df.columns:
        if "kicks" in stats_df.columns and "handballs" in stats_df.columns:
            stats_df["disposals"] = stats_df["kicks"] + stats_df["handballs"]
        else:
            logging.error("Disposals stat not found in player stats.")
            return
    player_stats = {}
    for pid, grp in stats_df.groupby("player_id"):
        disposals_list = grp["disposals"].dropna().tolist()
        goals_list = grp["goals"].dropna().tolist() if "goals" in grp.columns else []
        player_stats[pid] = {
            "games": len(grp),
            "disposals": disposals_list,
            "goals": goals_list
        }

    # Load team and player name mappings for output
    team_name_map = {}
    if team_dim_path.exists():
        team_df = pd.read_parquet(team_dim_path)
        if "team_id" in team_df.columns and "team_name_canon" in team_df.columns:
            team_name_map = dict(zip(team_df["team_id"], team_df["team_name_canon"]))
    player_name_map = {}
    if player_dim_path.exists():
        player_df = pd.read_parquet(player_dim_path)
        if "player_id" in player_df.columns and "player_name_canon" in player_df.columns:
            player_name_map = dict(zip(player_df["player_id"], player_df["player_name_canon"]))

    # Load fixtures to get home/away team for game labeling
    fixtures_df = pd.read_parquet(fixture_path)
    fixtures_df = fixtures_df[fixtures_df["season"] == args.season][["match_id", "round", "home_team_id", "away_team_id"]]

    combos = []  # to accumulate results
    for mid, grp in fair_df.groupby("match_id"):
        outcomes = grp.to_dict('records')
        # Consider all unique pairs of outcomes in this match
        for i in range(len(outcomes)):
            for j in range(i+1, len(outcomes)):
                a = outcomes[i]; b = outcomes[j]
                # Skip if both legs from the same market (mutually exclusive or redundant)
                if a["market_type"] == b["market_type"]:
                    if a["market_type"] == "match_result":
                        continue  # can't take two sides of match result
                    if a.get("line") == b.get("line"):
                        # same line (totals or player) and same market type
                        if a.get("selection_ref") == b.get("selection_ref"):
                            continue  # e.g. same player O/U or same total O/U
                # Calculate leg probabilities with modeling
                def modeled_prob(leg):
                    # Base probability from fair odds:
                    p_market = leg.get("fair_prob")
                    if p_market is None:
                        p_market = 1.0 / leg["price"]  # fallback, but fair_prob should exist
                    mtype = leg["market_type"]; sel = str(leg.get("selection") or "")
                    prob = float(p_market)
                    if mtype in ("player_disposals", "player_goals"):
                        line_val = float(leg.get("line") or 0.0)
                        # If it's a yes/no market with no line (like anytime scorer), set 0.5 line for logic
                        if mtype == "player_goals" and math.isnan(line_val):
                            line_val = 0.5
                            # Treat selection "Over" as "Yes (>=1)", "Under" as "No (0 goals)"
                            if sel.lower() in ["no", "under"]:
                                sel = "Under"
                            else:
                                sel = "Over"
                        pid = leg.get("selection_ref") or leg.get("selection")  # player identifier
                        # Get player stats if available:
                        if pid in player_stats:
                            stats = player_stats[pid]; games = stats["games"]
                        else:
                            stats = None; games = 0
                        emp_prob = None
                        if stats:
                            if mtype == "player_disposals":
                                # threshold for disposals
                                thresh = math.floor(line_val)
                                if sel.lower() == "over":
                                    # need > thresh (since line likely x.5, >thresh implies >= thresh+1)
                                    emp_count = sum(1 for d in stats["disposals"] if d > thresh)
                                else:  # under
                                    emp_count = sum(1 for d in stats["disposals"] if d <= thresh)
                                emp_prob = emp_count / games if games > 0 else None
                            elif mtype == "player_goals":
                                thresh = math.floor(line_val)
                                if sel.lower() == "over":
                                    emp_count = sum(1 for g in stats["goals"] if g > thresh)
                                else:
                                    emp_count = sum(1 for g in stats["goals"] if g <= thresh)
                                emp_prob = emp_count / games if games > 0 else None
                        # Blend empirical and market probabilities
                        if emp_prob is not None:
                            weight = 0.0
                            if games >= 5:
                                weight = min(1.0, games / 20.0)  # ramp up weight with sample size
                            prob = weight * emp_prob + (1 - weight) * prob
                    return max(0.0, min(1.0, prob))
                p1 = modeled_prob(a); p2 = modeled_prob(b)
                combo_prob = p1 * p2  # independence assumption
                combo_odds = a["price"] * b["price"]
                ev = combo_prob * combo_odds - 1.0
                if ev > 1e-9:  # positive EV (use a tiny epsilon to avoid float precision zero)
                    # Describe the legs
                    def describe_leg(leg):
                        mtype = leg["market_type"]; sel = str(leg.get("selection") or "")
                        if mtype == "match_result":
                            # leg["selection"] might be canonical team name or alias
                            team_id = leg.get("selection_ref")
                            team_label = team_name_map.get(team_id, str(leg.get("selection") or ""))
                            return f"{team_label.replace('_',' ').title()} to Win"
                        elif mtype == "total_points":
                            line_val = leg.get("line")
                            return f"{sel} {line_val} Total Points"
                        elif mtype == "player_disposals":
                            line_val = leg.get("line"); pid = leg.get("selection_ref")
                            pname = str(pid)
                            if pid in player_name_map:
                                pname = player_name_map[pid].replace('_',' ').title()
                            return f"{pname} {sel} {line_val} Disposals"
                        elif mtype == "player_goals":
                            line_val = leg.get("line") if pd.notnull(leg.get("line")) else 0.5
                            pid = leg.get("selection_ref"); pname = str(pid)
                            if pid in player_name_map:
                                pname = player_name_map[pid].replace('_',' ').title()
                            return f"{pname} {sel} {line_val} Goals"
                        else:
                            # Should not happen for allowed_types
                            return f"{mtype}: {sel}"
                    leg1_desc = describe_leg(a); leg2_desc = describe_leg(b)
                    combos.append({
                        "match_id": mid,
                        "leg1": leg1_desc,
                        "leg2": leg2_desc,
                        "combined_price": combo_odds,
                        "combo_prob": combo_prob,
                        "ev": ev
                    })
    if not combos:
        logging.info("No positive EV SGM combos found for season %s.", args.season)
        return
    combos_df = pd.DataFrame(combos)
    # Merge to get game info
    combos_df = combos_df.merge(fixtures_df, on="match_id", how="left")
    if not combos_df.empty and "home_team_id" in combos_df.columns:
        combos_df["home_name"] = combos_df["home_team_id"].map(team_name_map) if team_name_map else combos_df["home_team_id"]
        combos_df["away_name"] = combos_df["away_team_id"].map(team_name_map) if team_name_map else combos_df["away_team_id"]
        # Format game string e.g. "R10: TeamA @ TeamB"
        combos_df["game"] = combos_df.apply(lambda row: 
                                           f"R{int(row['round'])}: {str(row['away_name']).replace('_',' ').title()} @ {str(row['home_name']).replace('_',' ').title()}", 
                                           axis=1)
    else:
        combos_df["game"] = combos_df["match_id"].astype(str)
    # Sort by EV descending
    combos_df.sort_values("ev", ascending=False, inplace=True)
    # Save output
    out_path = root / "gold" / "sgm_candidates.parquet"
    combos_df.to_parquet(out_path, index=False)
    logging.info("Generated %d SGM candidates (positive EV) and saved to %s", len(combos_df), out_path)

if __name__ == "__main__":
    main()
