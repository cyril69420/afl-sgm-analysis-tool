"""
gold.models
===========

This module implements simple baseline models for win probability and totals
markets.  The win model is a blend of a logistic prior derived from seed
difference and the bookmaker's vig‑removed probabilities.  Totals markets
are assumed to be fair (50/50) in the absence of other information.

Functions
---------
get_team_probabilities() -> dict
    Compute bookmaker and model probabilities for each team in each game.

get_total_probabilities() -> dict
    Compute bookmaker and model probabilities for over/under markets in each
    game.

"""
from __future__ import annotations

from typing import Dict, Tuple

from .features import get_game_seeds, logistic_prob
from .markets import GAMES, implied_prob, remove_vig


def get_team_probabilities() -> Dict[str, Dict[str, Tuple[float, float]]]:
    """Return bookmaker and model win probabilities for each team.

    The bookmaker probability is vig‑removed from the head‑to‑head prices.  The
    model probability is the average of a logistic prior and the bookmaker
    probability.  Keys are game names and values are dicts keyed by team name
    with (book_p, model_p) tuples.
    """
    seeds = get_game_seeds()
    team_probs: Dict[str, Dict[str, Tuple[float, float]]] = {}
    for game, info in GAMES.items():
        home, away = info["home_team"], info["away_team"]
        home_odds, away_odds = info["home_odds"], info["away_odds"]
        p_home = implied_prob(home_odds)
        p_away = implied_prob(away_odds)
        p_home_unvig, p_away_unvig = remove_vig(p_home, p_away)
        # logistic prior: diff = away_seed - home_seed (positive => home better)
        hs, as_ = seeds.get(game, (0, 0))
        diff = as_ - hs
        logistic_home = logistic_prob(diff)
        logistic_away = 1.0 - logistic_home
        # blend equally
        model_home = 0.5 * logistic_home + 0.5 * p_home_unvig
        model_away = 0.5 * logistic_away + 0.5 * p_away_unvig
        team_probs[game] = {
            home: (p_home_unvig, model_home),
            away: (p_away_unvig, model_away),
        }
    return team_probs


def get_total_probabilities() -> Dict[str, Dict[str, Tuple[float, float]]]:
    """Return bookmaker and model probabilities for total points markets.

    For each game the bookmaker probabilities are vig‑removed from the over
    and under prices.  The model assumes a symmetric 50/50 distribution.
    Keys are game names and values are dicts keyed by market description
    (e.g. ``"Over 160.5"``) with (book_p, model_p) tuples.
    """
    totals: Dict[str, Dict[str, Tuple[float, float]]] = {}
    for game, info in GAMES.items():
        over_odds, under_odds = info["over_odds"], info["under_odds"]
        p_over = implied_prob(over_odds)
        p_under = implied_prob(under_odds)
        p_over_unvig, p_under_unvig = remove_vig(p_over, p_under)
        line = info["total_line"]
        totals[game] = {
            f"Over {line}": (p_over_unvig, 0.5),
            f"Under {line}": (p_under_unvig, 0.5),
        }
    return totals
