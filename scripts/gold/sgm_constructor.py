"""
gold.sgm_constructor
=====================

This module provides utilities to build candidate same‑game multis (SGMs)
from the pricing and model probabilities computed in the other gold
modules.  An SGM consists of between two and four legs drawn from the
head‑to‑head and totals markets for a single match.  Because legs within
the same game are positively correlated we apply a correlation penalty
when combining probabilities.  The penalty reduces the joint probability
below the naive product of individual leg probabilities to account for
overlap in outcomes.

For this demonstration we construct a small set of two‑leg SGMs.  For
each match we offer two SGMs:

* **Favourite and over** – the higher seeded team to win and the match
  total to exceed the bookmaker's points line.  This combination targets
  games expected to be open and high scoring.
* **Underdog and under** – the lower seeded team to win and the match
  total to stay below the line.  This is a contrarian angle that pays
  higher odds but carries lower probability.

Functions
---------
construct_candidate_sgms() -> list[dict]
    Build a list of candidate SGMs with pricing, probabilities, EV and
    explanatory rationale.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from .ev import apply_correlation, ev as ev_fn
from .markets import GAMES
from .models import get_team_probabilities, get_total_probabilities


def _pick_penalty(num_legs: int) -> float:
    """Return a correlation penalty for a given number of legs.

    In general the more legs included the higher the correlation and
    therefore the larger the discount applied to the joint probability.

    Parameters
    ----------
    num_legs : int
        Number of legs in the multi.

    Returns
    -------
    float
        A penalty between 0 and 1.
    """
    if num_legs <= 1:
        return 1.0
    elif num_legs == 2:
        return 0.90
    elif num_legs == 3:
        return 0.80
    else:
        # conservative penalty for 4+ legs
        return 0.70


def construct_candidate_sgms() -> List[Dict[str, object]]:
    """Construct a list of candidate same‑game multis for each match.

    The returned list contains dictionaries with keys:

    ``sgm_id`` : str
        A unique identifier for the SGM (e.g. ``"G1-OV-FAV"``).

    ``game`` : str
        Human‑readable matchup (e.g. ``"Collingwood vs Western Bulldogs"``).

    ``legs`` : list[str]
        The legs that make up the SGM (e.g. ``["Collingwood H2H", "Over 160.5"]``).

    ``price`` : float
        The combined decimal price (product of leg prices).

    ``book_prob`` : float
        The bookmaker implied probability after vig removal and correlation
        penalty.

    ``model_prob`` : float
        The model probability after applying the correlation penalty.

    ``ev`` : float
        The expected value per unit stake, computed as ``model_prob * price - 1``.

    ``rationale`` : str
        A short explanation of why the SGM was constructed.

    Returns
    -------
    list[dict]
        The constructed SGMs.
    """
    team_probs = get_team_probabilities()
    total_probs = get_total_probabilities()
    sgms: List[Dict[str, object]] = []
    for idx, (game, info) in enumerate(GAMES.items(), start=1):
        home_team = info["home_team"]
        away_team = info["away_team"]
        # Extract price and probabilities for head‑to‑head legs
        team_probs_game = team_probs.get(game, {})
        total_probs_game = total_probs.get(game, {})

        # Determine favourite (higher model probability) and underdog
        fav_team = home_team if team_probs_game[home_team][1] >= team_probs_game[away_team][1] else away_team
        unfav_team = away_team if fav_team == home_team else home_team

        # Favourite + Over
        fav_odds = info["home_odds"] if fav_team == home_team else info["away_odds"]
        fav_book_p, fav_model_p = team_probs_game[fav_team]
        over_market = next(k for k in total_probs_game if k.startswith("Over"))
        over_odds = info["over_odds"]
        over_book_p, over_model_p = total_probs_game[over_market]
        legs = [f"{fav_team} H2H", over_market]
        price = fav_odds * over_odds
        penalty = _pick_penalty(len(legs))
        book_prob = apply_correlation([fav_book_p, over_book_p], penalty)
        model_prob = apply_correlation([fav_model_p, over_model_p], penalty)
        sgms.append({
            "sgm_id": f"G{idx}-FAV-OVER",
            "game": game,
            "legs": legs,
            "price": round(price, 3),
            "book_prob": round(book_prob, 4),
            "model_prob": round(model_prob, 4),
            "ev": round(ev_fn(price, model_prob), 4),
            "rationale": (
                f"Backing the favourite {fav_team} to win with the over {over_market.split()[1]} line. "
                f"The favourite has a higher model probability and the game could be high scoring."
            ),
        })

        # Underdog + Under
        dog_team = unfav_team
        dog_odds = info["home_odds"] if dog_team == home_team else info["away_odds"]
        dog_book_p, dog_model_p = team_probs_game[dog_team]
        under_market = next(k for k in total_probs_game if k.startswith("Under"))
        under_odds = info["under_odds"]
        under_book_p, under_model_p = total_probs_game[under_market]
        legs = [f"{dog_team} H2H", under_market]
        price = dog_odds * under_odds
        book_prob = apply_correlation([dog_book_p, under_book_p], penalty)
        model_prob = apply_correlation([dog_model_p, under_model_p], penalty)
        sgms.append({
            "sgm_id": f"G{idx}-DOG-UNDER",
            "game": game,
            "legs": legs,
            "price": round(price, 3),
            "book_prob": round(book_prob, 4),
            "model_prob": round(model_prob, 4),
            "ev": round(ev_fn(price, model_prob), 4),
            "rationale": (
                f"Taking a contrarian view on {dog_team} with the under {under_market.split()[1]} line. "
                f"This long‑shot multi offers a high payout but relies on a low scoring upset."
            ),
        })
    return sgms
