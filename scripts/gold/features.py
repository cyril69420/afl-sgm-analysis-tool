"""
gold.features
==============

This module provides very simple feature engineering for the AFL SGM analysis
pipeline.  In a production pipeline these functions would compute rolling
statistics from the silver layer (team form, venue effects, rest days, etc.).
For the purposes of this demonstration we work with a minimal set of
hand‑crafted features.  A logistic function is used to translate a seed
difference into a baseline win probability.  Seeds roughly correspond to
ladder positions – lower values indicate better teams.

Functions
---------
get_game_seeds() -> dict
    Return a mapping of game names to (home_seed, away_seed).  In a real
    implementation these would be learned from historical results.

logistic_prob(diff: float, scale: float = 0.5) -> float
    Compute a logistic probability given a seed difference and a scale
    parameter.  The scale controls how quickly the probability moves away
    from 0.5 as the seed difference increases.

"""
from __future__ import annotations

import math
from typing import Dict, Tuple


def get_game_seeds() -> Dict[str, Tuple[int, int]]:
    """Return hard‑coded seed pairs for each finals match.

    The seeds roughly represent ladder finishing positions (1 = minor
    premiers).  These values were chosen for demonstration purposes only.

    Returns
    -------
    Dict[str, Tuple[int, int]]
        Mapping of game names to (home_seed, away_seed).
    """
    return {
        "Collingwood vs Western Bulldogs": (1, 8),
        "Brisbane vs Port Adelaide": (2, 3),
        "Melbourne vs Carlton": (4, 5),
        "St Kilda vs GWS Giants": (6, 7),
    }


def logistic_prob(diff: float, scale: float = 0.5) -> float:
    """Compute a logistic probability from a seed difference.

    Parameters
    ----------
    diff : float
        The difference in seeds (away_seed - home_seed).  A positive
        difference implies the home team is stronger.
    scale : float, optional
        Scale parameter controlling the steepness of the logistic curve,
        by default 0.5.

    Returns
    -------
    float
        A probability between 0 and 1 representing the home team win chance.
    """
    return 1.0 / (1.0 + math.exp(-diff / scale))
