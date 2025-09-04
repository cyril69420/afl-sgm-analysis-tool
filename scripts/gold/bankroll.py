"""
gold.bankroll
===============

Functions for sizing stakes across a portfolio of same‑game multis.  The
principal goal is to allocate a fixed bankroll across a set of bets such
that the expected profit (sum of stake × EV) is non‑negative when the
``breakeven`` flag is set.  Stakes are rounded to a minimum unit to
reflect practical bet sizing constraints.

The simplest strategy implemented here is to distribute equal stakes
across all positive expected value bets up to the bankroll limit.  If
there are no positive EV bets then the system will allocate a minimal
stake to the highest probability SGM while keeping the net expected
profit as close to zero as possible.

Functions
---------
allocate_bankroll(sgms: list[dict], bankroll: float, min_unit: float = 0.5, breakeven: bool = True) -> Tuple[list[dict], dict]
    Assign stake sizes to a list of SGM dicts and return a summary.
"""
from __future__ import annotations

from typing import Dict, List, Tuple


def allocate_bankroll(
    sgms: List[Dict[str, object]],
    bankroll: float,
    min_unit: float = 0.5,
    breakeven: bool = True,
) -> Tuple[List[Dict[str, object]], Dict[str, float]]:
    """Allocate stakes to SGMs subject to a bankroll and breakeven constraint.

    Parameters
    ----------
    sgms : list of dict
        Candidate SGMs with at least the ``ev`` field.
    bankroll : float
        Total amount of money to allocate.
    min_unit : float, optional
        Minimum stake increment (bets must be multiples of this), by default 0.5.
    breakeven : bool, optional
        If True, ensure the expected profit is non‑negative; otherwise
        allocate across all positive EV SGMs without regard to breakeven.

    Returns
    -------
    Tuple[list[dict], dict]
        The updated SGMs with a ``stake`` key and a portfolio summary
        containing total_stake, expected_profit and expected_roi.
    """
    # Sort by expected value descending
    sorted_sgms = sorted(sgms, key=lambda x: x.get("ev", 0), reverse=True)
    remaining = bankroll
    # Initialise stakes to zero
    for s in sorted_sgms:
        s["stake"] = 0.0
    # Identify positive EV SGMs
    positives = [s for s in sorted_sgms if s.get("ev", 0) > 0]
    if positives:
        idx = 0
        # Allocate equal min_unit stakes until bankroll exhausted
        while remaining >= min_unit and idx < len(positives):
            positives[idx]["stake"] += min_unit
            remaining -= min_unit
            idx = (idx + 1) % len(positives)
    else:
        # No positive EV bets: allocate a single min_unit to the
        # highest probability SGM (lowest price) if breakeven is False;
        # otherwise leave bankroll untouched.
        if not breakeven and sorted_sgms:
            sorted_sgms[0]["stake"] = min(min_unit, bankroll)
            remaining -= sorted_sgms[0]["stake"]
        # If breakeven and no positive EV, we skip betting entirely.
    # Compute expected profit and adjust for breakeven if necessary
    def portfolio_metrics() -> Tuple[float, float, float]:
        total_stake = sum(s["stake"] for s in sorted_sgms)
        expected_profit = sum(s["stake"] * s["ev"] for s in sorted_sgms)
        expected_roi = 0.0 if total_stake == 0 else expected_profit / total_stake
        return total_stake, expected_profit, expected_roi

    total_stake, expected_profit, expected_roi = portfolio_metrics()
    # If breakeven is requested and expected profit is negative, trim stakes
    if breakeven and expected_profit < 0 and total_stake > 0:
        # Remove stakes from lowest EV bets until breakeven achieved
        # Sort bets with stake in ascending order of EV (worst first)
        bets_with_stake = sorted(
            [s for s in sorted_sgms if s["stake"] > 0], key=lambda x: x.get("ev", 0)
        )
        for bet in bets_with_stake:
            while bet["stake"] > 0 and expected_profit < 0:
                bet["stake"] -= min_unit
                remaining += min_unit
                total_stake, expected_profit, expected_roi = portfolio_metrics()
            if expected_profit >= 0:
                break
    # Prepare summary
    summary = {
        "total_stake": round(total_stake, 2),
        "expected_profit": round(expected_profit, 4),
        "expected_roi": round(expected_roi, 4),
        "remaining_bankroll": round(remaining, 2),
    }
    return sorted_sgms, summary
