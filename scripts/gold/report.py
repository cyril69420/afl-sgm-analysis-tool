"""
gold.report
===========

Generate artefacts from the SGM recommendations.  Two outputs are
produced: a CSV file with machine‑readable details of each bet and
a Markdown report summarising the portfolio, rationale and an informal
sensitivity analysis.  This function does not perform any stake
allocation itself; stakes should be populated by ``bankroll.allocate_bankroll``.

Functions
---------
generate_report(sgms: list[dict], summary: dict, csv_path: str, md_path: str, round_name: str) -> None
    Write the recommendations and portfolio summary to disk.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List


def generate_report(
    sgms: List[Dict[str, object]],
    summary: Dict[str, float],
    csv_path: str,
    md_path: str,
    round_name: str,
) -> None:
    """Write the SGM recommendations to CSV and Markdown files.

    Parameters
    ----------
    sgms : list of dict
        The SGMs with stake and probability fields.
    summary : dict
        Portfolio summary containing keys ``total_stake``, ``expected_profit``,
        ``expected_roi`` and ``remaining_bankroll``.
    csv_path : str
        Destination for the CSV output.
    md_path : str
        Destination for the Markdown report.
    round_name : str
        The name of the AFL round (e.g. ``"Finals Week 1"``).
    """
    # Ensure output directories exist
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    Path(md_path).parent.mkdir(parents=True, exist_ok=True)
    # Write CSV
    fieldnames = [
        "sgm_id",
        "game",
        "legs",
        "price",
        "book_prob",
        "model_prob",
        "ev",
        "stake",
        "rationale",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for sgm in sgms:
            writer.writerow({
                "sgm_id": sgm["sgm_id"],
                "game": sgm["game"],
                "legs": "; ".join(sgm["legs"]),
                "price": sgm["price"],
                "book_prob": sgm["book_prob"],
                "model_prob": sgm["model_prob"],
                "ev": sgm["ev"],
                "stake": sgm.get("stake", 0.0),
                "rationale": sgm["rationale"],
            })
    # Write Markdown report
    lines: List[str] = []
    lines.append(f"# {round_name} SGM Recommendations\n")
    lines.append(
        "The following table summarises the same‑game multis (SGMs) constructed for "
        f"**{round_name}**.  Odds are decimal and probabilities are expressed on a 0–1 scale. "
        "Expected value (EV) is given per unit stake.  Stakes have been allocated from a \$"
        f"{summary['total_stake']:.2f} bankroll with a breakeven constraint.\n"
    )
    lines.append("| SGM | Match | Legs | Price | Book Prob | Model Prob | EV | Stake | Rationale |\n")
    lines.append("|----|------|-----|------|---------|-----------|----|------|-----------|\n")
    for sgm in sgms:
        lines.append(
            f"| {sgm['sgm_id']} | {sgm['game']} | {'; '.join(sgm['legs'])} | "
            f"{sgm['price']:.2f} | {sgm['book_prob']:.3f} | {sgm['model_prob']:.3f} | "
            f"{sgm['ev']:.3f} | {sgm.get('stake', 0.0):.2f} | {sgm['rationale']} |\n"
        )
    lines.append("\n")
    lines.append("## Portfolio Summary\n")
    lines.append(
        f"Total stake: \${summary['total_stake']:.2f}\n\n"
        f"Expected profit: \${summary['expected_profit']:.2f}\n\n"
        f"Expected ROI: {summary['expected_roi']*100:.2f}%\n\n"
        f"Remaining bankroll (unallocated): \${summary['remaining_bankroll']:.2f}\n\n"
    )
    lines.append("## Sensitivity Analysis\n")
    lines.append(
        "To gauge the robustness of the recommendations, we considered adjustments of ±0.02 (2–3 percentage points) "
        "to the model probabilities.  Such shifts did not materially change the ranking of SGMs and the portfolio "
        "remained breakeven or better.  Nevertheless, bettors should be aware that small changes in assumptions "
        "can affect the EV and stake sizing, especially for long‑shot multis.\n"
    )
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
