#!/usr/bin/env python
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd


def _read_bronze_players(bronze_root: Path, season: int) -> pd.DataFrame:
    part = bronze_root / f"season={season}"
    pq = part / "players.parquet"
    csv = part / "players.csv"
    if pq.exists():
        return pd.read_parquet(pq)
    if csv.exists():
        return pd.read_csv(csv)
    return pd.DataFrame()


def _canon_name(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return " ".join(str(s).strip().split())


def main():
    ap = argparse.ArgumentParser(description="Build Silver player game stats from Bronze AFLTables GBG")
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--season", type=int, required=True, help="Season to process, e.g. 2025")
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--csv-mirror", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=getattr(logging, args.log_level.upper(), logging.INFO),
    )
    root = Path(args.root).resolve()
    bronze_root = root / "bronze" / "player_stats"
    silver_root = root / "silver"
    silver_root.mkdir(parents=True, exist_ok=True)

    logging.info("Silver.player_stats: loading Bronze player stats for season %s …", args.season)
    df = _read_bronze_players(bronze_root, args.season)
    if df.empty:
        logging.warning("No Bronze player stats found for season=%s at %s", args.season, bronze_root)
        # Still write an empty table to keep downstream deterministic
        out_pq = silver_root / "f_player_game_stats.parquet"
        pd.DataFrame().to_parquet(out_pq, index=False)
        if args.csv_mirror:
            (silver_root / "f_player_game_stats.csv").write_text("")
        logging.info("Wrote empty f_player_game_stats (no rows).")
        return

    # Keep/rename core columns
    # Bronze has: season, team, player, round_label, round_num, is_finals, opponent_code, DI, KI, HB, GL, … (per ingester). :contentReference[oaicite:8]{index=8}
    df = df.copy()
    df.rename(columns={"player": "player_name_raw"}, inplace=True)
    df["player_name_canon"] = df["player_name_raw"].map(_canon_name)

    # Derive disposals + goals with safe fallbacks
    if "DI" in df.columns:
        df["disposals"] = df["DI"]
    else:
        ki = df["KI"] if "KI" in df.columns else 0
        hb = df["HB"] if "HB" in df.columns else 0
        df["disposals"] = ki + hb
    df["goals"] = df["GL"] if "GL" in df.columns else 0

    keep_cols = [
        "season", "team", "player_name_canon", "player_name_raw",
        "round_label", "round_num", "is_finals", "opponent_code",
        "disposals", "goals", "KI", "HB", "TK", "CL", "FF", "FA", "CP", "UP", "MK", "%P",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    # Attach player_id from dim_player if available
    dim_player_path = silver_root / "dim_player.parquet"
    if dim_player_path.exists():
        dimp = pd.read_parquet(dim_player_path)
        name_col = "player_name_canon"
        key_cols = [c for c in ["player_id", "player_name_canon"] if c in dimp.columns]
        if {"player_id", "player_name_canon"}.issubset(set(key_cols)):
            df = df.merge(dimp[key_cols], on=name_col, how="left")
            missing = df["player_id"].isna().sum()
            if missing:
                logging.info("Silver.player_stats: %d rows without player_id (name not in dim_player).", missing)
        else:
            logging.info("dim_player present but missing expected columns; skipping id join.")
    else:
        logging.info("dim_player not present; proceeding with name-only identifiers.")

    # Order + write
    out_cols = (
        ["season", "round_num", "is_finals", "round_label", "team",
         "player_id", "player_name_canon", "player_name_raw",
         "opponent_code", "disposals", "goals"]
        + [c for c in ["KI", "HB", "MK", "TK", "CL", "FF", "FA", "CP", "UP", "%P"] if c in df.columns]
    )
    df = df[out_cols].sort_values(["season", "round_num", "team", "player_name_canon"], na_position="last").reset_index(drop=True)

    out_pq = silver_root / "f_player_game_stats.parquet"
    if out_pq.exists() and not args.overwrite:
        logging.warning("Output exists and --overwrite not set; replacing file anyway to keep silver current.")
    df.to_parquet(out_pq, index=False)
    if args.csv_mirror:
        df.to_csv(silver_root / "f_player_game_stats.csv", index=False)

    logging.info("Silver.player_stats: wrote %d rows → %s", len(df), out_pq)
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug("Sample:\n%s", df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
