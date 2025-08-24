#!/usr/bin/env bash
set -euo pipefail
ROOT_WIN="C:\\Users\\Ethan\\SGM Agent Project"
ROOT="/c/Users/Ethan/SGM Agent Project"
SNAPSHOT_TS=$(date +"%Y%m%d_%H%M%S")
BRONZE_ODDS_DIR="$ROOT/bronze/odds/snapshot_ts=$SNAPSHOT_TS"
mkdir -p "$BRONZE_ODDS_DIR"
echo "[info] Snapshot: $SNAPSHOT_TS"
echo "[info] Writing to: $BRONZE_ODDS_DIR"
echo "bookmaker,game_ref,market_name,selection_name,line,price,event_time_utc" > "$BRONZE_ODDS_DIR/odds_example.csv"
echo "sportsbet,ESSvCAR,Match Result,Essendon,,1.95,2025-08-22T09:30:00Z" >> "$BRONZE_ODDS_DIR/odds_example.csv"
echo "sportsbet,ESSvCAR,Match Result,Carlton,,1.95,2025-08-22T09:30:00Z" >> "$BRONZE_ODDS_DIR/odds_example.csv"
echo "sportsbet,ESSvCAR,Total Points Over/Under,Over,170.5,1.87,2025-08-22T09:30:00Z" >> "$BRONZE_ODDS_DIR/odds_example.csv"
echo "sportsbet,ESSvCAR,Total Points Over/Under,Under,170.5,1.93,2025-08-22T09:30:00Z" >> "$BRONZE_ODDS_DIR/odds_example.csv"
echo "[done] Bronze odds snapshot created."
