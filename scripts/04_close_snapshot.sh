#!/usr/bin/env bash
set -euo pipefail
ROOT="/c/Users/Ethan/SGM Agent Project"
SNAPSHOT_TS=$(date +"%Y%m%d_%H%M%S")
BRONZE_ODDS_DIR="$ROOT/bronze/odds_closing/snapshot_ts=$SNAPSHOT_TS"
mkdir -p "$BRONZE_ODDS_DIR"
echo "[info] Capturing closing odds snapshot at $SNAPSHOT_TS -> $BRONZE_ODDS_DIR"
echo "[done] closing snapshot created."
