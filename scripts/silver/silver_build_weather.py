# scripts/silver/silver_build_weather.py
from __future__ import annotations

import argparse
import json
import logging
import shutil
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import duckdb
import pandas as pd


# -----------------------
# Small helpers
# -----------------------

def _norm_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower().replace("’", "'")
    out = []
    prev_space = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_space = False
        else:
            if not prev_space:
                out.append(" ")
                prev_space = True
    return " ".join("".join(out).split())


def _q(s: str) -> str:
    return s.replace("'", "''")


def _to_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def _write_df_parquet_partitioned(df: pd.DataFrame, out_dir: Path, partition_cols: List[str], overwrite: bool):
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.register("tbl", df)
    if overwrite and out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    cols = ", ".join(df.columns) if len(df.columns) else "*"
    part = ", ".join(partition_cols)
    con.execute(f"""
        COPY (SELECT {cols} FROM tbl)
        TO '{_q(out_dir.as_posix())}'
        (FORMAT PARQUET, PARTITION_BY ({part}))
    """)


# -----------------------
# CLI
# -----------------------

def parse_args():
    p = argparse.ArgumentParser("Build Silver weather facts: hourly + at-kickoff (ID-based join via dim_venue).")
    p.add_argument("--bronze-dir", default="bronze")
    p.add_argument("--silver-dir", default="silver")
    p.add_argument("--season", type=int, default=None)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--csv-mirror", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--hide-examples", action="store_true", help="Drop example fixtures/venues from processing & outputs.")
    p.add_argument("--max-minutes-to-ko", type=int, default=None,
                   help="If set, discard weather rows farther than this many minutes from KO when selecting at-KO.")
    p.add_argument("--venue-alias-csv", default="config/venues_aliases.csv",
                   help="CSV that maps alias -> canonical venue. Optional; we still snap to dim_venue.")
    p.add_argument("--debug-dir", default=None, help="Write CSV diagnostics here (e.g., silver_debug).")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level.")
    return p.parse_args()


# -----------------------
# Venue aliasing (maps raw names -> whatever exists in dim_venue)
# -----------------------

def _read_alias_csv_flexible(path: Path) -> pd.DataFrame:
    candidate = path
    if not candidate.exists():
        alt = path.parent / "venue_aliases.csv"  # also accept singular form
        candidate = alt if alt.exists() else path

    if not candidate.exists():
        return pd.DataFrame(columns=["alias", "venue_name_canon"])

    try:
        df = pd.read_csv(candidate)
        cols = [c.strip().lower() for c in df.columns]
        df.columns = cols

        if "alias" in cols and "venue_name_canon" in cols:
            use = df[["alias", "venue_name_canon"]].dropna(how="any")
        elif len(cols) >= 2:
            logging.warning(
                "Alias CSV %s has headers %s; assuming first->second are alias->canon.",
                candidate.as_posix(), cols
            )
            use = df[[cols[0], cols[1]]].rename(columns={cols[0]: "alias", cols[1]: "venue_name_canon"}).dropna(how="any")
        else:
            logging.warning("Alias CSV %s has insufficient columns; ignoring.", candidate.as_posix())
            return pd.DataFrame(columns=["alias", "venue_name_canon"])

        if use.empty:
            logging.warning("Alias CSV %s parsed but contains 0 usable rows.", candidate.as_posix())
        else:
            logging.info("Loaded %d alias rows from %s with columns %s",
                         len(use), candidate.as_posix(), list(use.columns))
        return use
    except Exception as e:
        logging.warning("Failed to read venue-alias CSV %s: %s", candidate.as_posix(), e)
        return pd.DataFrame(columns=["alias", "venue_name_canon"])


def _dynamic_synonyms(canon_set: set) -> List[Tuple[str, str]]:
    syn: List[Tuple[str, str]] = []
    # MCG family (map all to whatever exists in dim)
    mcg_targets = [x for x in ("the mcg", "mcg", "melbourne cricket ground") if x in canon_set]
    if mcg_targets:
        target = mcg_targets[0]
        for alias in ("melbourne cricket ground", "the mcg", "mcg"):
            if alias != target:
                syn.append((alias, target))
    # Sydney Showground / ENGIE / GIANTS family (map to whatever exists in dim)
    sss_targets = [x for x in ("engie stadium", "sydney showground stadium", "giants stadium") if x in canon_set]
    if sss_targets:
        target = sss_targets[0]
        for alias in ("engie stadium", "sydney showground stadium", "giants stadium", "sydney showground"):
            if alias != target:
                syn.append((alias, target))
    return syn


def _build_venue_alias_map(dim_venue_df: pd.DataFrame, user_alias_df: pd.DataFrame) -> pd.DataFrame:
    canon_df = dim_venue_df[["venue_name"]].drop_duplicates().copy()
    canon_df["canon_norm"] = canon_df["venue_name"].map(_norm_text)
    canon_set = set(canon_df["canon_norm"])

    # self-maps
    alias_rows = [{"alias": v, "venue_name_canon": v} for v in dim_venue_df["venue_name"].dropna().unique().tolist()]

    # dynamic synonyms (based on what actually exists in dim)
    for a, c in _dynamic_synonyms(canon_set):
        alias_rows.append({"alias": a, "venue_name_canon": c})

    # user-provided rows
    if not user_alias_df.empty:
        alias_rows.extend(user_alias_df.to_dict(orient="records"))

    alias_df = pd.DataFrame(alias_rows).drop_duplicates()
    alias_df["alias_norm"] = alias_df["alias"].map(_norm_text)
    alias_df["canon_norm_raw"] = alias_df["venue_name_canon"].map(_norm_text)

    # snap canon to dim via direct/containment
    def snap_to_dim(n: str) -> str:
        if n in canon_set:
            return n
        n_tok = f" {n} "
        for d in canon_set:
            d_tok = f" {d} "
            if n_tok in d_tok or d_tok in n_tok:
                return d
        return n

    alias_df["canon_norm"] = alias_df["canon_norm_raw"].map(snap_to_dim)
    alias_df = alias_df[["alias_norm", "canon_norm"]].drop_duplicates()
    return alias_df


# -----------------------
# Main
# -----------------------

def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.info("Starting silver_build_weather (log level=%s)", args.log_level.upper())

    bron = Path(args.bronze_dir)
    silv = Path(args.silver_dir)
    _ensure_dir(silv)
    dbg = Path(args.debug_dir) if args.debug_dir else None
    if dbg:
        _ensure_dir(dbg)

    # Inputs
    fix_path = silv / "f_fixture.parquet"
    dim_venue_path = silv / "dim_venue.parquet"
    if not fix_path.exists():
        raise SystemExit(f"{fix_path.as_posix()} not found. Build Silver core first.")
    if not dim_venue_path.exists():
        raise SystemExit(f"{dim_venue_path.as_posix()} not found. Build Silver core first.")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # Fixtures + venues (from Silver)
    con.execute(f"CREATE OR REPLACE VIEW f_fix AS SELECT * FROM read_parquet('{_q(fix_path.as_posix())}')")
    con.execute(f"CREATE OR REPLACE VIEW dim_venue AS SELECT * FROM read_parquet('{_q(dim_venue_path.as_posix())}')")

    fix_df = con.execute("""
        SELECT
          f.*,
          dv.venue_name,
          regexp_replace(regexp_replace(lower(dv.venue_name), '[^a-z0-9]+', ' ', 'g'), '\s+', ' ', 'g') AS venue_norm
        FROM f_fix f
        JOIN dim_venue dv USING(venue_id)
    """).fetchdf()

    # Sanity: fixture schema (we expect game_key, venue_id)
    if args.verbose:
        print("\n[Fixture columns]")
        print(sorted(list(fix_df.columns)))

    # Parse KO time
    fix_df["scheduled_time_utc"] = _to_ts(fix_df.get("scheduled_time_utc"))

    # Season filter
    if args.season is not None:
        fix_df = fix_df[fix_df["season"] == int(args.season)].copy()

    # Drop example fixtures if requested
    if args.hide_examples:
        before = len(fix_df)
        fix_df = fix_df[~fix_df["venue_norm"].str.contains("example", na=False)].copy()
        logging.info("[hide-examples] Dropped %d example fixtures based on venue", before - len(fix_df))

    if fix_df.empty:
        logging.warning("No fixtures after filters; writing empty weather outputs.")
        _write_empty_outputs(silv, args)
        return

    # Build alias map using dim_venue canon
    dimv_all = con.execute("SELECT DISTINCT venue_id, venue_name FROM dim_venue").fetchdf()
    dimv_all["venue_norm_canon"] = dimv_all["venue_name"].map(_norm_text)

    user_alias_df = _read_alias_csv_flexible(Path(args.venue_alias_csv))
    alias_map = _build_venue_alias_map(dimv_all[["venue_name"]], user_alias_df)
    if dbg is not None:
        alias_map.to_csv(dbg / "venue_alias_map_effective.csv", index=False)

    # Canonise fixture venue names (for display only; join will use venue_id)
    fix_df["venue_norm_canon"] = fix_df["venue_norm"]
    fix_df = fix_df.merge(alias_map, left_on="venue_norm", right_on="alias_norm", how="left")
    fix_df["venue_norm_canon"] = fix_df["canon_norm"].fillna(fix_df["venue_norm"])
    fix_df = fix_df.drop(columns=["alias_norm", "canon_norm"], errors="ignore")

    # ---- Load Bronze forecast (Hive partitions → season, round, venue)
    # Avoid duplicate partition columns by excluding them from *.
    wf_glob = (bron / "weather" / "forecast" / "season=*" / "round=*" / "venue=*" / "*").as_posix()
    weather_df = con.execute(f"""
        SELECT
          season::BIGINT AS season,
          round::VARCHAR AS round,
          venue::VARCHAR AS venue,
          * EXCLUDE (season, round, venue)
        FROM read_parquet('{_q(wf_glob)}', hive_partitioning=1)
    """).fetchdf()

    # As an extra guard, drop any accidental dups that may sneak in
    weather_df = weather_df.loc[:, ~weather_df.columns.duplicated()]

    if weather_df.empty:
        logging.warning("No weather forecast rows found in bronze/weather/forecast; writing empty outputs.")
        _write_empty_outputs(silv, args)
        return

    if args.season is not None:
        weather_df = weather_df[weather_df["season"] == int(args.season)].copy()

    # Normalise weather venue names and canonicalise to dim_venue, then attach venue_id
    weather_df["venue_norm"] = weather_df["venue"].map(_norm_text)
    if args.hide_examples:
        before = len(weather_df)
        weather_df = weather_df[~weather_df["venue_norm"].str.contains("example", na=False)].copy()
        logging.info("[hide-examples] Dropped %d example weather rows", before - len(weather_df))

    weather_df["venue_norm_canon"] = weather_df["venue_norm"]
    weather_df = weather_df.merge(alias_map, left_on="venue_norm", right_on="alias_norm", how="left")
    weather_df["venue_norm_canon"] = weather_df["canon_norm"].fillna(weather_df["venue_norm"])
    weather_df = weather_df.drop(columns=["alias_norm", "canon_norm"], errors="ignore")

    # Map canon name -> venue_id (ID-based joining from here on)
    weather_df = weather_df.merge(
        dimv_all[["venue_id", "venue_norm_canon"]],
        on="venue_norm_canon",
        how="left"
    )

    # ---- Coalesce weather timestamps
    time_candidates = [
        "weather_time_utc", "valid_at_utc", "forecast_time_utc",
        "forecast_hour_utc", "time_utc", "observed_at_utc"
    ]
    cap_candidates = ["captured_at_utc", "ingested_at_utc", "scraped_at_utc"]

    def coalesce_time(df: pd.DataFrame, cols: List[str]) -> pd.Series:
        z = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
        for c in cols:
            if c in df.columns:
                s = _to_ts(df[c])
                z = z.fillna(s)
        return z

    weather_df["weather_time_utc"] = coalesce_time(weather_df, time_candidates)
    weather_df["captured_at_utc"] = coalesce_time(weather_df, cap_candidates)

    # Common weather fields (keep if present)
    common_fields = [
        "temp_c", "feelslike_c", "wind_kph", "wind_dir", "wind_degree",
        "pressure_hpa", "humidity", "cloud", "precip_mm", "chance_of_rain_pct",
        "uv_index", "dewpoint_c", "visibility_km", "condition_text", "condition_code",
        # support alt names we saw in your sample
        "cloud_pct", "rain_prob", "wind_speed_ms", "wind_gust_ms", "tz", "provider", "lat", "lon"
    ]
    present_common = [c for c in common_fields if c in weather_df.columns]

    # payload_json for all extra columns
    base_cols = {"season", "round", "venue", "venue_norm", "venue_norm_canon", "venue_id",
                 "weather_time_utc", "captured_at_utc"}
    payload_cols = [c for c in weather_df.columns if c not in (set(present_common) | base_cols)]

    def to_json_row(row) -> str:
        d = {}
        for c in payload_cols:
            val = row.get(c)
            if pd.isna(val):
                continue
            d[c] = val
        try:
            return json.dumps(d, default=str)
        except Exception:
            return "{}"

    weather_df["payload_json"] = weather_df.apply(to_json_row, axis=1)

    # ---- Hourly fact: keep only weather rows that correspond to venues that actually have fixtures (ID-based)
    fx_venues = fix_df[["season", "venue_id"]].drop_duplicates()
    hr_df = weather_df.merge(fx_venues, on=["season", "venue_id"], how="inner")

    # Diagnostics (ID-based coverage)
    if args.verbose:
        fx_only = fx_venues.merge(weather_df[["season", "venue_id"]].drop_duplicates(),
                                  on=["season", "venue_id"], how="left", indicator=True) \
                           .query("_merge=='left_only'").drop(columns="_merge")
        wx_only = weather_df[["season", "venue_id"]].drop_duplicates().merge(fx_venues,
                                  on=["season", "venue_id"], how="left", indicator=True) \
                           .query("_merge=='left_only'").drop(columns="_merge")
        print("\n[Fixtures by season/venue_id]")
        print(fx_venues.groupby("season").agg(n_venues=("venue_id", "nunique")).reset_index().head(10))
        print("\n[Weather by season/venue_id]")
        print(weather_df.groupby("season").agg(n_venues=("venue_id", "nunique")).reset_index().head(10))
        print("\n[Unmatched fixtures venue_ids]")
        print(fx_only.head(20))
        print("\n[Unmatched weather venue_ids]")
        print(wx_only.head(20))

    # ---- At-KO fact: nearest weather to KO per game_key (ID-based join)
    needed_fix_cols = {"game_key", "season", "round", "scheduled_time_utc", "venue_id"}
    missing_fix = [c for c in needed_fix_cols if c not in fix_df.columns]
    if missing_fix:
        logging.warning("Fixtures missing columns %s; cannot compute at-KO. Writing hourly only.", missing_fix)
        _write_hourly(silv, hr_df, args)
        _write_empty_atko(silv, args)
        return

    fix_df = fix_df[fix_df["scheduled_time_utc"].notna()].copy()
    if fix_df.empty:
        logging.warning("No fixtures with valid scheduled_time_utc; writing hourly only.")
        _write_hourly(silv, hr_df, args)
        _write_empty_atko(silv, args)
        return

    # Core join: season + venue_id
    join_df = hr_df.merge(
        fix_df[["game_key", "season", "round", "scheduled_time_utc", "venue_id"]],
        on=["season", "venue_id"],
        how="inner",
        suffixes=("_w", "_f"),
    )

    # If both sides carried a game_key, pandas will suffix them. Coalesce to a single 'game_key'.
    if "game_key" not in join_df.columns:
        if "game_key_f" in join_df.columns:
            join_df["game_key"] = join_df["game_key_f"]
        elif "game_key_w" in join_df.columns:
            join_df["game_key"] = join_df["game_key_w"]

    # Prefer fixture round for outputs if both exist
    if "round_f" in join_df.columns:
        join_df["round"] = join_df["round_f"]
    elif "round_w" in join_df.columns and "round" not in join_df.columns:
        join_df["round"] = join_df["round_w"]

    # Debug if schema is off
    if args.verbose:
        print("\n[join_df columns]")
        print(sorted(list(join_df.columns)))

    required_cols = {"game_key", "scheduled_time_utc", "weather_time_utc"}
    if not required_cols.issubset(set(join_df.columns)):
        logging.error("weather×fixture join missing columns: %s", required_cols - set(join_df.columns))
        if dbg:
            hr_df.head(1000).to_csv(dbg / "hourly_join_input_sample.csv", index=False)
            fix_df.head(1000).to_csv(dbg / "fixture_join_input_sample.csv", index=False)
            join_df.head(1000).to_csv(dbg / "join_df_missing_cols.csv", index=False)
        _write_hourly(silv, hr_df, args)
        _write_empty_atko(silv, args)
        return

    if join_df.empty:
        logging.warning("weather×fixture join produced 0 rows; writing hourly only.")
        if dbg:
            hr_df.head(2000).to_csv(dbg / "hourly_join_input_sample.csv", index=False)
            fix_df.head(2000).to_csv(dbg / "fixture_join_input_sample.csv", index=False)
        _write_hourly(silv, hr_df, args)
        _write_empty_atko(silv, args)
        return

    # Ensure times are timestamps
    join_df["weather_time_utc"] = _to_ts(join_df["weather_time_utc"])
    join_df["captured_at_utc"] = _to_ts(join_df["captured_at_utc"])
    join_df["scheduled_time_utc"] = _to_ts(join_df["scheduled_time_utc"])

    # Compute absolute minutes to KO
    delta = (join_df["scheduled_time_utc"] - join_df["weather_time_utc"]).dt.total_seconds().abs() / 60.0
    join_df["minutes_to_ko_abs"] = delta

    # Optional maximum distance filter
    if args.max_minutes_to_ko is not None:
        before = len(join_df)
        join_df = join_df[join_df["minutes_to_ko_abs"] <= float(args.max_minutes_to_ko)].copy()
        logging.info("[max-mins] Dropped %d rows farther than %d minutes from KO",
                     before - len(join_df), int(args.max_minutes_to_ko))

    if join_df.empty:
        logging.warning("No at-KO candidates after minutes filter; writing hourly only.")
        _write_hourly(silv, hr_df, args)
        _write_empty_atko(silv, args)
        return

    # Sort for nearest (and newest capture on ties)
    join_df = join_df.sort_values(["game_key", "minutes_to_ko_abs", "captured_at_utc"],
                                  ascending=[True, True, False])

    # Pick one per game
    atko_df = join_df.groupby("game_key", as_index=False).first()

    # Final shapes
    hourly_keep = ["season", "round", "venue_id", "venue_norm_canon", "venue",
                   "weather_time_utc", "captured_at_utc", "payload_json"] + present_common
    atko_keep = [
        "game_key", "season", "round", "venue_id", "venue_norm_canon",
        "scheduled_time_utc", "weather_time_utc", "captured_at_utc", "minutes_to_ko_abs",
        "payload_json"
    ] + present_common

    hr_out = hr_df[[c for c in hourly_keep if c in hr_df.columns]].copy()
    atko_out = atko_df[[c for c in atko_keep if c in atko_df.columns]].copy()

    # ---- Write outputs
    _write_hourly(silv, hr_out, args)
    _write_atko(silv, atko_out, args)

    # ---- CSV mirrors
    if args.csv_mirror:
        _ensure_dir(Path("silver_csv_mirror"))
        hr_csv_dir = Path("silver_csv_mirror") / "f_weather_hourly"
        ak_csv_dir = Path("silver_csv_mirror") / "f_weather_at_kickoff"
        _ensure_dir(hr_csv_dir)
        _ensure_dir(ak_csv_dir)
        hr_out.to_csv(hr_csv_dir / "f_weather_hourly.csv", index=False)
        atko_out.to_csv(ak_csv_dir / "f_weather_at_kickoff.csv", index=False)
        print(f"✔ CSV mirror: {hr_csv_dir.as_posix()}/f_weather_hourly.csv")
        print(f"✔ CSV mirror: {ak_csv_dir.as_posix()}/f_weather_at_kickoff.csv")

    # Diagnostics
    if args.verbose:
        logging.info("[weather_hourly] rows=%d venues=%d (unique venue_id)",
                     len(hr_out), hr_out["venue_id"].nunique() if not hr_out.empty else 0)
        logging.info("[weather_at_kickoff] games=%d rows=%d",
                     atko_out["game_key"].nunique() if not atko_out.empty else 0, len(atko_out))
        if dbg:
            atko_out.head(100).to_csv(dbg / "atko_sample.csv", index=False)

    logging.info("Done.")


# -----------------------
# Writers
# -----------------------

def _write_hourly(silv: Path, hourly_df: pd.DataFrame, args):
    out_dir = silv / "f_weather_hourly.parquet"
    _write_df_parquet_partitioned(hourly_df, out_dir, ["season", "round"], args.overwrite)
    print(f"✔ Wrote {out_dir.as_posix()}")


def _write_atko(silv: Path, atko_df: pd.DataFrame, args):
    out_dir = silv / "f_weather_at_kickoff.parquet"
    _write_df_parquet_partitioned(atko_df, out_dir, ["season", "round"], args.overwrite)
    print(f"✔ Wrote {out_dir.as_posix()}")


def _write_empty_atko(silv: Path, args):
    empty_atko = pd.DataFrame(columns=[
        "game_key", "season", "round", "venue_id", "venue_norm_canon",
        "scheduled_time_utc", "weather_time_utc", "captured_at_utc",
        "minutes_to_ko_abs", "payload_json"
    ])
    _write_atko(silv, empty_atko, args)


def _write_empty_outputs(silv: Path, args):
    empty_hourly = pd.DataFrame(columns=[
        "season", "round", "venue_id", "venue_norm_canon", "venue",
        "weather_time_utc", "captured_at_utc", "payload_json"
    ])
    _write_hourly(silv, empty_hourly, args)
    _write_empty_atko(silv, args)


if __name__ == "__main__":
    main()
