from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

import duckdb


def q(s: str) -> str:
    return s.replace("'", "''")


def parse_args():
    p = argparse.ArgumentParser("Materialize latest pre-KO odds per (game, bookmaker, market, selection).")
    p.add_argument("--silver-dir", default="silver")
    p.add_argument("--season", type=int, default=None)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--csv-mirror", action="store_true")
    p.add_argument("--minutes-before", type=int, default=None,
                   help="If set, only consider odds captured within this many minutes prior to KO.")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    silv = Path(args.silver_dir)

    # Inputs
    odds_dir = silv / "f_odds_snapshot.parquet"   # partitioned directory (season=*/round=*/*.parquet)
    fix_path = silv / "f_fixture.parquet"         # single parquet file

    if not fix_path.exists():
        raise SystemExit(f"{fix_path.as_posix()} not found")

    # Recursively collect all parquet files under f_odds_snapshot.parquet
    odds_files = sorted(p.as_posix() for p in odds_dir.rglob("*.parquet"))
    if not odds_files:
        # Helpful diagnostics
        hint = (
            f"No Parquet files found under {odds_dir.as_posix()}.\n"
            f"Expected a Hive-style layout like {odds_dir.as_posix()}/season=YYYY/round=XYZ/part-*.parquet\n"
            f"If your previous step wrote partitioned files, make sure it succeeded and you’re pointing to the same --silver-dir.\n"
        )
        # Show a shallow listing of the folder to help spot path issues
        parent = odds_dir if odds_dir.exists() else odds_dir.parent
        listing = [p.as_posix() for p in (parent.glob("*"))]
        raise SystemExit(hint + f"Parent listing: {listing}")

    if args.verbose:
        logging.info("Found %d Parquet part files under %s", len(odds_files), odds_dir.as_posix())
        for s in odds_files[:5]:
            logging.info("  sample file: %s", s)

    # Build SQL array literal for read_parquet([...])
    files_sql = ", ".join(f"'{q(f)}'" for f in odds_files)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # Read odds (from explicit file list) with Hive partition columns (season, round) recovered from folder names
    # DuckDB supports list-of-files and hive_partitioning. :contentReference[oaicite:1]{index=1}
    con.execute(f"""
        CREATE OR REPLACE VIEW f_odds AS
        SELECT * FROM read_parquet([{files_sql}], hive_partitioning=1)
    """)

    # Read fixtures
    con.execute(f"""
        CREATE OR REPLACE VIEW f_fix AS
        SELECT * FROM read_parquet('{q(fix_path.as_posix())}')
    """)

    # Optional season filter for speed (applies to odds)
    season_clause = f"AND o.season = {int(args.season)}" if args.season else ""

    # Optional minutes window relative to KO (keep rows within X minutes before scheduled_time_utc)
    window_clause = ""
    if args.minutes_before and args.minutes_before > 0:
        window_clause = f"AND o.captured_at_utc >= f.scheduled_time_utc - INTERVAL {int(args.minutes_before)} MINUTE"

    # Join on game_key (robust even if round strings differ), keep season/round from odds
    # Pre-KO filter uses captured_at_utc <= scheduled_time_utc; minutes window optional.
    # DATE_DIFF (a.k.a. datediff) is available in DuckDB to compute minutes diff. :contentReference[oaicite:2]{index=2}
    con.execute(f"""
        CREATE OR REPLACE VIEW pre_ko AS
        SELECT
          o.game_key, o.bookmaker, o.market_group, o.market_name, o.selection,
          o.line, o.decimal_odds, o.captured_at_utc, o.source_kind,
          o.season, o.round,
          f.scheduled_time_utc,
          datediff('minute', o.captured_at_utc, f.scheduled_time_utc) AS minutes_to_ko
        FROM f_odds o
        JOIN f_fix  f USING (game_key)
        WHERE o.captured_at_utc <= f.scheduled_time_utc
          {window_clause}
          {season_clause}
    """)

    if args.verbose:
        cnts = con.execute("""
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT game_key) AS games,
              COUNT(DISTINCT bookmaker) AS n_books,
              MIN(minutes_to_ko) AS min_min_to_ko,
              MAX(minutes_to_ko) AS max_min_to_ko
            FROM pre_ko
        """).fetchdf()
        logging.info("[pre_ko] %s", cnts.to_dict(orient="records")[0])

    # Latest per (game_key, bookmaker, market_group, market_name, selection)
    con.execute("""
        CREATE OR REPLACE VIEW v_odds_latest_pre_ko AS
        SELECT *
        FROM (
          SELECT
            game_key, bookmaker, market_group, market_name, selection,
            line, decimal_odds, captured_at_utc, source_kind,
            season, round, scheduled_time_utc, minutes_to_ko,
            ROW_NUMBER() OVER (
              PARTITION BY game_key, bookmaker, market_group, market_name, selection
              ORDER BY captured_at_utc DESC
            ) AS rn
          FROM pre_ko
        )
        WHERE rn = 1
    """)

    if args.verbose:
        cnts2 = con.execute("""
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT game_key) AS games,
              COUNT(DISTINCT bookmaker) AS n_books
            FROM v_odds_latest_pre_ko
        """).fetchdf()
        logging.info("[v_odds_latest_pre_ko] %s", cnts2.to_dict(orient="records")[0])

        # quick sanity: show 5 most recent rows by capture time
        sample = con.execute("""
            SELECT game_key, bookmaker, market_group, market_name, selection,
                   decimal_odds, captured_at_utc, minutes_to_ko
            FROM v_odds_latest_pre_ko
            ORDER BY captured_at_utc DESC
            LIMIT 5
        """).fetchdf()
        logging.info("[sample latest rows]\n%s", sample)

    # Write partitioned parquet
    out_dir = silv / "v_odds_latest_pre_ko.parquet"
    if args.overwrite and out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)

    con.execute(f"""
        COPY (
          SELECT game_key, bookmaker, market_group, market_name, selection,
                 line, decimal_odds, captured_at_utc, source_kind,
                 season, round, scheduled_time_utc, minutes_to_ko
          FROM v_odds_latest_pre_ko
        )
        TO '{q(out_dir.as_posix())}'
        (FORMAT PARQUET, PARTITION_BY (season, round))
    """)

    if args.csv_mirror:
        csv_dir = Path("silver_csv_mirror") / "v_odds_latest_pre_ko"
        csv_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT * FROM v_odds_latest_pre_ko)
            TO '{q((csv_dir / "v_odds_latest_pre_ko.csv").as_posix())}' WITH (HEADER, DELIMITER ',')
        """)
        logging.info("CSV mirror: %s", (csv_dir / "v_odds_latest_pre_ko.csv").as_posix())

    print(f"✔ Wrote {out_dir.as_posix()}")
    if args.csv_mirror:
        print("✔ CSV mirror: silver_csv_mirror/v_odds_latest_pre_ko/v_odds_latest_pre_ko.csv")


if __name__ == "__main__":
    main()
