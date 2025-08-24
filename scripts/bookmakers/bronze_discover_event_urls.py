# scripts/bookmakers/bronze_discover_event_urls.py
from __future__ import annotations
import sys, os, csv, re, argparse, asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import yaml

# ---- Windows asyncio policy required for Playwright driver on Win ----
# Playwright runs a Node-based driver subprocess; Proactor loop is needed on Windows.
# Ref: Playwright Python "Getting started - Library" docs.
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass  # ok on newer Pythons
# :contentReference[oaicite:3]{index=3}

# ---- Project paths (Option A) ----
ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from playwright.async_api import async_playwright  # noqa: E402
from bookmakers import RESOLVERS                  # noqa: E402

BOOKS_YAML  = ROOT / "config" / "bookmakers.yaml"
ALIASES_CSV = ROOT / "config" / "team_aliases.csv"
GAMES_DIR   = ROOT / "bronze" / "games"
URLS_DIR    = ROOT / "bronze" / "urls"

# Base columns (kept first to avoid breaking existing consumers)
FIELDS_BASE = ["game_id","bookmaker","event_url","match_quality","found_at","notes"]
# Extended columns (opt-in via --extended)
FIELDS_EXT  = ["bookmaker_event_id","canonical_url","raw_url","discovered_at","mode"]

def _ts_now_local_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")

def _latest_games_csv() -> Path | None:
    snaps = sorted(GAMES_DIR.glob("snapshot_ts=*/games.csv"))
    return snaps[-1] if snaps else None

def _load_yaml(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _load_alias_map() -> dict[str,str]:
    amap: dict[str,str] = {}
    if ALIASES_CSV.exists():
        df = pd.read_csv(ALIASES_CSV)
        for _, r in df.iterrows():
            alias = str(r.get("alias","")).strip()
            team  = str(r.get("team","")).strip()
            if alias and team:
                amap[alias] = team
    return amap

def _canon(name: str, amap: dict[str,str]) -> str:
    return amap.get(name, name)

def _load_games(csv_path: Path, amap: dict[str,str]) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["home"] = df["home"].astype(str).map(lambda s: _canon(s, amap))
    df["away"] = df["away"].astype(str).map(lambda s: _canon(s, amap))
    df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce")
    return df[["game_id","home","away","kickoff_utc","venue"]]

def _canonicalize(url: str) -> str:
    # drop query/fragment
    try:
        from urllib.parse import urlsplit, urlunsplit
        u = urlsplit(url)
        return urlunsplit((u.scheme, u.netloc, u.path, "", ""))
    except Exception:
        return url

_EVENT_ID_RE = re.compile(r"(\d{5,})($|[/?#])")  # pick a numeric id with >=5 digits from the tail

def _parse_event_id(url: str, bookmaker: str) -> str:
    # Sportsbet event slugs often end with -{digits}; PointsBet event path ends with /{digits}
    # We'll heuristically grab the last 5+ digit run in the path.
    try:
        path = re.sub(r"[?#].*$", "", url)  # strip query/fragment
        m = _EVENT_ID_RE.search(path[::-1])  # search from end by reversing, then pull group
        if m:
            # reverse back the matched slice
            span = m.span(1)
            digits_rev = path[::-1][span[0]:span[1]]
            return digits_rev[::-1]
    except Exception:
        pass
    return ""

async def _resolve_one(Resolver, game: dict, ctx, alias_map: dict[str,str],
                       per_resolver_timeout: float,
                       extended: bool, mode: str) -> dict:
    async def _call():
        res = await Resolver.resolve(game, alias_map, ctx)
        row = {
            "game_id": game["game_id"],
            "bookmaker": Resolver.__name__.replace("Resolver","").lower(),
            "event_url": "",
            "match_quality": 0,
            "found_at": "none",
            "notes": "no url returned"
        }
        if res and res.get("event_url"):
            raw = str(res["event_url"])
            can = _canonicalize(raw)
            row.update({
                "event_url": can,
                "match_quality": int(res.get("match_quality", 0)),
                "found_at": res.get("found_at","browse"),
                "notes": res.get("notes",""),
            })
            if extended:
                row.update({
                    "bookmaker_event_id": _parse_event_id(can, row["bookmaker"]),
                    "canonical_url": can,
                    "raw_url": raw,
                    "discovered_at": datetime.now(timezone.utc).isoformat(),
                    "mode": mode
                })
        else:
            if extended:
                row.update({
                    "bookmaker_event_id": "",
                    "canonical_url": "",
                    "raw_url": "",
                    "discovered_at": datetime.now(timezone.utc).isoformat(),
                    "mode": mode
                })
        return row

    try:
        # Hard wall-clock timeout around each resolver so nothing hangs forever.
        # (Best practice with asyncio: bound waits using wait_for.) :contentReference[oaicite:4]{index=4}
        return await asyncio.wait_for(_call(), timeout=per_resolver_timeout)
    except asyncio.TimeoutError:
        base = {
            "game_id": game["game_id"],
            "bookmaker": Resolver.__name__.replace("Resolver","").lower(),
            "event_url": "", "match_quality": 0,
            "found_at": "timeout", "notes": f"timeout>{per_resolver_timeout}s"
        }
        if extended:
            base.update({
                "bookmaker_event_id": "", "canonical_url": "", "raw_url": "",
                "discovered_at": datetime.now(timezone.utc).isoformat(), "mode": mode
            })
        return base
    except Exception as e:
        base = {
            "game_id": game["game_id"],
            "bookmaker": Resolver.__name__.replace("Resolver","").lower(),
            "event_url": "", "match_quality": 0,
            "found_at": "error", "notes": f"{type(e).__name__}: {e}"
        }
        if extended:
            base.update({
                "bookmaker_event_id": "", "canonical_url": "", "raw_url": "",
                "discovered_at": datetime.now(timezone.utc).isoformat(), "mode": mode
            })
        return base

async def _process_batch(batch: list[dict], books: list[str], alias_map: dict[str,str],
                         headless: bool, nav_timeout_ms: int, op_timeout_ms: int,
                         per_resolver_timeout: float, extended: bool, mode: str) -> list[dict]:
    rows: list[dict] = []
    async with async_playwright() as p:
        # Fresh browser/context per batch reduces chance of long-run driver/pipe errors. :contentReference[oaicite:5]{index=5}
        browser = await p.chromium.launch(headless=headless)
        ctx = await browser.new_context(
            locale="en-AU",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language": "en-AU,en;q=0.9"}
        )
        # Use explicit timeouts; do not disable (0) which can wait forever. :contentReference[oaicite:6]{index=6}
        ctx.set_default_navigation_timeout(nav_timeout_ms)
        ctx.set_default_timeout(op_timeout_ms)

        try:
            for g in batch:
                for b in books:
                    Resolver = RESOLVERS.get(b)
                    if not Resolver:
                        r = {"game_id": g["game_id"], "bookmaker": b,
                             "event_url": "", "match_quality": 0,
                             "found_at": "error", "notes": "resolver not registered"}
                        if extended:
                            r.update({"bookmaker_event_id":"", "canonical_url":"", "raw_url":"",
                                      "discovered_at": datetime.now(timezone.utc).isoformat(), "mode": mode})
                        rows.append(r)
                        continue
                    rows.append(await _resolve_one(Resolver, g, ctx, alias_map,
                                                   per_resolver_timeout, extended, mode))
        finally:
            await ctx.close()
            await browser.close()
    return rows

def _iter_batches(items: list[dict], size: int):
    for i in range(0, len(items), size):
        yield items[i:i+size]

def main():
    ap = argparse.ArgumentParser(description="Discover bookmaker event URLs for AFL fixtures.")
    # Date window: crawl only what we need
    ap.add_argument("--from-days", type=int, default=-1, help="Include games from now+from_days (default: -1=yesterday).")
    ap.add_argument("--to-days",   type=int, default=30,  help="Include games up to now+to_days (default: 30).")
    # Batching & timeouts
    ap.add_argument("--batch-size", type=int, default=60, help="Games per browser/context batch (default: 60).")
    ap.add_argument("--per-resolver-timeout", type=float, default=35.0, help="Timeout per resolver call in seconds.")
    ap.add_argument("--nav-timeout-ms", type=int, default=20000, help="Playwright navigation timeout (ms).")
    ap.add_argument("--op-timeout-ms", type=int, default=15000, help="Playwright default operation timeout (ms).")
    # Headless / progress
    ap.add_argument("--headless", action="store_true", default=True, help="Run Chromium headless (default).")
    ap.add_argument("--no-headless", action="store_false", dest="headless")
    ap.add_argument("--progress-every", type=int, default=50, help="Print progress heartbeat every N rows.")
    # Output mode
    ap.add_argument("--extended", action="store_true", default=False,
                    help="Write extended columns (bookmaker_event_id, canonical_url, raw_url, discovered_at, mode).")
    ap.add_argument("--mode", choices=["upcoming","historic"], default="upcoming",
                    help="Discovery mode label for extended output (default: upcoming).")
    args = ap.parse_args()

    print(f"[discovery] ROOT={ROOT}")
    print(f"[discovery] BOOKS_YAML={BOOKS_YAML}")
    print(f"[discovery] ALIASES_CSV={ALIASES_CSV}")
    print(f"[discovery] GAMES_DIR={GAMES_DIR}")

    if not BOOKS_YAML.exists():
        raise FileNotFoundError(f"Missing config: {BOOKS_YAML}")
    if not GAMES_DIR.exists():
        raise FileNotFoundError(f"Missing bronze/games root: {GAMES_DIR}")

    games_csv = _latest_games_csv()
    if not games_csv:
        raise FileNotFoundError("No bronze/games snapshot found (games.csv).")

    cfg = _load_yaml(BOOKS_YAML)
    books = [b.lower() for b in (cfg.get("enabled") or []) if b.lower() in RESOLVERS]
    if not books:
        raise ValueError("No valid bookmakers enabled in config/bookmakers.yaml 'enabled'.")

    alias_map = _load_alias_map()
    df = _load_games(games_csv, alias_map)

    # Filter to requested window (UTC)
    now_utc = datetime.now(timezone.utc)
    start = now_utc + timedelta(days=args.from_days)
    end   = now_utc + timedelta(days=args.to_days)
    mask = (df["kickoff_utc"] >= start) & (df["kickoff_utc"] <= end)
    df = df.loc[mask].copy()
    games = df.to_dict("records")
    print(f"[discovery] filtering games from {start.isoformat()} to {end.isoformat()} -> {len(games)} games")

    out_dir = URLS_DIR / f"snapshot_ts={_ts_now_local_str()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "event_urls.csv"
    err_log = out_dir / "_errors.log"

    # Header (base first; extended appended only if enabled)
    header = FIELDS_BASE + (FIELDS_EXT if args.extended else [])
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header); w.writeheader(); f.flush(); os.fsync(f.fileno())

    total = len(games) * len(books)
    written = 0

    try:
        for batch in _iter_batches(games, args.batch_size):
            rows = asyncio.run(_process_batch(
                batch, books, alias_map,
                args.headless, args.nav_timeout_ms, args.op_timeout_ms,
                args.per_resolver_timeout, args.extended, args.mode
            ))
            # stream-append
            with out_csv.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=header)
                for r in rows:
                    # ensure all headers exist
                    for c in header:
                        r.setdefault(c, "")
                    w.writerow(r)
                f.flush(); os.fsync(f.fileno())
            written += len(rows)
            if args.progress_every and written % args.progress_every == 0:
                print(f"[discovery] progress {written}/{total} rows...", flush=True)
    except Exception as e:
        err_log.write_text(f"FATAL: {type(e).__name__}: {e}\n", encoding="utf-8")
        print(f"[discovery] FATAL (see {err_log}): {e}", file=sys.stderr)

    print(f"[discovery] wrote: {out_csv.resolve()}  rows={written}")

if __name__ == "__main__":
    main()
