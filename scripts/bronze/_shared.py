# scripts/bronze/_shared.py
# -*- coding: utf-8 -*-
"""
Shared utilities for Bronze ingest scripts.

Features:
- Consistent CLI flags: --dry-run, --limit (see add_common_test_flags / maybe_limit_df)
- Logging helper (get_logger)
- Filesystem helpers (ensure_dir, atomic_write)
- DataFrame helpers for Polars-first IO (write_df, read_any)
- HTTP session with retries (session_with_retries, fetch_text/json/bytes)
- Light string utilities (to_snake_case, normalize_ws)
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import logging
import os
import random
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

# --- DataFrame engines (Polars first; Pandas fallback for CSV/JSON) ---
try:
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover
    pl = None  # type: ignore

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

# --- HTTP stack with retries ---
try:
    import requests
    from requests.adapters import HTTPAdapter
    try:
        # urllib3 v2
        from urllib3.util.retry import Retry
    except Exception:  # pragma: no cover
        from requests.packages.urllib3.util.retry import Retry  # type: ignore
except Exception as e:  # pragma: no cover
    requests = None  # type: ignore
    HTTPAdapter = None  # type: ignore
    Retry = None  # type: ignore

from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore
import hashlib
try:
    import yaml  # optional
except Exception:  # pragma: no cover
    yaml = None  # type: ignore
import duckdb  # used for partitioned parquet writes

# ============================================================
# Logging
# ============================================================

def get_logger(name: str = __name__, level: str | int = None) -> logging.Logger:
    """
    Create a module-level logger with sensible defaults.
    Env override: LOG_LEVEL=DEBUG|INFO|WARNING|ERROR
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level_val = level or os.getenv("LOG_LEVEL", "INFO")
    if isinstance(level_val, str):
        level_val = getattr(logging, level_val.upper(), logging.INFO)
    logger.setLevel(level_val)  # type: ignore[arg-type]
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%dT%H:%M:%S")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOG = get_logger("bronze")


# ============================================================
# CLI helpers (standard flags)
# ============================================================

def add_common_test_flags(parser: argparse.ArgumentParser) -> None:
    """
    Standardise test flags across all Bronze scripts.

    --dry-run : do not write to disk, just print counts
    --limit   : cap rows/pages fetched for faster smoke tests
    """
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not write outputs, just run and print counts")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap the number of rows/pages for testing")


def maybe_limit_df(df: Any, limit: Optional[int]):
    """
    Return a limited view of the DataFrame if limit > 0.
    Supports Polars and Pandas; otherwise returns df unchanged.
    """
    if limit is None or limit <= 0:
        return df
    try:
        # Polars
        if pl is not None and isinstance(df, pl.DataFrame):
            return df.head(limit)
        # Pandas
        if pd is not None and isinstance(df, pd.DataFrame):
            return df.head(limit)
    except Exception:
        pass
    return df


# ============================================================
# Filesystem / IO
# ============================================================

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


@contextlib.contextmanager
def atomic_write(target: str | Path, mode: str = "wb"):
    """
    Atomic write: write to a temp file in the same dir, then replace.
    Useful to avoid partial files if a job crashes mid-write.
    """
    target = Path(target)
    ensure_dir(target.parent)
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f".{target.name}.tmp-", dir=str(target.parent))
    os.close(tmp_fd)
    try:
        with open(tmp_path, mode) as f:
            yield f
        os.replace(tmp_path, target)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)


def _is_polars_df(obj: Any) -> bool:
    return (pl is not None) and isinstance(obj, pl.DataFrame)


def _is_pandas_df(obj: Any) -> bool:
    return (pd is not None) and isinstance(obj, pd.DataFrame)


def write_df(df: Any, path: str | Path, fmt: str = "parquet",
             csv_kwargs: Dict[str, Any] | None = None,
             parquet_kwargs: Dict[str, Any] | None = None,
             json_kwargs: Dict[str, Any] | None = None) -> None:
    """
    Write a DataFrame to disk. Prefers Polars, falls back to Pandas.

    fmt: "parquet" (default), "csv", or "json"
    """
    path = Path(path)
    ensure_dir(path.parent)
    fmt = fmt.lower()

    if fmt == "parquet":
        parquet_kwargs = parquet_kwargs or {}
        if _is_polars_df(df):
            df.write_parquet(str(path), **parquet_kwargs)
            return
        if _is_pandas_df(df):
            df.to_parquet(str(path), index=False, **parquet_kwargs)
            return
        raise TypeError("write_df: unsupported df type for parquet")

    if fmt == "csv":
        csv_kwargs = csv_kwargs or {}
        if _is_polars_df(df):
            df.write_csv(str(path), **csv_kwargs)
            return
        if _is_pandas_df(df):
            df.to_csv(str(path), index=False, **csv_kwargs)
            return
        raise TypeError("write_df: unsupported df type for csv")

    if fmt == "json":
        json_kwargs = json_kwargs or {}
        if _is_polars_df(df):
            # Polars: convert to rows then dump
            rows = df.to_dicts()
            with atomic_write(path, "w") as f:
                json.dump(rows, f, **({"indent": 2} | json_kwargs))
            return
        if _is_pandas_df(df):
            with atomic_write(path, "w") as f:
                df.to_json(f, orient="records", indent=2, **json_kwargs)
            return
        raise TypeError("write_df: unsupported df type for json")

    raise ValueError(f"write_df: unsupported fmt={fmt}")


def read_any(paths_glob: Iterable[str]) -> Any:
    """
    Read multiple CSV/Parquet files into a single Polars DataFrame (if available),
    else a Pandas DataFrame. Nonexistent globs are ignored.
    """
    files: list[Path] = []
    for g in paths_glob:
        files.extend(Path().glob(g))
    if not files:
        return pl.DataFrame() if pl else (pd.DataFrame() if pd else [])

    # Prefer Polars for performance
    if pl is not None:
        frames = []
        for fp in files:
            if fp.suffix.lower() == ".parquet":
                frames.append(pl.read_parquet(str(fp)))
            elif fp.suffix.lower() == ".csv":
                frames.append(pl.read_csv(str(fp)))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # Fallback: Pandas
    if pd is not None:
        frames_pd = []
        for fp in files:
            if fp.suffix.lower() == ".parquet":
                frames_pd.append(pd.read_parquet(str(fp)))
            elif fp.suffix.lower() == ".csv":
                frames_pd.append(pd.read_csv(str(fp)))
        return pd.concat(frames_pd, ignore_index=True) if frames_pd else pd.DataFrame()

    return []


# --- replace the existing echo_df_info with this wider signature ---
def echo_df_info(df: Any, label: str = "df", note: str | None = None) -> None:
    """Log shape/columns for either Polars or Pandas; note==label alias."""
    title = note or label
    try:
        if _is_polars_df(df):
            LOG.info("%s: shape=%s cols=%s", title, df.shape, df.columns)
        elif _is_pandas_df(df):
            LOG.info("%s: shape=%s cols=%s", title, df.shape, list(df.columns))
        else:
            LOG.info("%s: (unknown df type) %r", title, type(df))
    except Exception:
        pass
# --- add the missing helpers used by bronze scripts ---

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_utc(dt_local: datetime, source_tz: str) -> datetime:
    """Convert a naive local datetime + IANA tz string to UTC."""
    if dt_local.tzinfo is None:
        if ZoneInfo is None:
            raise RuntimeError("zoneinfo not available; cannot convert to UTC")
        dt_local = dt_local.replace(tzinfo=ZoneInfo(source_tz))
    return dt_local.astimezone(timezone.utc)

def hash_row(parts: Iterable[Any]) -> str:
    """Stable SHA1 over a sequence of values with '|' separators."""
    h = hashlib.sha1()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()

def load_yaml(path: str | Path) -> dict:
    """Best-effort YAML loader → {} on missing/invalid."""
    p = Path(path)
    if not p.exists() or yaml is None:
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def load_env(path: str | Path) -> None:
    """Tiny .env parser (KEY=VALUE, ignores blanks/#)."""
    p = Path(path)
    if not p.exists():
        return
    try:
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
    except Exception:
        pass

# --- inside scripts/bronze/_shared.py ---

def parquet_write(df, output_root: Path, partition_cols=None, overwrite: bool=False):
    output_root = Path(output_root)
    ensure_dir(output_root)

    import duckdb
    con = duckdb.connect()
    con.register("df", df.to_pandas() if hasattr(df, "to_pandas") else df)

    if partition_cols:
        cols = ", ".join(partition_cols)
        ow = "TRUE" if overwrite else "FALSE"
        con.execute(
            f"COPY df TO '{output_root.as_posix()}' "
            f"(FORMAT PARQUET, PARTITION_BY ({cols}), OVERWRITE_OR_IGNORE {ow})"
        )
    else:
        # single-file write target must be a file path, not a directory
        # if you use this branch anywhere, handle overwrite by removing the file first
        raise NotImplementedError("single-file parquet_write not used for partitioned bronze")


# ============================================================
# HTTP (requests) with retries
# ============================================================

_UA_LIST = [
    # rotate a few generic UAs
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
]

def session_with_retries(total: int = 4,
                         backoff_factor: float = 0.5,
                         status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
                         allowed_methods: Iterable[str] = ("GET", "HEAD", "OPTIONS"),
                         timeout: int = 20) -> requests.Session:
    """
    Build a requests Session with urllib3 Retry strategy.
    """
    if requests is None or HTTPAdapter is None or Retry is None:  # pragma: no cover
        raise RuntimeError("requests/urllib3 not available")

    sess = requests.Session()
    sess.headers.update({"User-Agent": random.choice(_UA_LIST)})

    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=frozenset(m.upper() for m in allowed_methods),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20, pool_block=False)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.request_timeout = timeout  # custom attribute for our helpers
    return sess


def _req_timeout(sess: requests.Session, timeout: Optional[int]) -> int:
    return int(timeout if timeout is not None else getattr(sess, "request_timeout", 20))


def polite_sleep(min_s: float = 0.10, max_s: float = 0.35) -> None:
    time.sleep(random.uniform(min_s, max_s))


def fetch_text(url: str, session: Optional[requests.Session] = None, timeout: Optional[int] = None, **kw) -> str:
    s = session or session_with_retries()
    r = s.get(url, timeout=_req_timeout(s, timeout), **kw)
    r.raise_for_status()
    polite_sleep()
    r.encoding = r.encoding or "utf-8"
    return r.text


def fetch_json(url: str, session: Optional[requests.Session] = None, timeout: Optional[int] = None, **kw) -> Any:
    s = session or session_with_retries()
    r = s.get(url, timeout=_req_timeout(s, timeout), **kw)
    r.raise_for_status()
    polite_sleep()
    return r.json()


def fetch_bytes(url: str, session: Optional[requests.Session] = None, timeout: Optional[int] = None, **kw) -> bytes:
    s = session or session_with_retries()
    r = s.get(url, timeout=_req_timeout(s, timeout), **kw)
    r.raise_for_status()
    polite_sleep()
    return r.content


# ============================================================
# Small text utilities
# ============================================================

_ws_re = re.compile(r"\s+")
def normalize_ws(s: str | None) -> str:
    if s is None:
        return ""
    return _ws_re.sub(" ", str(s)).strip()


def to_snake_case(s: str | None) -> str:
    if not s:
        return ""
    s = normalize_ws(s).lower()
    s = re.sub(r"[^0-9a-z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# ============================================================
# Standard write contract for Bronze scripts
# ============================================================

def bronze_out_path(subdir: str, filename: str, base: str | Path = "bronze") -> Path:
    """
    Compose a bronze output path like bronze/<subdir>/<filename>
    """
    p = Path(base) / subdir
    ensure_dir(p)
    return p / filename


def write_bronze_df(df: Any, subdir: str, stem: str,
                    fmt: str = "parquet", base: str | Path = "bronze",
                    csv_kwargs: Dict[str, Any] | None = None,
                    parquet_kwargs: Dict[str, Any] | None = None,
                    json_kwargs: Dict[str, Any] | None = None) -> Path:
    """
    Write a DataFrame to bronze/<subdir>/<stem>.<ext>
    Returns the final Path.
    """
    ext = {"parquet": "parquet", "csv": "csv", "json": "json"}[fmt.lower()]
    path = bronze_out_path(subdir, f"{stem}.{ext}", base=base)
    LOG.info("Writing bronze -> %s", path)
    write_df(df, path, fmt=fmt, csv_kwargs=csv_kwargs,
             parquet_kwargs=parquet_kwargs, json_kwargs=json_kwargs)
    return path


def preview_or_write(df: Any, *, dry_run: bool, out_subdir: str, out_stem: str,
                     fmt: str = "parquet", base: str | Path = "bronze") -> Optional[Path]:
    """
    Common end-of-script pattern:
      - if dry_run: just log shape/columns
      - else: write to bronze and return Path
    """
    echo_df_info(df, label=f"{out_subdir}/{out_stem}")
    if dry_run:
        LOG.info("[DRY-RUN] Skipping write for %s/%s", out_subdir, out_stem)
        return None
    return write_bronze_df(df, out_subdir, out_stem, fmt=fmt, base=base)


# ============================================================
# Misc helpers
# ============================================================

def safe_len(x: Any) -> int:
    try:
        return len(x)  # type: ignore[arg-type]
    except Exception:
        return 0


def json_pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True)
