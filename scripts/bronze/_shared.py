"""
Shared helpers for the AFL SGM analysis tool Bronze layer.

This module centralises common functionality used by the Bronze ingestion
scripts, such as obtaining the current UTC time, converting between
timezones, hashing rows, writing Parquet files in a partitioned layout
and handling basic checkpointing. It also exposes convenience wrappers
around tenacity for retrying operations and helpers for loading
configuration files.

Additional helpers have been added in the `feat/sgm‑enhancements` branch
to support weather lookups and polite asynchronous delays.
"""

from __future__ import annotations

import json
import os
import time
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple, Dict

from zoneinfo import ZoneInfo
import yaml
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

import polars as pl  # for in‑memory dataframes


def utc_now() -> datetime:
    """Return the current time as a timezone‑aware UTC datetime.

    All timestamps written to the Bronze layer should call this function
    rather than ``datetime.utcnow()`` to ensure consistent timezone
    semantics. The returned object is tz‑aware with tzinfo=UTC.
    """
    return datetime.now(timezone.utc)


def to_utc(dt: datetime | str, source_tz: str) -> datetime:
    """Convert a naive or timezone‑aware datetime to UTC.

    Parameters
    ----------
    dt : datetime | str
        A datetime object or ISO‑8601 formatted string representing
        local time. If a string is provided it will be parsed with
        :func:`datetime.fromisoformat`. The input may be naive (no tzinfo)
        or already timezone‑aware.
    source_tz : str
        An IANA timezone name (e.g. ``Australia/Melbourne``). When
        ``dt`` is naive, it is first localised to this timezone before
        conversion. If ``dt`` already contains tzinfo this argument
        is ignored.

    Returns
    -------
    datetime
        A timezone‑aware datetime in UTC.
    """
    if isinstance(dt, str):
        dt_obj = datetime.fromisoformat(dt)
    else:
        dt_obj = dt
    if dt_obj.tzinfo is None:
        # Localise to source_tz
        try:
            tz = ZoneInfo(source_tz)
        except Exception as exc:
            raise ValueError(f"Invalid timezone {source_tz}") from exc
        dt_obj = dt_obj.replace(tzinfo=tz)
    return dt_obj.astimezone(timezone.utc)


def hash_row(fields: Iterable[Any]) -> str:
    """Compute a stable SHA‑256 hash for a list of values.

    The values are first coerced to strings and concatenated using
    ``|`` as a delimiter. The resulting bytes are hashed with
    SHA‑256. Use this helper to deduplicate odds snapshots and other
    records in the Bronze layer.
    """
    concatenated = "|".join("" if f is None else str(f) for f in fields)
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest()


def parquet_write(df: pl.DataFrame, path: str | Path, partition_cols: Optional[List[str]] = None) -> None:
    """Write a Polars DataFrame to a Parquet dataset with optional partitioning.

    This helper writes each partition as a separate Parquet file named
    using the current UNIX timestamp in milliseconds. It does not
    overwrite existing files and thus supports append‑only workflows.
    Partitioning is done by creating subdirectories under ``path`` in
    the form ``col=value`` for each partition column.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to write.
    path : str | Path
        Root directory for the Parquet dataset. Will be created if
        missing.
    partition_cols : list[str], optional
        Column names on which to partition the data. If provided the
        DataFrame is grouped by these columns and a separate Parquet
        file is written for each group. Columns used for partitioning
        remain in the file for downstream processing.
    """
    root = Path(path)
    root.mkdir(parents=True, exist_ok=True)
    if partition_cols:
        # Partition by the specified columns
        if not set(partition_cols).issubset(set(df.columns)):
            missing = set(partition_cols) - set(df.columns)
            raise ValueError(f"Partition columns missing from DataFrame: {missing}")
        groups = df.partition_by(partition_cols, as_dict=True)
        timestamp_ms = int(time.time() * 1000)
        for key_tuple, subset in groups.items():
            # Build subdirectory path col=value/...
            if not isinstance(key_tuple, tuple):
                key_tuple = (key_tuple,)
            subdir_parts = [f"{col}={val}" for col, val in zip(partition_cols, key_tuple)]
            subdir = root.joinpath(*subdir_parts)
            subdir.mkdir(parents=True, exist_ok=True)
            filename = f"{timestamp_ms}.parquet"
            subset.write_parquet(subdir / filename)
    else:
        timestamp_ms = int(time.time() * 1000)
        filename = f"{timestamp_ms}.parquet"
        df.write_parquet(root / filename)


def read_checkpoint(path: str | Path) -> Optional[dict[str, Any]]:
    """Read a checkpoint file from disk.

    A checkpoint file stores the progress of an ingestion script. It is
    a JSON file with arbitrary fields. Returns ``None`` if the file
    does not exist.
    """
    p = Path(path)
    if not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def write_checkpoint(path: str | Path, data: dict[str, Any]) -> None:
    """Write a checkpoint file to disk.

    The directory will be created if it does not already exist.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def retryable(max_retries: int = 4, backoff: float = 1.8):
    """Return a tenacity.retry decorator with sensible defaults.

    Usage::

        @retryable(max_retries=3)
        def fetch_page(url):
            ...

    Parameters
    ----------
    max_retries : int
        Maximum number of attempts before giving up.
    backoff : float
        Multiplier for exponential backoff (seconds).
    """
    return retry(stop=stop_after_attempt(max_retries), wait=wait_exponential(multiplier=backoff))


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file into a Python dictionary."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_env(path: str | Path) -> None:
    """Load environment variables from a .env file if present."""
    if Path(path).exists():
        load_dotenv(dotenv_path=path)


def load_venue_lookup(path: str | Path) -> Dict[str, Dict[str, Any]]:
    """Load the venue lookup table containing lat/lon/timezone.

    Parameters
    ----------
    path : str | Path
        Path to the YAML file mapping venue names to metadata.

    Returns
    -------
    dict
        A mapping of venue names to dictionaries containing ``lat``, ``lon`` and
        ``tz``. If the file does not exist or is empty an empty dict is
        returned.
    """
    p = Path(path)
    if not p.exists():
        logging.warning("Venue lookup file not found at %s", p)
        return {}
    data = load_yaml(p)
    return {str(k): v for k, v in data.items() if isinstance(v, dict)}


async def async_sleep_ms(milliseconds: int) -> None:
    """Asynchronous sleep helper expressed in milliseconds."""
    seconds = milliseconds / 1000.0
    # Use asyncio.sleep directly; import inside function to avoid an unconditional
    # dependency on asyncio for synchronous callers.
    import asyncio
    await asyncio.sleep(seconds)