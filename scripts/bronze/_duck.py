"""
Minimal DuckDB helper functions for Bronze ingestion.

This module encapsulates a few convenience wrappers around DuckDB for
querying Parquet datasets. DuckDB is extremely efficient at scanning
partitioned Parquet files and enables quick idempotency checks and
aggregations without loading entire datasets into memory.
"""

from __future__ import annotations

import duckdb
from pathlib import Path
from typing import Any, Iterable, List, Optional

def query_parquet(path: str | Path, sql: str, params: Optional[Iterable[Any]] = None):
    """Execute a SQL query against a Parquet dataset and return a pandas DataFrame.

    Parameters
    ----------
    path : str or Path
        Path to a directory containing Parquet files or a glob pattern.
    sql : str
        The SQL query to execute. Use ``events`` as the table name when
        referencing the Parquet files; this function will register the
        dataset as a view called ``events``.
    params : iterable, optional
        Parameters bound to the query.
    """
    con = duckdb.connect()
    # register dataset
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{path}')")
    return con.execute(sql, params or []).df()