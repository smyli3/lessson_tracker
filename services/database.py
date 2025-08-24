from __future__ import annotations
import duckdb
from typing import Any, Iterable

# Reuse the existing setup function to keep a single source of truth
from ingest import setup_database


class Database:
    """Lightweight wrapper around DuckDB connection.

    Provides a stable place to hang DB-related helpers without changing logic.
    """

    def __init__(self, db_path: str = 'flaik.duckdb') -> None:
        self.db_path = db_path
        self.conn: duckdb.DuckDBPyConnection = setup_database(db_path)

    def execute(self, sql: str, params: Iterable[Any] | None = None):
        if params is None:
            return self.conn.execute(sql)
        return self.conn.execute(sql, params)

    def query_df(self, sql: str, params: Iterable[Any] | None = None):
        import pandas as pd
        cur = self.execute(sql, params)
        return cur.df()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
