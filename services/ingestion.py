from __future__ import annotations
from typing import Sequence, Optional
import duckdb

from ingest import ingest_csv
from .database import Database


class IngestionService:
    """Service to handle CSV ingestion using existing ingest_csv logic.

    This is a thin wrapper to fit the new OOP structure without changing behavior.
    """

    def __init__(self, db: Optional[Database] = None, db_path: str = 'flaik.duckdb') -> None:
        self.db = db
        self.db_path = db_path

    def ingest_file(self, file_path: str) -> int:
        if self.db is not None:
            conn: duckdb.DuckDBPyConnection = self.db.conn
            return ingest_csv(file_path, conn=conn)
        # Fallback: let ingest_csv open/close its own connection
        return ingest_csv(file_path, db_path=self.db_path, conn=None)

    def ingest_files(self, file_paths: Sequence[str]) -> int:
        total = 0
        for fp in file_paths:
            total += self.ingest_file(fp)
        return total
