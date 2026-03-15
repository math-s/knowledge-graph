"""SQLite database connection for the read-only API."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Generator
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "knowledge-graph.db"

DB_PATH = Path(os.environ.get("KG_DB_PATH", str(DEFAULT_DB_PATH)))


def get_connection() -> sqlite3.Connection:
    """Open a read-only SQLite connection with row factory."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """FastAPI dependency that yields a DB connection."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
