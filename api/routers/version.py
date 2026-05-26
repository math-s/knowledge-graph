"""Build/version metadata for the deployed DB (read from the `kg_meta` table)."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from ..db import get_db

router = APIRouter(tags=["version"])


@router.get("/version")
def get_version(conn: sqlite3.Connection = Depends(get_db)) -> dict[str, str]:
    """Return the kg_meta keys stamped into this DB build.

    Empty `version` means the DB predates `kg_meta` and needs to be re-stamped
    via `python pipeline/scripts/stamp_db_meta.py`.
    """
    try:
        rows = conn.execute("SELECT key, value FROM kg_meta").fetchall()
    except sqlite3.OperationalError:
        return {"version": "", "pipeline_git_sha": "", "content_hash": ""}
    return {row["key"]: row["value"] for row in rows}
