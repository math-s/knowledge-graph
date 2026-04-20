"""Church Fathers pages endpoints."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db

router = APIRouter(prefix="/fathers", tags=["fathers"])


@router.get("")
def list_fathers_pages(db: sqlite3.Connection = Depends(get_db)):
    """List top-level pages (works / collections)."""
    # parent_id is NULL for root works; some rows use empty string instead
    rows = db.execute(
        "SELECT id, title FROM fathers_pages WHERE parent_id IS NULL OR parent_id = '' ORDER BY id"
    ).fetchall()
    return [{"id": r["id"], "title": r["title"]} for r in rows]


@router.get("/{page_id}")
def get_fathers_page(page_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get a page with its text and immediate children."""
    page = db.execute("SELECT * FROM fathers_pages WHERE id = ?", (page_id,)).fetchone()
    if not page:
        raise HTTPException(404, f"Page '{page_id}' not found")
    children = db.execute(
        "SELECT id, title FROM fathers_pages WHERE parent_id = ? ORDER BY id",
        (page_id,),
    ).fetchall()
    return {
        "id": page["id"],
        "parent_id": page["parent_id"] or None,
        "title": page["title"],
        "text": page["text"],
        "children": [{"id": c["id"], "title": c["title"]} for c in children],
    }
