"""Library documents and almanac endpoints."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db

router = APIRouter(prefix="/library", tags=["library"])

_VALID_CATEGORIES = ("docs", "almanac")


@router.get("")
def list_library(
    category: str | None = Query(None, description="Filter by category (docs, almanac)"),
    db: sqlite3.Connection = Depends(get_db),
):
    """List library documents, optionally filtered by category."""
    if category is not None and category not in _VALID_CATEGORIES:
        raise HTTPException(422, f"Unknown category {category!r}. Use: {', '.join(_VALID_CATEGORIES)}")
    if category:
        rows = db.execute(
            "SELECT id, category, title, year FROM library_docs WHERE category = ? ORDER BY year, title",
            (category,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, category, title, year FROM library_docs ORDER BY category, year, title"
        ).fetchall()
    return [{"id": r["id"], "category": r["category"], "title": r["title"], "year": r["year"]} for r in rows]


@router.get("/categories")
def list_categories(db: sqlite3.Connection = Depends(get_db)):
    """List available categories with document counts."""
    rows = db.execute(
        "SELECT category, COUNT(*) as count FROM library_docs GROUP BY category ORDER BY count DESC"
    ).fetchall()
    return [{"category": r["category"], "count": r["count"]} for r in rows]


@router.get("/{doc_id}")
def get_library_doc(doc_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get a library document with its full text."""
    row = db.execute("SELECT * FROM library_docs WHERE id = ?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Document '{doc_id}' not found")
    return {"id": row["id"], "category": row["category"], "title": row["title"], "year": row["year"], "text": row["text"]}
