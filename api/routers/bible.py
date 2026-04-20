"""Bible data endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Literal

from ..db import get_db
from ..utils import multilang_text

router = APIRouter(prefix="/bible", tags=["bible"])


def _row_to_book(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "abbreviation": row["abbreviation"],
        "testament": row["testament"],
        "category": row["category"],
        "total_verses": row["total_verses"],
        "total_chapters": row["total_chapters"],
        "citing_paragraphs": json.loads(row["citing_paragraphs_json"] or "[]"),
    }


@router.get("/books")
def list_books(db: sqlite3.Connection = Depends(get_db)):
    """List all Bible books with metadata."""
    rows = db.execute(
        """
        SELECT id, name, abbreviation, testament, category,
               total_verses, total_chapters, citing_paragraphs_json
        FROM bible_books ORDER BY rowid
        """
    ).fetchall()
    return [_row_to_book(r) for r in rows]


@router.get("/books/{book_id}")
def get_book(book_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get metadata for a single Bible book."""
    row = db.execute("SELECT * FROM bible_books WHERE id = ?", (book_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Book '{book_id}' not found")
    return _row_to_book(row)


@router.get("/books/{book_id}/chapters/{chapter}")
def get_chapter_verses(
    book_id: str,
    chapter: int,
    lang: Literal["en", "la", "pt", "el"] = Query("en", description="Language (en, la, pt, el)"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Get all verses for a chapter in a Bible book."""
    rows = db.execute(
        """
        SELECT verse, text_en, text_la, text_pt, text_el
        FROM bible_verses
        WHERE book_id = ? AND chapter = ?
        ORDER BY verse
        """,
        (book_id, chapter),
    ).fetchall()

    if not rows:
        raise HTTPException(404, f"No verses found for {book_id} chapter {chapter}")

    verses = [
        {"verse": r["verse"], "text": multilang_text(r, ("en", "la", "pt", "el"))}
        for r in rows
    ]

    return {"book_id": book_id, "chapter": chapter, "verse_count": len(verses), "verses": verses}
