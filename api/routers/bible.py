"""Bible data endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db

router = APIRouter(prefix="/bible", tags=["bible"])


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

    return [
        {
            "id": r["id"],
            "name": r["name"],
            "abbreviation": r["abbreviation"],
            "testament": r["testament"],
            "category": r["category"],
            "total_verses": r["total_verses"],
            "total_chapters": r["total_chapters"],
            "citing_paragraphs": json.loads(r["citing_paragraphs_json"] or "[]"),
        }
        for r in rows
    ]


@router.get("/books/{book_id}")
def get_book(book_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get metadata for a single Bible book."""
    row = db.execute(
        "SELECT * FROM bible_books WHERE id = ?", (book_id,)
    ).fetchone()
    if not row:
        raise HTTPException(404, f"Book '{book_id}' not found")
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


@router.get("/books/{book_id}/chapters/{chapter}")
def get_chapter_verses(
    book_id: str,
    chapter: int,
    lang: str = Query("en", description="Language (en, la, pt, el)"),
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

    verses = []
    for r in rows:
        text = {
            l: r[f"text_{l}"]
            for l in ("en", "la", "pt", "el")
            if r[f"text_{l}"]
        }
        verses.append({
            "verse": r["verse"],
            "text": text,
        })

    return {
        "book_id": book_id,
        "chapter": chapter,
        "verse_count": len(verses),
        "verses": verses,
    }
