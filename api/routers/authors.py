"""Patristic author and work endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db
from ..utils import multilang_text

router = APIRouter(prefix="/authors", tags=["authors"])


def _row_to_author(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "era": row["era"],
        "citing_paragraphs": json.loads(row["citing_paragraphs_json"] or "[]"),
        "work_count": row["work_count"],
    }


@router.get("")
def list_authors(db: sqlite3.Connection = Depends(get_db)):
    """List all authors with metadata."""
    rows = db.execute(
        "SELECT id, name, era, citing_paragraphs_json, work_count FROM authors ORDER BY name"
    ).fetchall()
    return [_row_to_author(r) for r in rows]


@router.get("/{author_id}")
def get_author(author_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get an author with metadata."""
    row = db.execute("SELECT * FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Author '{author_id}' not found")
    return _row_to_author(row)


@router.get("/{author_id}/works")
def get_author_works(author_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get all works for an author, with chapters and sections."""
    author = db.execute("SELECT id, name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not author:
        raise HTTPException(404, f"Author '{author_id}' not found")

    work_rows = db.execute(
        "SELECT id, title, source_url, chapter_count FROM author_works WHERE author_id = ? ORDER BY title",
        (author_id,),
    ).fetchall()

    # Fetch all chapters for this author's works in one query
    chapter_rows = db.execute(
        """
        SELECT pc.id, pc.work_id, pc.number, pc.title
        FROM patristic_chapters pc
        JOIN author_works aw ON aw.id = pc.work_id
        WHERE aw.author_id = ?
        ORDER BY pc.work_id, pc.number
        """,
        (author_id,),
    ).fetchall()

    chapter_ids = [ch["id"] for ch in chapter_rows]
    section_rows: list[sqlite3.Row] = []
    if chapter_ids:
        ph = ",".join("?" for _ in chapter_ids)
        section_rows = db.execute(
            f"""
            SELECT id, chapter_id, number, text_en, text_la, text_el
            FROM patristic_sections WHERE chapter_id IN ({ph})
            ORDER BY chapter_id, number
            """,
            chapter_ids,
        ).fetchall()

    # Group sections by chapter_id
    sections_by_chapter: dict[str, list[dict]] = {}
    for sec in section_rows:
        sections_by_chapter.setdefault(sec["chapter_id"], []).append({
            "id": sec["id"],
            "number": sec["number"],
            "text": multilang_text(sec, ("en", "la", "el")),
        })

    # Group chapters by work_id
    chapters_by_work: dict[str, list[dict]] = {}
    for ch in chapter_rows:
        chapters_by_work.setdefault(ch["work_id"], []).append({
            "id": ch["id"],
            "number": ch["number"],
            "title": ch["title"],
            "sections": sections_by_chapter.get(ch["id"], []),
        })

    works = [
        {
            "id": w["id"],
            "title": w["title"],
            "source_url": w["source_url"],
            "chapter_count": w["chapter_count"],
            "chapters": chapters_by_work.get(w["id"], []),
        }
        for w in work_rows
    ]

    return {"author_id": author_id, "author_name": author["name"], "works": works}
