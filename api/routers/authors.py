"""Patristic author and work endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db

router = APIRouter(prefix="/authors", tags=["authors"])


@router.get("")
def list_authors(db: sqlite3.Connection = Depends(get_db)):
    """List all authors with metadata."""
    rows = db.execute(
        "SELECT id, name, era, citing_paragraphs_json, work_count FROM authors ORDER BY name"
    ).fetchall()

    return [
        {
            "id": r["id"],
            "name": r["name"],
            "era": r["era"],
            "citing_paragraphs": json.loads(r["citing_paragraphs_json"] or "[]"),
            "work_count": r["work_count"],
        }
        for r in rows
    ]


@router.get("/{author_id}")
def get_author(author_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get an author with metadata."""
    row = db.execute("SELECT * FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Author '{author_id}' not found")
    return {
        "id": row["id"],
        "name": row["name"],
        "era": row["era"],
        "citing_paragraphs": json.loads(row["citing_paragraphs_json"] or "[]"),
        "work_count": row["work_count"],
    }


@router.get("/{author_id}/works")
def get_author_works(author_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get all works for an author, with chapters and sections."""
    # Verify author exists
    author = db.execute("SELECT id, name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not author:
        raise HTTPException(404, f"Author '{author_id}' not found")

    work_rows = db.execute(
        "SELECT id, title, source_url, chapter_count FROM author_works WHERE author_id = ? ORDER BY title",
        (author_id,),
    ).fetchall()

    works = []
    for w in work_rows:
        chapter_rows = db.execute(
            "SELECT id, number, title FROM patristic_chapters WHERE work_id = ? ORDER BY number",
            (w["id"],),
        ).fetchall()

        chapters = []
        for ch in chapter_rows:
            section_rows = db.execute(
                "SELECT id, number, text_en, text_la, text_el FROM patristic_sections WHERE chapter_id = ? ORDER BY number",
                (ch["id"],),
            ).fetchall()

            sections = []
            for sec in section_rows:
                text = {
                    lang: sec[f"text_{lang}"]
                    for lang in ("en", "la", "el")
                    if sec[f"text_{lang}"]
                }
                sections.append({
                    "id": sec["id"],
                    "number": sec["number"],
                    "text": text,
                })

            chapters.append({
                "id": ch["id"],
                "number": ch["number"],
                "title": ch["title"],
                "sections": sections,
            })

        works.append({
            "id": w["id"],
            "title": w["title"],
            "source_url": w["source_url"],
            "chapter_count": w["chapter_count"],
            "chapters": chapters,
        })

    return {"author_id": author_id, "author_name": author["name"], "works": works}
