"""CCC paragraph endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db

router = APIRouter(prefix="/paragraphs", tags=["paragraphs"])


def _row_to_paragraph(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "text": {
            lang: row[f"text_{lang}"]
            for lang in ("en", "la", "pt")
            if row[f"text_{lang}"]
        },
        "part": row["part"],
        "section": row["section"],
        "chapter": row["chapter"],
        "article": row["article"],
        "themes": json.loads(row["themes_json"] or "[]"),
        "footnotes": json.loads(row["footnotes_json"] or "[]"),
    }


@router.get("")
def list_paragraphs(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    theme: str | None = Query(None, description="Filter by theme"),
    db: sqlite3.Connection = Depends(get_db),
):
    """List paragraphs with pagination, optionally filtered by theme."""
    offset = (page - 1) * limit

    if theme:
        total = db.execute(
            "SELECT COUNT(*) FROM paragraph_themes WHERE theme_id = ?", (theme,)
        ).fetchone()[0]
        rows = db.execute(
            """
            SELECT p.* FROM paragraphs p
            JOIN paragraph_themes pt ON p.id = pt.paragraph_id
            WHERE pt.theme_id = ?
            ORDER BY p.id
            LIMIT ? OFFSET ?
            """,
            (theme, limit, offset),
        ).fetchall()
    else:
        total = db.execute("SELECT COUNT(*) FROM paragraphs").fetchone()[0]
        rows = db.execute(
            "SELECT * FROM paragraphs ORDER BY id LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "paragraphs": [_row_to_paragraph(r) for r in rows],
    }


@router.get("/{paragraph_id}")
def get_paragraph(
    paragraph_id: int,
    db: sqlite3.Connection = Depends(get_db),
):
    """Get a single paragraph with all its citations."""
    row = db.execute(
        "SELECT * FROM paragraphs WHERE id = ?", (paragraph_id,)
    ).fetchone()
    if not row:
        raise HTTPException(404, f"Paragraph {paragraph_id} not found")

    para = _row_to_paragraph(row)

    # Cross references
    cross_refs = db.execute(
        "SELECT target_id FROM paragraph_cross_refs WHERE paragraph_id = ?",
        (paragraph_id,),
    ).fetchall()
    para["cross_references"] = [r["target_id"] for r in cross_refs]

    # Bible citations
    bible_cites = db.execute(
        "SELECT book, reference FROM paragraph_bible_citations WHERE paragraph_id = ?",
        (paragraph_id,),
    ).fetchall()
    para["bible_citations"] = [dict(r) for r in bible_cites]

    # Document citations
    doc_cites = db.execute(
        "SELECT document, section FROM paragraph_document_citations WHERE paragraph_id = ?",
        (paragraph_id,),
    ).fetchall()
    para["document_citations"] = [dict(r) for r in doc_cites]

    # Author citations
    author_cites = db.execute(
        "SELECT author FROM paragraph_author_citations WHERE paragraph_id = ?",
        (paragraph_id,),
    ).fetchall()
    para["author_citations"] = [r["author"] for r in author_cites]

    return para
