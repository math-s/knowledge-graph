"""Ecclesiastical document endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db
from ..utils import multilang_text

router = APIRouter(prefix="/documents", tags=["documents"])


def _row_to_document(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "abbreviation": row["abbreviation"],
        "category": row["category"],
        "source_url": row["source_url"],
        "fetchable": bool(row["fetchable"]),
        "section_count": row["section_count"],
        "available_langs": json.loads(row["available_langs_json"] or "[]"),
        "citing_paragraphs": json.loads(row["citing_paragraphs_json"] or "[]"),
    }


@router.get("")
def list_documents(db: sqlite3.Connection = Depends(get_db)):
    """List all documents with metadata."""
    rows = db.execute(
        """
        SELECT id, name, abbreviation, category, source_url, fetchable,
               section_count, available_langs_json, citing_paragraphs_json
        FROM documents ORDER BY name
        """
    ).fetchall()
    return [_row_to_document(r) for r in rows]


@router.get("/{doc_id}")
def get_document(doc_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get a document with metadata."""
    row = db.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Document '{doc_id}' not found")
    return _row_to_document(row)


@router.get("/{doc_id}/sections")
def get_document_sections(doc_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get all sections for a document."""
    rows = db.execute(
        """
        SELECT section_num, text_en, text_la, text_pt
        FROM document_sections
        WHERE document_id = ?
        ORDER BY CAST(section_num AS INTEGER)
        """,
        (doc_id,),
    ).fetchall()

    if not rows:
        raise HTTPException(404, f"No sections found for document '{doc_id}'")

    sections = {r["section_num"]: multilang_text(r, ("en", "la", "pt")) for r in rows}
    return {"document_id": doc_id, "section_count": len(sections), "sections": sections}
