"""Ecclesiastical document endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db

router = APIRouter(prefix="/documents", tags=["documents"])


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

    return [
        {
            "id": r["id"],
            "name": r["name"],
            "abbreviation": r["abbreviation"],
            "category": r["category"],
            "source_url": r["source_url"],
            "fetchable": bool(r["fetchable"]),
            "section_count": r["section_count"],
            "available_langs": json.loads(r["available_langs_json"] or "[]"),
            "citing_paragraphs": json.loads(r["citing_paragraphs_json"] or "[]"),
        }
        for r in rows
    ]


@router.get("/{doc_id}")
def get_document(doc_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get a document with metadata."""
    row = db.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Document '{doc_id}' not found")
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

    sections = {}
    for r in rows:
        text = {
            lang: r[f"text_{lang}"]
            for lang in ("en", "la", "pt")
            if r[f"text_{lang}"]
        }
        sections[r["section_num"]] = text

    return {"document_id": doc_id, "section_count": len(sections), "sections": sections}
