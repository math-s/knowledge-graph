"""CCC paragraph endpoints."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db
from ..utils import multilang_text

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


@router.get("/parts")
def list_paragraph_parts(db: sqlite3.Connection = Depends(get_db)):
    """Return lightweight [{id, part}] for all paragraphs (~10KB)."""
    rows = db.execute("SELECT id, part FROM paragraphs ORDER BY id").fetchall()
    return [{"id": r["id"], "part": r["part"]} for r in rows]


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


@router.get("/{paragraph_id}/full")
def get_paragraph_full(
    paragraph_id: int,
    db: sqlite3.Connection = Depends(get_db),
):
    """Get a paragraph with all cited source texts resolved.

    Returns every Bible verse, patristic section, and document section cited
    by this paragraph, with full multilingual text, so callers don't need
    to make separate lookups per citation.
    """
    row = db.execute("SELECT * FROM paragraphs WHERE id = ?", (paragraph_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Paragraph {paragraph_id} not found")

    para = _row_to_paragraph(row)

    # Cross-references (other paragraph IDs)
    para["cross_references"] = [
        r["target_id"] for r in db.execute(
            "SELECT target_id FROM paragraph_cross_refs WHERE paragraph_id = ?",
            (paragraph_id,),
        ).fetchall()
    ]

    node_id = f"p:{paragraph_id}"

    # Resolve Bible verse texts via graph edges
    verse_edges = db.execute(
        "SELECT target FROM graph_edges WHERE source = ? AND target LIKE 'bible-verse:%'",
        (node_id,),
    ).fetchall()
    verse_node_ids = [r["target"] for r in verse_edges]

    bible_sources: list[dict] = []
    if verse_node_ids:
        # Parse node IDs like "bible-verse:john-1:14" → (book_id, chapter, verse)
        parsed = []
        for nid in verse_node_ids:
            ref = nid[len("bible-verse:"):]  # "john-1:14"
            try:
                left, verse_num = ref.rsplit(":", 1)
                book_id, chapter_num = left.rsplit("-", 1)
                parsed.append((nid, book_id, int(chapter_num), int(verse_num)))
            except ValueError:
                continue

        if parsed:
            ph = ",".join("(?,?,?)" for _ in parsed)
            flat: list = []
            for _, book_id, ch, vs in parsed:
                flat.extend([book_id, ch, vs])
            verse_rows = db.execute(
                f"""
                SELECT book_id, chapter, verse, text_en, text_la, text_pt, text_el
                FROM bible_verses
                WHERE (book_id, chapter, verse) IN (VALUES {ph})
                """,
                flat,
            ).fetchall()
            text_by_ref = {
                (r["book_id"], r["chapter"], r["verse"]): r for r in verse_rows
            }
            for nid, book_id, ch, vs in parsed:
                vrow = text_by_ref.get((book_id, ch, vs))
                if vrow:
                    bible_sources.append({
                        "node_id": nid,
                        "book_id": book_id,
                        "chapter": ch,
                        "verse": vs,
                        "text": multilang_text(vrow, ("en", "la", "pt", "el")),
                    })

    para["sources_bible"] = bible_sources

    # Resolve patristic section texts via graph edges
    pat_edges = db.execute(
        "SELECT target FROM graph_edges WHERE source = ? AND target LIKE 'patristic-section:%'",
        (node_id,),
    ).fetchall()
    pat_ids = [r["target"][len("patristic-section:"):] for r in pat_edges]

    patristic_sources: list[dict] = []
    if pat_ids:
        ph = ",".join("?" for _ in pat_ids)
        sec_rows = db.execute(
            f"""
            SELECT ps.id, ps.chapter_id, ps.text_en, ps.text_la, ps.text_el,
                   pc.work_id, aw.author_id
            FROM patristic_sections ps
            JOIN patristic_chapters pc ON pc.id = ps.chapter_id
            JOIN author_works aw ON aw.id = pc.work_id
            WHERE ps.id IN ({ph})
            """,
            pat_ids,
        ).fetchall()
        for sec in sec_rows:
            patristic_sources.append({
                "section_id": sec["id"],
                "work_id": sec["work_id"],
                "author_id": sec["author_id"],
                "text": multilang_text(sec, ("en", "la", "el")),
            })

    para["sources_patristic"] = patristic_sources

    # Resolve document section texts from citation table
    doc_cite_rows = db.execute(
        "SELECT document, section FROM paragraph_document_citations WHERE paragraph_id = ?",
        (paragraph_id,),
    ).fetchall()

    document_sources: list[dict] = []
    for cite in doc_cite_rows:
        sec_row = db.execute(
            "SELECT text_en, text_la, text_pt FROM document_sections WHERE document_id = ? AND section_num = ?",
            (cite["document"], cite["section"]),
        ).fetchone()
        document_sources.append({
            "document_id": cite["document"],
            "section": cite["section"],
            "text": multilang_text(sec_row, ("en", "la", "pt")) if sec_row else {},
        })

    para["sources_documents"] = document_sources

    # Author-level citations (no specific section resolved)
    para["sources_authors"] = [
        r["author"] for r in db.execute(
            "SELECT author FROM paragraph_author_citations WHERE paragraph_id = ?",
            (paragraph_id,),
        ).fetchall()
    ]

    return para
