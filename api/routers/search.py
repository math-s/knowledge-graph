"""Full-text search across paragraphs, Bible verses, and patristic texts."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Query

from ..db import get_db

router = APIRouter(tags=["search"])

LANG_COLUMNS = {
    "en": "text_en",
    "la": "text_la",
    "pt": "text_pt",
    "el": "text_el",
}


@router.get("/search")
def search(
    q: str = Query(..., min_length=2, description="Search query"),
    lang: str = Query("en", description="Preferred language (en, la, pt)"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search across CCC paragraphs and source nodes using FTS5."""
    results = []

    rows = db.execute(
        """
        SELECT entry_id, entry_type,
               snippet(search_fts, 2, '<mark>', '</mark>', '…', 40) AS snippet_en,
               snippet(search_fts, 3, '<mark>', '</mark>', '…', 40) AS snippet_la,
               snippet(search_fts, 4, '<mark>', '</mark>', '…', 40) AS snippet_pt,
               rank
        FROM search_fts
        WHERE search_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (q, limit),
    ).fetchall()

    for row in rows:
        # Pick the best snippet for the requested language
        snippet_col = f"snippet_{lang}" if f"snippet_{lang}" in row.keys() else "snippet_en"
        snippet = row[snippet_col] or row["snippet_en"] or ""

        results.append({
            "id": row["entry_id"],
            "type": row["entry_type"],
            "snippet": snippet,
            "rank": row["rank"],
        })

    return {"query": q, "lang": lang, "count": len(results), "results": results}


@router.get("/search/bible")
def search_bible(
    q: str = Query(..., min_length=2, description="Search query"),
    lang: str = Query("en", description="Language to search (en, la, pt, el)"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search within Bible verse text."""
    col = LANG_COLUMNS.get(lang, "text_en")
    rows = db.execute(
        f"""
        SELECT book_id, chapter, verse,
               snippet(bible_verses_fts, 3, '<mark>', '</mark>', '…', 40) AS snippet_en,
               snippet(bible_verses_fts, 4, '<mark>', '</mark>', '…', 40) AS snippet_la,
               snippet(bible_verses_fts, 5, '<mark>', '</mark>', '…', 40) AS snippet_pt,
               snippet(bible_verses_fts, 6, '<mark>', '</mark>', '…', 40) AS snippet_el,
               rank
        FROM bible_verses_fts
        WHERE bible_verses_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (q, limit),
    ).fetchall()

    results = []
    for row in rows:
        snippet_key = f"snippet_{lang}"
        snippet = row[snippet_key] if snippet_key in row.keys() else row["snippet_en"]
        results.append({
            "book_id": row["book_id"],
            "chapter": int(row["chapter"]),
            "verse": int(row["verse"]),
            "snippet": snippet or "",
            "rank": row["rank"],
        })

    return {"query": q, "lang": lang, "count": len(results), "results": results}


@router.get("/search/patristic")
def search_patristic(
    q: str = Query(..., min_length=2, description="Search query"),
    lang: str = Query("en", description="Language to search (en, la, el)"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search within patristic text."""
    rows = db.execute(
        """
        SELECT section_id, work_id, author_id,
               snippet(patristic_sections_fts, 3, '<mark>', '</mark>', '…', 40) AS snippet_en,
               snippet(patristic_sections_fts, 4, '<mark>', '</mark>', '…', 40) AS snippet_la,
               snippet(patristic_sections_fts, 5, '<mark>', '</mark>', '…', 40) AS snippet_el,
               rank
        FROM patristic_sections_fts
        WHERE patristic_sections_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (q, limit),
    ).fetchall()

    results = []
    for row in rows:
        snippet_key = f"snippet_{lang}"
        snippet = row[snippet_key] if snippet_key in row.keys() else row["snippet_en"]
        results.append({
            "section_id": row["section_id"],
            "work_id": row["work_id"],
            "author_id": row["author_id"],
            "snippet": snippet or "",
            "rank": row["rank"],
        })

    return {"query": q, "lang": lang, "count": len(results), "results": results}
