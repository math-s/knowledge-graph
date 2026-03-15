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

SNIPPET_LANGS = ("en", "la", "pt")
BIBLE_SNIPPET_LANGS = ("en", "la", "pt", "el")
PATRISTIC_SNIPPET_LANGS = ("en", "la", "el")


def _pick_snippet(row: sqlite3.Row, lang: str, available: tuple[str, ...] = SNIPPET_LANGS) -> str:
    """Pick the best snippet for the requested language."""
    key = f"snippet_{lang}"
    if key in row.keys():
        val = row[key]
        if val:
            return val
    # Fallback through available languages
    for l in available:
        val = row.get(f"snippet_{l}", "")
        if val:
            return val
    return ""


def _all_snippets(row: sqlite3.Row, available: tuple[str, ...] = SNIPPET_LANGS) -> dict[str, str]:
    """Return all non-empty snippets as a dict."""
    out = {}
    for l in available:
        val = row.get(f"snippet_{l}", "")
        if val:
            out[l] = val
    return out


@router.get("/search")
def search(
    q: str = Query(..., min_length=2, description="Search query"),
    lang: str = Query("en", description="Preferred language (en, la, pt)"),
    bilingual: bool = Query(False, description="Return all available translations per result"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search across CCC paragraphs and source nodes using FTS5."""
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

    results = []
    for row in rows:
        entry: dict = {
            "id": row["entry_id"],
            "type": row["entry_type"],
            "snippet": _pick_snippet(row, lang),
            "rank": row["rank"],
        }
        if bilingual:
            entry["translations"] = _all_snippets(row)
        results.append(entry)

    return {"query": q, "lang": lang, "count": len(results), "results": results}


@router.get("/search/bible")
def search_bible(
    q: str = Query(..., min_length=2, description="Search query"),
    lang: str = Query("en", description="Language to search (en, la, pt, el)"),
    bilingual: bool = Query(False, description="Return all available translations per result"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search within Bible verse text."""
    rows = db.execute(
        """
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
        entry: dict = {
            "book_id": row["book_id"],
            "chapter": int(row["chapter"]),
            "verse": int(row["verse"]),
            "snippet": _pick_snippet(row, lang, BIBLE_SNIPPET_LANGS),
            "rank": row["rank"],
        }
        if bilingual:
            entry["translations"] = _all_snippets(row, BIBLE_SNIPPET_LANGS)
        results.append(entry)

    return {"query": q, "lang": lang, "count": len(results), "results": results}


@router.get("/search/patristic")
def search_patristic(
    q: str = Query(..., min_length=2, description="Search query"),
    lang: str = Query("en", description="Language to search (en, la, el)"),
    bilingual: bool = Query(False, description="Return all available translations per result"),
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
        entry: dict = {
            "section_id": row["section_id"],
            "work_id": row["work_id"],
            "author_id": row["author_id"],
            "snippet": _pick_snippet(row, lang, PATRISTIC_SNIPPET_LANGS),
            "rank": row["rank"],
        }
        if bilingual:
            entry["translations"] = _all_snippets(row, PATRISTIC_SNIPPET_LANGS)
        results.append(entry)

    return {"query": q, "lang": lang, "count": len(results), "results": results}
