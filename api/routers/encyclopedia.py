"""Catholic Encyclopedia endpoints, including cross-corpus graph bridges."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db

router = APIRouter(prefix="/encyclopedia", tags=["encyclopedia"])


def _row_to_article(row: sqlite3.Row, full_text: bool = False) -> dict:
    out = {
        "id": row["id"],
        "title": row["title"],
        "summary": row["summary"],
        "url": row["url"],
    }
    if full_text:
        out["text"] = row["text_en"]
    return out


@router.get("")
def search_encyclopedia(
    q: str = Query(..., min_length=2, description="Full-text search query"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search the Catholic Encyclopedia (1907–1913, ~11,600 articles) via FTS."""
    try:
        rows = db.execute(
            """
            SELECT e.id, e.title, e.summary, e.url,
                   snippet(encyclopedia_fts, 2, '<mark>', '</mark>', '…', 40) AS snippet,
                   encyclopedia_fts.rank
            FROM encyclopedia_fts
            JOIN encyclopedia e ON e.id = encyclopedia_fts.id
            WHERE encyclopedia_fts MATCH ?
            ORDER BY encyclopedia_fts.rank
            LIMIT ?
            """,
            (q, limit),
        ).fetchall()
    except sqlite3.OperationalError as e:
        raise HTTPException(400, f"Invalid FTS query: {e}")

    return {
        "query": q,
        "count": len(rows),
        "results": [
            {
                "id": r["id"],
                "title": r["title"],
                "summary": r["summary"],
                "snippet": r["snippet"],
                "rank": r["rank"],
            }
            for r in rows
        ],
    }


@router.get("/{article_id}")
def get_article(article_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get a single encyclopedia article with its full text."""
    row = db.execute("SELECT * FROM encyclopedia WHERE id = ?", (article_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Article '{article_id}' not found")
    return _row_to_article(row, full_text=True)


@router.get("/{article_id}/related")
def get_related_articles(
    article_id: str,
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Get encyclopedia articles related to this one via cross-references.

    Uses the ency_cross_reference graph edges (412k edges) which encode
    hyperlinks between articles in the original encyclopedia.
    """
    exists = db.execute("SELECT 1 FROM encyclopedia WHERE id = ?", (article_id,)).fetchone()
    if not exists:
        raise HTTPException(404, f"Article '{article_id}' not found")

    node_id = f"ency:{article_id}"
    rows = db.execute(
        """
        SELECT e.id, e.title, e.summary, e.url,
               ge.edge_type,
               CASE WHEN ge.source = ? THEN 'outbound' ELSE 'inbound' END AS direction
        FROM graph_edges ge
        JOIN encyclopedia e ON e.id = CASE
            WHEN ge.source = ? THEN SUBSTR(ge.target, 6)
            ELSE SUBSTR(ge.source, 6)
        END
        WHERE ge.edge_type = 'ency_cross_reference'
          AND (ge.source = ? OR ge.target = ?)
        LIMIT ?
        """,
        (node_id, node_id, node_id, node_id, limit),
    ).fetchall()

    return {
        "article_id": article_id,
        "count": len(rows),
        "related": [
            {
                "id": r["id"],
                "title": r["title"],
                "summary": r["summary"],
                "direction": r["direction"],
            }
            for r in rows
        ],
    }


@router.get("/{article_id}/paragraphs")
def get_article_paragraphs(
    article_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Get CCC paragraphs that are discussed in this encyclopedia article.

    Uses the discussed_in graph edges (3,118 edges) which link paragraphs
    to the encyclopedia articles that discuss them.
    """
    exists = db.execute("SELECT 1 FROM encyclopedia WHERE id = ?", (article_id,)).fetchone()
    if not exists:
        raise HTTPException(404, f"Article '{article_id}' not found")

    node_id = f"ency:{article_id}"
    rows = db.execute(
        """
        SELECT p.id, p.text_en, p.part, p.section, p.chapter, p.article
        FROM graph_edges ge
        JOIN paragraphs p ON p.id = CAST(SUBSTR(ge.source, 3) AS INTEGER)
        WHERE ge.edge_type = 'discussed_in' AND ge.target = ?
        ORDER BY p.id
        """,
        (node_id,),
    ).fetchall()

    return {
        "article_id": article_id,
        "count": len(rows),
        "paragraphs": [
            {
                "id": r["id"],
                "text_en": r["text_en"],
                "part": r["part"],
                "section": r["section"],
            }
            for r in rows
        ],
    }
