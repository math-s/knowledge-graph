"""Full-text search across paragraphs, Bible verses, and patristic texts."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db
from .lexicon import resolve_bible_text, resolve_patristic_text

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


@router.get("/search/lemma")
def search_by_lemma(
    q: str = Query(..., min_length=1, description="Term to resolve via the lexicon (matches lemma + definition)"),
    lang: str = Query("both", description="'la', 'el', or 'both' — language of lemma to match"),
    source_type: str | None = Query(
        None, description="Restrict to 'bible-verse' or 'patristic-section'"
    ),
    snippet_chars: int = Query(200, ge=0, le=2000),
    max_lemmas: int = Query(20, ge=1, le=100, description="How many top-ranked lemmas to expand the query into"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Lemma-aware search: resolves the query to matching lemmas via the
    lexicon FTS, then returns sources containing any of those lemmas.

    Differs from /search/bible — that one matches the literal string, while
    this one matches the *concept*: querying "love" surfaces verses with any
    inflected form of amare/amo/diligo/ἀγαπάω/φιλέω, not just verses that
    literally contain "love" in English.
    """
    langs = ("la", "el") if lang == "both" else (lang,)
    if any(l not in ("la", "el") for l in langs):
        raise HTTPException(404, f"Unknown lemma lang: {lang!r} (use 'la', 'el', or 'both')")

    # Step 1: resolve query → lemma_ids via FTS, then re-rank by corpus
    # frequency. Pure FTS rank surfaces hapax proper nouns ("Cotiso" for
    # "king") above canonical lemmas like "rex1"; corpus-weighted ranking
    # prefers lemmas actually used in the texts. Pull a wider FTS window
    # (3× max_lemmas) so we have enough candidates to re-rank.
    matched_lemmas: list[dict] = []
    # Pull a wide FTS window then re-rank by corpus frequency (pre-aggregated
    # in lemma_corpus_freq so this is an indexed join, not a full scan).
    fts_limit = max(max_lemmas * 10, 200)
    for l in langs:
        fts = f"lemma_{l}_fts"
        try:
            rows = db.execute(
                f"""
                WITH hits AS (
                    SELECT id, lemma, rank
                    FROM {fts}
                    WHERE {fts} MATCH ?
                    ORDER BY rank
                    LIMIT ?
                )
                SELECT h.id, h.lemma, h.rank,
                       COALESCE(f.tokens, 0) AS corpus_freq
                FROM hits h
                LEFT JOIN lemma_corpus_freq f
                  ON f.lang = ? AND f.lemma_id = h.id
                ORDER BY corpus_freq DESC, h.rank ASC
                LIMIT ?
                """,
                (q, fts_limit, l, max_lemmas),
            ).fetchall()
        except sqlite3.OperationalError as e:
            raise HTTPException(400, f"Invalid FTS query: {e}")
        for r in rows:
            matched_lemmas.append({
                "lang": l,
                "id": r["id"],
                "lemma": r["lemma"],
                "corpus_freq": r["corpus_freq"],
            })

    if not matched_lemmas:
        return {
            "query": q, "lang": lang, "lemmas_matched": [],
            "total": 0, "limit": limit, "offset": offset, "results": [],
        }

    # Step 2: find sources containing any matched lemma. Score by distinct
    # lemmas hit (richer overlap > more occurrences of one lemma).
    lemma_keys = [(m["lang"], m["id"]) for m in matched_lemmas]
    lang_placeholders = ",".join("?" for _ in lemma_keys)
    flat: list = []
    for ml, mid in lemma_keys:
        flat.extend([ml, mid])
    where = ["(t.lang, t.lemma_id) IN (VALUES " + lang_placeholders.replace("?", "(?,?)") + ")"]
    if source_type:
        where.append("t.source_type = ?")
        flat.append(source_type)
    where_sql = " AND ".join(where)

    total = db.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM text_lemma_forms t WHERE {where_sql} GROUP BY t.source_type, t.source_id)",
        flat,
    ).fetchone()[0]

    rows = db.execute(
        f"""
        SELECT t.source_type, t.source_id, t.lang,
               COUNT(DISTINCT t.lemma_id) AS distinct_lemmas,
               COUNT(*) AS total_hits,
               GROUP_CONCAT(DISTINCT t.lemma_id) AS hit_lemma_ids,
               GROUP_CONCAT(DISTINCT t.form)     AS hit_forms
        FROM text_lemma_forms t
        WHERE {where_sql}
        GROUP BY t.source_type, t.source_id, t.lang
        ORDER BY distinct_lemmas DESC, total_hits DESC, t.source_id
        LIMIT ? OFFSET ?
        """,
        [*flat, limit, offset],
    ).fetchall()

    # Resolve text per source (per-language column).
    by_lang_st: dict[tuple[str, str], list[str]] = {}
    for r in rows:
        by_lang_st.setdefault((r["lang"], r["source_type"]), []).append(r["source_id"])
    text_lookup: dict[tuple[str, str, str], str] = {}
    for (l, st), sids in by_lang_st.items():
        if st == "bible-verse":
            d = resolve_bible_text(db, sids, l)
        else:
            d = resolve_patristic_text(db, sids, l)
        for sid, txt in d.items():
            text_lookup[(l, st, sid)] = txt

    results = []
    for r in rows:
        text = text_lookup.get((r["lang"], r["source_type"], r["source_id"]), "")
        if snippet_chars and text and len(text) > snippet_chars:
            text = text[:snippet_chars].rstrip() + "…"
        results.append({
            "source_type": r["source_type"],
            "source_id": r["source_id"],
            "lang": r["lang"],
            "distinct_lemmas": r["distinct_lemmas"],
            "total_hits": r["total_hits"],
            "matched_lemma_ids": (r["hit_lemma_ids"] or "").split(","),
            "matched_forms": (r["hit_forms"] or "").split(",") if r["hit_forms"] else [],
            "text": text,
        })

    return {
        "query": q,
        "lang": lang,
        "lemmas_matched": matched_lemmas,
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": results,
    }


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
