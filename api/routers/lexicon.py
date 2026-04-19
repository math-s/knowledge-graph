"""Latin (Lewis & Short) and Greek (LSJ-Logeion) lexicon endpoints.

Lemmas are referenced by `id`, which is the source-key from the upstream
dictionary:
  - Latin: L&S `key` (ASCII, e.g. "abditivus", "amare", "verbum1")
  - Greek: LSJ `key` in Perseus beta-code (e.g. "lo/gos", "*xristo/s")

Greek ids contain `/`, `*`, `(`, `)` etc., so they're passed via query string
rather than path params to avoid URL-encoding pain.
"""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import get_db

router = APIRouter(prefix="/lexicon", tags=["lexicon"])

VALID_LANGS = ("la", "el")


def _check_lang(lang: str) -> None:
    if lang not in VALID_LANGS:
        raise HTTPException(404, f"Unknown lexicon language: {lang!r} (use 'la' or 'el')")


def _entry_to_dict(row: sqlite3.Row, lang: str, db: sqlite3.Connection) -> dict:
    out: dict = {
        "id": row["id"],
        "lang": lang,
        "lemma": row["lemma"],
        "pos": row["pos"],
        "definition": row["definition_en"],
        "source_ref": row["source_ref"],
    }
    if lang == "el":
        out["gender"] = row["gender"]
        out["etymology"] = row["etymology"]
    morph = row["morph_json"]
    if morph:
        try:
            out["morph"] = json.loads(morph)
        except (TypeError, ValueError):
            pass
    out.update(_occurrences_summary(db, lang, row["id"]))
    return out


def _occurrences_summary(db: sqlite3.Connection, lang: str, lemma_id: str) -> dict:
    """Aggregate stats for a lemma — total occurrences, breakdown, top forms."""
    rows = db.execute(
        """
        SELECT source_type,
               COUNT(*) AS tokens,
               COUNT(DISTINCT source_id) AS sources
        FROM text_lemma_forms
        WHERE lang = ? AND lemma_id = ?
        GROUP BY source_type
        """,
        (lang, lemma_id),
    ).fetchall()
    by_type = {r["source_type"]: {"tokens": r["tokens"], "sources": r["sources"]} for r in rows}
    total_tokens = sum(v["tokens"] for v in by_type.values())
    total_sources = sum(v["sources"] for v in by_type.values())

    forms = db.execute(
        """
        SELECT form, COUNT(*) AS cnt
        FROM text_lemma_forms
        WHERE lang = ? AND lemma_id = ?
        GROUP BY form
        ORDER BY cnt DESC
        LIMIT 8
        """,
        (lang, lemma_id),
    ).fetchall()
    return {
        "occurrences": {
            "total_tokens": total_tokens,
            "total_sources": total_sources,
            "by_source_type": by_type,
            "top_forms": [{"form": r["form"], "count": r["cnt"]} for r in forms],
        }
    }


@router.get("/{lang}/entry")
def get_entry(
    lang: str,
    id: str | None = Query(None, description="Source key, e.g. 'verbum' or 'lo/gos'"),
    lemma: str | None = Query(None, description="Lookup by surface lemma instead of id"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Fetch a single lemma entry by `id` or by `lemma`."""
    _check_lang(lang)
    if not id and not lemma:
        raise HTTPException(400, "Provide either ?id=... or ?lemma=...")

    table = f"lemma_{lang}"
    if id:
        row = db.execute(
            f"SELECT * FROM {table} WHERE id = ?", (id,)
        ).fetchone()
    else:
        row = db.execute(
            f"SELECT * FROM {table} WHERE lemma = ? LIMIT 1", (lemma,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"No {lang} lemma matching {id or lemma!r}")
    return _entry_to_dict(row, lang, db)


def split_bible_source_id(source_id: str) -> tuple[str, int, int] | None:
    """Parse 'matthew-5:3' or '1-chronicles-1:1' into (book_id, chapter, verse)."""
    try:
        left, verse = source_id.rsplit(":", 1)
        book_id, chapter = left.rsplit("-", 1)
        return book_id, int(chapter), int(verse)
    except (ValueError, AttributeError):
        return None


def resolve_bible_text(db: sqlite3.Connection, source_ids: list[str], lang: str) -> dict[str, str]:
    """Bulk fetch Bible verse text for a list of source_ids."""
    if not source_ids:
        return {}
    parsed = {sid: split_bible_source_id(sid) for sid in source_ids}
    text_col = f"text_{lang}"
    out: dict[str, str] = {}
    by_book: dict[str, list] = {}
    for sid, parts in parsed.items():
        if parts is None:
            continue
        book_id, chapter, verse = parts
        by_book.setdefault(book_id, []).append((chapter, verse, sid))
    for book_id, picks in by_book.items():
        cv_pairs = [(c, v) for (c, v, _) in picks]
        placeholders = ",".join("(?,?)" for _ in cv_pairs)
        flat: list = []
        for c, v in cv_pairs:
            flat.extend([c, v])
        rows = db.execute(
            f"""
            SELECT chapter, verse, {text_col} AS text
            FROM bible_verses
            WHERE book_id = ? AND (chapter, verse) IN (VALUES {placeholders})
            """,
            [book_id, *flat],
        ).fetchall()
        text_by_cv = {(r["chapter"], r["verse"]): r["text"] for r in rows}
        for c, v, sid in picks:
            txt = text_by_cv.get((c, v))
            if txt:
                out[sid] = txt
    return out


def resolve_patristic_text(db: sqlite3.Connection, source_ids: list[str], lang: str) -> dict[str, str]:
    if not source_ids:
        return {}
    text_col = f"text_{lang}"
    placeholders = ",".join("?" for _ in source_ids)
    rows = db.execute(
        f"SELECT id, {text_col} AS text FROM patristic_sections WHERE id IN ({placeholders})",
        source_ids,
    ).fetchall()
    return {r["id"]: r["text"] for r in rows if r["text"]}


# Curated corpus slices on top of bible_books.testament. Useful for hapax /
# vocab queries where you want a meaningful subset of the canon.
_CORPUS_OVERRIDES = {
    "gospels": ["matthew", "mark", "luke", "john"],
    "pauline": [
        "romans", "1-corinthians", "2-corinthians", "galatians", "ephesians",
        "philippians", "colossians", "1-thessalonians", "2-thessalonians",
        "1-timothy", "2-timothy", "titus", "philemon",
    ],
    "catholic-epistles": ["james", "1-peter", "2-peter", "1-john", "2-john", "3-john", "jude"],
}


def _book_ids_for_corpus(db: sqlite3.Connection, corpus: str) -> list[str] | None:
    """Resolve a corpus name to a list of book_ids, or None for 'all'."""
    if corpus == "all":
        return None
    if corpus in _CORPUS_OVERRIDES:
        return list(_CORPUS_OVERRIDES[corpus])
    if corpus in ("nt", "ot"):
        testament = "new" if corpus == "nt" else "old"
        return [r[0] for r in db.execute(
            "SELECT id FROM bible_books WHERE testament = ?", (testament,)
        )]
    # Treat as a single book_id
    exists = db.execute("SELECT 1 FROM bible_books WHERE id = ?", (corpus,)).fetchone()
    if not exists:
        raise HTTPException(404, f"Unknown corpus / book: {corpus!r}")
    return [corpus]


def _book_filter_clause(book_ids: list[str] | None) -> tuple[str, list]:
    """Return (sql_fragment, params) restricting source_id to one or more books.
    Empty fragment when no filter applies. Used by paths still on text_lemma_forms."""
    if not book_ids:
        return "", []
    patterns = [f"{b}-%" for b in book_ids]
    placeholders = " OR ".join(["t.source_id LIKE ?"] * len(patterns))
    return f"AND ({placeholders})", patterns


def _book_in_clause(book_ids: list[str] | None) -> tuple[str, list]:
    """Return (`AND book_id IN (?, ?, …)`, params). Used by paths reading from
    bible_lemma_per_book where book_id is a first-class column."""
    if not book_ids:
        return "", []
    placeholders = ",".join("?" for _ in book_ids)
    return f"AND book_id IN ({placeholders})", list(book_ids)


@router.get("/{lang}/hapax")
def get_hapax(
    lang: str,
    corpus: str = Query("nt", description="nt | ot | all | gospels | pauline | catholic-epistles | <book_id>"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Lemmas that appear exactly once in the chosen corpus slice (hapax legomena)."""
    _check_lang(lang)
    book_ids = _book_ids_for_corpus(db, corpus)
    book_clause, book_params = _book_filter_clause(book_ids)

    dict_table = f"lemma_{lang}"
    dict_join_text_col = "definition_en"
    # Use the pre-aggregated per-book table — instant for any corpus slice.
    # Hapax = lemmas with SUM(tokens)=1 across the chosen books.
    book_in_clause, book_in_params = _book_in_clause(book_ids)
    rows = db.execute(
        f"""
        WITH counts AS (
            SELECT lemma_id, SUM(tokens) AS cnt
            FROM bible_lemma_per_book
            WHERE lang = ? {book_in_clause}
            GROUP BY lemma_id
            HAVING cnt = 1
        )
        SELECT c.lemma_id, l.lemma, l.{dict_join_text_col} AS definition, l.pos
        FROM counts c
        JOIN {dict_table} l ON l.id = c.lemma_id
        ORDER BY c.lemma_id
        LIMIT ? OFFSET ?
        """,
        [lang, *book_in_params, limit, offset],
    ).fetchall()
    # Look up the source verse for each hapax (one-shot indexed query)
    if rows:
        ph_l = ",".join("?" for _ in rows)
        params = [lang, *[r["lemma_id"] for r in rows]]
        forms_by_id = {}
        for fr in db.execute(
            f"SELECT lemma_id, MIN(source_id) AS source_id, MIN(form) AS form "
            f"FROM text_lemma_forms WHERE lang=? AND lemma_id IN ({ph_l}) "
            f"AND source_type='bible-verse' GROUP BY lemma_id",
            params,
        ):
            forms_by_id[fr["lemma_id"]] = (fr["source_id"], fr["form"])
    else:
        forms_by_id = {}

    total = db.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT lemma_id FROM bible_lemma_per_book
            WHERE lang = ? {book_in_clause}
            GROUP BY lemma_id HAVING SUM(tokens) = 1
        )
        """,
        [lang, *book_in_params],
    ).fetchone()[0]

    # Resolve verse text for the source_ids on the page
    sids = [forms_by_id.get(r["lemma_id"], ("", ""))[0] for r in rows]
    text_by_sid = resolve_bible_text(db, [s for s in sids if s], lang)

    results = []
    for r in rows:
        sid, form = forms_by_id.get(r["lemma_id"], ("", ""))
        results.append({
            "id": r["lemma_id"],
            "lemma": r["lemma"],
            "pos": r["pos"],
            "definition": (r["definition"][:200].rstrip() + "…") if r["definition"] and len(r["definition"]) > 200 else r["definition"],
            "form": form,
            "source_id": sid,
            "verse_text": text_by_sid.get(sid, ""),
        })
    return {
        "lang": lang,
        "corpus": corpus,
        "books": book_ids,
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@router.get("/{lang}/vocab")
def get_vocab(
    lang: str,
    corpus: str = Query("all", description="nt | ot | all | gospels | pauline | catholic-epistles | <book_id>"),
    pos: str | None = Query(None, description="Filter by Universal POS (NOUN, VERB, ADJ, ADV, PROPN, ...)"),
    min_count: int = Query(2, ge=1, description="Minimum occurrence count"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Top lemmas by frequency in a corpus slice. POS filter useful for
    'top 50 verbs in Romans'-style queries."""
    _check_lang(lang)
    book_ids = _book_ids_for_corpus(db, corpus)
    dict_table = f"lemma_{lang}"

    if book_ids is None:
        # Whole-corpus path: served by the pre-aggregated table to avoid an 8M-row
        # GROUP BY on every request. Aggregate across pos rows when no pos filter.
        pos_clause = ""
        pos_params: list = []
        if pos:
            pos_clause = "AND f.pos = ?"
            pos_params = [pos.upper()]
        rows = db.execute(
            f"""
            SELECT f.lemma_id,
                   SUM(f.tokens)  AS cnt,
                   SUM(f.sources) AS verses,
                   l.lemma, l.pos AS dict_pos, l.definition_en AS definition
            FROM bible_lemma_pos_freq f
            JOIN {dict_table} l ON l.id = f.lemma_id
            WHERE f.lang = ?
              {pos_clause}
            GROUP BY f.lemma_id
            HAVING cnt >= ?
            ORDER BY cnt DESC
            LIMIT ? OFFSET ?
            """,
            [lang, *pos_params, min_count, limit, offset],
        ).fetchall()
    else:
        # Per-book path: served by the pre-aggregated `bible_lemma_per_book`
        # table — instant for any subset of books, including 27/46-book OR'd
        # corpora like nt/ot.
        book_in_clause, book_in_params = _book_in_clause(book_ids)
        pos_clause = ""
        pos_params = []
        if pos:
            pos_clause = "AND f.pos = ?"
            pos_params = [pos.upper()]
        rows = db.execute(
            f"""
            SELECT f.lemma_id,
                   SUM(f.tokens)  AS cnt,
                   SUM(f.sources) AS verses,
                   l.lemma, l.pos AS dict_pos, l.definition_en AS definition
            FROM bible_lemma_per_book f
            JOIN {dict_table} l ON l.id = f.lemma_id
            WHERE f.lang = ? {book_in_clause}
              {pos_clause}
            GROUP BY f.lemma_id
            HAVING cnt >= ?
            ORDER BY cnt DESC
            LIMIT ? OFFSET ?
            """,
            [lang, *book_in_params, *pos_params, min_count, limit, offset],
        ).fetchall()

    return {
        "lang": lang,
        "corpus": corpus,
        "books": book_ids,
        "pos": pos,
        "min_count": min_count,
        "limit": limit,
        "offset": offset,
        "results": [
            {
                "id": r["lemma_id"],
                "lemma": r["lemma"],
                "pos": r["dict_pos"],
                "count": r["cnt"],
                "verses": r["verses"],
                "definition_preview": (r["definition"][:160].rstrip() + "…") if r["definition"] and len(r["definition"]) > 160 else r["definition"],
            }
            for r in rows
        ],
    }


@router.get("/{lang}/occurrences")
def get_occurrences(
    lang: str,
    id: str = Query(..., description="Lemma id"),
    source_type: str | None = Query(
        None, description="Filter to one source type (bible-verse | patristic-section)"
    ),
    snippet_chars: int = Query(200, ge=0, le=2000),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Paginated list of texts that contain this lemma, with text snippets."""
    _check_lang(lang)
    where = ["lang = ?", "lemma_id = ?"]
    params: list = [lang, id]
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)

    total = db.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT 1 FROM text_lemma_forms
          WHERE {' AND '.join(where)}
          GROUP BY source_type, source_id
        )
        """,
        params,
    ).fetchone()[0]

    rows = db.execute(
        f"""
        SELECT source_type, source_id,
               COUNT(*) AS token_count,
               GROUP_CONCAT(DISTINCT form) AS forms
        FROM text_lemma_forms
        WHERE {' AND '.join(where)}
        GROUP BY source_type, source_id
        ORDER BY source_id
        LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()

    bible_ids = [r["source_id"] for r in rows if r["source_type"] == "bible-verse"]
    pat_ids = [r["source_id"] for r in rows if r["source_type"] == "patristic-section"]
    bible_text = resolve_bible_text(db, bible_ids, lang)
    pat_text = resolve_patristic_text(db, pat_ids, lang)

    results = []
    for r in rows:
        sid = r["source_id"]
        if r["source_type"] == "bible-verse":
            text = bible_text.get(sid, "")
        else:
            text = pat_text.get(sid, "")
        if snippet_chars and text and len(text) > snippet_chars:
            text = text[:snippet_chars].rstrip() + "…"
        results.append({
            "source_type": r["source_type"],
            "source_id": sid,
            "token_count": r["token_count"],
            "forms": (r["forms"] or "").split(",") if r["forms"] else [],
            "text": text,
        })

    return {
        "id": id,
        "lang": lang,
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@router.get("/{lang}")
def search_lexicon(
    lang: str,
    q: str = Query(..., min_length=1, description="FTS query (matches lemma + definition)"),
    lemma_only: bool = Query(False, description="Match only against the lemma column"),
    limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search the lexicon. Returns ranked hits with a definition snippet."""
    _check_lang(lang)
    fts = f"lemma_{lang}_fts"
    match = f'lemma:"{q}"' if lemma_only else q
    try:
        rows = db.execute(
            f"""
            SELECT id, lemma,
                   snippet({fts}, 2, '<mark>', '</mark>', '…', 20) AS snippet,
                   rank
            FROM {fts}
            WHERE {fts} MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (match, limit),
        ).fetchall()
    except sqlite3.OperationalError as e:
        raise HTTPException(400, f"Invalid FTS query: {e}")

    results = [
        {
            "id": r["id"],
            "lemma": r["lemma"],
            "snippet": r["snippet"],
            "rank": r["rank"],
        }
        for r in rows
    ]
    return {"query": q, "lang": lang, "count": len(results), "results": results}
