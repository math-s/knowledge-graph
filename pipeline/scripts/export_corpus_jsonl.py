"""Export the current DB state to corpus JSONL in data/corpus/.

Bootstrap script that captures the current clean DB as the canonical
JSONL source of truth. After this, inject/refresh scripts write here
first; the DB is rebuilt from these files via load_corpus_to_db.py.

Directory layout:
    data/corpus/
        ccc/paragraphs.jsonl
        documents/<doc_id>/sections.jsonl
        documents/_catalog.json           ← document metadata
        summa/articles.jsonl
        patristic/sections.jsonl
        encyclopedia/articles.jsonl
        bible/<book_id>/verses.jsonl
        bible/_catalog.json               ← book metadata
        library/docs.jsonl

Each corpus directory also receives a manifest.json with row counts
and the export timestamp so stale exports are detectable.

Usage:
    python pipeline/scripts/export_corpus_jsonl.py
    python pipeline/scripts/export_corpus_jsonl.py --corpus ccc
    python pipeline/scripts/export_corpus_jsonl.py --corpus documents
    python pipeline/scripts/export_corpus_jsonl.py --corpus documents --doc-id denzinger-hunermann
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
CORPUS_DIR = PROJECT_ROOT / "data" / "corpus"

ALL_CORPORA = ["ccc", "documents", "summa", "patristic", "encyclopedia", "bible", "library"]


# ── helpers ──────────────────────────────────────────────────────────────────

def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Convert sqlite3.Row to dict, skipping NULLs and parsing embedded JSON."""
    d: dict[str, Any] = {}
    for key in row.keys():
        val = row[key]
        if val is None:
            continue
        # Inline JSON fields: embed as real objects, not escaped strings
        if key.endswith("_json") and isinstance(val, str):
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
        d[key] = val
    return d


def _write_jsonl(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    return len(rows)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _write_manifest(directory: Path, counts: dict[str, int]) -> None:
    _write_json(directory / "manifest.json", {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
    })


# ── exporters ────────────────────────────────────────────────────────────────

def export_ccc(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, text_en, text_la, text_pt, part, section, chapter, article,"
        "       themes_json, footnotes_json "
        "FROM paragraphs ORDER BY id"
    ).fetchall()
    out_dir = CORPUS_DIR / "ccc"
    n = _write_jsonl(out_dir / "paragraphs.jsonl", [_row_to_dict(r) for r in rows])
    _write_manifest(out_dir, {"paragraphs": n})
    print(f"  ccc/paragraphs.jsonl: {n} rows")


def export_documents(conn: sqlite3.Connection, doc_filter: str | None = None) -> None:
    # Document catalog (metadata)
    catalog_rows = conn.execute(
        "SELECT id, name, abbreviation, category, source_url, fetchable,"
        "       citing_paragraphs_json, section_count, available_langs_json "
        "FROM documents ORDER BY id"
    ).fetchall()
    catalog = [_row_to_dict(r) for r in catalog_rows]

    out_dir = CORPUS_DIR / "documents"
    _write_json(out_dir / "_catalog.json", catalog)
    print(f"  documents/_catalog.json: {len(catalog)} documents")

    # Sections grouped by document_id
    query = (
        "SELECT document_id, section_num, text_en, text_la, text_pt, meta_json "
        "FROM document_sections "
    )
    params: tuple = ()
    if doc_filter:
        query += "WHERE document_id = ? "
        params = (doc_filter,)
    query += "ORDER BY document_id, section_num"

    sections = conn.execute(query, params).fetchall()

    by_doc: dict[str, list[dict]] = {}
    for row in sections:
        did = row["document_id"]
        by_doc.setdefault(did, []).append(_row_to_dict(row))

    total = 0
    for doc_id, doc_rows in sorted(by_doc.items()):
        path = out_dir / doc_id / "sections.jsonl"
        n = _write_jsonl(path, doc_rows)
        total += n
    _write_manifest(out_dir, {"sections": total, "documents": len(by_doc)})
    print(f"  documents/*/sections.jsonl: {total} rows across {len(by_doc)} documents")


def export_summa(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, question_id, article_num, title, text "
        "FROM summa_articles ORDER BY id"
    ).fetchall()
    out_dir = CORPUS_DIR / "summa"
    n = _write_jsonl(out_dir / "articles.jsonl", [_row_to_dict(r) for r in rows])
    _write_manifest(out_dir, {"articles": n})
    print(f"  summa/articles.jsonl: {n} rows")


def export_patristic(conn: sqlite3.Connection) -> None:
    # chapter_id format: "<author>/<work>/<chapter_num>"
    # Group by author/work so files stay under ~10MB each
    rows = conn.execute(
        "SELECT id, chapter_id, number, text_en, text_la, text_el "
        "FROM patristic_sections ORDER BY chapter_id, number"
    ).fetchall()

    by_work: dict[str, list[dict]] = {}
    for row in rows:
        parts = row["chapter_id"].split("/")
        work_key = "/".join(parts[:2]) if len(parts) >= 2 else parts[0]
        by_work.setdefault(work_key, []).append(_row_to_dict(row))

    out_dir = CORPUS_DIR / "patristic"
    total = 0
    for work_key, work_rows in sorted(by_work.items()):
        author, work = work_key.split("/", 1)
        path = out_dir / author / work / "sections.jsonl"
        total += _write_jsonl(path, work_rows)
    _write_manifest(out_dir, {"sections": total, "works": len(by_work)})
    print(f"  patristic/<author>/<work>/sections.jsonl: {total} rows across {len(by_work)} works")


def export_encyclopedia(conn: sqlite3.Connection) -> None:
    # Split by first letter of title so files stay under ~5MB each
    rows = conn.execute(
        "SELECT id, title, summary, text_en, url "
        "FROM encyclopedia ORDER BY title COLLATE NOCASE, id"
    ).fetchall()

    by_letter: dict[str, list[dict]] = {}
    for row in rows:
        letter = (row["title"] or "0")[0].lower()
        if not letter.isalpha():
            letter = "0"
        by_letter.setdefault(letter, []).append(_row_to_dict(row))

    out_dir = CORPUS_DIR / "encyclopedia"
    total = 0
    for letter, letter_rows in sorted(by_letter.items()):
        path = out_dir / letter / "articles.jsonl"
        total += _write_jsonl(path, letter_rows)
    _write_manifest(out_dir, {"articles": total, "buckets": len(by_letter)})
    print(f"  encyclopedia/<letter>/articles.jsonl: {total} rows across {len(by_letter)} buckets")


def export_bible(conn: sqlite3.Connection) -> None:
    # Book catalog
    catalog_rows = conn.execute(
        "SELECT id, name, abbreviation, testament, category "
        "FROM bible_books ORDER BY id"
    ).fetchall()
    catalog = [_row_to_dict(r) for r in catalog_rows]

    out_dir = CORPUS_DIR / "bible"
    _write_json(out_dir / "_catalog.json", catalog)
    print(f"  bible/_catalog.json: {len(catalog)} books")

    # Verses grouped by book
    verses = conn.execute(
        "SELECT book_id, chapter, verse, text_en, text_la, text_pt, text_el "
        "FROM bible_verses ORDER BY book_id, chapter, verse"
    ).fetchall()

    by_book: dict[str, list[dict]] = {}
    for row in verses:
        bid = row["book_id"]
        by_book.setdefault(bid, []).append(_row_to_dict(row))

    total = 0
    for book_id, book_rows in sorted(by_book.items()):
        path = out_dir / book_id / "verses.jsonl"
        n = _write_jsonl(path, book_rows)
        total += n
    _write_manifest(out_dir, {"verses": total, "books": len(by_book)})
    print(f"  bible/*/verses.jsonl: {total} rows across {len(by_book)} books")


def export_library(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, category, title, year, text "
        "FROM library_docs ORDER BY id"
    ).fetchall()
    out_dir = CORPUS_DIR / "library"
    n = _write_jsonl(out_dir / "docs.jsonl", [_row_to_dict(r) for r in rows])
    _write_manifest(out_dir, {"docs": n})
    print(f"  library/docs.jsonl: {n} rows")


# ── programmatic sync (used by fix scripts) ───────────────────────────────────

def sync(corpus: str, doc_id: str | None = None,
         db_path: Path = DB_PATH) -> None:
    """Re-export a single corpus (or one document) from DB → JSONL.

    Called by fix scripts after they commit DB changes so the JSONL
    stays in sync without a full re-export.

    Example:
        from pipeline.scripts.export_corpus_jsonl import sync as sync_corpus
        sync_corpus("documents", doc_id="denzinger-hunermann")
        sync_corpus("summa")
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        if corpus == "documents" and doc_id:
            export_documents(conn, doc_filter=doc_id)
        else:
            EXPORTERS[corpus](conn)
    finally:
        conn.close()


# ── main ─────────────────────────────────────────────────────────────────────

EXPORTERS = {
    "ccc":          export_ccc,
    "documents":    export_documents,
    "summa":        export_summa,
    "patristic":    export_patristic,
    "encyclopedia": export_encyclopedia,
    "bible":        export_bible,
    "library":      export_library,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corpus", choices=ALL_CORPORA,
        help="Export only this corpus (default: all)",
    )
    parser.add_argument(
        "--doc-id",
        help="When --corpus=documents, export only this document_id",
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    corpora = [args.corpus] if args.corpus else ALL_CORPORA
    print(f"Exporting {len(corpora)} corpus/corpora → {CORPUS_DIR}")

    try:
        for corpus in corpora:
            if corpus == "documents" and args.doc_id:
                export_documents(conn, doc_filter=args.doc_id)
            else:
                EXPORTERS[corpus](conn)
    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
