"""Load corpus JSONL files into the SQLite database.

Reads data/corpus/ and populates all content tables. The SQLite DB
can be fully absent (created from scratch) or already exist (content
tables are replaced, graph tables are left intact).

Workflow after a fresh git clone:
    make db-pull                        # fast: download pre-built DB from S3
    # — OR —
    make corpus-load                    # slow: rebuild content from JSONL

Tables managed (content layer):
    paragraphs, document_sections, documents,
    summa_articles, summa_parts, summa_questions,
    patristic_sections, patristic_chapters,
    encyclopedia, encyclopedia_cross_refs,
    bible_verses, bible_books,
    library_docs

Tables NOT managed (derived — rebuilt by graph pipeline):
    graph_nodes, graph_edges,
    paragraph_cross_refs, paragraph_themes, paragraph_entities,
    paragraph_topics, paragraph_bible_citations,
    paragraph_document_citations, paragraph_author_citations,
    authors, author_works, entities, themes, topics,
    *_fts (rebuilt here after load)

Usage:
    python pipeline/scripts/load_corpus_to_db.py
    python pipeline/scripts/load_corpus_to_db.py --corpus ccc
    python pipeline/scripts/load_corpus_to_db.py --corpus documents --doc-id denzinger-hunermann
    python pipeline/scripts/load_corpus_to_db.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
CORPUS_DIR = PROJECT_ROOT / "data" / "corpus"

ALL_CORPORA = ["ccc", "documents", "summa", "patristic", "encyclopedia", "bible", "library"]


# ── Schema ────────────────────────────────────────────────────────────────────
# Only content tables. Graph tables are created by export_sqlite.py /
# inject scripts and are not managed here.

CONTENT_SCHEMA = """\
CREATE TABLE IF NOT EXISTS paragraphs (
    id             INTEGER PRIMARY KEY,
    text_en        TEXT,
    text_la        TEXT,
    text_pt        TEXT,
    part           TEXT,
    section        TEXT,
    chapter        TEXT,
    article        TEXT,
    themes_json    TEXT,
    footnotes_json TEXT
);

CREATE TABLE IF NOT EXISTS documents (
    id                     TEXT PRIMARY KEY,
    name                   TEXT,
    abbreviation           TEXT,
    category               TEXT,
    source_url             TEXT,
    fetchable              INTEGER,
    citing_paragraphs_json TEXT,
    section_count          INTEGER,
    available_langs_json   TEXT
);
CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category);

CREATE TABLE IF NOT EXISTS document_sections (
    document_id TEXT NOT NULL,
    section_num TEXT NOT NULL,
    text_en     TEXT,
    text_la     TEXT,
    text_pt     TEXT,
    meta_json   TEXT,
    PRIMARY KEY (document_id, section_num)
);
CREATE INDEX IF NOT EXISTS idx_doc_sections_doc ON document_sections(document_id);

CREATE TABLE IF NOT EXISTS summa_parts (
    num  INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS summa_questions (
    id           TEXT PRIMARY KEY,
    part_num     INTEGER NOT NULL,
    question_num INTEGER NOT NULL,
    title        TEXT,
    summary      TEXT
);
CREATE TABLE IF NOT EXISTS summa_articles (
    id          TEXT PRIMARY KEY,
    question_id TEXT NOT NULL,
    article_num INTEGER NOT NULL,
    title       TEXT,
    text        TEXT
);
CREATE INDEX IF NOT EXISTS idx_sa_q ON summa_articles(question_id);

CREATE TABLE IF NOT EXISTS patristic_chapters (
    id      TEXT PRIMARY KEY,
    work_id TEXT NOT NULL,
    number  INTEGER,
    title   TEXT
);
CREATE INDEX IF NOT EXISTS idx_patristic_chapters_work ON patristic_chapters(work_id);

CREATE TABLE IF NOT EXISTS patristic_sections (
    id         TEXT PRIMARY KEY,
    chapter_id TEXT NOT NULL,
    number     INTEGER,
    text_en    TEXT,
    text_la    TEXT,
    text_el    TEXT
);
CREATE INDEX IF NOT EXISTS idx_patristic_sections_chapter ON patristic_sections(chapter_id);

CREATE TABLE IF NOT EXISTS encyclopedia (
    id      TEXT PRIMARY KEY,
    title   TEXT NOT NULL,
    summary TEXT,
    text_en TEXT,
    url     TEXT
);
CREATE TABLE IF NOT EXISTS encyclopedia_cross_refs (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (source_id, target_id)
);
CREATE INDEX IF NOT EXISTS idx_ecr_target ON encyclopedia_cross_refs(target_id);

CREATE TABLE IF NOT EXISTS bible_books (
    id                     TEXT PRIMARY KEY,
    name                   TEXT,
    abbreviation           TEXT,
    testament              TEXT,
    category               TEXT,
    total_verses           INTEGER,
    total_chapters         INTEGER,
    citing_paragraphs_json TEXT
);
CREATE TABLE IF NOT EXISTS bible_verses (
    book_id TEXT    NOT NULL,
    chapter INTEGER NOT NULL,
    verse   INTEGER NOT NULL,
    text_en TEXT,
    text_la TEXT,
    text_pt TEXT,
    text_el TEXT,
    PRIMARY KEY (book_id, chapter, verse)
);

CREATE TABLE IF NOT EXISTS library_docs (
    id       TEXT PRIMARY KEY,
    category TEXT,
    title    TEXT,
    year     INTEGER,
    text     TEXT
);
CREATE INDEX IF NOT EXISTS idx_libdoc_year ON library_docs(year);

CREATE VIRTUAL TABLE IF NOT EXISTS encyclopedia_fts USING fts5(
    id, title, summary, text_en,
    content=encyclopedia, content_rowid=rowid
);
CREATE VIRTUAL TABLE IF NOT EXISTS bible_verses_fts USING fts5(
    book_id, chapter, verse, text_en, text_la, text_pt, text_el
);
CREATE VIRTUAL TABLE IF NOT EXISTS patristic_sections_fts USING fts5(
    section_id, work_id, author_id, text_en, text_la, text_el
);
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _serialize_json_fields(record: dict) -> dict:
    """Re-serialize any dict/list values back to JSON strings for SQLite."""
    out = {}
    for k, v in record.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def _upsert(cur: sqlite3.Cursor, table: str, records: list[dict]) -> int:
    if not records:
        return 0
    cols = list(records[0].keys())
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
    rows = [[r.get(c) for c in cols] for r in records]
    cur.executemany(sql, rows)
    return len(rows)


# ── loaders ───────────────────────────────────────────────────────────────────

def load_ccc(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    t0 = time.time()
    path = CORPUS_DIR / "ccc" / "paragraphs.jsonl"
    if not path.exists():
        print("  ccc: no file found, skipping")
        return
    records = [_serialize_json_fields(r) for r in _read_jsonl(path)]
    if dry_run:
        print(f"  ccc: would load {len(records)} paragraphs")
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM paragraphs")
    n = _upsert(cur, "paragraphs", records)
    conn.commit()
    print(f"  ccc: {n} paragraphs loaded ({time.time()-t0:.1f}s)")


def load_documents(conn: sqlite3.Connection, doc_filter: str | None = None,
                   dry_run: bool = False) -> None:
    t0 = time.time()
    out_dir = CORPUS_DIR / "documents"

    # Catalog (document metadata)
    catalog_path = out_dir / "_catalog.json"
    if catalog_path.exists() and not doc_filter:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        if not dry_run:
            cur = conn.cursor()
            cur.execute("DELETE FROM documents")
            _upsert(cur, "documents", catalog)
            conn.commit()
        print(f"  documents/_catalog: {len(catalog)} document rows")

    # Sections
    doc_dirs = sorted(out_dir.iterdir()) if not doc_filter else [out_dir / doc_filter]
    total = 0
    loaded_docs = 0
    for doc_dir in doc_dirs:
        if not doc_dir.is_dir() or doc_dir.name.startswith("_"):
            continue
        sections_path = doc_dir / "sections.jsonl"
        if not sections_path.exists():
            continue
        records = [_serialize_json_fields(r) for r in _read_jsonl(sections_path)]
        if not records:
            continue
        total += len(records)
        loaded_docs += 1
        if not dry_run:
            doc_id = doc_dir.name
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM document_sections WHERE document_id = ?", (doc_id,)
            )
            _upsert(cur, "document_sections", records)
    if not dry_run:
        conn.commit()
    verb = "would load" if dry_run else "loaded"
    print(f"  documents: {verb} {total} sections across {loaded_docs} docs ({time.time()-t0:.1f}s)")


def load_summa(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    t0 = time.time()
    path = CORPUS_DIR / "summa" / "articles.jsonl"
    if not path.exists():
        print("  summa: no file found, skipping")
        return
    records = _read_jsonl(path)
    if dry_run:
        print(f"  summa: would load {len(records)} articles")
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM summa_articles")
    n = _upsert(cur, "summa_articles", records)
    conn.commit()
    print(f"  summa: {n} articles loaded ({time.time()-t0:.1f}s)")


def load_patristic(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    t0 = time.time()
    patristic_dir = CORPUS_DIR / "patristic"
    if not patristic_dir.exists():
        print("  patristic: no directory found, skipping")
        return

    all_records: list[dict] = []
    for author_dir in sorted(patristic_dir.iterdir()):
        if not author_dir.is_dir():
            continue
        for work_dir in sorted(author_dir.iterdir()):
            if not work_dir.is_dir():
                continue
            sections_path = work_dir / "sections.jsonl"
            if sections_path.exists():
                all_records.extend(_read_jsonl(sections_path))

    if dry_run:
        print(f"  patristic: would load {len(all_records)} sections")
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM patristic_sections")
    n = _upsert(cur, "patristic_sections", all_records)
    conn.commit()
    print(f"  patristic: {n} sections loaded ({time.time()-t0:.1f}s)")


def load_encyclopedia(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    t0 = time.time()
    ency_dir = CORPUS_DIR / "encyclopedia"
    if not ency_dir.exists():
        print("  encyclopedia: no directory found, skipping")
        return

    all_records: list[dict] = []
    for letter_dir in sorted(ency_dir.iterdir()):
        if not letter_dir.is_dir():
            continue
        articles_path = letter_dir / "articles.jsonl"
        if articles_path.exists():
            all_records.extend(_read_jsonl(articles_path))

    if dry_run:
        print(f"  encyclopedia: would load {len(all_records)} articles")
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM encyclopedia")
    n = _upsert(cur, "encyclopedia", all_records)
    conn.commit()
    # Rebuild FTS (content table — must use 'rebuild' command)
    cur.execute("INSERT INTO encyclopedia_fts(encyclopedia_fts) VALUES('rebuild')")
    conn.commit()
    print(f"  encyclopedia: {n} articles loaded + FTS rebuilt ({time.time()-t0:.1f}s)")


def load_bible(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    t0 = time.time()
    bible_dir = CORPUS_DIR / "bible"
    if not bible_dir.exists():
        print("  bible: no directory found, skipping")
        return

    # Book catalog
    catalog_path = bible_dir / "_catalog.json"
    if catalog_path.exists():
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        if not dry_run:
            cur = conn.cursor()
            cur.execute("DELETE FROM bible_books")
            _upsert(cur, "bible_books", catalog)
            conn.commit()
        print(f"  bible/_catalog: {len(catalog)} books")

    # Verses
    all_verses: list[dict] = []
    for book_dir in sorted(bible_dir.iterdir()):
        if not book_dir.is_dir():
            continue
        verses_path = book_dir / "verses.jsonl"
        if verses_path.exists():
            all_verses.extend(_read_jsonl(verses_path))

    if dry_run:
        print(f"  bible: would load {len(all_verses)} verses")
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM bible_verses")
    n = _upsert(cur, "bible_verses", all_verses)
    # Rebuild bible FTS
    cur.execute("DELETE FROM bible_verses_fts")
    fts_rows = [
        (v.get("book_id"), v.get("chapter"), v.get("verse"),
         v.get("text_en"), v.get("text_la"), v.get("text_pt"), v.get("text_el"))
        for v in all_verses
    ]
    cur.executemany(
        "INSERT INTO bible_verses_fts(book_id,chapter,verse,text_en,text_la,text_pt,text_el)"
        " VALUES (?,?,?,?,?,?,?)",
        fts_rows,
    )
    conn.commit()
    print(f"  bible: {n} verses loaded + FTS rebuilt ({time.time()-t0:.1f}s)")


def load_library(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    t0 = time.time()
    path = CORPUS_DIR / "library" / "docs.jsonl"
    if not path.exists():
        print("  library: no file found, skipping")
        return
    records = _read_jsonl(path)
    if dry_run:
        print(f"  library: would load {len(records)} docs")
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM library_docs")
    n = _upsert(cur, "library_docs", records)
    conn.commit()
    print(f"  library: {n} docs loaded ({time.time()-t0:.1f}s)")


def rebuild_patristic_fts(conn: sqlite3.Connection) -> None:
    """Rebuild patristic FTS from current patristic_sections + patristic_chapters."""
    cur = conn.cursor()
    cur.execute("DELETE FROM patristic_sections_fts")
    rows = conn.execute(
        "SELECT ps.id, pc.work_id,"
        "       SUBSTR(pc.work_id, 1, INSTR(pc.work_id, '/') - 1) as author_id,"
        "       ps.text_en, ps.text_la, ps.text_el "
        "FROM patristic_sections ps "
        "JOIN patristic_chapters pc ON ps.chapter_id = pc.id"
    ).fetchall()
    cur.executemany(
        "INSERT INTO patristic_sections_fts"
        "(section_id,work_id,author_id,text_en,text_la,text_el)"
        " VALUES (?,?,?,?,?,?)",
        [tuple(r) for r in rows],
    )
    conn.commit()
    print(f"  patristic FTS rebuilt ({len(rows)} rows)")


# ── main ──────────────────────────────────────────────────────────────────────

LOADERS = {
    "ccc":          load_ccc,
    "documents":    load_documents,
    "summa":        load_summa,
    "patristic":    load_patristic,
    "encyclopedia": load_encyclopedia,
    "bible":        load_bible,
    "library":      load_library,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", choices=ALL_CORPORA,
                        help="Load only this corpus (default: all)")
    parser.add_argument("--doc-id",
                        help="When --corpus=documents, load only this document_id")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be loaded without writing")
    args = parser.parse_args()

    if not CORPUS_DIR.exists():
        raise SystemExit(f"Corpus dir not found: {CORPUS_DIR}  (run export_corpus_jsonl.py first)")

    # Create DB if absent; apply content schema
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(CONTENT_SCHEMA)
    conn.commit()

    corpora = [args.corpus] if args.corpus else ALL_CORPORA
    print(f"Loading {len(corpora)} corpus/corpora → {DB_PATH}")
    if args.dry_run:
        print("  [dry-run] no changes will be written")

    try:
        for corpus in corpora:
            if corpus == "documents" and args.doc_id:
                load_documents(conn, doc_filter=args.doc_id, dry_run=args.dry_run)
            else:
                LOADERS[corpus](conn, dry_run=args.dry_run)

        # Rebuild patristic FTS last (needs patristic_chapters which may be absent
        # if patristic was the only corpus loaded without prior chapter data)
        if not args.dry_run and ("patristic" in corpora):
            try:
                rebuild_patristic_fts(conn)
            except Exception as e:
                print(f"  patristic FTS skipped (patristic_chapters not populated yet): {e}")
    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
