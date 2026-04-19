"""Inject contains_lemma edges + text_lemma_forms rows into knowledge-graph.db.

Lemmatizes Latin/Greek text from the existing source tables (bible_verses,
patristic_sections) using the Stanza wrapper in pipeline.src.lemmatizer, then
materializes:

  - text_lemma_forms  — one row per content token: (source, lang, position,
                        form, lemma, lemma_id, pos, feats)
  - graph_edges       — deduped (source_node → lemma_node, 'contains_lemma')
  - lemma_parse_cache — (text_hash, lang) → tokens_json, so re-runs are free

Idempotent: rows already present in text_lemma_forms for a given
(source_type, source_id, lang) are skipped unless --force is passed.

Usage:
    # Smoke test on one Bible book
    uv run --project pipeline --extra lemma python -m pipeline.scripts.inject_lemma_edges \\
        --source bible --book matthew --lang la

    # Full run
    uv run --project pipeline --extra lemma python -m pipeline.scripts.inject_lemma_edges \\
        --source all
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from dataclasses import asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.lemmatizer import Lemmatizer, Token, chunk_text, hash_text  # noqa: E402

SCHEMA = """\
CREATE TABLE IF NOT EXISTS lemma_parse_cache (
    text_hash   TEXT PRIMARY KEY,
    lang        TEXT NOT NULL,
    tokens_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS text_lemma_forms (
    source_type TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    lang        TEXT NOT NULL,
    position    INTEGER NOT NULL,
    form        TEXT NOT NULL,
    lemma       TEXT NOT NULL,
    lemma_id    TEXT,
    pos         TEXT,
    feats       TEXT,
    PRIMARY KEY (source_type, source_id, lang, position)
);
CREATE INDEX IF NOT EXISTS idx_tlf_lemma_id          ON text_lemma_forms(lemma_id);
CREATE INDEX IF NOT EXISTS idx_tlf_source            ON text_lemma_forms(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_tlf_source_id         ON text_lemma_forms(source_id);
CREATE INDEX IF NOT EXISTS idx_tlf_lang_lemma_id     ON text_lemma_forms(lang, lemma_id);
-- Per-book queries (hapax / vocab with corpus filter): source_id LIKE 'book-%'
-- becomes a covering range scan that filters lang and lemma_id from the index.
CREATE INDEX IF NOT EXISTS idx_tlf_source_lang_lemma ON text_lemma_forms(source_id, lang, lemma_id);
"""


# ---------------------------------------------------------------------------
# Source iterators — yield (source_type, source_id, text)
# ---------------------------------------------------------------------------

def iter_bible(conn: sqlite3.Connection, lang: str, book: str | None, limit: int | None):
    col = f"text_{lang}"
    sql = f"SELECT book_id, chapter, verse, {col} AS text FROM bible_verses WHERE {col} IS NOT NULL AND {col} != ''"
    args: list = []
    if book:
        sql += " AND book_id = ?"
        args.append(book)
    sql += " ORDER BY book_id, chapter, verse"
    if limit:
        sql += f" LIMIT {int(limit)}"
    for row in conn.execute(sql, args):
        # Match existing graph node id format: bible-verse:<book>-<chapter>:<verse>
        source_id = f"{row['book_id']}-{row['chapter']}:{row['verse']}"
        yield "bible-verse", source_id, row["text"]


def iter_patristic(conn: sqlite3.Connection, lang: str, author: str | None, limit: int | None):
    col = f"text_{lang}"
    sql = f"SELECT id, {col} AS text FROM patristic_sections WHERE {col} IS NOT NULL AND {col} != ''"
    args: list = []
    if author:
        sql += " AND id LIKE ?"
        args.append(f"{author}/%")
    sql += " ORDER BY id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    for row in conn.execute(sql, args):
        yield "patristic-section", row["id"], row["text"]


# ---------------------------------------------------------------------------
# Caching layer
# ---------------------------------------------------------------------------

def cached_parse(
    lemmer: Lemmatizer,
    text: str,
    conn: sqlite3.Connection,
) -> list[Token]:
    h = hash_text(lemmer.lang, text)
    row = conn.execute(
        "SELECT tokens_json FROM lemma_parse_cache WHERE text_hash = ?", (h,)
    ).fetchone()
    if row is not None:
        raw = json.loads(row[0])
        return [Token(**t) for t in raw]
    tokens = lemmer.parse(text)
    conn.execute(
        "INSERT OR REPLACE INTO lemma_parse_cache VALUES (?, ?, ?)",
        (h, lemmer.lang, json.dumps([asdict(t) for t in tokens], ensure_ascii=False)),
    )
    return tokens


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def process(
    lemmer: Lemmatizer,
    conn: sqlite3.Connection,
    iterator,
    *,
    force: bool,
    commit_every: int = 200,
) -> dict:
    """Lemmatize each (source_type, source_id, text) and materialize tables/edges."""
    seen_existing = {
        (st, sid)
        for (st, sid) in conn.execute(
            "SELECT DISTINCT source_type, source_id FROM text_lemma_forms WHERE lang = ?",
            (lemmer.lang,),
        )
    }
    print(f"  [{lemmer.lang}] {len(seen_existing)} source rows already lemmatized")

    n_processed = 0
    n_skipped = 0
    n_form_rows = 0
    n_edges_added = 0
    cache_hits = 0
    parse_t = 0.0

    pending_forms: list[tuple] = []
    pending_edges: set[tuple[str, str, str]] = set()

    for source_type, source_id, text in iterator:
        if not force and (source_type, source_id) in seen_existing:
            n_skipped += 1
            continue

        # Chunked, cached parse — long sections (Summa, Tertullian) get split on
        # sentence boundaries so Stanza never sees a multi-MB document at once.
        # Cache key is per-chunk, so re-runs hit the cache even after re-chunking.
        chunks = chunk_text(text)
        tokens: list[Token] = []
        for chunk in chunks:
            h = hash_text(lemmer.lang, chunk)
            cached = conn.execute(
                "SELECT tokens_json FROM lemma_parse_cache WHERE text_hash = ?", (h,)
            ).fetchone()
            if cached:
                chunk_tokens = [Token(**t) for t in json.loads(cached[0])]
                cache_hits += 1
            else:
                t0 = time.time()
                chunk_tokens = lemmer.parse(chunk)
                parse_t += time.time() - t0
                conn.execute(
                    "INSERT OR REPLACE INTO lemma_parse_cache VALUES (?, ?, ?)",
                    (h, lemmer.lang, json.dumps([asdict(t) for t in chunk_tokens], ensure_ascii=False)),
                )
            tokens.extend(chunk_tokens)

        for pos_idx, t in enumerate(tokens):
            lemma_id, _alts = lemmer.resolve_token(t)
            pending_forms.append((
                source_type, source_id, lemmer.lang, pos_idx,
                t.form, t.lemma, lemma_id, t.pos, t.feats,
            ))
            if lemma_id:
                src_node = f"{source_type}:{source_id}"
                tgt_node = f"lemma-{lemmer.lang}:{lemma_id}"
                pending_edges.add((src_node, tgt_node, "contains_lemma"))

        n_processed += 1

        if n_processed % commit_every == 0:
            n_form_rows += _flush_forms(conn, pending_forms)
            n_edges_added += _flush_edges(conn, pending_edges)
            pending_forms.clear()
            pending_edges.clear()
            conn.commit()

    n_form_rows += _flush_forms(conn, pending_forms)
    n_edges_added += _flush_edges(conn, pending_edges)
    conn.commit()

    print(
        f"  [{lemmer.lang}] processed={n_processed} skipped={n_skipped} "
        f"cache_hits={cache_hits} forms+={n_form_rows} edges+={n_edges_added} "
        f"parse={parse_t:.1f}s"
    )
    return {
        "processed": n_processed,
        "skipped": n_skipped,
        "cache_hits": cache_hits,
        "forms": n_form_rows,
        "edges": n_edges_added,
        "parse_seconds": round(parse_t, 1),
    }


def _flush_forms(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    cur = conn.executemany(
        "INSERT OR REPLACE INTO text_lemma_forms VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    return cur.rowcount


def _flush_edges(conn: sqlite3.Connection, edges: set[tuple[str, str, str]]) -> int:
    if not edges:
        return 0
    # graph_edges has no UNIQUE constraint — pre-filter against existing rows
    # for these source nodes to avoid duplicates accumulating across runs.
    src_nodes = {e[0] for e in edges}
    placeholders = ",".join("?" for _ in src_nodes)
    existing = {
        (r[0], r[1])
        for r in conn.execute(
            f"SELECT source, target FROM graph_edges "
            f"WHERE edge_type = 'contains_lemma' AND source IN ({placeholders})",
            list(src_nodes),
        )
    }
    new_rows = [e for e in edges if (e[0], e[1]) not in existing]
    if not new_rows:
        return 0
    conn.executemany("INSERT INTO graph_edges VALUES (?,?,?)", new_rows)
    return len(new_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["bible", "patristic", "all"], default="bible")
    parser.add_argument("--lang", choices=["la", "el", "both"], default="both")
    parser.add_argument("--book", help="bible book filter (e.g. matthew)")
    parser.add_argument("--author", help="patristic author filter (e.g. thomas-aquinas)")
    parser.add_argument("--limit", type=int, help="limit number of source rows")
    parser.add_argument("--force", action="store_true", help="reprocess even if already lemmatized")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)

    langs = ("la", "el") if args.lang == "both" else (args.lang,)
    sources = (
        ("bible",) if args.source == "bible"
        else ("patristic",) if args.source == "patristic"
        else ("bible", "patristic")
    )

    overall_t0 = time.time()
    for lang in langs:
        print(f"\n=== Lemmatizing lang={lang} sources={sources} ===")
        lemmer = Lemmatizer(lang, conn)
        for src in sources:
            if src == "bible":
                it = iter_bible(conn, lang, book=args.book, limit=args.limit)
                print(f"--- bible / {lang} ---")
            else:
                it = iter_patristic(conn, lang, author=args.author, limit=args.limit)
                tag = f" / {args.author}" if args.author else ""
                print(f"--- patristic{tag} / {lang} ---")
            process(lemmer, conn, it, force=args.force)

    # Final stats
    n_forms = conn.execute("SELECT COUNT(*) FROM text_lemma_forms").fetchone()[0]
    n_edges = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type = 'contains_lemma'"
    ).fetchone()[0]
    n_cache = conn.execute("SELECT COUNT(*) FROM lemma_parse_cache").fetchone()[0]
    conn.close()

    print(
        f"\nDone in {time.time() - overall_t0:.1f}s. "
        f"text_lemma_forms={n_forms} contains_lemma_edges={n_edges} cache={n_cache}"
    )


if __name__ == "__main__":
    main()
