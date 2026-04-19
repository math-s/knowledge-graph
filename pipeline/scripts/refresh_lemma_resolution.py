"""Re-materialize text_lemma_forms + contains_lemma edges from the parse cache.

Use this after changing LemmaIndex resolution logic in pipeline.src.lemmatizer
(e.g. fixing a homograph priority bug). The parse cache stores Stanza's raw
output keyed by sha1(lang + chunk_text); lemma_id is resolved at materialization
time. So clearing text_lemma_forms and re-running with the cache lets us apply
the new resolution in minutes (no Stanza calls) instead of hours.

Always idempotent. Safe to run repeatedly.

Usage:
    uv run --project pipeline --extra lemma python -m pipeline.scripts.refresh_lemma_resolution
"""

from __future__ import annotations

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

LANGS = ("la", "el")


def _treebank_for(source_type: str, source_id: str, lang: str) -> str | None:
    """Pick which treebank's cached parses to use for this source.

    Summa Latin was lemmatized with `ittb` (Aquinas-native) because proiel
    mangles scholastic vocabulary. Everything else uses the default.
    """
    if (
        source_type == "patristic-section"
        and lang == "la"
        and source_id.startswith("thomas-aquinas/")
    ):
        return "ittb"
    return None


def _resolve_for_text(
    lemmer: Lemmatizer,
    conn: sqlite3.Connection,
    text: str,
    treebank: str | None = None,
) -> list[tuple[Token, str | None]]:
    """Re-resolve a (cached) parse against the current LemmaIndex.
    Returns list of (token, lemma_id_or_None)."""
    chunks = chunk_text(text)
    out: list[tuple[Token, str | None]] = []
    for chunk in chunks:
        h = hash_text(lemmer.lang, chunk, treebank=treebank)
        row = conn.execute(
            "SELECT tokens_json FROM lemma_parse_cache WHERE text_hash = ?", (h,)
        ).fetchone()
        if row is None:
            # Cache miss — would require Stanza. We refuse here to keep the
            # script Stanza-free; user should run inject_lemma_edges instead.
            raise RuntimeError(
                f"Cache miss for {lemmer.lang} chunk (sha1={h}, treebank={treebank or 'default'}). "
                f"Run inject_lemma_edges (or relemmatize_summa_ittb) first."
            )
        for raw in json.loads(row[0]):
            tok = Token(**raw)
            lemma_id, _alts = lemmer.resolve_token(tok)
            out.append((tok, lemma_id))
    return out


def _iter_sources(conn: sqlite3.Connection, lang: str):
    """Yield every source row that should be re-materialized for this lang."""
    col = f"text_{lang}"
    for r in conn.execute(
        f"SELECT book_id, chapter, verse, {col} AS text FROM bible_verses "
        f"WHERE {col} IS NOT NULL AND {col} != '' ORDER BY book_id, chapter, verse"
    ):
        yield "bible-verse", f"{r['book_id']}-{r['chapter']}:{r['verse']}", r["text"]
    for r in conn.execute(
        f"SELECT id, {col} AS text FROM patristic_sections "
        f"WHERE {col} IS NOT NULL AND {col} != '' ORDER BY id"
    ):
        yield "patristic-section", r["id"], r["text"]


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    cache_count = conn.execute("SELECT COUNT(*) FROM lemma_parse_cache").fetchone()[0]
    forms_before = conn.execute("SELECT COUNT(*) FROM text_lemma_forms").fetchone()[0]
    edges_before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='contains_lemma'"
    ).fetchone()[0]
    print(
        f"Cache has {cache_count} entries. "
        f"text_lemma_forms={forms_before}, contains_lemma_edges={edges_before}"
    )

    print("\nClearing text_lemma_forms + contains_lemma edges...")
    t0 = time.time()
    conn.execute("DELETE FROM text_lemma_forms")
    conn.execute("DELETE FROM graph_edges WHERE edge_type = 'contains_lemma'")
    conn.commit()
    print(f"  cleared in {time.time() - t0:.1f}s")

    overall_t0 = time.time()
    for lang in LANGS:
        print(f"\n=== Re-resolving lang={lang} ===")
        lemmer = Lemmatizer(lang, conn)

        # Quick spot-check: surface a couple of homograph fixes for sanity
        for canary in ("rex", "petrus", "iesus") if lang == "la" else ("xristo/s",):
            resolved, _ = lemmer.index.resolve(canary)
            print(f"  index.resolve({canary!r}) → {resolved!r}")

        n_sources = 0
        n_forms = 0
        pending_forms: list[tuple] = []
        pending_edges: set[tuple[str, str, str]] = set()
        t_lang = time.time()

        for source_type, source_id, text in _iter_sources(conn, lang):
            tb = _treebank_for(source_type, source_id, lang)
            try:
                resolved = _resolve_for_text(lemmer, conn, text, treebank=tb)
            except RuntimeError as e:
                print(f"  ! {source_type}:{source_id} — {e}")
                continue
            for pos_idx, (tok, lemma_id) in enumerate(resolved):
                pending_forms.append((
                    source_type, source_id, lemmer.lang, pos_idx,
                    tok.form, tok.lemma, lemma_id, tok.pos, tok.feats,
                ))
                if lemma_id:
                    pending_edges.add((
                        f"{source_type}:{source_id}",
                        f"lemma-{lemmer.lang}:{lemma_id}",
                        "contains_lemma",
                    ))
            n_sources += 1
            n_forms += len(resolved)
            if n_sources % 5000 == 0:
                conn.executemany(
                    "INSERT INTO text_lemma_forms VALUES (?,?,?,?,?,?,?,?,?)",
                    pending_forms,
                )
                pending_forms.clear()
                conn.commit()
                print(f"  ...{n_sources} sources, {n_forms} forms ({time.time() - t_lang:.0f}s)")

        # Flush remaining forms
        if pending_forms:
            conn.executemany(
                "INSERT INTO text_lemma_forms VALUES (?,?,?,?,?,?,?,?,?)",
                pending_forms,
            )
        # Bulk insert edges (deduped via the set)
        conn.executemany(
            "INSERT INTO graph_edges VALUES (?,?,?)",
            list(pending_edges),
        )
        conn.commit()
        print(
            f"  done: {n_sources} sources, {n_forms} forms, "
            f"{len(pending_edges)} edges in {time.time() - t_lang:.0f}s"
        )

    # Pre-aggregate per-lemma corpus frequencies so /search/lemma and similar
    # endpoints can rank without scanning 8M+ text_lemma_forms rows on every
    # query. Rebuilt fresh each run.
    print("\nBuilding lemma_corpus_freq table...")
    t0 = time.time()
    conn.executescript(
        """
        DROP TABLE IF EXISTS lemma_corpus_freq;
        CREATE TABLE lemma_corpus_freq (
            lang     TEXT NOT NULL,
            lemma_id TEXT NOT NULL,
            tokens   INTEGER NOT NULL,
            sources  INTEGER NOT NULL,
            PRIMARY KEY (lang, lemma_id)
        );
        INSERT INTO lemma_corpus_freq (lang, lemma_id, tokens, sources)
        SELECT lang, lemma_id, COUNT(*), COUNT(DISTINCT source_id)
        FROM text_lemma_forms
        WHERE lemma_id IS NOT NULL
        GROUP BY lang, lemma_id;
        CREATE INDEX idx_lcf_tokens ON lemma_corpus_freq(lang, tokens DESC);
        """
    )
    conn.commit()
    n_freq = conn.execute("SELECT COUNT(*) FROM lemma_corpus_freq").fetchone()[0]
    print(f"  built {n_freq} rows in {time.time() - t0:.1f}s")

    # Bible-scoped, POS-aware aggregation for the /lexicon/{lang}/vocab
    # endpoint when corpus='all'. Skips the 8M-row scan entirely.
    print("Building bible_lemma_pos_freq table...")
    t0 = time.time()
    conn.executescript(
        """
        DROP TABLE IF EXISTS bible_lemma_pos_freq;
        CREATE TABLE bible_lemma_pos_freq (
            lang     TEXT NOT NULL,
            lemma_id TEXT NOT NULL,
            pos      TEXT,
            tokens   INTEGER NOT NULL,
            sources  INTEGER NOT NULL
        );
        INSERT INTO bible_lemma_pos_freq (lang, lemma_id, pos, tokens, sources)
        SELECT lang, lemma_id, pos, COUNT(*), COUNT(DISTINCT source_id)
        FROM text_lemma_forms
        WHERE source_type='bible-verse' AND lemma_id IS NOT NULL
        GROUP BY lang, lemma_id, pos;
        CREATE INDEX idx_blpf_lookup ON bible_lemma_pos_freq(lang, pos, tokens DESC);
        """
    )
    conn.commit()
    n_blpf = conn.execute("SELECT COUNT(*) FROM bible_lemma_pos_freq").fetchone()[0]
    print(f"  built {n_blpf} rows in {time.time() - t0:.1f}s")

    # Per-book breakdown for vocab/hapax with corpus filter (gospels, pauline,
    # nt, ot, single-book). Without this, OR-ing N LIKE 'book-%' patterns
    # against text_lemma_forms takes 6-10s for NT/OT slices.
    print("Building bible_lemma_per_book table...")
    t0 = time.time()
    conn.executescript(
        """
        DROP TABLE IF EXISTS bible_lemma_per_book;
        CREATE TABLE bible_lemma_per_book (
            book_id  TEXT NOT NULL,
            lang     TEXT NOT NULL,
            lemma_id TEXT NOT NULL,
            pos      TEXT,
            tokens   INTEGER NOT NULL,
            sources  INTEGER NOT NULL
        );
        """
    )
    # Build in Python — source_id parsing avoids SQL string gymnastics
    rows = conn.execute(
        """
        SELECT source_id, lang, lemma_id, pos, COUNT(*) AS tokens, COUNT(DISTINCT source_id) AS sources
        FROM text_lemma_forms
        WHERE source_type='bible-verse' AND lemma_id IS NOT NULL
        GROUP BY source_id, lang, lemma_id, pos
        """
    ).fetchall()
    by_book: dict[tuple, list[int]] = {}
    for r in rows:
        sid = r[0]
        try:
            left, _ = sid.rsplit(":", 1)
            book_id, _ = left.rsplit("-", 1)
        except ValueError:
            continue
        key = (book_id, r[1], r[2], r[3])  # (book_id, lang, lemma_id, pos)
        if key in by_book:
            by_book[key][0] += r[4]
            by_book[key][1] += r[5]
        else:
            by_book[key] = [r[4], r[5]]
    conn.executemany(
        "INSERT INTO bible_lemma_per_book VALUES (?,?,?,?,?,?)",
        [(b, l, lm, p, tk, sr) for (b, l, lm, p), (tk, sr) in by_book.items()],
    )
    conn.executescript(
        """
        CREATE INDEX idx_blpb_book_lang_pos ON bible_lemma_per_book(book_id, lang, pos);
        CREATE INDEX idx_blpb_book_lang_lemma ON bible_lemma_per_book(book_id, lang, lemma_id);
        """
    )
    conn.commit()
    n_blpb = conn.execute("SELECT COUNT(*) FROM bible_lemma_per_book").fetchone()[0]
    print(f"  built {n_blpb} rows in {time.time() - t0:.1f}s")

    # Refresh planner stats so the query planner uses the indexes correctly
    # after a large re-write. Cheap, but only useful if indexes exist.
    print("Running ANALYZE...")
    t0 = time.time()
    conn.execute("ANALYZE text_lemma_forms")
    conn.execute("ANALYZE lemma_corpus_freq")
    conn.execute("ANALYZE bible_lemma_pos_freq")
    conn.commit()
    print(f"  done in {time.time() - t0:.1f}s")

    forms_after = conn.execute("SELECT COUNT(*) FROM text_lemma_forms").fetchone()[0]
    edges_after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='contains_lemma'"
    ).fetchone()[0]

    # Verify the canary fix actually landed
    rex_count = conn.execute(
        "SELECT COUNT(*) FROM text_lemma_forms WHERE lang='la' AND lemma_id='rex1'"
    ).fetchone()[0]
    Rex2_count = conn.execute(
        "SELECT COUNT(*) FROM text_lemma_forms WHERE lang='la' AND lemma_id='Rex2'"
    ).fetchone()[0]

    print(
        f"\nDone in {time.time() - overall_t0:.0f}s. "
        f"forms: {forms_before} → {forms_after}, edges: {edges_before} → {edges_after}"
    )
    print(f"Canary check: rex1 occurrences = {rex_count}, Rex2 = {Rex2_count}")
    if rex_count > 0 and Rex2_count == 0:
        print("  ✓ rex now resolves to rex1 (the word 'king'), not Rex2 (proper noun)")
    elif Rex2_count > 0:
        print("  ✗ Rex2 still appearing — check LemmaIndex sort order in lemmatizer.py")

    conn.close()


if __name__ == "__main__":
    main()
