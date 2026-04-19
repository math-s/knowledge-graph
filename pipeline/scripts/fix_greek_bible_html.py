"""Fix HTML/markup pollution in bible_verses.text_el for ~15 NT verses.

Symptom: a few Greek verses (1-Cor 8:9, Romans 1:10, etc.) have residual
markup like `no=s4452>` (broken Strong's number tags) and English Douay-Rheims
text bleeding in after a verse-marker span. Stanza dutifully lemmatizes the
garbage, producing nonsense lemmas (HTML attribute fragments mid-token).

This script:
  1. Cleans bible_verses.text_el for affected verses (cut at first `<`,
     strip broken attribute fragments).
  2. Invalidates the affected lemma_parse_cache rows (by text_hash).
  3. Deletes those verses' text_lemma_forms rows + contains_lemma edges.
  4. Re-lemmatizes them via the standard Lemmatizer.

Idempotent (no-op on a clean DB).

Usage:
    uv run --project pipeline --extra lemma python -m pipeline.scripts.fix_greek_bible_html
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
import time
from dataclasses import asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.lemmatizer import Lemmatizer, hash_text  # noqa: E402

# Strip broken attribute fragments like `no=s4452>`, `m0=r.dsm-personal`, etc.
_FRAGMENT_PATTERNS = [
    re.compile(r"\b[a-zA-Z]+\d*=[\w.\-/]+>"),  # no=s4452>, class=foo>
    re.compile(r"\b[a-zA-Z]\d=[\w.\-]+"),       # m0=r.dsm-personal (no >)
]


def clean_el(text: str) -> str:
    if not text:
        return text
    cut_at = text.find("<")
    if cut_at >= 0:
        text = text[:cut_at]
    for pat in _FRAGMENT_PATTERNS:
        text = pat.sub("", text)
    # Collapse whitespace introduced by stripping
    text = re.sub(r"\s+", " ", text).strip()
    return text


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    affected = conn.execute(
        """
        SELECT book_id, chapter, verse, text_el
        FROM bible_verses
        WHERE text_el IS NOT NULL AND text_el != ''
          AND (text_el GLOB '*<*' OR text_el GLOB '*=s[0-9]*' OR text_el GLOB '*m0=*')
        """
    ).fetchall()
    print(f"Found {len(affected)} affected verses")
    if not affected:
        print("Nothing to do.")
        return

    cleaned_pairs: list[tuple[str, str, str, str]] = []  # (book, chap, verse, new_text)
    for r in affected:
        new_text = clean_el(r["text_el"])
        if new_text == r["text_el"]:
            continue
        cleaned_pairs.append((r["book_id"], r["chapter"], r["verse"], new_text))

    print(f"Will rewrite {len(cleaned_pairs)} verses")

    # Compute source_ids and the OLD text_hashes (cache invalidation)
    source_ids = [
        f"{book}-{chap}:{verse}" for (book, chap, verse, _) in cleaned_pairs
    ]
    old_hashes = [
        hash_text("el", r["text_el"]) for r in affected
        if clean_el(r["text_el"]) != r["text_el"]
    ]

    # 1. Update bible_verses
    conn.executemany(
        "UPDATE bible_verses SET text_el = ? WHERE book_id = ? AND chapter = ? AND verse = ?",
        [(new, book, chap, verse) for (book, chap, verse, new) in cleaned_pairs],
    )
    conn.commit()
    print(f"  ✓ rewrote bible_verses.text_el for {len(cleaned_pairs)} rows")

    # 2. Invalidate cache
    cur = conn.executemany(
        "DELETE FROM lemma_parse_cache WHERE text_hash = ? AND lang = 'el'",
        [(h,) for h in old_hashes],
    )
    conn.commit()
    print(f"  ✓ cleared {cur.rowcount} stale parse-cache rows")

    # 3. Delete stale text_lemma_forms + contains_lemma edges
    placeholders = ",".join("?" for _ in source_ids)
    conn.execute(
        f"DELETE FROM text_lemma_forms "
        f"WHERE source_type='bible-verse' AND lang='el' AND source_id IN ({placeholders})",
        source_ids,
    )
    edge_sources = [f"bible-verse:{sid}" for sid in source_ids]
    edge_placeholders = ",".join("?" for _ in edge_sources)
    conn.execute(
        f"DELETE FROM graph_edges "
        f"WHERE edge_type='contains_lemma' AND source IN ({edge_placeholders})",
        edge_sources,
    )
    conn.commit()
    print(f"  ✓ cleared text_lemma_forms + contains_lemma edges for affected verses")

    # 4. Re-lemmatize. Stanza will run for each (no cache hits since we
    #    invalidated). Bible-verse-LA edges from these verses are unaffected.
    print("\nRe-lemmatizing...")
    t0 = time.time()
    lemmer = Lemmatizer("el", conn)
    new_form_rows: list[tuple] = []
    new_edges: set[tuple[str, str, str]] = set()
    for (book, chap, verse, new_text) in cleaned_pairs:
        sid = f"{book}-{chap}:{verse}"
        tokens = lemmer.parse(new_text)
        # Cache the fresh parse
        h = hash_text("el", new_text)
        conn.execute(
            "INSERT OR REPLACE INTO lemma_parse_cache VALUES (?, 'el', ?)",
            (h, json.dumps([asdict(t) for t in tokens], ensure_ascii=False)),
        )
        for pos_idx, t in enumerate(tokens):
            lemma_id, _ = lemmer.index.resolve(t.lemma)
            new_form_rows.append((
                "bible-verse", sid, "el", pos_idx,
                t.form, t.lemma, lemma_id, t.pos, t.feats,
            ))
            if lemma_id:
                new_edges.add((
                    f"bible-verse:{sid}",
                    f"lemma-el:{lemma_id}",
                    "contains_lemma",
                ))

    conn.executemany(
        "INSERT INTO text_lemma_forms VALUES (?,?,?,?,?,?,?,?,?)",
        new_form_rows,
    )
    conn.executemany(
        "INSERT INTO graph_edges VALUES (?,?,?)",
        list(new_edges),
    )
    conn.commit()

    # Refresh corpus-freq for el (tiny — affects a handful of lemmas)
    conn.execute("DELETE FROM lemma_corpus_freq WHERE lang = 'el'")
    conn.execute(
        """
        INSERT INTO lemma_corpus_freq (lang, lemma_id, tokens, sources)
        SELECT 'el', lemma_id, COUNT(*), COUNT(DISTINCT source_id)
        FROM text_lemma_forms
        WHERE lang = 'el' AND lemma_id IS NOT NULL
        GROUP BY lemma_id
        """
    )
    conn.commit()

    # Quick verification
    leftover = conn.execute(
        "SELECT COUNT(*) FROM text_lemma_forms "
        "WHERE lang='el' AND lemma LIKE '%class=%'"
    ).fetchone()[0]
    print(
        f"\nDone in {time.time() - t0:.1f}s. "
        f"Re-materialized {len(new_form_rows)} forms, {len(new_edges)} edges."
    )
    print(f"Garbage-lemma rows remaining (should be 0): {leftover}")

    conn.close()


if __name__ == "__main__":
    main()
