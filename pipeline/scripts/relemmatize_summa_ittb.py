"""Re-lemmatize Summa Latin sections using the `ittb` Stanza treebank.

The default proiel treebank (biblical/early-Christian Latin) handles Vulgate
well but mangles Aquinas's scholastic Latin: `inquantum`, `compr(eh)endo`,
`seipsus`, `est,um`, `calendar` (Roman-numeral confusion), `greek.expression`
(quoted-Greek marker). The `ittb` treebank is trained on the Index Thomisticus
and handles these correctly.

Cache namespace is treebank-aware (`hash_text(lang, text, treebank='ittb')`),
so old proiel-derived cache entries for Summa become inert; bible-verse cache
is unaffected.

Usage:
    uv run --project pipeline --extra lemma python -m pipeline.scripts.relemmatize_summa_ittb
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
from pipeline.src.lemmatizer import Lemmatizer, chunk_text, hash_text  # noqa: E402

AUTHOR_PREFIX = "thomas-aquinas/"
TREEBANK = "ittb"


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, text_la FROM patristic_sections "
        "WHERE id LIKE ? AND text_la IS NOT NULL AND text_la != '' "
        "ORDER BY id",
        (f"{AUTHOR_PREFIX}%",),
    ).fetchall()
    print(f"Found {len(rows)} Summa sections with Latin text")

    # Clear old (proiel-derived) Summa rows so re-materialization is clean.
    n_before_forms = conn.execute(
        "SELECT COUNT(*) FROM text_lemma_forms WHERE source_type='patristic-section' "
        "AND lang='la' AND source_id LIKE ?",
        (f"{AUTHOR_PREFIX}%",),
    ).fetchone()[0]
    n_before_edges = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='contains_lemma' "
        "AND source LIKE ?",
        (f"patristic-section:{AUTHOR_PREFIX}%",),
    ).fetchone()[0]

    print(f"\nClearing {n_before_forms} old form rows + {n_before_edges} old edges...")
    conn.execute(
        "DELETE FROM text_lemma_forms WHERE source_type='patristic-section' "
        "AND lang='la' AND source_id LIKE ?",
        (f"{AUTHOR_PREFIX}%",),
    )
    conn.execute(
        "DELETE FROM graph_edges WHERE edge_type='contains_lemma' AND source LIKE ?",
        (f"patristic-section:{AUTHOR_PREFIX}%",),
    )
    conn.commit()

    print(f"\nLemmatizing with treebank={TREEBANK}...")
    lemmer = Lemmatizer("la", conn, treebank=TREEBANK)

    # Quick sanity: how does ittb handle the previously-mangled words?
    # (This forces pipeline init so we can show the real output.)
    sample_text = (
        "ostendit Plato in Timaeo, et inquantum maxime perfecta. "
        "comprehendit autem se ipsum sicut est."
    )
    sample_tokens = lemmer.parse(sample_text)
    print("Sanity tokens (form → lemma):")
    for t in sample_tokens:
        if t.lemma in ("inquantus", "inquantum", "compr(eh)endo", "seipsus", "est,um"):
            marker = "  ⚠️  still bad: "
        else:
            marker = "  "
        print(f"{marker}{t.form!r} → {t.lemma!r}")

    n_sections = 0
    n_forms = 0
    n_cache_hits = 0
    parse_t = 0.0
    pending_forms: list[tuple] = []
    pending_edges: set[tuple[str, str, str]] = set()
    t_start = time.time()

    for r in rows:
        sid = r["id"]
        text = r["text_la"]
        chunks = chunk_text(text)
        section_tokens: list = []
        for chunk in chunks:
            h = hash_text("la", chunk, treebank=TREEBANK)
            cached = conn.execute(
                "SELECT tokens_json FROM lemma_parse_cache WHERE text_hash = ?", (h,)
            ).fetchone()
            if cached:
                from pipeline.src.lemmatizer import Token
                chunk_tokens = [Token(**t) for t in json.loads(cached[0])]
                n_cache_hits += 1
            else:
                t0 = time.time()
                chunk_tokens = lemmer.parse(chunk)
                parse_t += time.time() - t0
                conn.execute(
                    "INSERT OR REPLACE INTO lemma_parse_cache VALUES (?, 'la', ?)",
                    (h, json.dumps([asdict(t) for t in chunk_tokens], ensure_ascii=False)),
                )
            section_tokens.extend(chunk_tokens)

        for pos_idx, t in enumerate(section_tokens):
            lemma_id, _ = lemmer.index.resolve(t.lemma)
            pending_forms.append((
                "patristic-section", sid, "la", pos_idx,
                t.form, t.lemma, lemma_id, t.pos, t.feats,
            ))
            if lemma_id:
                pending_edges.add((
                    f"patristic-section:{sid}",
                    f"lemma-la:{lemma_id}",
                    "contains_lemma",
                ))
        n_sections += 1
        n_forms += len(section_tokens)
        if n_sections % 10 == 0:
            conn.executemany(
                "INSERT INTO text_lemma_forms VALUES (?,?,?,?,?,?,?,?,?)",
                pending_forms,
            )
            pending_forms.clear()
            conn.commit()
            print(f"  ...{n_sections} sections, {n_forms} forms, {time.time() - t_start:.0f}s")

    if pending_forms:
        conn.executemany(
            "INSERT INTO text_lemma_forms VALUES (?,?,?,?,?,?,?,?,?)",
            pending_forms,
        )
    conn.executemany(
        "INSERT INTO graph_edges VALUES (?,?,?)",
        list(pending_edges),
    )
    conn.commit()

    # Refresh corpus-freq for la (since Summa contributes a lot)
    print("\nRefreshing lemma_corpus_freq for la...")
    conn.execute("DELETE FROM lemma_corpus_freq WHERE lang = 'la'")
    conn.execute(
        """
        INSERT INTO lemma_corpus_freq (lang, lemma_id, tokens, sources)
        SELECT 'la', lemma_id, COUNT(*), COUNT(DISTINCT source_id)
        FROM text_lemma_forms
        WHERE lang = 'la' AND lemma_id IS NOT NULL
        GROUP BY lemma_id
        """
    )
    conn.commit()

    # Verification: check the bad-lemma cluster is gone (or much reduced)
    bad = conn.execute(
        """
        SELECT lemma, COUNT(*) AS cnt FROM text_lemma_forms
        WHERE source_type='patristic-section' AND lang='la' AND source_id LIKE ?
          AND lemma IN ('inquantus','inquantum','compr(eh)endo','seipsus','est,um','calendar','greek.expression')
        GROUP BY lemma ORDER BY cnt DESC
        """,
        (f"{AUTHOR_PREFIX}%",),
    ).fetchall()

    print(
        f"\nDone in {time.time() - t_start:.0f}s. "
        f"sections={n_sections} forms={n_forms} edges={len(pending_edges)} "
        f"parse={parse_t:.0f}s cache_hits={n_cache_hits}"
    )
    if bad:
        print("⚠️  Bad-lemma residue (should be zero/near-zero):")
        for r in bad:
            print(f"  {r['lemma']:<22} {r['cnt']}")
    else:
        print("✓ Bad-lemma cluster fully cleared.")

    conn.close()


if __name__ == "__main__":
    main()
