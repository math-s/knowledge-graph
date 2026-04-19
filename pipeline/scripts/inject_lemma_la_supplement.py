"""Add a supplement of biblical proper-noun lemmas to lemma_la.

L&S (the Lewis & Short Latin dictionary we use for lemma_la) doesn't catalog
biblical proper nouns. As a result, OT-genealogy-heavy books like 1-Chronicles
sit at ~78% lemma-lookup rate where the rest of the Vulgate is ~91-97%.

This script harvests the most common unmatched PROPN lemmas from the corpus,
inserts them as supplementary lemma_la rows with `source_ref='supplement:bible-propn'`,
and lets the LemmaIndex pick them up on next load. No definitions yet — just
existence is enough to land contains_lemma edges and lift coverage.

Idempotent: skips already-supplemented rows. Re-runnable after each
re-lemmatization to pick up newly-revealed unmatched proper nouns.

Usage:
    uv run --project pipeline --extra lemma python -m pipeline.scripts.inject_lemma_la_supplement \\
        [--top N]                      # how many top-frequency PROPN lemmas to add (default 500)
        [--min-occurrences M]          # only add lemmas seen at least M times (default 3)
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

LEMMA_LA_COLOR = "#8A6FAF"
DEFAULT_SIZE = 2.5
SOURCE_REF = "supplement:bible-propn"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top", type=int, default=500)
    parser.add_argument("--min-occurrences", type=int, default=3)
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Pull top-N unmatched PROPN lemmas from the bible (skip patristic — those
    # carry their own scholastic/proper-noun mix that's better fixed elsewhere).
    candidates = conn.execute(
        """
        SELECT lemma, COUNT(*) AS cnt
        FROM text_lemma_forms
        WHERE lang='la' AND source_type='bible-verse'
          AND lemma_id IS NULL AND pos='PROPN'
        GROUP BY lemma
        HAVING cnt >= ?
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (args.min_occurrences, args.top),
    ).fetchall()
    print(f"Pulled {len(candidates)} candidate proper-noun lemmas")

    # Skip lemmas already in lemma_la under any id, OR under our supplement ids
    existing = {
        r[0].lower()
        for r in conn.execute("SELECT id FROM lemma_la").fetchall()
    }

    new_lemma_rows: list[tuple] = []
    new_fts_rows: list[tuple] = []
    new_node_rows: list[tuple] = []
    skipped_existing = 0
    empty_json = json.dumps([])

    for r in candidates:
        lemma = r["lemma"].strip()
        if not lemma:
            continue
        # Use a stable, namespaced id so future maintenance can find/replace
        # the auto-generated set without collision with real L&S entries.
        new_id = f"bib:{lemma}"
        if new_id.lower() in existing or lemma.lower() in existing:
            skipped_existing += 1
            continue
        existing.add(new_id.lower())

        definition = (
            f"Biblical proper noun (Vulgate Latin). "
            f"Auto-supplemented from {r['cnt']} occurrence(s) in the Vulgate; "
            f"no Lewis & Short entry."
        )
        new_lemma_rows.append((new_id, lemma, "noun", definition, None, SOURCE_REF))
        new_fts_rows.append((new_id, lemma, definition))
        new_node_rows.append((
            f"lemma-la:{new_id}",
            lemma,
            "lemma_la",
            0.0, 0.0,
            DEFAULT_SIZE,
            LEMMA_LA_COLOR,
            "",
            0,
            0,
            empty_json, empty_json, empty_json,
        ))

    print(
        f"Inserting {len(new_lemma_rows)} new supplement rows "
        f"(skipped {skipped_existing} that already exist in some form)"
    )

    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.executemany(
        "INSERT INTO lemma_la VALUES (?,?,?,?,?,?)", new_lemma_rows,
    )
    cur.executemany(
        "INSERT INTO lemma_la_fts (id, lemma, definition_en) VALUES (?,?,?)",
        new_fts_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        new_node_rows,
    )
    conn.commit()

    total = conn.execute(
        "SELECT COUNT(*) FROM lemma_la WHERE source_ref = ?", (SOURCE_REF,)
    ).fetchone()[0]
    print(f"Done. lemma_la now has {total} bible-propn supplement rows.")
    print(
        "Run refresh_lemma_resolution to re-resolve text_lemma_forms against the new entries."
    )

    conn.close()


if __name__ == "__main__":
    main()
