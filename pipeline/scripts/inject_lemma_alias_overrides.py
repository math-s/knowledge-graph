"""Hand-curated overrides for known Stanza lemmatizer mistakes.

Both Stanza treebanks we use (proiel for biblical, ittb for Aquinas) emit a
small set of consistently wrong lemmas for very common forms — distinct
errors per treebank. We map each known-bad lemma to the correct
canonical id so the LemmaIndex can resolve it.

Schema lets us extend without code changes; LemmaIndex consults this table at
construction time and treats overrides as an additional alias source.

Usage:
    uv run --project pipeline --extra lemma python -m pipeline.scripts.inject_lemma_alias_overrides
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

# (lang, bad_lemma, correct_id, note). Add freely as we discover more.
OVERRIDES: list[tuple[str, str, str, str]] = [
    # --- ittb (Aquinas) basic-verb misstems ---
    ("la", "vido",     "video",      "ittb stems videre/vidit/etc. as fictitious 'vido'"),
    ("la", "vigo",     "venio",      "ittb stems venit as fictitious 'vigo'"),
    # --- proiel scholastic-vocabulary failures (would re-emerge if we ever switched Summa back) ---
    ("la", "inquantus",     "in",         "proiel splits 'in quantum' wrong; map fragment to 'in'"),
    ("la", "inquantum",     "in",         "same"),
    ("la", "compr(eh)endo", "comprehendo","proiel leaks alternation brackets into the lemma"),
    ("la", "seipsus",       "ipse",       "proiel mis-stems 'se ipse' reflexive"),
]


SCHEMA = """\
CREATE TABLE IF NOT EXISTS lemma_alias (
    lang        TEXT NOT NULL,
    bad_lemma   TEXT NOT NULL,
    correct_id  TEXT NOT NULL,
    note        TEXT,
    PRIMARY KEY (lang, bad_lemma)
);
"""


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA)
    cur = conn.executemany(
        "INSERT OR REPLACE INTO lemma_alias (lang, bad_lemma, correct_id, note) VALUES (?,?,?,?)",
        OVERRIDES,
    )
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM lemma_alias").fetchone()[0]
    print(f"Inserted/updated {len(OVERRIDES)} aliases. lemma_alias has {n} rows.")
    conn.close()


if __name__ == "__main__":
    main()
