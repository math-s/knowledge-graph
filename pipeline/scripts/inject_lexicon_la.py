"""One-shot: inject Latin lemma nodes from Lewis & Short into knowledge-graph.db.

Reads pre-parsed JSON from pipeline/data/raw/lexicons/lewis-short-json/ (one file
per starting letter). Populates lemma_la + lemma_la_fts tables and inserts
lemma-la:<id> nodes into graph_nodes. Idempotent — safe to re-run.

Source: https://github.com/IohannesArnold/lewis-short-json (CC-BY-SA 4.0,
derived from PerseusDL/lexica).

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_lexicon_la
"""

from __future__ import annotations

import glob
import json
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "lexicons" / "lewis-short-json"

LEMMA_LA_COLOR = "#8A6FAF"  # muted purple — distinct from existing node types
DEFAULT_SIZE = 2.5

SCHEMA = """\
CREATE TABLE IF NOT EXISTS lemma_la (
    id            TEXT PRIMARY KEY,
    lemma         TEXT NOT NULL,
    pos           TEXT,
    definition_en TEXT NOT NULL,
    morph_json    TEXT,
    source_ref    TEXT
);
CREATE INDEX IF NOT EXISTS idx_lemma_la_lemma ON lemma_la(lemma);

CREATE VIRTUAL TABLE IF NOT EXISTS lemma_la_fts USING fts5(
    id UNINDEXED, lemma, definition_en, tokenize='unicode61'
);
"""


def build_definition(entry: dict) -> str:
    parts: list[str] = []
    main = entry.get("main_notes")
    if main:
        parts.append(main.strip())
    for s in entry.get("senses") or []:
        if isinstance(s, str) and s.strip():
            parts.append(s.strip())
    return "\n\n".join(parts).strip()


def build_morph_json(entry: dict) -> str | None:
    morph = {
        k: entry[k]
        for k in (
            "gender",
            "declension",
            "title_genitive",
            "alternative_orthography",
            "alternative_genative",
            "greek_word",
            "entry_type",
        )
        if entry.get(k) is not None and entry.get(k) != ""
    }
    return json.dumps(morph, ensure_ascii=False) if morph else None


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    files = sorted(glob.glob(str(RAW_DIR / "ls_*.json")))
    if not files:
        raise SystemExit(f"No L&S JSON files in {RAW_DIR}")
    print(f"Found {len(files)} L&S JSON files")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)

    existing_keys: set[str] = {
        r[0] for r in conn.execute("SELECT id FROM lemma_la").fetchall()
    }

    lemma_rows: list[tuple] = []
    fts_rows: list[tuple] = []
    node_rows: list[tuple] = []
    skipped_no_key = 0
    skipped_no_def = 0
    skipped_existing = 0

    empty_json = json.dumps([])
    for f in files:
        with open(f) as fh:
            entries = json.load(fh)
        for e in entries:
            key = (e.get("key") or "").strip()
            if not key:
                skipped_no_key += 1
                continue
            if key in existing_keys:
                skipped_existing += 1
                continue
            lemma = (e.get("title_orthography") or key).strip()
            definition = build_definition(e)
            if not definition:
                skipped_no_def += 1
                continue
            pos = e.get("part_of_speech")
            morph_json = build_morph_json(e)
            source_ref = f"lewis-short:{key}"

            existing_keys.add(key)  # guard against intra-file dups too
            lemma_rows.append((key, lemma, pos, definition, morph_json, source_ref))
            fts_rows.append((key, lemma, definition))
            node_rows.append((
                f"lemma-la:{key}",
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
        f"Parsed {len(lemma_rows)} new entries "
        f"(skipped {skipped_existing} existing, {skipped_no_key} no-key, {skipped_no_def} no-definition)"
    )

    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.executemany(
        "INSERT INTO lemma_la VALUES (?,?,?,?,?,?)",
        lemma_rows,
    )
    cur.executemany(
        "INSERT INTO lemma_la_fts (id, lemma, definition_en) VALUES (?,?,?)",
        fts_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    nodes_added = cur.rowcount
    conn.commit()

    total_lemmas = conn.execute("SELECT COUNT(*) FROM lemma_la").fetchone()[0]
    total_la_nodes = conn.execute(
        "SELECT COUNT(*) FROM graph_nodes WHERE node_type = 'lemma_la'"
    ).fetchone()[0]
    conn.close()

    print()
    print(f"Inserted {len(lemma_rows)} new lemma rows, {nodes_added} new graph nodes")
    print(f"DB now has: {total_lemmas} lemma_la rows, {total_la_nodes} lemma_la nodes")


if __name__ == "__main__":
    main()
