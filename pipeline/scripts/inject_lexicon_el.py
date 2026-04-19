"""One-shot: inject Greek lemma nodes from LSJ-Logeion into knowledge-graph.db.

Reads TEI XML from pipeline/data/raw/lexicons/lsj-logeion/greatscott*.xml
(Liddell-Scott-Jones, Helma Dik's Chicago/Logeion edit — Unicode Greek, lightly
corrected). Populates lemma_el + lemma_el_fts and inserts lemma-el:<key> nodes
into graph_nodes. Idempotent.

Source: https://github.com/helmadik/LSJLogeion (CC-BY-SA 4.0, derived from
PerseusDL/lexica). The `key` attribute is Perseus beta-code (e.g. "lo/gos",
"*a") and is kept as-is for primary-key stability; the `lemma` field is the
Unicode <head> text.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_lexicon_el
"""

from __future__ import annotations

import glob
import json
import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "lexicons" / "lsj-logeion"

LEMMA_EL_COLOR = "#6FAF8A"  # muted green — matches LA purple as a sibling
DEFAULT_SIZE = 2.5

SCHEMA = """\
CREATE TABLE IF NOT EXISTS lemma_el (
    id            TEXT PRIMARY KEY,
    lemma         TEXT NOT NULL,
    pos           TEXT,
    gender        TEXT,
    definition_en TEXT NOT NULL,
    etymology     TEXT,
    morph_json    TEXT,
    source_ref    TEXT
);
CREATE INDEX IF NOT EXISTS idx_lemma_el_lemma ON lemma_el(lemma);

CREATE VIRTUAL TABLE IF NOT EXISTS lemma_el_fts USING fts5(
    id UNINDEXED, lemma, definition_en, tokenize='unicode61'
);
"""


def text_content(elem: ET.Element) -> str:
    """Recursive plain-text extraction; collapses whitespace."""
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(text_content(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(" ".join(parts).split())


def parse_entry(div2: ET.Element) -> dict | None:
    key = (div2.get("key") or "").strip()
    if not key:
        return None

    head_elem = div2.find("head")
    lemma = text_content(head_elem) if head_elem is not None else key
    if not lemma:
        lemma = key

    itype_elem = div2.find("itype")
    pos = text_content(itype_elem) if itype_elem is not None else None

    gen_elem = div2.find("gen")
    gender = text_content(gen_elem) if gen_elem is not None else None

    etym_elem = div2.find("etym")
    etymology = text_content(etym_elem) if etym_elem is not None else None

    sense_parts: list[str] = []
    for sense in div2.iterfind("sense"):
        n = sense.get("n") or ""
        body = text_content(sense)
        if not body:
            continue
        sense_parts.append(f"{n}. {body}".strip(". ").strip())
    definition = "\n\n".join(sense_parts).strip()

    if not definition:
        # Fall back to whole-entry text minus the head — keep cross-refs at least
        whole = text_content(div2)
        definition = whole.replace(lemma, "", 1).strip(" ,.;")
    if not definition:
        return None

    morph = {
        k: v for k, v in {
            "type": div2.get("type"),
            "orig_id": div2.get("orig_id"),
        }.items() if v
    }
    morph_json = json.dumps(morph, ensure_ascii=False) if morph else None

    return {
        "id": key,
        "lemma": lemma,
        "pos": pos,
        "gender": gender,
        "definition": definition,
        "etymology": etymology,
        "morph_json": morph_json,
    }


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    files = sorted(glob.glob(str(RAW_DIR / "greatscott*.xml")))
    if not files:
        raise SystemExit(f"No LSJ XML files in {RAW_DIR}")
    print(f"Found {len(files)} LSJ XML files")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)

    existing_keys: set[str] = {
        r[0] for r in conn.execute("SELECT id FROM lemma_el").fetchall()
    }

    lemma_rows: list[tuple] = []
    fts_rows: list[tuple] = []
    node_rows: list[tuple] = []
    parsed = 0
    skipped_empty = 0
    skipped_existing = 0

    empty_json = json.dumps([])
    for f in files:
        # iterparse keeps memory low across the 86 files
        for _, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "div2":
                continue
            entry = parse_entry(elem)
            elem.clear()  # free memory after handling
            if entry is None:
                skipped_empty += 1
                continue
            key = entry["id"]
            if key in existing_keys:
                skipped_existing += 1
                continue
            existing_keys.add(key)
            parsed += 1
            lemma_rows.append((
                key,
                entry["lemma"],
                entry["pos"],
                entry["gender"],
                entry["definition"],
                entry["etymology"],
                entry["morph_json"],
                f"lsj-logeion:{key}",
            ))
            fts_rows.append((key, entry["lemma"], entry["definition"]))
            node_rows.append((
                f"lemma-el:{key}",
                entry["lemma"],
                "lemma_el",
                0.0, 0.0,
                DEFAULT_SIZE,
                LEMMA_EL_COLOR,
                "",
                0,
                0,
                empty_json, empty_json, empty_json,
            ))

    print(
        f"Parsed {parsed} new entries "
        f"(skipped {skipped_existing} existing, {skipped_empty} empty/no-key)"
    )

    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.executemany(
        "INSERT INTO lemma_el VALUES (?,?,?,?,?,?,?,?)",
        lemma_rows,
    )
    cur.executemany(
        "INSERT INTO lemma_el_fts (id, lemma, definition_en) VALUES (?,?,?)",
        fts_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    nodes_added = cur.rowcount
    conn.commit()

    total_lemmas = conn.execute("SELECT COUNT(*) FROM lemma_el").fetchone()[0]
    total_el_nodes = conn.execute(
        "SELECT COUNT(*) FROM graph_nodes WHERE node_type = 'lemma_el'"
    ).fetchone()[0]
    conn.close()

    print()
    print(f"Inserted {parsed} new lemma rows, {nodes_added} new graph nodes")
    print(f"DB now has: {total_lemmas} lemma_el rows, {total_el_nodes} lemma_el nodes")


if __name__ == "__main__":
    main()
