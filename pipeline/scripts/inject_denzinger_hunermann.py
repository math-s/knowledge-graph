"""Inject Denzinger-Hünermann as a first-class document.

This is the modern Portuguese+Latin edition of Enchiridion Symbolorum
(Denzinger-Hünermann — DH numbering, post-1963 renumbering). Lives
separately from:
  - ``denzinger-deferrari-30`` (EN, old numbering) — sibling script
  - ``denzinger-schonmetzer``  (DS stub with 185 CCC citations) — untouched

Source: ``/Users/matheusandradesilva/src/converter/out-pt/``
  - ``entries.jsonl`` : 3918 entry records (+ section headers + classification
    trace)

Each ``entry`` record is written 1:1 to ``document_sections`` keyed on
``denz``. The ``denz_range`` field on header entries (e.g. "2400-2502" on
DH 2400 which is the header for the 103-proposition Unigenitus Dei Filius
document) is informational — each individual number within the range
already exists as its own entry, so we do NOT expand ranges.

What lands in ``document_sections``:
  - ``text_la``  ← entry.text_latin
  - ``text_pt``  ← entry.text_portuguese
  - ``text_en``  = NULL (filled later by a multilang backfill if an EN
    edition appears)
  - ``meta_json``: path, page, attribution, work, location_note, intro_pt,
    editorial_ref, denz_range, footnotes, and text_bilingual (which holds
    Greek/Latin-side-by-side-with-PT content that doesn't fit cleanly in
    a single ``text_*`` column).

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_denzinger_hunermann
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
SOURCE_DIR = Path("/Users/matheusandradesilva/src/converter/out-pt")

DOC_ID = "denzinger-hunermann"
DOC_NAME = "Denzinger-Hünermann, Compêndio dos Símbolos"
DOC_ABBR = "DH"
DOC_CATEGORY = "reference"
DOC_SOURCE_URL = None
DOC_AVAILABLE_LANGS = ["pt", "la"]

log = logging.getLogger("inject_denzinger_hunermann")


def ensure_meta_column(cur: sqlite3.Cursor) -> None:
    cols = {row[1] for row in cur.execute("PRAGMA table_info(document_sections)")}
    if "meta_json" not in cols:
        cur.execute("ALTER TABLE document_sections ADD COLUMN meta_json TEXT")
        log.info("added document_sections.meta_json column")


def load_entries() -> list[dict]:
    out: list[dict] = []
    with (SOURCE_DIR / "entries.jsonl").open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("type") == "entry":
                out.append(rec)
    return out


def build_section_rows(entries: list[dict]) -> list[tuple]:
    """One row per entry, keyed on denz. Fail loud on duplicate denz."""
    rows: list[tuple] = []
    seen: set[str] = set()
    for e in entries:
        denz = e.get("denz")
        if not denz:
            # Header entries without a denz shouldn't exist in this file
            # per prior inspection; skip defensively if they do appear.
            continue
        if denz in seen:
            raise RuntimeError(f"duplicate denz in PT source: {denz!r}")
        seen.add(denz)

        text_la = (e.get("text_latin") or None)
        text_pt = (e.get("text_portuguese") or None)
        if isinstance(text_la, str):
            text_la = text_la.strip() or None
        if isinstance(text_pt, str):
            text_pt = text_pt.strip() or None

        meta = {
            "path":          e.get("path") or {},
            "page":          e.get("page"),
            "attribution":   e.get("attribution"),
            "work":          e.get("work"),
            "location_note": e.get("location_note"),
            "intro_pt":      e.get("intro_pt"),
            "editorial_ref": e.get("editorial_ref"),
            "denz_range":    e.get("denz_range"),
            "footnotes":     e.get("footnotes") or [],
            "text_bilingual": e.get("text_bilingual"),
        }
        # Drop null-valued keys to keep meta_json compact.
        meta = {k: v for k, v in meta.items() if v not in (None, "", [], {})}

        rows.append((DOC_ID, denz, None, text_la, text_pt,
                     json.dumps(meta, ensure_ascii=False)))
    return rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        ensure_meta_column(cur)

        entries = load_entries()
        log.info("loaded %d entry records from entries.jsonl", len(entries))

        section_rows = build_section_rows(entries)
        log.info("built %d section rows (1:1 with entries, no range expansion)",
                 len(section_rows))

        # Stats before insert.
        la_count = sum(1 for r in section_rows if r[3])
        pt_count = sum(1 for r in section_rows if r[4])
        bi_count = sum(
            1 for r in section_rows
            if "text_bilingual" in json.loads(r[5])
        )
        range_count = sum(
            1 for r in section_rows if "denz_range" in json.loads(r[5])
        )
        log.info("  %d sections with Latin text", la_count)
        log.info("  %d sections with Portuguese text", pt_count)
        log.info("  %d sections with bilingual side-by-side block", bi_count)
        log.info("  %d sections carry a denz_range header marker", range_count)

        # Upsert document row.
        cur.execute(
            """
            INSERT INTO documents (id, name, abbreviation, category, source_url,
                                   fetchable, citing_paragraphs_json,
                                   section_count, available_langs_json)
            VALUES (?,?,?,?,?,0,'[]',?,?)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              abbreviation=excluded.abbreviation,
              category=excluded.category,
              source_url=excluded.source_url,
              fetchable=excluded.fetchable,
              section_count=excluded.section_count,
              available_langs_json=excluded.available_langs_json
            """,
            (
                DOC_ID, DOC_NAME, DOC_ABBR, DOC_CATEGORY, DOC_SOURCE_URL,
                len(section_rows), json.dumps(DOC_AVAILABLE_LANGS),
            ),
        )

        cur.execute("DELETE FROM document_sections WHERE document_id=?", (DOC_ID,))
        cur.executemany(
            """
            INSERT INTO document_sections
              (document_id, section_num, text_en, text_la, text_pt, meta_json)
            VALUES (?,?,?,?,?,?)
            """,
            section_rows,
        )

        conn.commit()

        log.info("inserted %d document_sections rows for %s", len(section_rows), DOC_ID)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
