"""Inject Denzinger (Deferrari 30th ed., 1955) as a first-class document.

This is the English translation of Enchiridion Symbolorum that uses the
*old* (pre-Hünermann) Denzinger numbering. It lives separately from
``denzinger-schonmetzer`` (the 32nd–36th ed. stub already in the DB with
CCC citations pointing at it) and from ``denzinger-hunermann`` (modern
numbering, Portuguese + Latin — ingested by a sibling script).

Source: ``/Users/matheusandradesilva/src/converter/out/``
  - ``entries.jsonl``  : 3005 paragraph records + frontmatter/headings
  - ``footnotes.jsonl``: 716 footnotes
  - ``hierarchy.json`` : authors/documents/chapters tree

What this script does:
  1. Reads paragraph records from entries.jsonl.
  2. Folds continuation paragraphs (``denz = null``) into the preceding
     denz-numbered owner, joining their text with blank lines. Scripture/
     cross/footnote refs from the continuations are unioned onto the owner.
  3. Upserts a ``documents`` row for ``denzinger-deferrari-30``.
  4. Replaces the document's ``document_sections`` idempotently.
  5. Stores rich per-section metadata (path, page, scripture_refs,
     cross_refs, footnote_refs, source paragraph pages) in a new
     ``meta_json`` column on ``document_sections`` (column added on first
     run).

Later backfill scripts read ``meta_json`` to wire ``cites`` edges to
bible verses, other denz sections, patristic works, and councils.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_denzinger_deferrari
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
SOURCE_DIR = Path("/Users/matheusandradesilva/src/converter/out")

DOC_ID = "denzinger-deferrari-30"
DOC_NAME = "Enchiridion Symbolorum (Denzinger, 30th ed.)"
DOC_ABBR = "Denz"
DOC_CATEGORY = "reference"
DOC_SOURCE_URL = None  # scanned PDF → JSONL; no canonical URL
DOC_AVAILABLE_LANGS = ["en"]

log = logging.getLogger("inject_denzinger_deferrari")


def ensure_meta_column(cur: sqlite3.Cursor) -> None:
    """Add ``document_sections.meta_json`` if the column doesn't exist yet."""
    cols = {row[1] for row in cur.execute("PRAGMA table_info(document_sections)")}
    if "meta_json" not in cols:
        cur.execute("ALTER TABLE document_sections ADD COLUMN meta_json TEXT")
        log.info("added document_sections.meta_json column")


def load_paragraphs() -> list[dict]:
    """Read entries.jsonl, return only paragraph records in file order."""
    out: list[dict] = []
    with (SOURCE_DIR / "entries.jsonl").open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("type") == "paragraph":
                out.append(rec)
    return out


def group_by_owner(paragraphs: list[dict]) -> dict[str, dict]:
    """Carry the last non-null ``denz`` forward over continuation paragraphs.

    Returns ``{denz_key: aggregated_section}`` where each aggregated section
    holds the joined text plus a ``paragraphs`` list preserving per-source
    metadata (pages, scripture_refs, cross_refs, footnote_refs, path).
    """
    groups: dict[str, dict] = {}
    current: str | None = None
    for p in paragraphs:
        dz = p.get("denz")
        if dz is not None:
            current = dz
        if current is None:
            # Orphan paragraphs before the first numbered denz — these are
            # bibliography abbreviation lists. Skip; they don't belong to the
            # main corpus.
            continue
        g = groups.setdefault(current, {
            "denz": current,
            "paragraphs": [],
            "texts": [],
            "path_first": p.get("path") or {},
            "pages": [],
            "scripture_refs": [],
            "cross_refs": [],
            "footnote_refs": [],
        })
        text = (p.get("text") or "").strip()
        if text:
            g["texts"].append(text)
        page = p.get("page")
        if page is not None and page not in g["pages"]:
            g["pages"].append(page)
        # Store each source paragraph as a record (for later reference).
        g["paragraphs"].append({
            "page": page,
            "path": p.get("path") or {},
            "scripture_refs": p.get("scripture_refs") or [],
            "cross_refs": p.get("cross_refs") or [],
            "footnote_refs": p.get("footnote_refs") or [],
        })
        # Roll up refs onto the aggregated section (deduplicated at end).
        g["scripture_refs"].extend(p.get("scripture_refs") or [])
        g["cross_refs"].extend(p.get("cross_refs") or [])
        g["footnote_refs"].extend(p.get("footnote_refs") or [])
    return groups


def dedupe_scripture(refs: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in refs:
        key = r.get("ref") or ""
        if not key or key in seen:
            continue
        seen.add(key)
        out.append({"ref": key})
    return out


def dedupe_cross(refs: list[dict]) -> list[str]:
    seen: set[str] = set()
    for r in refs:
        t = r.get("target")
        if t:
            seen.add(str(t))
    return sorted(seen, key=lambda s: (len(s), s))


def build_section_rows(groups: dict[str, dict]) -> list[tuple]:
    rows: list[tuple] = []
    for denz, g in groups.items():
        text = "\n\n".join(g["texts"]).strip() or None
        meta = {
            "path": g["path_first"],
            "pages": g["pages"],
            "scripture_refs": dedupe_scripture(g["scripture_refs"]),
            "cross_refs": dedupe_cross(g["cross_refs"]),
            "footnote_refs": sorted({str(x) for x in g["footnote_refs"]}),
            "paragraphs": g["paragraphs"],
        }
        rows.append((DOC_ID, denz, text, None, None, json.dumps(meta, ensure_ascii=False)))
    return rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        ensure_meta_column(cur)

        paragraphs = load_paragraphs()
        log.info("loaded %d paragraph records from entries.jsonl", len(paragraphs))

        groups = group_by_owner(paragraphs)
        log.info("grouped into %d distinct denz keys", len(groups))

        multi = sum(1 for g in groups.values() if len(g["paragraphs"]) > 1)
        log.info("  %d keys have >1 source paragraph (continuations folded in)", multi)

        section_rows = build_section_rows(groups)

        # Upsert the document row.
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

        # Idempotent replace of this document's sections.
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

        # Summary stats.
        total_refs = sum(
            len(json.loads(r[5])["scripture_refs"]) for r in section_rows
        )
        total_cross = sum(
            len(json.loads(r[5])["cross_refs"]) for r in section_rows
        )
        log.info("inserted %d document_sections rows", len(section_rows))
        log.info("  %d unique scripture refs across all sections", total_refs)
        log.info("  %d unique internal cross-refs across all sections", total_cross)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
