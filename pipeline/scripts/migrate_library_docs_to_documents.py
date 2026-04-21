"""Promote entries in ``library_docs`` (category='docs') to first-class rows
in the structured ``documents`` / ``document_sections`` tables.

``library_docs`` stores 187 papal encyclicals, apostolic letters, and CDF
declarations as single-blob text. The ``documents`` table has only 39 rows
total, most from vatican.va with numbered paragraphs. This script fills the
gap by slugifying each library doc's title into a ``documents`` id, parsing
numbered paragraphs out of its text, and writing structured section rows.

Skips any library doc whose slugified id already exists in ``documents``
(e.g. Humanae Vitae, Gaudium et Spes, Centesimus Annus — already ingested
from vatican.va with cleaner structure).

Category assignment:
  - ``cdf-declaration`` for CDF documents (``docs_df*``, ``docs_cf*``)
  - ``encyclical`` for everything else

Usage:
    uv run --project pipeline python -m pipeline.scripts.migrate_library_docs_to_documents
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("migrate_library_docs")

# Strip trailing " (YYYY)" from titles when producing the display name
TITLE_YEAR_RE = re.compile(r"\s*\(\s*\d{3,4}\s*\)\s*$")

# Splits on " NNN. " when followed by an uppercase letter — paragraph number
# boundary for numbered encyclicals (Evangelium Vitae, Veritatis Splendor, etc.)
PARAGRAPH_SPLIT_RE = re.compile(r"(?<=[\s.])(\d{1,3})\.\s+(?=[A-Z])")

MIN_SECTION_LEN = 40


def _slugify(text: str) -> str:
    t = text.lower()
    t = re.sub(r"[^\w\s-]", "", t)
    t = re.sub(r"[\s_]+", "-", t).strip("-")
    t = re.sub(r"-+", "-", t)
    return t


def _category_for(lib_id: str) -> str:
    prefix = lib_id.removeprefix("docs_")[:2]
    if prefix in ("df", "cf"):
        return "cdf-declaration"
    return "encyclical"


def _parse_sections(text: str) -> list[tuple[str, str]]:
    """Split into (section_num, section_text) pairs. Returns single
    ("text", body) pair if no numbered paragraphs detected."""
    # First pass: find all boundary positions
    matches = list(PARAGRAPH_SPLIT_RE.finditer(text))
    if len(matches) < 3:
        # No structured numbering — return whole text as single section
        return [("text", text.strip())]

    sections: list[tuple[str, str]] = []
    # Preamble: everything before the first numbered chunk
    first_start = matches[0].start()
    preamble = text[:first_start].strip()
    if preamble and len(preamble) >= MIN_SECTION_LEN:
        sections.append(("preamble", preamble))

    for i, m in enumerate(matches):
        num = m.group(1)
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[body_start:body_end].strip()
        if body and len(body) >= MIN_SECTION_LEN:
            # Dedup: if same num seen twice (from nested numbered lists),
            # append to existing
            idx = next(
                (j for j, (n, _) in enumerate(sections) if n == num), None
            )
            if idx is not None:
                sections[idx] = (num, sections[idx][1] + "\n\n" + body)
            else:
                sections.append((num, body))
    return sections


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        existing_ids = {
            r[0] for r in cur.execute("SELECT id FROM documents").fetchall()
        }
        log.info("documents already holds %d rows", len(existing_ids))

        lib_rows = cur.execute(
            "SELECT id, title, year, text FROM library_docs WHERE category='docs'"
        ).fetchall()
        log.info("library_docs.docs: %d rows", len(lib_rows))

        doc_rows: list[tuple] = []
        section_rows: list[tuple] = []
        skipped_overlap: list[str] = []
        skipped_empty: list[str] = []

        for lib_id, title, year, text in lib_rows:
            if not text:
                skipped_empty.append(lib_id)
                continue

            name = TITLE_YEAR_RE.sub("", title).strip()
            slug = _slugify(name)
            if not slug:
                skipped_empty.append(lib_id)
                continue
            if slug in existing_ids:
                skipped_overlap.append(f"{lib_id} -> {slug}")
                continue

            parsed = _parse_sections(text)
            category = _category_for(lib_id)

            source_url = f"https://www.newadvent.org/library/{lib_id.removeprefix('docs_')}"
            doc_rows.append((
                slug,
                name,
                "",  # abbreviation
                category,
                source_url,
                0,   # fetchable — local
                "[]",
                len(parsed),
                json.dumps(["en"]),
            ))
            for num, body in parsed:
                section_rows.append((slug, num, body, None, None))
            existing_ids.add(slug)

        log.info("  %d overlaps skipped", len(skipped_overlap))
        for s in skipped_overlap[:5]:
            log.info("    %s", s)
        if len(skipped_overlap) > 5:
            log.info("    ... (%d more)", len(skipped_overlap) - 5)
        log.info("  %d empty/bad skipped", len(skipped_empty))

        # Clear any prior migration runs
        new_ids = [r[0] for r in doc_rows]
        if new_ids:
            ph = ",".join(["?"] * len(new_ids))
            cur.execute(
                f"DELETE FROM document_sections WHERE document_id IN ({ph})",
                new_ids,
            )

        cur.executemany(
            "INSERT OR REPLACE INTO documents VALUES (?,?,?,?,?,?,?,?,?)",
            doc_rows,
        )
        cur.executemany(
            "INSERT INTO document_sections VALUES (?,?,?,?,?)",
            section_rows,
        )
        conn.commit()

        log.info(
            "Migrated %d library docs, %d sections total",
            len(doc_rows),
            len(section_rows),
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
