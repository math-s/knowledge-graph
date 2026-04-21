"""Bootstrap stub ``documents`` rows for the 14 ecumenical councils not yet
ingested with full text.

Source: ``library_docs.almanac_14388a`` ("The 21 Ecumenical Councils") — a
compact summary of all 21 councils compiled from the Catholic Encyclopedia.

Writes one row per stub council to ``documents`` with:
  - ``category='council-ecumenical'``
  - ``section_count=1``
  - one ``document_sections`` row containing the almanac summary

Councils already in ``documents`` (the 7 in Percival's NPNF2 Vol. 14) are
left untouched. Canonical-text backfill (Lateran, Trent, Vatican I, etc.)
is a separate follow-up from papalencyclicals.net / vatican.va.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_council_stubs_from_almanac
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("inject_council_stubs")

# Roman-numeral → (id, abbreviation). Councils I–VII already have full text
# from the Percival corpus, so they're only included here to allow the regex
# parse loop to walk the text (we skip inserting rows for them).
COUNCIL_IDS: dict[str, tuple[str, str]] = {
    "I":     ("nicaea-i",            "Nic I"),
    "II":    ("constantinople-i",    "Const I"),
    "III":   ("ephesus",             "Ephes"),
    "IV":    ("chalcedon",           "Chal"),
    "V":     ("constantinople-ii",   "Const II"),
    "VI":    ("constantinople-iii",  "Const III"),
    "VII":   ("nicaea-ii",           "Nic II"),
    "VIII":  ("constantinople-iv",   "Const IV"),
    "IX":    ("lateran-i",           "Lat I"),
    "X":     ("lateran-ii",          "Lat II"),
    "XI":    ("lateran-iii",         "Lat III"),
    "XII":   ("lateran-iv",          "Lat IV"),
    "XIII":  ("lyon-i",              "Lyon I"),
    "XIV":   ("lyon-ii",             "Lyon II"),
    "XV":    ("vienne",              "Vienne"),
    "XVI":   ("constance",           "Const."),
    "XVII":  ("basel-florence",      "Bas-Flor"),
    "XVIII": ("lateran-v",           "Lat V"),
    "XIX":   ("trent",               ""),
    "XX":    ("vatican-i",           "Vat I"),
    "XXI":   ("vatican-ii",          "Vat II"),
}

# Anchor entry starts on the strong triple signature "roman. NAME Year(s):".
# Roman numeral alternatives are ordered longest-first so longer matches win.
ENTRY_START_RE = re.compile(
    r"\b(?P<num>XXI|XX|XIX|XVIII|XVII|XVI|XV|XIV|XIII|XII|XI|X|IX|VIII|VII|VI|V|IV|III|II|I)\."
    r"\s+(?P<name>[A-Z][A-Z/ \-]+?)\s+Years?:\s+(?P<year>\d{3,4}(?:-\d{3,4})?)\s+",
    re.DOTALL,
)


def _parse_year(s: str) -> int | None:
    m = re.match(r"^(\d{3,4})", s)
    return int(m.group(1)) if m else None


def _titlecase(name: str) -> str:
    # "FIRST LATERAN COUNCIL" -> "First Lateran Council"
    return " ".join(w.capitalize() if w.isalpha() else w for w in name.strip().split())


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT text FROM library_docs WHERE id='almanac_14388a'"
        ).fetchone()
        if not row:
            log.error("almanac_14388a not found in library_docs")
            return 1
        text = row[0]

        existing = {
            r[0] for r in cur.execute("SELECT id FROM documents").fetchall()
        }

        doc_rows: list[tuple] = []
        section_rows: list[tuple] = []
        starts = list(ENTRY_START_RE.finditer(text))
        log.info("Parsed %d council entries from almanac", len(starts))

        for i, m in enumerate(starts):
            num = m.group("num")
            meta = COUNCIL_IDS.get(num)
            if not meta:
                log.warning("  %s: no id mapping; skipping", num)
                continue
            cid, abbrev = meta
            if cid in existing:
                log.info("  %s -> %s (already in documents, skipping)", num, cid)
                continue

            name = _titlecase(m.group("name"))
            year = _parse_year(m.group("year"))
            body_start = m.end()
            body_end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
            body = text[body_start:body_end]
            # Strip leading "Summary:" / "Source:" prefix and trailing "Further Reading:..."
            body = re.sub(r"^\s*(?:Summary|Source):\s*", "", body, count=1)
            body = re.sub(r"\s*Further Reading:.*$", "", body, count=1, flags=re.DOTALL)
            summary = body.strip()
            section_text = f"{name} ({m.group('year')})\n\n{summary}".strip()

            doc_rows.append((
                cid,
                name,
                abbrev,
                "council-ecumenical",
                "",           # source_url — to be filled when we ingest full text
                0,            # fetchable — stub
                "[]",         # citing_paragraphs_json
                1,            # section_count
                json.dumps(["en"]),
            ))
            section_rows.append((cid, "summary", section_text, None, None))
            log.info("  %s -> %s (year %s, %d-char summary)", num, cid, year, len(summary))

        cur.executemany(
            "INSERT OR REPLACE INTO documents VALUES (?,?,?,?,?,?,?,?,?)",
            doc_rows,
        )
        # Clear old stub sections before inserting
        if doc_rows:
            ids = [r[0] for r in doc_rows]
            ph = ",".join(["?"] * len(ids))
            cur.execute(
                f"DELETE FROM document_sections WHERE document_id IN ({ph}) AND section_num='summary'",
                ids,
            )
            cur.executemany(
                "INSERT INTO document_sections VALUES (?,?,?,?,?)",
                section_rows,
            )
        conn.commit()
        log.info("Inserted %d stub councils, %d summary sections", len(doc_rows), len(section_rows))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
