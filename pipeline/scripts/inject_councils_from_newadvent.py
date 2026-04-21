"""Inject early ecumenical councils + local synods from the Percival corpus.

Parses the 19 HTML files at ``data/raw/newadvent/fathers/38*.htm`` (NPNF2
Vol. 14, Henry Percival, *The Seven Ecumenical Councils*) and writes them
as first-class ``documents`` + ``document_sections`` rows alongside
Vatican II.

Citations *into* these councils (CCC, patristic, etc.) are handled by a
later backfill step.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_councils_from_newadvent
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PIPELINE_ROOT = PROJECT_ROOT / "pipeline"
FATHERS_DIR = PIPELINE_ROOT / "data" / "raw" / "newadvent" / "fathers"
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
NEWADVENT_BASE = "https://www.newadvent.org/fathers"

ECUMENICAL = "council-ecumenical"
LOCAL = "council-local"

COUNCILS: dict[str, dict] = {
    "3801": {"id": "nicaea-i",           "name": "First Council of Nicaea",         "abbreviation": "Nic I",    "category": ECUMENICAL},
    "3802": {"id": "ancyra",             "name": "Council of Ancyra",               "abbreviation": "",         "category": LOCAL},
    "3803": {"id": "neocaesarea",        "name": "Council of Neocaesarea",          "abbreviation": "",         "category": LOCAL},
    "3804": {"id": "gangra",             "name": "Synod of Gangra",                 "abbreviation": "",         "category": LOCAL},
    "3805": {"id": "antioch",            "name": "Synod of Antioch in Encaeniis",   "abbreviation": "",         "category": LOCAL},
    "3806": {"id": "laodicea",           "name": "Synod of Laodicea",               "abbreviation": "",         "category": LOCAL},
    "3808": {"id": "constantinople-i",   "name": "First Council of Constantinople", "abbreviation": "Const I",  "category": ECUMENICAL},
    "3809": {"id": "constantinople-382", "name": "Synod of Constantinople (382)",   "abbreviation": "",         "category": LOCAL},
    "3810": {"id": "ephesus",            "name": "Council of Ephesus",              "abbreviation": "Ephes",    "category": ECUMENICAL},
    "3811": {"id": "chalcedon",          "name": "Council of Chalcedon",            "abbreviation": "Chal",     "category": ECUMENICAL},
    "3812": {"id": "constantinople-ii",  "name": "Second Council of Constantinople","abbreviation": "Const II", "category": ECUMENICAL},
    "3813": {"id": "constantinople-iii", "name": "Third Council of Constantinople", "abbreviation": "Const III","category": ECUMENICAL},
    "3814": {"id": "trullo",             "name": "Council in Trullo",               "abbreviation": "",         "category": LOCAL},
    "3815": {"id": "sardica",            "name": "Council of Sardica",              "abbreviation": "",         "category": LOCAL},
    "3816": {"id": "carthage-419",       "name": "Council of Carthage (419)",       "abbreviation": "",         "category": LOCAL},
    "3817": {"id": "constantinople-394", "name": "Council of Constantinople (394)", "abbreviation": "",         "category": LOCAL},
    "3818": {"id": "carthage-257",       "name": "Council of Carthage (257)",       "abbreviation": "",         "category": LOCAL},
    "3819": {"id": "nicaea-ii",          "name": "Second Council of Nicaea",        "abbreviation": "Nic II",   "category": ECUMENICAL},
    "3820": {"id": "apostolic-canons",   "name": "The Apostolic Canons",            "abbreviation": "",         "category": LOCAL},
}

CANON_RE = re.compile(r"^\s*canon\s+(\d+)", re.IGNORECASE)

log = logging.getLogger("inject_councils")


def _slugify(text: str) -> str:
    t = re.sub(r"[^\w\s-]", "", text.lower()).strip()
    t = re.sub(r"[\s_-]+", "-", t)
    return t.strip("-")


def _parse_file(html: str) -> list[tuple[str, str]]:
    """Walk the body in document order and group paragraphs by h2/h3 heading.

    Canon headings (``<h3>Canon N</h3>``) produce bare-numeric section_nums,
    matching how councils are cited ("Nicaea I c. 6"). Other headings get
    slugified (e.g. ``the-nicene-creed``, ``the-synodal-letter``).
    """
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", id="springfield2") or soup.body
    if not container:
        return []

    sections: list[tuple[str, str]] = []
    current_num: str | None = None
    buf: list[str] = []

    def flush() -> None:
        nonlocal buf
        if current_num and buf:
            text = "\n\n".join(s for s in (p.strip() for p in buf) if s)
            if text:
                idx = next((i for i, (n, _) in enumerate(sections) if n == current_num), None)
                if idx is not None:
                    sections[idx] = (current_num, sections[idx][1] + "\n\n" + text)
                else:
                    sections.append((current_num, text))
        buf = []

    for el in container.find_all(["h1", "h2", "h3", "p"]):
        if el.find_parent("div", class_="pub"):
            continue
        if el.name == "h1":
            continue
        if el.name == "h2":
            flush()
            current_num = _slugify(el.get_text(" ", strip=True)) or None
        elif el.name == "h3":
            flush()
            head = el.get_text(" ", strip=True)
            m = CANON_RE.search(head)
            current_num = m.group(1) if m else (_slugify(head) or None)
        elif el.name == "p":
            text = el.get_text(" ", strip=True)
            if not text:
                continue
            if current_num is None:
                current_num = "text"
            buf.append(text)
    flush()
    return sections


def _build_rows(file_num: str, meta: dict) -> tuple[tuple, list[tuple]]:
    html = (FATHERS_DIR / f"{file_num}.htm").read_text(encoding="utf-8", errors="replace")
    parsed = _parse_file(html)

    doc_id = meta["id"]
    source_url = f"{NEWADVENT_BASE}/{file_num}.htm"
    doc_row = (
        doc_id,
        meta["name"],
        meta["abbreviation"],
        meta["category"],
        source_url,
        0,  # fetchable — content is already local
        "[]",  # citing_paragraphs_json; populated by a later backfill
        len(parsed),
        json.dumps(["en"]),
    )
    section_rows = [(doc_id, num, text, None, None) for num, text in parsed]
    return doc_row, section_rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    doc_rows: list[tuple] = []
    section_rows: list[tuple] = []
    for file_num, meta in COUNCILS.items():
        doc_row, srows = _build_rows(file_num, meta)
        doc_rows.append(doc_row)
        section_rows.extend(srows)
        log.info("  %-20s %3d sections (%s)", meta["id"], len(srows), file_num)

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        council_ids = [r[0] for r in doc_rows]
        placeholders = ",".join(["?"] * len(council_ids))
        cur.execute(
            f"DELETE FROM document_sections WHERE document_id IN ({placeholders})",
            council_ids,
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
    finally:
        conn.close()

    log.info("Wrote %d councils, %d sections total", len(doc_rows), len(section_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
