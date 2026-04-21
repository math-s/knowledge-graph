"""Fetch and ingest the 1983 Code of Canon Law (CIC) from vatican.va.

The ``cic`` row already exists in ``documents`` as a stub (category=canon-law,
no sections). This script downloads the 45 sub-pages linked from
``cic_index_en.html``, parses canons 1-1752 (including sub-paragraphs
``§1 §2 …``), writes one row per canon into ``document_sections`` keyed by
canon number, and refreshes the ``cic`` document row.

Downloads are cached under ``pipeline/data/raw/documents/cic/``.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_cic_canons
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "documents" / "cic"

log = logging.getLogger("inject_cic_canons")

CIC_BASE_URL = "https://www.vatican.va/archive/cod-iuris-canonici/eng/documents/"

# All 45 sub-pages linked from cic_index_en.html, in order
CIC_PAGES: list[str] = [
    "cic_introduction_en.html",
    "cic_lib1-cann1-6_en.html",
    "cic_lib1-cann7-22_en.html",
    "cic_lib1-cann23-28_en.html",
    "cic_lib1-cann29-34_en.html",
    "cic_lib1-cann35-93_en.html",
    "cic_lib1-cann94-95_en.html",
    "cic_lib1-cann96-123_en.html",
    "cic_lib1-cann124-128_en.html",
    "cic_lib1-cann129-144_en.html",
    "cic_lib1-cann145-196_en.html",
    "cic_lib1-cann197-199_en.html",
    "cic_lib1-cann200-203_en.html",
    "cic_lib2-cann204-207_en.html",
    "cic_lib2-cann208-329_en.html",
    "cic_lib2-cann330-367_en.html",
    "cic_lib2-cann368-430_en.html",
    "cic_lib2-cann431-459_en.html",
    "cic_lib2-cann460-572_en.html",
    "cic_lib2-cann573-606_en.html",
    "cic_lib2-cann607-709_en.html",
    "cic_lib2-cann710-730_en.html",
    "cic_lib2-cann731-746_en.html",
    "cic_lib3-cann747-755_en.html",
    "cic_lib3-cann756-780_en.html",
    "cic_lib3-cann781-792_en.html",
    "cic_lib3-cann793-821_en.html",
    "cic_lib3-cann822-833_en.html",
    "cic_lib4-cann834-878_en.html",
    "cic_lib4-cann879-958_en.html",
    "cic_lib4-cann959-997_en.html",
    "cic_lib4-cann998-1165_en.html",
    "cic_lib4-cann1166-1190_en.html",
    "cic_lib4-cann1191-1204_en.html",
    "cic_lib4-cann1205-1243_en.html",
    "cic_lib4-cann1244-1253_en.html",
    "cic_lib5-cann1254-1310_en.html",
    "cic_lib6-cann1311-1363_en.html",
    "cic_lib6-cann1364-1399_en.html",
    "cic_lib7-cann1400-1500_en.html",
    "cic_lib7-cann1501-1670_en.html",
    "cic_lib7-cann1671-1716_en.html",
    "cic_lib7-cann1717-1731_en.html",
    "cic_lib7-cann1732-1752_en.html",
]

# Canon header. Accepts "Can.", "Cann.", and the stray typo "Ca." used on
# a couple of pages. Captures the canon number and a trailing "n" marker
# that indicates a post-1983 revised version (from the 2016 motu proprio
# *De concordia inter Codices* and similar).
CANON_START_RE = re.compile(
    r"^\s*(?:<b>)?\s*Ca(?:n{1,2})?\.?\s*(\d{1,4})\s*(?P<n>n\b)?",
    re.IGNORECASE,
)

# Detects the "n" marker inside body text (e.g. "n §2. ..." or "n Can. 265")
# which also indicates a revised-version paragraph.
REVISED_MARKER_RE = re.compile(r"^\s*n\s+(?:§|Can\.)", re.IGNORECASE)

# Divider the vatican.va pages use between the revised Latin version and the
# original English version of a canon.
EARLIER_VERSION_RE = re.compile(r"\[\s*earlier\s+version\s*\]", re.IGNORECASE)

# Canon numbers that appear in section headers as ranges ("Cann. 1321 - 1330")
# and should NOT be treated as canon starts.
RANGE_REF_RE = re.compile(
    r"\bCann\.\s*\d+\s*[-–—]\s*\d+", re.IGNORECASE
)


def _fetch(filename: str) -> str:
    """Download and cache a CIC sub-page."""
    cache = RAW_DIR / filename
    if cache.exists():
        return cache.read_text(encoding="utf-8", errors="replace")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    url = CIC_BASE_URL + filename
    log.info("  GET %s", url)
    resp = requests.get(url, timeout=30, headers={"User-Agent": "KnowledgeGraphPipeline/1.0"})
    resp.raise_for_status()
    cache.write_text(resp.text, encoding="utf-8")
    time.sleep(0.3)  # be polite to vatican.va
    return resp.text


def _candidate_blocks(soup: BeautifulSoup) -> list[str]:
    """Walk the main content and return a flat ordered list of text blocks.

    We take each ``<p>`` as a block, and additionally rescue any orphan
    ``<b>Can. N</b>…`` run that vatican.va forgot to wrap in a ``<p>``
    (seen at canon 1330). Done by scanning ``<b>`` tags whose text matches
    a canon header and pairing them with following sibling text up to the
    next ``<p>`` or ``<b>`` of the same kind.
    """
    blocks: list[str] = []
    for p in soup.find_all("p"):
        blocks.append(p.get_text(" ", strip=True))

    for b in soup.find_all("b"):
        b_text = b.get_text(" ", strip=True)
        if not CANON_START_RE.match(b_text):
            continue
        if b.find_parent("p") is not None:
            continue
        # Gather the tail: text after </b> until the next block-level element
        parts = [b_text]
        for sib in b.next_siblings:
            if getattr(sib, "name", None) in {"p", "b", "div"}:
                break
            if hasattr(sib, "get_text"):
                parts.append(sib.get_text(" ", strip=True))
            else:
                parts.append(str(sib).strip())
        rescued = " ".join(s for s in parts if s).strip()
        if rescued:
            blocks.append(rescued)
    return [b for b in blocks if b]


def _parse_canons(html: str) -> dict[int, list[str]]:
    """Return {canon_num: [paragraph_text, ...]} for all canons on a page.

    Handles the "revised Latin first, English original second" layout that
    appears on pages where the 2016 motu proprio *De concordia inter Codices*
    modified a canon: the page lists the new Latin text, an "[Earlier
    version]" divider, then the English original. We prefer the English
    original when both are present, so downstream theme/entity classifiers
    (which only read English) still have content.
    """
    soup = BeautifulSoup(html, "html.parser")
    blocks = _candidate_blocks(soup)

    canons: dict[int, list[str]] = {}
    revised_only: set[int] = set()  # canons whose captured text is revised Latin
    current: int | None = None
    current_is_revised = False
    past_earlier_divider = False

    for text in blocks:
        if EARLIER_VERSION_RE.search(text):
            past_earlier_divider = True
            current = None
            continue
        if RANGE_REF_RE.search(text) and not CANON_START_RE.match(text):
            # pure range reference inside a section header; ignore
            continue

        m = CANON_START_RE.match(text)
        if m:
            num = int(m.group(1))
            is_revised = bool(m.group("n")) and not past_earlier_divider
            body = text[m.end():].lstrip(" .\u00a0—–-")

            if num in canons:
                # Already have content. Only overwrite if the prior capture was
                # revised-Latin and this one isn't.
                if num in revised_only and not is_revised:
                    canons[num] = [body] if body else []
                    revised_only.discard(num)
                    current = num
                    current_is_revised = False
                else:
                    current = None  # skip this duplicate/revised repeat
                continue
            canons[num] = [body] if body else []
            current = num
            current_is_revised = is_revised
            if is_revised:
                revised_only.add(num)
        elif current is not None:
            # sub-paragraph (§2, 1/, etc.). Skip if it's a "n §" revised marker
            # line for a canon we already have the English version of.
            if REVISED_MARKER_RE.match(text) and not current_is_revised:
                continue
            canons[current].append(text)

    return canons


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    all_canons: dict[int, list[str]] = {}
    for page in CIC_PAGES:
        html = _fetch(page)
        parsed = _parse_canons(html)
        if parsed:
            # Merge: should be no overlap since each canon lives on one page
            for n, paras in parsed.items():
                if n in all_canons:
                    log.warning("  canon %d appeared on multiple pages; keeping first", n)
                else:
                    all_canons[n] = paras
        else:
            log.info("  %s: no canons parsed (likely introduction page)", page)

    log.info("parsed %d canons total (expected 1752)", len(all_canons))
    missing = [n for n in range(1, 1753) if n not in all_canons]
    if missing:
        log.warning("  missing canon numbers: %s%s",
                    missing[:20], "..." if len(missing) > 20 else "")

    # Write to DB
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Clear old sections, refresh doc row
        cur.execute("DELETE FROM document_sections WHERE document_id='cic'")
        cur.execute(
            """UPDATE documents
                 SET section_count=?, fetchable=0, available_langs_json=?,
                     citing_paragraphs_json='[]'
               WHERE id='cic'""",
            (len(all_canons), json.dumps(["en"])),
        )

        section_rows = [
            ("cic", str(n), "\n\n".join(paras), None, None)
            for n, paras in sorted(all_canons.items())
        ]
        cur.executemany(
            "INSERT INTO document_sections VALUES (?,?,?,?,?)",
            section_rows,
        )
        conn.commit()
        log.info("wrote %d CIC canon sections", len(section_rows))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
