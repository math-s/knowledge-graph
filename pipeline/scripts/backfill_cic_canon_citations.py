"""Rescan CCC footnotes for Code-of-Canon-Law citations and upgrade them
from document-level to canon-level addressing.

Existing ``paragraph_document_citations`` rows with ``document='cic'`` were
created by the original footnote parser, which recognised that "CIC" was
cited but didn't capture the canon number. Those 87 rows all have
``section IS NULL``. This script rescans each paragraph's
``footnotes_json`` for canon references in forms like:

  - "CIC, can. 748 # 2"
  - "CIC, Can. 208"
  - "CIC, cann. 368-369"  (range → emits one citation per canon)
  - "CIC Can. 204 para 1"

Writes section-numbered citations and emits
``p:<N> -> document-section:cic/<canon>`` edges wherever the canon exists
in ``document_sections`` (per the paragraph-level preference).

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_cic_canon_citations
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("backfill_cic_canon_citations")


# Node-id helpers (single source of truth).
def p_node(paragraph_id: int) -> str:
    return f"p:{paragraph_id}"


def doc_node(slug: str) -> str:
    return f"document:{slug}"


def doc_section_node(slug: str, section: str) -> str:
    return f"document-section:{slug}/{section}"


# Matches a CIC canon reference. Accepts:
#   "CIC, can. 748"        -> single
#   "CIC, Can. 208"        -> single
#   "CIC, cann. 368-369"   -> range (emit both endpoints)
#   "CIC Can. 204 para 1"  -> single (paragraph sub-id not yet modelled)
#   "CIC can. 748 # 2"     -> single (§ handled by dropping # 2 suffix)
CIC_REF_RE = re.compile(
    r"""\bCIC
        (?:[\s,]+)?                # optional separator
        (?:Cann?\.?|cann?\.?)\s*   # Can. / can. / Cann. / cann.
        (?P<start>\d{1,4})
        (?:\s*[-–—]\s*(?P<end>\d{1,4}))?   # optional end of range
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_cic_canons(text: str) -> set[int]:
    """Return the set of canon numbers referenced in *text*."""
    out: set[int] = set()
    for m in CIC_REF_RE.finditer(text):
        start = int(m.group("start"))
        end = int(m.group("end")) if m.group("end") else start
        if end < start or end - start > 200:
            # Guard against absurd ranges
            end = start
        out.update(range(start, end + 1))
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        valid_canons = {
            row[0]
            for row in cur.execute(
                "SELECT section_num FROM document_sections WHERE document_id='cic'"
            )
        }
        log.info("CIC has %d valid canon sections", len(valid_canons))

        # Remove any prior CIC citations so we can repopulate idempotently.
        cur.execute("DELETE FROM paragraph_document_citations WHERE document='cic'")
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type='cites' AND (target='document:cic' OR target LIKE 'document-section:cic/%')"
        )

        new_citation_rows: list[tuple[int, str, str | None]] = []
        new_edge_rows: set[tuple[str, str, str]] = set()
        citing_paragraphs: set[int] = set()
        doc_level_only: set[int] = set()
        section_hits = 0

        rows = cur.execute(
            "SELECT id, footnotes_json FROM paragraphs WHERE footnotes_json LIKE '%CIC%'"
        ).fetchall()
        for p_id, footnotes_json in rows:
            try:
                fns = json.loads(footnotes_json or "[]")
            except json.JSONDecodeError:
                continue
            combined = " \n ".join(str(f) for f in fns)
            canons = parse_cic_canons(combined)
            if not canons:
                continue
            citing_paragraphs.add(p_id)

            resolved = {c for c in canons if str(c) in valid_canons}
            unresolved = canons - resolved

            for c in sorted(resolved):
                new_citation_rows.append((p_id, "cic", str(c)))
                new_edge_rows.add(
                    (p_node(p_id), doc_section_node("cic", str(c)), "cites")
                )
                section_hits += 1
            if unresolved and not resolved:
                # No canons in our captured 1749 — fall back to doc-level
                new_citation_rows.append((p_id, "cic", None))
                new_edge_rows.add((p_node(p_id), doc_node("cic"), "cites"))
                doc_level_only.add(p_id)

        cur.executemany(
            "INSERT INTO paragraph_document_citations(paragraph_id, document, section) VALUES (?,?,?)",
            new_citation_rows,
        )
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            list(new_edge_rows),
        )
        cur.execute(
            "UPDATE documents SET citing_paragraphs_json=? WHERE id='cic'",
            (json.dumps(sorted(citing_paragraphs)),),
        )
        conn.commit()

        log.info("scanned %d CCC paragraphs with 'CIC' in footnotes", len(rows))
        log.info("  %d citing paragraphs", len(citing_paragraphs))
        log.info("  %d section-level citations (p:N -> document-section:cic/<canon>)",
                 section_hits)
        log.info("  %d paragraphs fell back to document-level (canon outside our 1749)",
                 len(doc_level_only))
        log.info("  %d total citation rows", len(new_citation_rows))
        log.info("  %d total cites edges", len(new_edge_rows))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
