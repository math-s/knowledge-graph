"""Rebuild the ``document-section`` slice of ``search_fts``.

Background: ``search_fts`` is the unified full-text index over CCC paragraphs,
documents, sections, authors, bible books, and patristic works. It was last
populated with 355 document-section rows, but the DB now holds 17,535 of
them (after the library migration, CIC injection, council stubs, and the
Denzinger ingests). This script deletes the stale document-section rows and
re-inserts one row per ``document_sections`` row with:

  - entry_id   = ``document-section:<doc_id>/<section_num>``
  - entry_type = ``document-section``
  - text_en    = ``text_en``
  - text_la    = ``text_la``
  - text_pt    = ``text_pt``
  - themes     = space-separated theme ids from ``graph_edges`` (``has_theme``)

Idempotent: only touches the document-section slice. Other entry types
(paragraph, document, author, bible-book, patristic-work) are left alone.

Usage:
    uv run --project pipeline python -m pipeline.scripts.rebuild_document_sections_fts
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("rebuild_document_sections_fts")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Build a {node_id: " theme1 theme2 ..." } lookup from graph_edges.
        log.info("loading themes per section…")
        themes_by_node: dict[str, list[str]] = {}
        for src, tgt in cur.execute(
            """
            SELECT source, target FROM graph_edges
             WHERE edge_type='has_theme'
               AND source LIKE 'document-section:%'
            """
        ):
            themes_by_node.setdefault(src, []).append(
                tgt[len("theme:"):] if tgt.startswith("theme:") else tgt
            )
        log.info("  %d sections have theme tags", len(themes_by_node))

        # Load every document_sections row.
        rows = cur.execute(
            "SELECT document_id, section_num, text_en, text_la, text_pt "
            "  FROM document_sections"
        ).fetchall()
        log.info("loaded %d document_sections rows", len(rows))

        # Replace the document-section slice.
        cur.execute(
            "DELETE FROM search_fts WHERE entry_type='document-section'"
        )
        before = cur.rowcount
        log.info("  deleted %d stale document-section rows from search_fts", before)

        new_rows: list[tuple] = []
        for doc_id, section_num, t_en, t_la, t_pt in rows:
            entry_id = f"document-section:{doc_id}/{section_num}"
            themes = " ".join(sorted(themes_by_node.get(entry_id, [])))
            new_rows.append((
                entry_id,
                "document-section",
                t_en or "",
                t_la or "",
                t_pt or "",
                themes,
            ))
        cur.executemany(
            "INSERT INTO search_fts (entry_id, entry_type, text_en, text_la, text_pt, themes) "
            "VALUES (?,?,?,?,?,?)",
            new_rows,
        )
        log.info("  inserted %d document-section rows", len(new_rows))

        conn.commit()

        # Post-check: how many Denzinger sections are searchable now?
        denz = cur.execute(
            "SELECT COUNT(*) FROM search_fts "
            " WHERE entry_type='document-section' "
            "   AND entry_id LIKE 'document-section:denzinger-%'"
        ).fetchone()[0]
        log.info("  %d Denzinger sections now in FTS", denz)

        # Quick smoke test
        sample = cur.execute(
            "SELECT entry_id FROM search_fts "
            " WHERE entry_type='document-section' AND text_pt MATCH 'transubstanciação' LIMIT 3"
        ).fetchall()
        log.info("  smoke: sections matching 'transubstanciação': %s", [s[0] for s in sample])
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
