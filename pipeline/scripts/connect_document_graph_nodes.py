"""Backfill missing ``graph_nodes`` and ``child_of`` edges for the
``documents`` / ``document_sections`` subgraph.

Gaps that motivated this:
  - The library → documents migration wrote 175 new rows to ``documents``
    without creating corresponding ``document:<id>`` graph nodes.
  - Almost no ``document_sections`` had matching ``document-section:<id>/<n>``
    graph nodes (only ~355 of 9500 did, and none had ``child_of`` edges
    linking them back to their parent document node).

After this runs, every row in ``documents`` has a ``document:<id>`` node and
every row in ``document_sections`` has a ``document-section:<doc_id>/<n>``
node wired to its parent via a ``child_of`` edge.

Usage:
    uv run --project pipeline python -m pipeline.scripts.connect_document_graph_nodes
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("connect_document_graph_nodes")

# Colors / sizes copied from the existing encyclical palette
DOC_COLOR = "#E6A23C"
DOC_SIZE = 6.0
SECTION_COLOR = "#F0C78C"
SECTION_SIZE = 2.5


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # --- pass 1: document: nodes ---
        docs = cur.execute("SELECT id, name FROM documents").fetchall()
        doc_rows = [
            (
                f"document:{did}",
                name or did,
                "document",
                0.0, 0.0, DOC_SIZE, DOC_COLOR, "",
                0, 0, "[]", "[]", "[]",
            )
            for did, name in docs
        ]
        cur.executemany(
            "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            doc_rows,
        )
        added_docs = cur.rowcount
        log.info("document nodes: %d processed, %d newly inserted", len(doc_rows), added_docs)

        # --- pass 2: document-section: nodes ---
        sections = cur.execute(
            "SELECT document_id, section_num FROM document_sections"
        ).fetchall()
        section_rows = [
            (
                f"document-section:{did}/{num}",
                f"{did} §{num}",
                "document-section",
                0.0, 0.0, SECTION_SIZE, SECTION_COLOR, "",
                0, 0, "[]", "[]", "[]",
            )
            for did, num in sections
        ]
        cur.executemany(
            "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            section_rows,
        )
        added_sections = cur.rowcount
        log.info("document-section nodes: %d processed, %d newly inserted", len(section_rows), added_sections)

        # --- pass 3: child_of edges section -> document ---
        edge_rows = [
            (f"document-section:{did}/{num}", f"document:{did}", "child_of")
            for did, num in sections
        ]
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows,
        )
        added_edges = cur.rowcount
        log.info("child_of edges: %d processed, %d newly inserted", len(edge_rows), added_edges)

        conn.commit()

        # Post-check
        dangling_docs = cur.execute(
            """
            SELECT COUNT(*) FROM documents d
            LEFT JOIN graph_nodes n ON n.id = 'document:' || d.id
            WHERE n.id IS NULL
            """
        ).fetchone()[0]
        dangling_sections = cur.execute(
            """
            SELECT COUNT(*) FROM document_sections ds
            LEFT JOIN graph_nodes n
              ON n.id = 'document-section:' || ds.document_id || '/' || ds.section_num
            WHERE n.id IS NULL
            """
        ).fetchone()[0]
        log.info("post-check: %d documents still missing nodes, %d sections still missing nodes",
                 dangling_docs, dangling_sections)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
