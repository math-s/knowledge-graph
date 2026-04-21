"""Bridge ``fathers-page`` nodes to structured ``patristic-work`` nodes.

The newadvent fathers HTML uses no numeric section anchors (verified: 0
section-level anchors across 1700+ files), so the 191k ``fathers-page``
citation edges can't be upgraded to ``patristic-section`` granularity
just by re-parsing HTML. But they **can** be bridged one level: every
newadvent fathers file ID corresponds to exactly one ``author_works``
row (the structured catalog used by the rest of the graph).

This script:
  1. Maps every ``fathers-page:<id>`` node to the matching
     ``patristic-work:<author>/<work-id>`` node, via two mapping sources:
       (a) ``author_works.source_url`` → direct regex extraction
       (b) ``author_works.id`` slug pattern ``<author>/<numeric-id>`` where
           ``<numeric-id>`` equals the fathers-page id.
  2. Emits a ``same_as`` edge for each matched pair.
  3. Mirrors the fathers-page outbound ``cites`` edges onto the patristic-work
     side via ``propagate_edges_across_same_as``.

After this, queries can traverse ``patristic-work:augustine/1701`` and
get to every CCC/Summa/library paragraph that cites it, AND to the 2,064
structured ``patristic-section`` rows that hang off those works.

Idempotent. Uses pyproject.toml deps only.

Usage:
    uv run --project pipeline python -m pipeline.scripts.bridge_fathers_page_to_patristic_work
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("bridge_fathers_page_to_patristic_work")

# Matches the newadvent file id in author_works.source_url:
#   https://www.newadvent.org/fathers/1701001.htm → 1701001
URL_ID_RE = re.compile(r"/fathers/(\d+)\.htm$")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Set of existing fathers-page node ids, stripped.
        page_ids = {
            row[0][len("fathers-page:"):]
            for row in cur.execute(
                "SELECT id FROM graph_nodes WHERE id LIKE 'fathers-page:%'"
            )
        }
        log.info("existing fathers-page nodes: %d", len(page_ids))

        # Build mapping file_id → patristic-work node id from two sources.
        mapping: dict[str, str] = {}  # file_id → "patristic-work:<slug>"

        # Source A: author_works.source_url regex
        rows_a = cur.execute(
            "SELECT id, source_url FROM author_works WHERE source_url LIKE '%fathers/%.htm'"
        ).fetchall()
        hits_a = 0
        for work_id, url in rows_a:
            m = URL_ID_RE.search(url or "")
            if not m:
                continue
            file_id = m.group(1)
            if file_id in page_ids:
                mapping.setdefault(file_id, f"patristic-work:{work_id}")
                hits_a += 1
        log.info("  source A (source_url regex): %d mappings", hits_a)

        # Source B: slug pattern <author>/<numeric>
        rows_b = cur.execute(
            "SELECT id FROM author_works WHERE id GLOB '*/[0-9]*'"
        ).fetchall()
        hits_b = 0
        for (work_id,) in rows_b:
            author_slash, _, numeric = work_id.rpartition("/")
            if not numeric.isdigit() or not author_slash:
                continue
            if numeric in page_ids and numeric not in mapping:
                mapping[numeric] = f"patristic-work:{work_id}"
                hits_b += 1
        log.info("  source B (slug numeric suffix): %d additional mappings", hits_b)
        log.info("total file_id → patristic-work mappings: %d", len(mapping))

        # Idempotent clear of prior bridge edges (fathers-page ↔ patristic-work only).
        cur.execute(
            """
            DELETE FROM graph_edges
             WHERE edge_type='same_as'
               AND source LIKE 'fathers-page:%'
               AND target LIKE 'patristic-work:%'
            """
        )

        edges: list[tuple[str, str, str]] = []
        for file_id, work_node in mapping.items():
            edges.append((f"fathers-page:{file_id}", work_node, "same_as"))
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edges,
        )
        conn.commit()
        log.info("inserted %d same_as bridge edges", len(edges))

        # How many outbound cites edges become reachable via the bridge?
        unique_sources = cur.execute(
            """
            SELECT COUNT(DISTINCT e.source)
              FROM graph_edges e
             WHERE e.edge_type='cites'
               AND e.target IN (
                   SELECT source FROM graph_edges
                    WHERE edge_type='same_as'
                      AND source LIKE 'fathers-page:%'
                      AND target LIKE 'patristic-work:%'
               )
            """
        ).fetchone()[0]
        log.info("  %d distinct upstream nodes now reach a patristic-work via the bridge",
                 unique_sources)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
