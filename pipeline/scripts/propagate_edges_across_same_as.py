"""Propagate outbound ``cites`` edges across ``same_as`` pairs.

When two nodes are marked ``same_as``, any semantic edge leaving one of
them should also leave the other. This pass walks every ``same_as`` edge
and mirrors edges of selected types onto the twin so downstream queries
don't have to traverse through ``same_as`` manually.

Edge types mirrored (to the "minted" twin — the council section side):
  - ``cites`` → bible-verse, author, document, document-section

We skip ``child_of``, ``has_theme``, ``mentions``, ``same_as``, and
``part_of`` — those are structural/derived and get recomputed by their
own backfills.

Idempotent: uses ``INSERT OR IGNORE``.

Scope: currently only materialized-council sections (the 839 rows created
by ``materialize_council_sections_from_dh.py``). Identified by being the
**source** side of a ``same_as`` to a DH section.

Usage:
    uv run --project pipeline python -m pipeline.scripts.propagate_edges_across_same_as
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

HUNERMANN = "denzinger-hunermann"

MIRROR_EDGE_TYPES = {"cites"}

log = logging.getLogger("propagate_edges_across_same_as")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Load every same_as pair where one side is a DH section and the
        # other is a materialized council section.
        pairs = cur.execute(
            f"""
            SELECT source, target FROM graph_edges
             WHERE edge_type='same_as'
               AND target LIKE 'document-section:{HUNERMANN}/%'
               AND source LIKE 'document-section:%'
               AND source NOT LIKE 'document-section:{HUNERMANN}/%'
            """
        ).fetchall()
        log.info("found %d materialized-council ↔ DH same_as pairs", len(pairs))

        # Group: dh_node -> list of mint nodes (usually 1, but allow >1)
        dh_to_mints: dict[str, list[str]] = defaultdict(list)
        for mint, dh in pairs:
            dh_to_mints[dh].append(mint)

        # Collect all DH sources with their outbound cites edges.
        dh_nodes = list(dh_to_mints.keys())
        placeholders = ",".join("?" for _ in dh_nodes)
        dh_edges = cur.execute(
            f"""
            SELECT source, target, edge_type FROM graph_edges
             WHERE edge_type IN ({','.join('?' for _ in MIRROR_EDGE_TYPES)})
               AND source IN ({placeholders})
            """,
            (*MIRROR_EDGE_TYPES, *dh_nodes),
        ).fetchall() if dh_nodes else []
        log.info("  %d outbound edges on DH side to mirror", len(dh_edges))

        mirrored: list[tuple[str, str, str]] = []
        for src, tgt, etype in dh_edges:
            # Don't mirror edges whose target is the minted twin itself
            # (prevents trivial self-loops if any exist).
            for mint in dh_to_mints[src]:
                if tgt == mint:
                    continue
                mirrored.append((mint, tgt, etype))

        # De-dupe before insert
        unique_mirrored = set(mirrored)
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            list(unique_mirrored),
        )
        conn.commit()
        log.info("mirrored %d unique edges onto council sections",
                 len(unique_mirrored))

        # Post-check sample
        trent_bible = cur.execute(
            """
            SELECT COUNT(*) FROM graph_edges
             WHERE edge_type='cites'
               AND source LIKE 'document-section:trent/%'
               AND target LIKE 'bible-verse:%'
            """
        ).fetchone()[0]
        log.info("  post-check: document-section:trent/* → bible-verse:* = %d",
                 trent_bible)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
