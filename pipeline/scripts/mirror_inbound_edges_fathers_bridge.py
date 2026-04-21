"""Mirror inbound ``cites`` edges from ``fathers-page:*`` onto the bridged
``patristic-work:*`` node.

Complement to ``bridge_fathers_page_to_patristic_work.py``: once the
same_as bridges exist, we want queries against the structured patristic
side (e.g. "what CCC paragraphs cite patristic-work:augustine/1701?")
to return the same results as queries against the legacy page side.

For every ``same_as`` edge ``fathers-page:X -> patristic-work:Y``:
  - find every inbound ``cites`` edge ``N -> fathers-page:X``
  - emit ``N -> patristic-work:Y`` (edge_type=cites)

Idempotent via ``INSERT OR IGNORE``. Does not delete anything — the legacy
fathers-page edges remain and stay queryable.

Usage:
    uv run --project pipeline python -m pipeline.scripts.mirror_inbound_edges_fathers_bridge
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("mirror_inbound_edges_fathers_bridge")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Build the bridge map: fathers-page node → patristic-work node
        bridges = dict(cur.execute(
            """
            SELECT source, target FROM graph_edges
             WHERE edge_type='same_as'
               AND source LIKE 'fathers-page:%'
               AND target LIKE 'patristic-work:%'
            """
        ).fetchall())
        log.info("active bridges: %d", len(bridges))
        if not bridges:
            return 0

        # Gather every inbound cites edge whose target is one of our bridged pages.
        page_nodes = list(bridges.keys())
        placeholders = ",".join("?" for _ in page_nodes)
        inbound = cur.execute(
            f"""
            SELECT source, target FROM graph_edges
             WHERE edge_type='cites' AND target IN ({placeholders})
            """,
            page_nodes,
        ).fetchall()
        log.info("inbound cites edges to bridged pages: %d", len(inbound))

        mirrored: set[tuple[str, str, str]] = {
            (src, bridges[tgt], "cites") for src, tgt in inbound
        }
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            list(mirrored),
        )
        conn.commit()
        log.info("wrote %d mirrored inbound edges to patristic-work side",
                 len(mirrored))

        # Sample query showing new reachability
        cnt = cur.execute(
            """
            SELECT COUNT(DISTINCT source) FROM graph_edges
             WHERE edge_type='cites' AND target LIKE 'patristic-work:augustine/%'
            """
        ).fetchone()[0]
        log.info("  e.g., distinct nodes citing any Augustine patristic-work: %d", cnt)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
