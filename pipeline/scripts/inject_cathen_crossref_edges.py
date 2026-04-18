"""Inject article→article cross-reference edges into the graph.

Reads ``encyclopedia_cross_refs`` (populated by
``refresh_encyclopedia_from_newadvent``) and adds an ``ency_cross_reference``
edge for each (src, tgt) where both articles exist as graph nodes.

Edges are canonicalized undirected (sorted endpoint pair) to avoid storing
both (A,B) and (B,A) when article A references B and B references A.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_cathen_crossref_edges
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("inject_cathen_crossref")


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    # Encyclopedia node set
    ency_ids = {
        row[0].removeprefix("ency:")
        for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='encyclopedia'"
        )
    }
    log.info("Graph has %d encyclopedia nodes", len(ency_ids))

    rows = conn.execute(
        "SELECT source_id, target_id FROM encyclopedia_cross_refs"
    ).fetchall()
    log.info("Loaded %d raw cross-ref rows", len(rows))

    edge_set: set[tuple[str, str]] = set()
    skipped_missing = 0
    for src, tgt in rows:
        if src not in ency_ids or tgt not in ency_ids:
            skipped_missing += 1
            continue
        if src == tgt:
            continue
        a, b = sorted((src, tgt))
        edge_set.add((f"ency:{a}", f"ency:{b}"))

    log.info(
        "Computed %d unique edges (skipped %d with missing endpoints)",
        len(edge_set), skipped_missing,
    )

    edge_rows = [(a, b, "ency_cross_reference") for a, b in edge_set]

    before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='ency_cross_reference'"
    ).fetchone()[0]
    CHUNK = 50_000
    for i in range(0, len(edge_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows[i : i + CHUNK],
        )
        conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='ency_cross_reference'"
    ).fetchone()[0]
    added = after - before

    # Degree bump — edge_set already contains fully-prefixed node IDs
    deg: dict[str, int] = defaultdict(int)
    for a, b in edge_set:
        deg[a] += 1
        deg[b] += 1
    deg_items = list(deg.items())
    for i in range(0, len(deg_items), CHUNK):
        cur.executemany(
            "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
            [(inc, nid) for nid, inc in deg_items[i : i + CHUNK]],
        )
        conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    avg_deg = conn.execute("SELECT AVG(degree) FROM graph_nodes").fetchone()[0]
    conn.close()

    log.info("")
    log.info("Inserted %d new ency_cross_reference edges", added)
    log.info("DB now has %d nodes, %d edges (avg degree %.1f)",
             total_nodes, total_edges, avg_deg)


if __name__ == "__main__":
    main()
