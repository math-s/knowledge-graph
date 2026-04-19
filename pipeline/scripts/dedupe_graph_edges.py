"""Deduplicate graph_edges and add a UNIQUE (source, target, edge_type) PK.

The original ``graph_edges`` table had no unique constraint, so the many
``INSERT OR IGNORE`` calls across injection scripts could not actually
deduplicate — they only skipped on primary-key conflict that didn't exist.
Duplicate rows silently accumulated (~284k in practice), inflating degree
counts and polluting query results with phantom repeats.

This script is idempotent: re-running it after the fix is a no-op.

  * Rebuilds graph_edges with PRIMARY KEY (source, target, edge_type)
  * Preserves all unique rows via INSERT OR IGNORE
  * Adds source / target / edge_type indexes
  * Rebuilds the ``degree`` column on graph_nodes from the deduplicated edges

Usage:
    uv run --project pipeline python -m pipeline.scripts.dedupe_graph_edges
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("dedupe_graph_edges")


def has_primary_key(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='graph_edges'"
    ).fetchone()
    return bool(row and "PRIMARY KEY" in row[0].upper())


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    before = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    log.info("graph_edges rows before: %d", before)

    if has_primary_key(conn):
        log.info("graph_edges already has a PRIMARY KEY — nothing to migrate")
    else:
        cur.executescript(
            """
            BEGIN;
            CREATE TABLE graph_edges_new (
                source    TEXT NOT NULL,
                target    TEXT NOT NULL,
                edge_type TEXT NOT NULL,
                PRIMARY KEY (source, target, edge_type)
            );
            INSERT OR IGNORE INTO graph_edges_new (source, target, edge_type)
            SELECT source, target, edge_type FROM graph_edges;
            DROP TABLE graph_edges;
            ALTER TABLE graph_edges_new RENAME TO graph_edges;
            CREATE INDEX IF NOT EXISTS idx_graph_edges_src  ON graph_edges(source);
            CREATE INDEX IF NOT EXISTS idx_graph_edges_tgt  ON graph_edges(target);
            CREATE INDEX IF NOT EXISTS idx_graph_edges_type ON graph_edges(edge_type);
            COMMIT;
            """
        )

    after = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    log.info("graph_edges rows after:  %d (removed %d duplicates)",
             after, before - after)

    # Rebuild degree from scratch so it reflects deduplicated edges
    log.info("Rebuilding graph_nodes.degree from deduplicated edges...")
    deg: dict[str, int] = defaultdict(int)
    for src, tgt in conn.execute("SELECT source, target FROM graph_edges"):
        deg[src] += 1
        deg[tgt] += 1
    cur.execute("UPDATE graph_nodes SET degree = 0")
    CHUNK = 50_000
    items = list(deg.items())
    for i in range(0, len(items), CHUNK):
        cur.executemany(
            "UPDATE graph_nodes SET degree = ? WHERE id = ?",
            [(d, nid) for nid, d in items[i : i + CHUNK]],
        )
    conn.commit()

    avg = conn.execute("SELECT AVG(degree) FROM graph_nodes").fetchone()[0]
    log.info("Avg degree: %.1f", avg)
    conn.close()


if __name__ == "__main__":
    main()
