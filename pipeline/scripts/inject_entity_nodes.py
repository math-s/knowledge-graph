"""Promote entity definitions to first-class graph nodes.

Adds one ``entity:<id>`` node per entry in ENTITY_DEFINITIONS and wires
``mentions`` edges from every CCC paragraph or encyclopedia article that
references it. This replaces the implicit hub structure (pairwise
shared_entity edges) with an explicit hub — mega-entities like 'grace' or
'church' now have O(N) fanout from a single node instead of O(N²) pairwise
cliques.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_entity_nodes
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.entity_extraction import ENTITY_DEFINITIONS  # noqa: E402

# One color per category — muted palette, distinct from existing node colors
CATEGORY_COLORS: dict[str, str] = {
    "trinitarian":   "#8E6E95",
    "christology":   "#6E8EB0",
    "sacraments":    "#B07A5A",
    "ecclesiology":  "#6B9E7E",
    "soteriology":   "#C4847C",
    "moral":         "#B0A36E",
    "virtues":       "#7EA89E",
    "eschatology":   "#9E7EA8",
    "mariology":     "#C48FA6",
    "liturgy":       "#8FA6C4",
    "prayer":        "#A6C48F",
    "anthropology":  "#A89E7E",
    "revelation":    "#C4B08F",
}
DEFAULT_COLOR = "#999999"
SIZE = 8.0

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("inject_entity_nodes")


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    empty_json = json.dumps([])

    # --- Build node rows ----------------------------------------------------
    node_rows = []
    for e in ENTITY_DEFINITIONS:
        color = CATEGORY_COLORS.get(e.category, DEFAULT_COLOR)
        node_rows.append((
            f"entity:{e.id}",
            e.label,
            "entity",
            0.0, 0.0,
            SIZE,
            color,
            e.category,  # reuse `part` column for category tag
            0,           # degree — bumped below
            0,           # community
            empty_json,
            empty_json,
            empty_json,
        ))
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    conn.commit()
    log.info("Added/ignored %d entity nodes", len(node_rows))

    # --- Gather mention edges ----------------------------------------------
    edges: list[tuple[str, str, str]] = []

    pe = conn.execute("SELECT paragraph_id, entity_id FROM paragraph_entities").fetchall()
    for pid, eid in pe:
        edges.append((f"p:{pid}", f"entity:{eid}", "mentions"))
    log.info("Paragraph-entity mentions: %d", len(pe))

    if conn.execute(
        "SELECT name FROM sqlite_master WHERE name='encyclopedia_entities'"
    ).fetchone():
        ee = conn.execute(
            "SELECT article_id, entity_id FROM encyclopedia_entities"
        ).fetchall()
        for aid, eid in ee:
            edges.append((f"ency:{aid}", f"entity:{eid}", "mentions"))
        log.info("Article-entity mentions: %d", len(ee))
    else:
        log.info("No encyclopedia_entities table — skipping article-entity edges")

    # --- Insert edges -------------------------------------------------------
    before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='mentions'"
    ).fetchone()[0]
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        edges,
    )
    conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='mentions'"
    ).fetchone()[0]
    added = after - before

    # --- Bump degrees -------------------------------------------------------
    deg: dict[str, int] = defaultdict(int)
    for src, tgt, _ in edges:
        deg[src] += 1
        deg[tgt] += 1
    cur.executemany(
        "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
        [(inc, nid) for nid, inc in deg.items()],
    )
    conn.commit()

    # --- Report -------------------------------------------------------------
    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    entity_count = conn.execute(
        "SELECT COUNT(*) FROM graph_nodes WHERE node_type='entity'"
    ).fetchone()[0]

    # Top hubs by degree
    top_hubs = conn.execute("""
        SELECT label, degree FROM graph_nodes
        WHERE node_type='entity'
        ORDER BY degree DESC LIMIT 10
    """).fetchall()
    conn.close()

    log.info("")
    log.info("Inserted %d new mentions edges", added)
    log.info("DB now has %d nodes, %d edges", total_nodes, total_edges)
    log.info("Entity nodes: %d", entity_count)
    log.info("Top entity hubs:")
    for label, deg in top_hubs:
        log.info("  %-28s  %d mentions", label, deg)


if __name__ == "__main__":
    main()
