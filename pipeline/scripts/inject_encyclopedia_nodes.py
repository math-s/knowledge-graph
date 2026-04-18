"""One-shot: inject encyclopedia nodes + discussed_in edges into knowledge-graph.db.

Mirrors the logic of pipeline.src.graph_builder.add_encyclopedia_nodes but
writes directly to the already-exported SQLite graph, bypassing the full
pipeline rerun. New nodes get layout (0, 0) — re-layout is a separate step.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_encyclopedia_nodes
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.entity_extraction import ENTITY_DEFINITIONS  # noqa: E402

ENCYCLOPEDIA_COLOR = "#C77B8A"
DEFAULT_SIZE = 4.0


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    existing_ency = conn.execute(
        "SELECT COUNT(*) FROM graph_nodes WHERE node_type = 'encyclopedia'"
    ).fetchone()[0]
    if existing_ency:
        print(f"Found {existing_ency} existing encyclopedia nodes — will skip duplicates.")

    articles = conn.execute(
        "SELECT id, title FROM encyclopedia"
    ).fetchall()
    if not articles:
        raise SystemExit("No encyclopedia articles in DB.")
    print(f"Loaded {len(articles)} encyclopedia articles")

    # Entity id -> label (from curated definitions)
    entity_label_by_id = {e.id: e.label for e in ENTITY_DEFINITIONS}

    # Normalized title -> article row (prefer shortest title on ambiguity)
    by_title: dict[str, sqlite3.Row] = {}
    for row in articles:
        key = (row["title"] or "").strip().lower()
        if not key:
            continue
        existing = by_title.get(key)
        if existing is None or len(row["title"]) < len(existing["title"]):
            by_title[key] = row

    # Build edges: CCC paragraph -> encyclopedia article via matching entity label
    pe_rows = conn.execute(
        "SELECT paragraph_id, entity_id FROM paragraph_entities"
    ).fetchall()
    print(f"Loaded {len(pe_rows)} paragraph-entity rows")

    edges: set[tuple[str, str]] = set()
    for r in pe_rows:
        label = entity_label_by_id.get(r["entity_id"])
        if not label:
            continue
        match = by_title.get(label.strip().lower())
        if not match:
            continue
        edges.add((f"p:{r['paragraph_id']}", f"ency:{match['id']}"))

    print(f"Computed {len(edges)} discussed_in edges")

    # Degree for each encyclopedia node = number of incoming discussed_in edges
    degree_by_node: dict[str, int] = {}
    for _, target in edges:
        degree_by_node[target] = degree_by_node.get(target, 0) + 1

    # Build node rows
    node_rows = []
    empty_json = json.dumps([])
    for row in articles:
        node_id = f"ency:{row['id']}"
        node_rows.append((
            node_id,
            row["title"] or row["id"],
            "encyclopedia",
            0.0,
            0.0,
            DEFAULT_SIZE,
            ENCYCLOPEDIA_COLOR,
            "",
            degree_by_node.get(node_id, 0),
            0,
            empty_json,
            empty_json,
            empty_json,
        ))

    edge_rows = [(src, tgt, "discussed_in") for (src, tgt) in edges]

    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    nodes_added = cur.rowcount if cur.rowcount is not None else 0
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        edge_rows,
    )
    edges_added = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()

    # Bump paragraph degrees by the number of new discussed_in edges each one receives
    paragraph_new_degree: dict[str, int] = {}
    for src, _ in edges:
        paragraph_new_degree[src] = paragraph_new_degree.get(src, 0) + 1
    cur.executemany(
        "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
        [(inc, nid) for nid, inc in paragraph_new_degree.items()],
    )
    conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    total_ency = conn.execute(
        "SELECT COUNT(*) FROM graph_nodes WHERE node_type = 'encyclopedia'"
    ).fetchone()[0]
    total_discussed = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type = 'discussed_in'"
    ).fetchone()[0]
    conn.close()

    print()
    print(f"Inserted {nodes_added} new nodes, {edges_added} new edges")
    print(f"DB now has: {total_nodes} nodes, {total_edges} edges")
    print(f"  encyclopedia nodes: {total_ency}")
    print(f"  discussed_in edges: {total_discussed}")


if __name__ == "__main__":
    main()
