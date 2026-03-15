"""Graph endpoints — per-theme subgraph queries."""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, Query

from ..db import get_db

router = APIRouter(prefix="/graph", tags=["graph"])

# Edge types that produce very dense subgraphs; excluded by default
DENSE_EDGE_TYPES = {"shared_topic"}


@router.get("/themes")
def list_themes(db: sqlite3.Connection = Depends(get_db)):
    """List all themes with paragraph counts."""
    rows = db.execute(
        "SELECT id, label, count FROM themes ORDER BY count DESC"
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/theme/{theme_id}")
def graph_by_theme(
    theme_id: str,
    include_dense: bool = Query(False, description="Include high-cardinality edge types (shared_topic)"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for a theme: themed paragraphs + 1-hop neighbors + edges."""

    # 1. Find seed paragraph node IDs
    seed_rows = db.execute(
        "SELECT 'p:' || paragraph_id AS node_id FROM paragraph_themes WHERE theme_id = ?",
        (theme_id,),
    ).fetchall()
    seed_ids = {r["node_id"] for r in seed_rows}

    if not seed_ids:
        return {"theme": theme_id, "nodes": [], "edges": []}

    placeholders = ",".join("?" for _ in seed_ids)
    seed_list = list(seed_ids)

    # 2. Find all edges touching seed nodes
    edge_rows = db.execute(
        f"""
        SELECT source, target, edge_type FROM graph_edges
        WHERE source IN ({placeholders}) OR target IN ({placeholders})
        """,
        seed_list + seed_list,
    ).fetchall()

    # Filter dense edge types unless requested
    excluded = set() if include_dense else DENSE_EDGE_TYPES
    edges = []
    neighbor_ids: set[str] = set()
    for r in edge_rows:
        if r["edge_type"] in excluded:
            continue
        edges.append({
            "source": r["source"],
            "target": r["target"],
            "edge_type": r["edge_type"],
        })
        neighbor_ids.add(r["source"])
        neighbor_ids.add(r["target"])

    # 3. Collect all node IDs (seeds + neighbors)
    all_ids = seed_ids | neighbor_ids
    id_list = list(all_ids)
    ph = ",".join("?" for _ in id_list)

    node_rows = db.execute(
        f"""
        SELECT id, label, node_type, x, y, size, color, part, degree, community,
               themes_json, entities_json
        FROM graph_nodes WHERE id IN ({ph})
        """,
        id_list,
    ).fetchall()

    nodes = []
    for r in node_rows:
        nodes.append({
            "id": r["id"],
            "label": r["label"],
            "node_type": r["node_type"],
            "x": r["x"],
            "y": r["y"],
            "size": r["size"],
            "color": r["color"],
            "part": r["part"],
            "degree": r["degree"],
            "community": r["community"],
            "themes": json.loads(r["themes_json"] or "[]"),
            "entities": json.loads(r["entities_json"] or "[]"),
            "is_seed": r["id"] in seed_ids,
        })

    return {
        "theme": theme_id,
        "seed_count": len(seed_ids),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/community/{community_id}")
def graph_by_community(
    community_id: int,
    include_dense: bool = Query(False),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for a community."""
    seed_rows = db.execute(
        "SELECT id FROM graph_nodes WHERE community = ?",
        (community_id,),
    ).fetchall()
    seed_ids = {r["id"] for r in seed_rows}

    if not seed_ids:
        return {"community": community_id, "nodes": [], "edges": []}

    # Get edges between community members only (no expansion)
    id_list = list(seed_ids)
    ph = ",".join("?" for _ in id_list)

    excluded = set() if include_dense else DENSE_EDGE_TYPES
    edge_rows = db.execute(
        f"""
        SELECT source, target, edge_type FROM graph_edges
        WHERE source IN ({ph}) AND target IN ({ph})
        """,
        id_list + id_list,
    ).fetchall()

    edges = [
        {"source": r["source"], "target": r["target"], "edge_type": r["edge_type"]}
        for r in edge_rows
        if r["edge_type"] not in excluded
    ]

    node_rows = db.execute(
        f"""
        SELECT id, label, node_type, x, y, size, color, part, degree, community,
               themes_json, entities_json
        FROM graph_nodes WHERE id IN ({ph})
        """,
        id_list,
    ).fetchall()

    nodes = [
        {
            "id": r["id"],
            "label": r["label"],
            "node_type": r["node_type"],
            "x": r["x"],
            "y": r["y"],
            "size": r["size"],
            "color": r["color"],
            "part": r["part"],
            "degree": r["degree"],
            "community": r["community"],
            "themes": json.loads(r["themes_json"] or "[]"),
            "entities": json.loads(r["entities_json"] or "[]"),
        }
        for r in node_rows
    ]

    return {
        "community": community_id,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/stats")
def graph_stats(db: sqlite3.Connection = Depends(get_db)):
    """Return summary statistics about the graph."""
    node_counts = db.execute(
        "SELECT node_type, COUNT(*) as count FROM graph_nodes GROUP BY node_type ORDER BY count DESC"
    ).fetchall()
    edge_counts = db.execute(
        "SELECT edge_type, COUNT(*) as count FROM graph_edges GROUP BY edge_type ORDER BY count DESC"
    ).fetchall()
    communities = db.execute(
        "SELECT COUNT(DISTINCT community) as count FROM graph_nodes"
    ).fetchone()

    return {
        "total_nodes": sum(r["count"] for r in node_counts),
        "total_edges": sum(r["count"] for r in edge_counts),
        "total_communities": communities["count"],
        "nodes_by_type": {r["node_type"]: r["count"] for r in node_counts},
        "edges_by_type": {r["edge_type"]: r["count"] for r in edge_counts},
    }
