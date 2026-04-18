"""Graph endpoints — per-theme, per-paragraph, and multi-theme subgraph queries."""

from __future__ import annotations

import json
import sqlite3

from typing import Optional

from fastapi import APIRouter, Depends, Query

from ..db import get_db

router = APIRouter(prefix="/graph", tags=["graph"])

# Edge types that produce very dense subgraphs; excluded by default
DENSE_EDGE_TYPES = {"shared_topic"}


def _format_node(r: sqlite3.Row, seed_ids: set[str] | None = None) -> dict:
    return {
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
        "topics": json.loads(r["topics_json"] or "[]"),
        **({"is_seed": r["id"] in seed_ids} if seed_ids is not None else {}),
    }


def _paginated_seeds(
    db: sqlite3.Connection,
    base_sql: str,
    params: tuple,
    limit: Optional[int],
    offset: int,
) -> set[str]:
    """Run a seed query with optional LIMIT/OFFSET and return the node_id set."""
    sql = base_sql
    query_params: list = list(params)
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        query_params.extend([limit, offset])
    elif offset > 0:
        sql += " LIMIT -1 OFFSET ?"
        query_params.append(offset)
    rows = db.execute(sql, query_params).fetchall()
    return {r["node_id"] for r in rows}


def _expand_subgraph(
    db: sqlite3.Connection,
    seed_ids: set[str],
    include_dense: bool = False,
) -> tuple[list[dict], list[dict]]:
    """Given seed node IDs, return their 1-hop neighborhood (nodes + edges)."""
    if not seed_ids:
        return [], []

    placeholders = ",".join("?" for _ in seed_ids)
    seed_list = list(seed_ids)

    edge_rows = db.execute(
        f"""
        SELECT source, target, edge_type FROM graph_edges
        WHERE source IN ({placeholders}) OR target IN ({placeholders})
        """,
        seed_list + seed_list,
    ).fetchall()

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

    all_ids = seed_ids | neighbor_ids
    id_list = list(all_ids)
    ph = ",".join("?" for _ in id_list)

    node_rows = db.execute(
        f"""
        SELECT id, label, node_type, x, y, size, color, part, degree, community,
               themes_json, entities_json, topics_json
        FROM graph_nodes WHERE id IN ({ph})
        """,
        id_list,
    ).fetchall()

    nodes = [_format_node(r, seed_ids) for r in node_rows]
    return nodes, edges


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
    limit: Optional[int] = Query(None, ge=1, description="Max seed paragraphs to include"),
    offset: int = Query(0, ge=0, description="Seed paragraph offset"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for a theme: themed paragraphs + 1-hop neighbors + edges."""
    total_row = db.execute(
        "SELECT COUNT(*) AS c FROM paragraph_themes WHERE theme_id = ?",
        (theme_id,),
    ).fetchone()
    total_seeds = total_row["c"] if total_row else 0

    if total_seeds == 0:
        return {"theme": theme_id, "total_seeds": 0, "nodes": [], "edges": []}

    seed_ids = _paginated_seeds(
        db,
        "SELECT 'p:' || paragraph_id AS node_id FROM paragraph_themes "
        "WHERE theme_id = ? ORDER BY paragraph_id",
        (theme_id,),
        limit,
        offset,
    )

    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)

    return {
        "theme": theme_id,
        "total_seeds": total_seeds,
        "seed_count": len(seed_ids),
        "offset": offset,
        "limit": limit,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/paragraph/{paragraph_id}")
def graph_by_paragraph(
    paragraph_id: int,
    depth: int = Query(1, ge=1, le=2, description="Hop depth (1 or 2)"),
    include_dense: bool = Query(False),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph around a specific paragraph.

    depth=1: the paragraph + its direct neighbors.
    depth=2: expand one more hop from those neighbors.
    """
    seed_id = f"p:{paragraph_id}"

    # Verify node exists
    exists = db.execute(
        "SELECT 1 FROM graph_nodes WHERE id = ?", (seed_id,)
    ).fetchone()
    if not exists:
        return {"paragraph": paragraph_id, "nodes": [], "edges": []}

    seed_ids = {seed_id}
    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)

    if depth >= 2:
        # Second hop: expand from all nodes we found
        hop2_seeds = {n["id"] for n in nodes}
        nodes, edges = _expand_subgraph(db, hop2_seeds, include_dense)
        # Re-mark seeds
        for n in nodes:
            n["is_seed"] = n["id"] == seed_id

    return {
        "paragraph": paragraph_id,
        "depth": depth,
        "seed_count": 1,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/node/{node_id:path}")
def graph_by_node(
    node_id: str,
    include_dense: bool = Query(False),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph around any node (Bible book, author, document, etc.)."""
    exists = db.execute(
        "SELECT 1 FROM graph_nodes WHERE id = ?", (node_id,)
    ).fetchone()
    if not exists:
        return {"node": node_id, "nodes": [], "edges": []}

    seed_ids = {node_id}
    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)

    return {
        "node": node_id,
        "seed_count": 1,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/connect")
def graph_connect(
    sources: str = Query(..., description="Comma-separated node IDs to connect"),
    include_dense: bool = Query(False),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph connecting multiple seed nodes.

    Finds the 1-hop neighborhood of all specified nodes and returns the
    intersection — nodes and edges that bridge between the seeds.
    """
    source_ids = [s.strip() for s in sources.split(",") if s.strip()]
    if len(source_ids) < 2:
        return {"error": "Provide at least 2 node IDs separated by commas"}

    seed_ids = set(source_ids)

    # Get the union of all neighborhoods
    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)

    return {
        "seeds": source_ids,
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
    limit: Optional[int] = Query(None, ge=1),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for a community."""
    total_row = db.execute(
        "SELECT COUNT(*) AS c FROM graph_nodes WHERE community = ?",
        (community_id,),
    ).fetchone()
    total_seeds = total_row["c"] if total_row else 0
    if total_seeds == 0:
        return {"community": community_id, "total_seeds": 0, "nodes": [], "edges": []}

    seed_sql = "SELECT id FROM graph_nodes WHERE community = ? ORDER BY degree DESC, id"
    seed_params: list = [community_id]
    if limit is not None:
        seed_sql += " LIMIT ? OFFSET ?"
        seed_params.extend([limit, offset])
    elif offset > 0:
        seed_sql += " LIMIT -1 OFFSET ?"
        seed_params.append(offset)
    seed_rows = db.execute(seed_sql, seed_params).fetchall()
    seed_ids = {r["id"] for r in seed_rows}

    if not seed_ids:
        return {
            "community": community_id,
            "total_seeds": total_seeds,
            "nodes": [],
            "edges": [],
        }

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
               themes_json, entities_json, topics_json
        FROM graph_nodes WHERE id IN ({ph})
        """,
        id_list,
    ).fetchall()

    nodes = [_format_node(r) for r in node_rows]

    return {
        "community": community_id,
        "total_seeds": total_seeds,
        "seed_count": len(seed_ids),
        "offset": offset,
        "limit": limit,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/filter")
def graph_by_filter(
    themes: Optional[str] = Query(None, description="Comma-separated theme IDs (AND logic)"),
    entities: Optional[str] = Query(None, description="Comma-separated entity IDs (AND logic)"),
    topics: Optional[str] = Query(None, description="Comma-separated topic IDs (AND logic)"),
    include_dense: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1, description="Max seed paragraphs to include"),
    offset: int = Query(0, ge=0, description="Seed paragraph offset"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for paragraphs matching ALL specified filters (AND logic).

    Each filter type narrows the result: a paragraph must have every listed theme
    AND every listed entity AND every listed topic to be included.
    The 1-hop neighborhood (cited sources, cross-refs) is expanded from the seeds.
    """
    theme_list = [t.strip() for t in themes.split(",") if t.strip()] if themes else []
    entity_list = [e.strip() for e in entities.split(",") if e.strip()] if entities else []
    topic_list = [int(t.strip()) for t in topics.split(",") if t.strip()] if topics else []

    if not theme_list and not entity_list and not topic_list:
        return {"filters": {}, "total_seeds": 0, "nodes": [], "edges": []}

    # Start with all paragraph IDs, then intersect with each filter
    candidate_ids: set[int] | None = None

    for theme_id in theme_list:
        rows = db.execute(
            "SELECT paragraph_id FROM paragraph_themes WHERE theme_id = ?",
            (theme_id,),
        ).fetchall()
        ids = {r["paragraph_id"] for r in rows}
        candidate_ids = ids if candidate_ids is None else candidate_ids & ids

    for entity_id in entity_list:
        rows = db.execute(
            "SELECT paragraph_id FROM paragraph_entities WHERE entity_id = ?",
            (entity_id,),
        ).fetchall()
        ids = {r["paragraph_id"] for r in rows}
        candidate_ids = ids if candidate_ids is None else candidate_ids & ids

    for topic_id in topic_list:
        rows = db.execute(
            "SELECT paragraph_id FROM paragraph_topics WHERE topic_id = ?",
            (topic_id,),
        ).fetchall()
        ids = {r["paragraph_id"] for r in rows}
        candidate_ids = ids if candidate_ids is None else candidate_ids & ids

    if not candidate_ids:
        return {
            "filters": {"themes": theme_list, "entities": entity_list, "topics": topic_list},
            "total_seeds": 0,
            "nodes": [],
            "edges": [],
        }

    sorted_candidates = sorted(candidate_ids)
    total_seeds = len(sorted_candidates)
    if limit is not None:
        window = sorted_candidates[offset : offset + limit]
    else:
        window = sorted_candidates[offset:]
    seed_ids = {f"p:{pid}" for pid in window}
    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)

    return {
        "filters": {"themes": theme_list, "entities": entity_list, "topics": topic_list},
        "total_seeds": total_seeds,
        "seed_count": len(seed_ids),
        "offset": offset,
        "limit": limit,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/entities")
def list_entities(db: sqlite3.Connection = Depends(get_db)):
    """List all entities with counts."""
    rows = db.execute(
        "SELECT id, label, category, count FROM entities ORDER BY category, count DESC"
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/topics")
def list_topics(db: sqlite3.Connection = Depends(get_db)):
    """List all topics with terms."""
    rows = db.execute("SELECT id, terms_json FROM topics ORDER BY id").fetchall()
    return [{"id": r["id"], "terms": json.loads(r["terms_json"] or "[]")} for r in rows]


@router.get("/entity/{entity_id}")
def graph_by_entity(
    entity_id: str,
    include_dense: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for an entity: paragraphs with this entity + 1-hop neighbors."""
    total_row = db.execute(
        "SELECT COUNT(*) AS c FROM paragraph_entities WHERE entity_id = ?",
        (entity_id,),
    ).fetchone()
    total_seeds = total_row["c"] if total_row else 0
    if total_seeds == 0:
        return {"entity": entity_id, "total_seeds": 0, "nodes": [], "edges": []}

    seed_ids = _paginated_seeds(
        db,
        "SELECT 'p:' || paragraph_id AS node_id FROM paragraph_entities "
        "WHERE entity_id = ? ORDER BY paragraph_id",
        (entity_id,),
        limit,
        offset,
    )
    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)
    return {
        "entity": entity_id,
        "total_seeds": total_seeds,
        "seed_count": len(seed_ids),
        "offset": offset,
        "limit": limit,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/topic/{topic_id}")
def graph_by_topic(
    topic_id: int,
    include_dense: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the subgraph for a topic: paragraphs with this topic + 1-hop neighbors."""
    total_row = db.execute(
        "SELECT COUNT(*) AS c FROM paragraph_topics WHERE topic_id = ?",
        (topic_id,),
    ).fetchone()
    total_seeds = total_row["c"] if total_row else 0
    if total_seeds == 0:
        return {"topic": topic_id, "total_seeds": 0, "nodes": [], "edges": []}

    seed_ids = _paginated_seeds(
        db,
        "SELECT 'p:' || paragraph_id AS node_id FROM paragraph_topics "
        "WHERE topic_id = ? ORDER BY paragraph_id",
        (topic_id,),
        limit,
        offset,
    )
    nodes, edges = _expand_subgraph(db, seed_ids, include_dense)
    return {
        "topic": topic_id,
        "total_seeds": total_seeds,
        "seed_count": len(seed_ids),
        "offset": offset,
        "limit": limit,
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
