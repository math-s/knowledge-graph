"""Export graph data to JSON files for the web UI."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import community as community_louvain
import networkx as nx

from .models import GraphData, GraphEdge, GraphNode, Paragraph

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DATA_DIR = PROJECT_ROOT / "web" / "public" / "data"


def compute_communities(G: nx.Graph) -> dict[str, int]:
    """Detect communities using Louvain method."""
    # Only use paragraph nodes for community detection
    paragraph_subgraph = G.subgraph(
        [n for n in G.nodes if G.nodes[n].get("node_type") == "paragraph"]
    )
    if paragraph_subgraph.number_of_nodes() == 0:
        return {}

    partition = community_louvain.best_partition(paragraph_subgraph)
    logger.info("Detected %d communities", len(set(partition.values())))
    return partition


def export_graph(
    G: nx.Graph,
    positions: dict[str, tuple[float, float]],
    paragraphs: list[Paragraph],
) -> None:
    """Export graph to JSON files for the web UI."""
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Compute communities
    communities = compute_communities(G)

    # Build node list
    nodes: list[GraphNode] = []
    for node_id in G.nodes:
        data = G.nodes[node_id]
        x, y = positions.get(node_id, (0.0, 0.0))
        degree = G.degree(node_id)

        # Size: structural nodes larger, paragraph nodes sized by degree
        if data.get("node_type") == "structure":
            size = 8.0
        else:
            size = max(2.0, min(15.0, 2.0 + degree * 0.5))

        nodes.append(GraphNode(
            id=node_id,
            label=data.get("label", node_id),
            node_type=data.get("node_type", "paragraph"),
            x=x,
            y=y,
            size=size,
            color=data.get("color", "#666666"),
            part=data.get("part", ""),
            degree=degree,
            community=communities.get(node_id, 0),
        ))

    # Build edge list
    edges: list[GraphEdge] = []
    for source, target, data in G.edges(data=True):
        edges.append(GraphEdge(
            source=source,
            target=target,
            edge_type=data.get("edge_type", "cross_reference"),
        ))

    # Export graph.json
    graph_data = GraphData(nodes=nodes, edges=edges)
    graph_path = WEB_DATA_DIR / "graph.json"
    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump(graph_data.model_dump(), f, ensure_ascii=False)
    logger.info("Exported graph.json: %d nodes, %d edges", len(nodes), len(edges))

    # Export paragraphs.json (full text for lazy loading)
    paragraphs_data = []
    for p in paragraphs:
        paragraphs_data.append({
            "id": p.id,
            "text": p.text,
            "footnotes": p.footnotes,
            "cross_references": p.cross_references,
            "part": p.part,
            "section": p.section,
            "chapter": p.chapter,
            "article": p.article,
        })
    paragraphs_path = WEB_DATA_DIR / "paragraphs.json"
    with open(paragraphs_path, "w", encoding="utf-8") as f:
        json.dump(paragraphs_data, f, ensure_ascii=False)
    logger.info("Exported paragraphs.json: %d paragraphs", len(paragraphs_data))

    # Export search-index.json (lightweight for Fuse.js)
    search_data = []
    for p in paragraphs:
        search_data.append({
            "id": p.id,
            "text": p.text[:300],  # Truncate for search index
            "part": p.part,
            "section": p.section,
            "chapter": p.chapter,
            "article": p.article,
        })
    search_path = WEB_DATA_DIR / "search-index.json"
    with open(search_path, "w", encoding="utf-8") as f:
        json.dump(search_data, f, ensure_ascii=False)
    logger.info("Exported search-index.json: %d entries", len(search_data))
