"""Export graph data to JSON files for the web UI."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import community as community_louvain
import networkx as nx

from .models import GraphData, GraphEdge, GraphNode, Paragraph
from .themes import THEME_DEFINITIONS

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
    paragraphs_en: list[Paragraph],
    paragraphs_pt: list[Paragraph] | None = None,
) -> None:
    """Export graph to JSON files for the web UI.

    Args:
        G: The graph.
        positions: Node positions.
        paragraphs_en: English paragraph list.
        paragraphs_pt: Optional Portuguese paragraph list.  When provided,
            paragraphs.json and search-index.json gain bilingual fields.
    """
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Build paragraph lookup for themes
    para_lookup = {p.id: p for p in paragraphs_en}

    # Compute communities
    communities = compute_communities(G)

    # Build node list
    nodes: list[GraphNode] = []
    for node_id in G.nodes:
        data = G.nodes[node_id]
        x, y = positions.get(node_id, (0.0, 0.0))
        degree = G.degree(node_id)
        node_type = data.get("node_type", "paragraph")

        # Size: type-dependent sizing
        if node_type == "structure":
            size = 8.0
        elif node_type in ("bible", "author", "document"):
            # Source nodes sized by degree (number of citing paragraphs)
            size = max(4.0, min(25.0, 4.0 + degree * 0.04))
        else:
            size = max(2.0, min(15.0, 2.0 + degree * 0.5))

        # Get themes for paragraph nodes
        themes: list[str] = []
        if node_type == "paragraph" and node_id.startswith("p:"):
            pid = int(node_id[2:])
            para = para_lookup.get(pid)
            if para:
                themes = para.themes

        nodes.append(GraphNode(
            id=node_id,
            label=data.get("label", node_id),
            node_type=node_type,
            x=x,
            y=y,
            size=size,
            color=data.get("color", "#666666"),
            part=data.get("part", ""),
            degree=degree,
            community=communities.get(node_id, 0),
            themes=themes,
        ))

    # Build edge list
    edges: list[GraphEdge] = []
    for source, target, data in G.edges(data=True):
        edges.append(GraphEdge(
            source=source,
            target=target,
            edge_type=data.get("edge_type", "cross_reference"),
        ))

    # Export graph.json (language-independent)
    graph_data = GraphData(nodes=nodes, edges=edges)
    graph_path = WEB_DATA_DIR / "graph.json"
    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump(graph_data.model_dump(), f, ensure_ascii=False)
    logger.info("Exported graph.json: %d nodes, %d edges", len(nodes), len(edges))

    # Build PT lookup by id
    pt_map: dict[int, Paragraph] = {}
    if paragraphs_pt:
        for p in paragraphs_pt:
            pt_map[p.id] = p

    # Export paragraphs.json (bilingual when PT available)
    paragraphs_data = []
    for p in paragraphs_en:
        pt = pt_map.get(p.id)

        # Collect unique Bible, author, and document citations
        bible_citations: list[str] = []
        author_citations: list[str] = []
        document_citations: list[str] = []
        seen_bible: set[str] = set()
        seen_author: set[str] = set()
        seen_document: set[str] = set()
        for pf in p.parsed_footnotes:
            for br in pf.bible_refs:
                if br.book not in seen_bible:
                    seen_bible.add(br.book)
                    bible_citations.append(br.book)
            for ar in pf.author_refs:
                if ar.author not in seen_author:
                    seen_author.add(ar.author)
                    author_citations.append(ar.author)
            for dr in pf.document_refs:
                if dr.document not in seen_document:
                    seen_document.add(dr.document)
                    document_citations.append(dr.document)

        entry: dict = {
            "id": p.id,
            "text": {"en": p.text, "pt": pt.text if pt else ""},
            "footnotes": {"en": p.footnotes, "pt": pt.footnotes if pt else []},
            "cross_references": p.cross_references,
            "bible_citations": bible_citations,
            "author_citations": author_citations,
            "document_citations": document_citations,
            "themes": p.themes,
            "part": {"en": p.part, "pt": pt.part if pt else ""},
            "section": {"en": p.section, "pt": pt.section if pt else ""},
            "chapter": {"en": p.chapter, "pt": pt.chapter if pt else ""},
            "article": {"en": p.article, "pt": pt.article if pt else ""},
        }

        paragraphs_data.append(entry)

    paragraphs_path = WEB_DATA_DIR / "paragraphs.json"
    with open(paragraphs_path, "w", encoding="utf-8") as f:
        json.dump(paragraphs_data, f, ensure_ascii=False)
    logger.info("Exported paragraphs.json: %d paragraphs", len(paragraphs_data))

    # Export search-index.json (bilingual when PT available)
    search_data: list[dict] = []
    for p in paragraphs_en:
        pt = pt_map.get(p.id)
        search_data.append({
            "id": p.id,
            "text": p.text[:300],
            "text_pt": pt.text[:300] if pt else "",
            "themes": " ".join(p.themes),
            "part": p.part,
            "section": p.section,
            "chapter": p.chapter,
            "article": p.article,
        })

    # Also add source nodes to search index
    for node_id in G.nodes:
        ndata = G.nodes[node_id]
        ntype = ndata.get("node_type", "")
        if ntype in ("bible", "author", "document"):
            search_data.append({
                "id": node_id,
                "text": ndata.get("label", node_id),
                "themes": "",
                "part": "",
                "section": "",
                "chapter": "",
                "article": "",
            })

    search_path = WEB_DATA_DIR / "search-index.json"
    with open(search_path, "w", encoding="utf-8") as f:
        json.dump(search_data, f, ensure_ascii=False)
    logger.info("Exported search-index.json: %d entries", len(search_data))

    # Export themes.json metadata
    theme_counts: dict[str, int] = {}
    for p in paragraphs_en:
        for t in p.themes:
            theme_counts[t] = theme_counts.get(t, 0) + 1

    themes_meta: dict[str, dict] = {}
    for td in THEME_DEFINITIONS:
        themes_meta[td.id] = {
            "label": td.label,
            "count": theme_counts.get(td.id, 0),
        }
    themes_path = WEB_DATA_DIR / "themes.json"
    with open(themes_path, "w", encoding="utf-8") as f:
        json.dump(themes_meta, f, ensure_ascii=False)
    logger.info("Exported themes.json: %d themes", len(themes_meta))
