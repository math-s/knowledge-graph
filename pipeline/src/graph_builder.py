"""Build NetworkX graph from parsed CCC data."""

from __future__ import annotations

import logging

import networkx as nx

from .models import Paragraph, StructuralNode

logger = logging.getLogger(__name__)

# 5 colors: Prologue + 4 Parts
PART_COLORS: dict[str, str] = {}
PALETTE = ["#888888", "#4E79A7", "#E15759", "#76B7B2", "#F28E2B"]


def _assign_part_color(part: str) -> str:
    """Assign a color based on the Part name."""
    if part not in PART_COLORS:
        idx = len(PART_COLORS) % len(PALETTE)
        PART_COLORS[part] = PALETTE[idx]
    return PART_COLORS[part]


def build_graph(
    paragraphs: list[Paragraph],
    structures: list[StructuralNode],
) -> nx.Graph:
    """Build the full knowledge graph."""
    G = nx.Graph()

    valid_ids = {p.id for p in paragraphs}

    # Add paragraph nodes
    for p in paragraphs:
        G.add_node(
            f"p:{p.id}",
            node_type="paragraph",
            label=f"CCC {p.id}",
            part=p.part,
            section=p.section,
            chapter=p.chapter,
            article=p.article,
            color=_assign_part_color(p.part),
            text_preview=p.text[:150] if p.text else "",
        )

    # Add structural nodes
    for s in structures:
        G.add_node(
            f"struct:{s.id}",
            node_type="structure",
            label=s.label,
            level=s.level,
            color="#CCCCCC",
        )

    # Add cross-reference edges
    cross_ref_count = 0
    for p in paragraphs:
        for ref_id in p.cross_references:
            if ref_id in valid_ids and ref_id > p.id:  # avoid duplicates
                G.add_edge(f"p:{p.id}", f"p:{ref_id}", edge_type="cross_reference")
                cross_ref_count += 1

    logger.info("Added %d cross-reference edges", cross_ref_count)

    # Add belongs_to edges: paragraph -> its structural node
    struct_ids = {s.id for s in structures}
    for s in structures:
        for para_id in s.paragraph_ids:
            G.add_edge(f"p:{para_id}", f"struct:{s.id}", edge_type="belongs_to")

    # Add child_of edges: structural hierarchy
    for s in structures:
        if s.parent_id and s.parent_id in struct_ids:
            G.add_edge(f"struct:{s.id}", f"struct:{s.parent_id}", edge_type="child_of")

    logger.info(
        "Graph built: %d nodes, %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )

    return G
