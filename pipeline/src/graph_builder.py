"""Build NetworkX graph from parsed CCC data."""

from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations

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


def add_shared_theme_edges(G: nx.Graph, paragraphs: list[Paragraph], min_shared: int = 4) -> nx.Graph:
    """Add edges between paragraphs that share at least `min_shared` themes."""
    # Build theme → paragraph list index
    theme_to_paras: dict[str, list[int]] = defaultdict(list)
    for p in paragraphs:
        for t in p.themes:
            theme_to_paras[t].append(p.id)

    # Count shared themes per paragraph pair
    pair_counts: dict[tuple[int, int], int] = defaultdict(int)
    for theme_id, para_ids in theme_to_paras.items():
        for a, b in combinations(para_ids, 2):
            key = (min(a, b), max(a, b))
            pair_counts[key] += 1

    # Add edges for pairs meeting the threshold
    edge_count = 0
    for (a, b), count in pair_counts.items():
        if count >= min_shared:
            src, tgt = f"p:{a}", f"p:{b}"
            if G.has_node(src) and G.has_node(tgt) and not G.has_edge(src, tgt):
                G.add_edge(src, tgt, edge_type="shared_theme", shared_count=count)
                edge_count += 1

    logger.info(
        "Added %d shared-theme edges (threshold: %d+ shared themes)",
        edge_count,
        min_shared,
    )
    return G


# Source node colors
SOURCE_COLORS = {
    "bible": "#59A14F",   # Green
    "author": "#B07AA1",  # Purple
    "document": "#EDC948",  # Gold/amber
}


def add_source_nodes(G: nx.Graph, paragraphs: list[Paragraph]) -> nx.Graph:
    """Add Bible book, patristic author, and document nodes with cites edges."""
    bible_labels: dict[str, str] = {}  # canon_id → display name
    author_labels: dict[str, str] = {}  # canon_id → display name
    document_labels: dict[str, str] = {}  # canon_id → display name

    # Collect all unique sources and build edges
    cites_edges: set[tuple[str, str]] = set()  # (paragraph_node_id, source_node_id)

    for p in paragraphs:
        para_node = f"p:{p.id}"
        for pf in p.parsed_footnotes:
            for br in pf.bible_refs:
                source_node = f"bible:{br.book}"
                cites_edges.add((para_node, source_node))
                if br.book not in bible_labels:
                    # Capitalize nicely for display
                    bible_labels[br.book] = br.book.replace("-", " ").title()
            for ar in pf.author_refs:
                source_node = f"author:{ar.author}"
                cites_edges.add((para_node, source_node))
                if ar.author not in author_labels:
                    author_labels[ar.author] = ar.author.replace("-", " ").title()
            for dr in pf.document_refs:
                source_node = f"document:{dr.document}"
                cites_edges.add((para_node, source_node))
                if dr.document not in document_labels:
                    document_labels[dr.document] = dr.document.replace("-", " ").title()

    # Add Bible book nodes
    for book_id, label in bible_labels.items():
        node_id = f"bible:{book_id}"
        G.add_node(
            node_id,
            node_type="bible",
            label=label,
            color=SOURCE_COLORS["bible"],
        )

    # Add author nodes
    for author_id, label in author_labels.items():
        node_id = f"author:{author_id}"
        G.add_node(
            node_id,
            node_type="author",
            label=label,
            color=SOURCE_COLORS["author"],
        )

    # Add document nodes
    for doc_id, label in document_labels.items():
        node_id = f"document:{doc_id}"
        G.add_node(
            node_id,
            node_type="document",
            label=label,
            color=SOURCE_COLORS["document"],
        )

    # Add cites edges (deduplicated by set)
    for para_node, source_node in cites_edges:
        G.add_edge(para_node, source_node, edge_type="cites")

    logger.info(
        "Added source nodes: %d Bible books, %d authors, %d documents, %d cites edges",
        len(bible_labels),
        len(author_labels),
        len(document_labels),
        len(cites_edges),
    )

    return G
