"""Build NetworkX graph from parsed CCC data."""

from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations

import networkx as nx

from .models import BibleBookFull, DocumentSource, Paragraph, PatristicWork, StructuralNode, resolve_lang
from .fetch_bible import parse_reference

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
            text_preview=resolve_lang(p.text, "en")[:150] if p.text else "",
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


def add_shared_entity_edges(
    G: nx.Graph,
    paragraphs: list[Paragraph],
    min_shared: int = 3,
    max_group_size: int = 500,
) -> nx.Graph:
    """Add edges between paragraphs that share at least `min_shared` entities."""
    # Build entity -> paragraph list index
    entity_to_paras: dict[str, list[int]] = defaultdict(list)
    for p in paragraphs:
        for e in p.entities:
            entity_to_paras[e].append(p.id)

    # Count shared entities per paragraph pair
    pair_counts: dict[tuple[int, int], int] = defaultdict(int)
    skipped = 0
    for entity_id, para_ids in entity_to_paras.items():
        if len(para_ids) > max_group_size:
            skipped += 1
            continue
        for a, b in combinations(para_ids, 2):
            key = (min(a, b), max(a, b))
            pair_counts[key] += 1

    if skipped:
        logger.info("Skipped %d entity groups exceeding cap of %d", skipped, max_group_size)

    # Add edges for pairs meeting the threshold
    edge_count = 0
    for (a, b), count in pair_counts.items():
        if count >= min_shared:
            src, tgt = f"p:{a}", f"p:{b}"
            if G.has_node(src) and G.has_node(tgt) and not G.has_edge(src, tgt):
                G.add_edge(src, tgt, edge_type="shared_entity", shared_count=count)
                edge_count += 1

    logger.info(
        "Added %d shared-entity edges (threshold: %d+ shared entities)",
        edge_count,
        min_shared,
    )
    return G


def add_shared_topic_edges(
    G: nx.Graph,
    paragraphs: list[Paragraph],
    min_weight: float = 0.15,
    max_group_size: int = 300,
) -> nx.Graph:
    """Add edges between paragraphs that share a dominant topic.

    Only considers topics with weight >= min_weight for each paragraph.
    """
    # Build topic -> list of (para_id, weight) where weight >= min_weight
    topic_to_paras: dict[int, list[int]] = defaultdict(list)
    for p in paragraphs:
        for topic_id, weight in p.topics:
            if weight >= min_weight:
                topic_to_paras[topic_id].append(p.id)

    # Add edges for paragraphs sharing a dominant topic
    edge_count = 0
    skipped = 0
    for topic_id, para_ids in topic_to_paras.items():
        if len(para_ids) > max_group_size:
            skipped += 1
            continue
        for a, b in combinations(para_ids, 2):
            src, tgt = f"p:{min(a, b)}", f"p:{max(a, b)}"
            if G.has_node(src) and G.has_node(tgt) and not G.has_edge(src, tgt):
                G.add_edge(src, tgt, edge_type="shared_topic", topic_id=topic_id)
                edge_count += 1

    if skipped:
        logger.info("Skipped %d topic groups exceeding cap of %d", skipped, max_group_size)

    logger.info(
        "Added %d shared-topic edges (weight threshold: %.2f)",
        edge_count,
        min_weight,
    )
    return G


# Source node colors
SOURCE_COLORS = {
    "bible": "#59A14F",   # Green
    "author": "#B07AA1",  # Purple
    "document": "#EDC948",  # Gold/amber
}

# Bible hierarchy colors
BIBLE_HIERARCHY_COLORS = {
    "bible-testament": "#3D7A35",
    "bible-book": "#59A14F",
    "bible-chapter": "#7BC474",
    "bible-verse": "#A3D99B",
}

# Patristic hierarchy colors
PATRISTIC_HIERARCHY_COLORS = {
    "patristic-work": "#9B6AA1",  # Lighter purple than author (#B07AA1)
}

# Document hierarchy colors
DOCUMENT_HIERARCHY_COLORS = {
    "document-section": "#F5DD7A",  # Lighter amber than document (#EDC948)
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


def add_bible_hierarchy(
    G: nx.Graph,
    bible_books: dict[str, BibleBookFull],
    paragraphs: list[Paragraph],
) -> nx.Graph:
    """Add full Bible hierarchy: testament -> book -> chapter -> verse nodes.

    Replaces the flat bible:{book_id} nodes with a hierarchical structure.
    Rewires CCC cites edges to point to specific verse nodes where possible.
    """
    if not bible_books:
        logger.info("No Bible data — skipping hierarchy")
        return G

    # Remove existing flat Bible book nodes and their cites edges
    old_bible_nodes = [n for n in G.nodes if G.nodes[n].get("node_type") == "bible"]
    for node in old_bible_nodes:
        G.remove_node(node)

    # Add testament nodes
    for testament_id, testament_label in [("ot", "Old Testament"), ("nt", "New Testament")]:
        node_id = f"bible-testament:{testament_id}"
        G.add_node(
            node_id,
            node_type="bible-testament",
            label=testament_label,
            color=BIBLE_HIERARCHY_COLORS["bible-testament"],
        )

    # Add book, chapter, and verse nodes
    book_count = 0
    chapter_count = 0
    verse_count = 0

    for book_id, book in bible_books.items():
        # Book node
        book_node = f"bible-book:{book_id}"
        testament_node = f"bible-testament:{'ot' if book.testament == 'old' else 'nt'}"
        G.add_node(
            book_node,
            node_type="bible-book",
            label=book.name,
            color=BIBLE_HIERARCHY_COLORS["bible-book"],
            testament=book.testament,
            category=book.category,
        )
        G.add_edge(book_node, testament_node, edge_type="child_of")
        book_count += 1

        for ch_num, chapter in book.chapters.items():
            # Chapter node
            ch_node = f"bible-chapter:{book_id}-{ch_num}"
            G.add_node(
                ch_node,
                node_type="bible-chapter",
                label=f"{book.name} {ch_num}",
                color=BIBLE_HIERARCHY_COLORS["bible-chapter"],
            )
            G.add_edge(ch_node, book_node, edge_type="child_of")
            chapter_count += 1

            for v_num in chapter.verses:
                # Verse node
                v_node = f"bible-verse:{book_id}-{ch_num}:{v_num}"
                G.add_node(
                    v_node,
                    node_type="bible-verse",
                    label=f"{book.abbreviation} {ch_num}:{v_num}",
                    color=BIBLE_HIERARCHY_COLORS["bible-verse"],
                )
                G.add_edge(v_node, ch_node, edge_type="child_of")
                verse_count += 1

    logger.info(
        "Added Bible hierarchy: 2 testaments, %d books, %d chapters, %d verses",
        book_count,
        chapter_count,
        verse_count,
    )

    # Rewire CCC cites edges to specific verses
    # Build index of which verses exist in the graph
    verse_nodes: set[str] = {n for n in G.nodes if G.nodes[n].get("node_type") == "bible-verse"}
    chapter_nodes: set[str] = {n for n in G.nodes if G.nodes[n].get("node_type") == "bible-chapter"}
    book_nodes: set[str] = {n for n in G.nodes if G.nodes[n].get("node_type") == "bible-book"}

    cites_count = 0
    for p in paragraphs:
        para_node = f"p:{p.id}"
        if not G.has_node(para_node):
            continue

        for pf in p.parsed_footnotes:
            for br in pf.bible_refs:
                if br.reference:
                    # Try to link to specific verses
                    parsed = parse_reference(br.reference)
                    linked = False
                    for chapter, verse in parsed:
                        if verse == 0:
                            # Whole chapter reference
                            ch_node = f"bible-chapter:{br.book}-{chapter}"
                            if ch_node in chapter_nodes:
                                if not G.has_edge(para_node, ch_node):
                                    G.add_edge(para_node, ch_node, edge_type="cites")
                                    cites_count += 1
                                    linked = True
                        else:
                            v_node = f"bible-verse:{br.book}-{chapter}:{verse}"
                            if v_node in verse_nodes:
                                if not G.has_edge(para_node, v_node):
                                    G.add_edge(para_node, v_node, edge_type="cites")
                                    cites_count += 1
                                    linked = True

                    # If no specific verse was linked, link to book
                    if not linked:
                        book_node = f"bible-book:{br.book}"
                        if book_node in book_nodes and not G.has_edge(para_node, book_node):
                            G.add_edge(para_node, book_node, edge_type="cites")
                            cites_count += 1
                else:
                    # No reference — link to book level
                    book_node = f"bible-book:{br.book}"
                    if book_node in book_nodes and not G.has_edge(para_node, book_node):
                        G.add_edge(para_node, book_node, edge_type="cites")
                        cites_count += 1

    logger.info("Rewired %d CCC-to-Bible cites edges", cites_count)
    return G


def add_bible_crossref_edges(
    G: nx.Graph,
    crossrefs: dict[str, list[str]],
) -> nx.Graph:
    """Add bible_cross_reference edges from TSK cross-reference data.

    Args:
        G: The graph to add edges to.
        crossrefs: Dict mapping verse_id -> list[verse_id].
            Verse IDs use format "book_id-chapter:verse".
    """
    if not crossrefs:
        logger.info("No cross-references to add")
        return G

    verse_nodes = {n for n in G.nodes if G.nodes[n].get("node_type") == "bible-verse"}

    edge_count = 0
    for source_verse_id, target_verse_ids in crossrefs.items():
        source_node = f"bible-verse:{source_verse_id}"
        if source_node not in verse_nodes:
            continue

        for target_verse_id in target_verse_ids:
            target_node = f"bible-verse:{target_verse_id}"
            if target_node in verse_nodes and not G.has_edge(source_node, target_node):
                G.add_edge(source_node, target_node, edge_type="bible_cross_reference")
                edge_count += 1

    logger.info("Added %d bible_cross_reference edges", edge_count)
    return G


def add_patristic_work_hierarchy(
    G: nx.Graph,
    patristic_works: dict[str, list[PatristicWork]],
    paragraphs: list[Paragraph],
) -> nx.Graph:
    """Add patristic work nodes with child_of edges to author nodes.

    Creates patristic-work:{author_id}/{work_id} nodes linked to their
    author:{author_id} parent. Rewires CCC cites edges to work-level
    nodes where the footnote parser resolved a specific work.
    """
    if not patristic_works:
        logger.info("No patristic works — skipping hierarchy")
        return G

    work_count = 0
    for author_id, works in patristic_works.items():
        author_node = f"author:{author_id}"
        if not G.has_node(author_node):
            continue

        for work in works:
            work_node = f"patristic-work:{work.id}"
            G.add_node(
                work_node,
                node_type="patristic-work",
                label=work.title,
                color=PATRISTIC_HIERARCHY_COLORS["patristic-work"],
                author_id=author_id,
            )
            G.add_edge(work_node, author_node, edge_type="child_of")
            work_count += 1

    logger.info("Added %d patristic work nodes", work_count)

    # Rewire CCC cites edges to work-level nodes where possible
    work_nodes = {
        n for n in G.nodes if G.nodes[n].get("node_type") == "patristic-work"
    }

    cites_count = 0
    for p in paragraphs:
        para_node = f"p:{p.id}"
        if not G.has_node(para_node):
            continue

        for pf in p.parsed_footnotes:
            for ar in pf.author_refs:
                if ar.work:
                    work_node = f"patristic-work:{ar.author}/{ar.work}"
                    if work_node in work_nodes:
                        if not G.has_edge(para_node, work_node):
                            G.add_edge(para_node, work_node, edge_type="cites")
                            cites_count += 1

    logger.info("Added %d CCC-to-work cites edges", cites_count)
    return G


def add_document_section_hierarchy(
    G: nx.Graph,
    document_sources: dict[str, DocumentSource],
    paragraphs: list[Paragraph],
) -> nx.Graph:
    """Add document section nodes with child_of edges to document nodes.

    Creates document-section:{doc_id}/{section_num} nodes linked to their
    document:{doc_id} parent. Rewires CCC cites edges to section-level
    nodes where the footnote parser resolved a specific section.
    """
    if not document_sources:
        logger.info("No document sources — skipping section hierarchy")
        return G

    section_count = 0
    for doc_id, doc in document_sources.items():
        doc_node = f"document:{doc_id}"
        if not G.has_node(doc_node):
            continue

        for sec_num in doc.sections:
            sec_node = f"document-section:{doc_id}/{sec_num}"
            G.add_node(
                sec_node,
                node_type="document-section",
                label=f"{doc.abbreviation} {sec_num}",
                color=DOCUMENT_HIERARCHY_COLORS["document-section"],
                document_id=doc_id,
            )
            G.add_edge(sec_node, doc_node, edge_type="child_of")
            section_count += 1

    logger.info("Added %d document section nodes", section_count)

    # Rewire CCC cites edges to section-level nodes where possible
    section_nodes = {
        n for n in G.nodes if G.nodes[n].get("node_type") == "document-section"
    }

    cites_count = 0
    for p in paragraphs:
        para_node = f"p:{p.id}"
        if not G.has_node(para_node):
            continue

        for pf in p.parsed_footnotes:
            for dr in pf.document_refs:
                if dr.section:
                    sec_node = f"document-section:{dr.document}/{dr.section}"
                    if sec_node in section_nodes:
                        if not G.has_edge(para_node, sec_node):
                            G.add_edge(para_node, sec_node, edge_type="cites")
                            cites_count += 1

    logger.info("Added %d CCC-to-section cites edges", cites_count)
    return G
