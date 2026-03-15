"""Build shared-citation edges between CCC paragraphs that cite the same sources."""

from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations

import networkx as nx

from .models import Paragraph

logger = logging.getLogger(__name__)


def add_shared_citation_edges(
    G: nx.Graph,
    paragraphs: list[Paragraph],
    min_shared: int = 2,
    max_group_size: int = 200,
) -> nx.Graph:
    """Add edges between paragraphs that share citation targets.

    Uses existing parsed_footnotes data (no new parsing). Finds paragraphs
    that cite the same Bible verse, Church Father work, or document section.
    Adds shared_citation edges for pairs sharing >= min_shared targets.

    Args:
        G: The graph to add edges to.
        paragraphs: List of CCC paragraphs with parsed_footnotes.
        min_shared: Minimum number of shared citation targets for an edge.
        max_group_size: Cap on group size to prevent combinatorial explosion.
    """
    # Build inverted index: citation_target -> list of paragraph IDs
    target_to_paras: dict[str, list[int]] = defaultdict(list)

    for p in paragraphs:
        for pf in p.parsed_footnotes:
            # Bible references: verse-level granularity
            for br in pf.bible_refs:
                if br.reference:
                    target = f"bible:{br.book}/{br.reference}"
                else:
                    target = f"bible:{br.book}"
                if p.id not in target_to_paras[target]:
                    target_to_paras[target].append(p.id)

            # Patristic references: work-level granularity
            for ar in pf.author_refs:
                if ar.work:
                    target = f"author:{ar.author}/{ar.work}"
                else:
                    target = f"author:{ar.author}"
                if p.id not in target_to_paras[target]:
                    target_to_paras[target].append(p.id)

            # Document references: section-level granularity
            for dr in pf.document_refs:
                if dr.section:
                    target = f"document:{dr.document}/{dr.section}"
                else:
                    target = f"document:{dr.document}"
                if p.id not in target_to_paras[target]:
                    target_to_paras[target].append(p.id)

    logger.info(
        "Citation index: %d unique targets across %d paragraphs",
        len(target_to_paras),
        sum(1 for p in paragraphs if p.parsed_footnotes),
    )

    # Count shared citation targets per paragraph pair
    pair_counts: dict[tuple[int, int], int] = defaultdict(int)
    skipped_groups = 0

    for target, para_ids in target_to_paras.items():
        if len(para_ids) > max_group_size:
            skipped_groups += 1
            continue
        if len(para_ids) < 2:
            continue
        for a, b in combinations(para_ids, 2):
            key = (min(a, b), max(a, b))
            pair_counts[key] += 1

    if skipped_groups:
        logger.info("Skipped %d citation groups exceeding cap of %d", skipped_groups, max_group_size)

    # Add edges for pairs meeting the threshold
    edge_count = 0
    for (a, b), count in pair_counts.items():
        if count >= min_shared:
            src, tgt = f"p:{a}", f"p:{b}"
            if G.has_node(src) and G.has_node(tgt) and not G.has_edge(src, tgt):
                G.add_edge(src, tgt, edge_type="shared_citation", shared_count=count)
                edge_count += 1

    logger.info(
        "Added %d shared-citation edges (threshold: %d+ shared targets)",
        edge_count,
        min_shared,
    )
    return G
