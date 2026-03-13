"""Compute graph layout using ForceAtlas2."""

from __future__ import annotations

import logging
import math

import networkx as nx
from fa2_modified import ForceAtlas2

logger = logging.getLogger(__name__)

# Layout parameter presets for different graph sizes
_LAYOUT_PRESETS = {
    "small": {  # < 5,000 nodes (current CCC-only)
        "iterations": 500,
        "scalingRatio": 3.0,
        "gravity": 1.5,
        "barnesHutTheta": 1.2,
    },
    "medium": {  # 5,000 - 20,000 nodes
        "iterations": 750,
        "scalingRatio": 5.0,
        "gravity": 2.0,
        "barnesHutTheta": 1.5,
    },
    "large": {  # 20,000 - 50,000 nodes (full Bible)
        "iterations": 1000,
        "scalingRatio": 10.0,
        "gravity": 3.0,
        "barnesHutTheta": 2.0,
    },
    "xlarge": {  # 50,000+ nodes (full corpus)
        "iterations": 1500,
        "scalingRatio": 15.0,
        "gravity": 5.0,
        "barnesHutTheta": 2.5,
    },
}


def _select_preset(node_count: int) -> dict:
    """Select layout parameters based on node count."""
    if node_count < 5000:
        return _LAYOUT_PRESETS["small"]
    elif node_count < 20000:
        return _LAYOUT_PRESETS["medium"]
    elif node_count < 50000:
        return _LAYOUT_PRESETS["large"]
    else:
        return _LAYOUT_PRESETS["xlarge"]


def _pre_position_clusters(G: nx.Graph) -> dict[str, tuple[float, float]]:
    """Pre-position nodes by type to give ForceAtlas2 a better starting layout.

    Places different node types in different quadrants to help the layout
    converge faster for large graphs.
    """
    positions: dict[str, tuple[float, float]] = {}
    node_count = G.number_of_nodes()

    if node_count < 5000:
        # Small graph: no pre-positioning needed
        return positions

    # Cluster centers (spread proportional to graph size)
    spread = math.sqrt(node_count) * 2.0
    cluster_centers = {
        "paragraph": (0.0, 0.0),
        "structure": (-spread * 0.5, -spread * 0.5),
        "bible-testament": (spread * 1.0, 0.0),
        "bible-book": (spread * 1.0, 0.0),
        "bible-chapter": (spread * 1.2, 0.2 * spread),
        "bible-verse": (spread * 1.2, 0.2 * spread),
        "author": (-spread * 0.8, spread * 0.5),
        "patristic-work": (-spread * 0.8, spread * 0.5),
        "document": (spread * 0.3, -spread * 0.8),
        "document-section": (spread * 0.3, -spread * 0.8),
    }

    import random
    rng = random.Random(42)  # deterministic

    for node_id in G.nodes:
        node_type = G.nodes[node_id].get("node_type", "paragraph")
        cx, cy = cluster_centers.get(node_type, (0.0, 0.0))
        # Add jitter
        jitter = spread * 0.3
        x = cx + rng.uniform(-jitter, jitter)
        y = cy + rng.uniform(-jitter, jitter)
        positions[node_id] = (x, y)

    return positions


def compute_layout(G: nx.Graph) -> dict[str, tuple[float, float]]:
    """Compute ForceAtlas2 layout for the graph.

    Automatically scales parameters based on graph size.
    Returns a dict mapping node IDs to (x, y) positions.
    """
    node_count = G.number_of_nodes()
    preset = _select_preset(node_count)

    logger.info(
        "Computing ForceAtlas2 layout for %d nodes (preset: iterations=%d, scaling=%.1f, gravity=%.1f)...",
        node_count,
        preset["iterations"],
        preset["scalingRatio"],
        preset["gravity"],
    )

    forceatlas2 = ForceAtlas2(
        outboundAttractionDistribution=True,
        linLogMode=False,
        adjustSizes=False,
        edgeWeightInfluence=1.0,
        jitterTolerance=1.0,
        barnesHutOptimize=True,
        barnesHutTheta=preset["barnesHutTheta"],
        multiThreaded=False,
        scalingRatio=preset["scalingRatio"],
        strongGravityMode=False,
        gravity=preset["gravity"],
        verbose=False,
    )

    # Pre-position for large graphs
    initial_pos = _pre_position_clusters(G) or None

    positions = forceatlas2.forceatlas2_networkx_layout(
        G,
        pos=initial_pos,
        iterations=preset["iterations"],
    )

    logger.info("Layout computed.")
    return positions
