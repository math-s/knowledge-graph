"""Compute graph layout using ForceAtlas2."""

from __future__ import annotations

import logging

import networkx as nx
from fa2_modified import ForceAtlas2

logger = logging.getLogger(__name__)


def compute_layout(G: nx.Graph) -> dict[str, tuple[float, float]]:
    """Compute ForceAtlas2 layout for the graph.

    Returns a dict mapping node IDs to (x, y) positions.
    """
    logger.info("Computing ForceAtlas2 layout for %d nodes...", G.number_of_nodes())

    forceatlas2 = ForceAtlas2(
        outboundAttractionDistribution=True,
        linLogMode=False,
        adjustSizes=False,
        edgeWeightInfluence=1.0,
        jitterTolerance=1.0,
        barnesHutOptimize=True,
        barnesHutTheta=1.2,
        multiThreaded=False,
        scalingRatio=2.0,
        strongGravityMode=False,
        gravity=1.0,
        verbose=False,
    )

    positions = forceatlas2.forceatlas2_networkx_layout(G, pos=None, iterations=500)

    logger.info("Layout computed.")
    return positions
