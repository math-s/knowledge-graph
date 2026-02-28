"""Orchestrate the full data pipeline."""

import logging
import sys
from pathlib import Path

# Ensure the project root is on the path so pipeline.src imports work
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.src.ingest import run as run_ingest
from pipeline.src.footnote_parser import parse_all_footnotes
from pipeline.src.themes import assign_themes
from pipeline.src.graph_builder import build_graph, add_source_nodes
from pipeline.src.layout import compute_layout
from pipeline.src.export import export_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("=== Step 1: Ingest CCC data ===")
    paragraphs, structures = run_ingest()

    logger.info("=== Step 2: Parse footnotes ===")
    paragraphs = parse_all_footnotes(paragraphs)

    logger.info("=== Step 3: Assign themes ===")
    paragraphs = assign_themes(paragraphs)

    logger.info("=== Step 4: Build graph ===")
    G = build_graph(paragraphs, structures)

    logger.info("=== Step 5: Add source nodes ===")
    G = add_source_nodes(G, paragraphs)

    logger.info("=== Step 6: Compute layout ===")
    positions = compute_layout(G)

    logger.info("=== Step 7: Export for web ===")
    export_graph(G, positions, paragraphs)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
