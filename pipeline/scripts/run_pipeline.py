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
from pipeline.src.graph_builder import build_graph, add_shared_theme_edges, add_source_nodes
from pipeline.src.layout import compute_layout
from pipeline.src.export import export_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RAW_PT_PATH = Path(project_root) / "data" / "raw" / "ccc-pt.json"


def main() -> None:
    logger.info("=== Step 1: Ingest CCC data (English) ===")
    paragraphs_en, structures_en = run_ingest(lang="en")

    paragraphs_pt = None
    if RAW_PT_PATH.exists():
        logger.info("=== Step 1b: Ingest CCC data (Portuguese) ===")
        # Graph topology is built from English only; PT structures are unused
        paragraphs_pt, _structures_pt = run_ingest(lang="pt")
        logger.info("Portuguese paragraphs: %d", len(paragraphs_pt))
    else:
        logger.info("Portuguese data not found at %s — skipping PT", RAW_PT_PATH)

    logger.info("=== Step 2: Parse footnotes ===")
    paragraphs_en = parse_all_footnotes(paragraphs_en)

    logger.info("=== Step 3: Assign themes ===")
    paragraphs_en = assign_themes(paragraphs_en)

    logger.info("=== Step 4: Build graph ===")
    G = build_graph(paragraphs_en, structures_en)

    logger.info("=== Step 5: Add shared-theme edges ===")
    G = add_shared_theme_edges(G, paragraphs_en)

    logger.info("=== Step 6: Add source nodes ===")
    G = add_source_nodes(G, paragraphs_en)

    logger.info("=== Step 7: Compute layout ===")
    positions = compute_layout(G)

    logger.info("=== Step 8: Export for web ===")
    export_graph(G, positions, paragraphs_en, paragraphs_pt)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
