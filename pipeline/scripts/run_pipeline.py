"""Orchestrate the full data pipeline."""

import argparse
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
from pipeline.src.export import export_graph, export_sources
from pipeline.src.fetch_bible import fetch_bible_texts
from pipeline.src.fetch_documents import fetch_document_texts
from pipeline.src.fetch_patristic import fetch_patristic_texts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the CCC knowledge graph pipeline")
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip source text fetching (Step 4) for offline/CI use",
    )
    args = parser.parse_args()

    logger.info("=== Step 1: Ingest CCC data ===")
    paragraphs, structures = run_ingest()

    logger.info("=== Step 2: Parse footnotes ===")
    paragraphs = parse_all_footnotes(paragraphs)

    logger.info("=== Step 3: Assign themes ===")
    paragraphs = assign_themes(paragraphs)

    # Step 4: Fetch source texts
    bible_sources = {}
    document_sources = {}
    author_sources = {}
    if args.skip_fetch:
        logger.info("=== Step 4: Fetch sources (SKIPPED — --skip-fetch) ===")
    else:
        logger.info("=== Step 4: Fetch source texts ===")
        bible_sources = fetch_bible_texts(paragraphs)
        document_sources = fetch_document_texts(paragraphs)
        author_sources = fetch_patristic_texts(paragraphs)

    logger.info("=== Step 5: Build graph ===")
    G = build_graph(paragraphs, structures)

    logger.info("=== Step 6: Add shared-theme edges ===")
    G = add_shared_theme_edges(G, paragraphs)

    logger.info("=== Step 7: Add source nodes ===")
    G = add_source_nodes(G, paragraphs)

    logger.info("=== Step 8: Compute layout ===")
    positions = compute_layout(G)

    logger.info("=== Step 9: Export for web ===")
    export_graph(G, positions, paragraphs)

    logger.info("=== Step 10: Export sources ===")
    export_sources(bible_sources, document_sources, author_sources)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
