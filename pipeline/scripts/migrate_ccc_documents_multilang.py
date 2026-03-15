#!/usr/bin/env python3
"""One-time migration: add Latin/Portuguese texts to CCC paragraphs and documents.

Loads the latest checkpoint, migrates Paragraph.text and DocumentSource.sections
from plain strings to MultiLangText dicts, then runs the CCC and document
multilingual fetchers.

Usage:
    python -m pipeline.scripts.migrate_ccc_documents_multilang [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import pickle
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CHECKPOINTS_DIR = Path(__file__).resolve().parent.parent / "data" / "checkpoints"


def _load_latest_checkpoint() -> tuple[int, dict]:
    """Load the highest-numbered checkpoint available."""
    files = sorted(CHECKPOINTS_DIR.glob("step_*.pkl"))
    if not files:
        raise FileNotFoundError("No checkpoints found in " + str(CHECKPOINTS_DIR))
    latest = files[-1]
    step_num = int(latest.stem.split("_")[1])
    logger.info("Loading checkpoint: %s (step %d)", latest.name, step_num)
    with open(latest, "rb") as f:
        state = pickle.load(f)
    return step_num, state


def _migrate_paragraphs_to_multilang(paragraphs: list) -> int:
    """Convert Paragraph.text from plain str to MultiLangText {"en": text}.

    Pickle bypasses Pydantic validators, so old checkpoints have text as str.
    Returns the number of paragraphs migrated.
    """
    migrated = 0
    for p in paragraphs:
        if isinstance(p.text, str):
            p.text = {"en": p.text}
            migrated += 1
    return migrated


def _migrate_documents_to_multilang(document_sources: dict) -> int:
    """Convert DocumentSource.sections values from str to MultiLangText.

    Returns the number of sections migrated.
    """
    migrated = 0
    for doc in document_sources.values():
        new_sections = {}
        for sec_num, sec_text in doc.sections.items():
            if isinstance(sec_text, str):
                new_sections[sec_num] = {"en": sec_text}
                migrated += 1
            else:
                new_sections[sec_num] = sec_text
        doc.sections = new_sections
    return migrated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate CCC paragraphs and documents to multilingual"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done without fetching",
    )
    parser.add_argument(
        "--skip-ccc", action="store_true",
        help="Skip CCC multilingual fetch",
    )
    parser.add_argument(
        "--skip-documents", action="store_true",
        help="Skip document multilingual fetch",
    )
    args = parser.parse_args()

    t0 = time.time()
    step_num, state = _load_latest_checkpoint()

    paragraphs = state.get("paragraphs")
    document_sources = state.get("document_sources")

    if not paragraphs:
        logger.error("No paragraphs in checkpoint — nothing to migrate")
        return

    # Migrate data formats (str -> MultiLangText)
    para_migrated = _migrate_paragraphs_to_multilang(paragraphs)
    logger.info("Migrated %d paragraphs to MultiLangText", para_migrated)

    doc_migrated = 0
    if document_sources:
        doc_migrated = _migrate_documents_to_multilang(document_sources)
        logger.info("Migrated %d document sections to MultiLangText", doc_migrated)

    # Show current state
    logger.info(
        "State: %d paragraphs, %d documents (%d fetchable)",
        len(paragraphs),
        len(document_sources) if document_sources else 0,
        sum(1 for d in (document_sources or {}).values() if d.fetchable),
    )

    if args.dry_run:
        logger.info("[DRY RUN] Would fetch CCC in Latin and Portuguese from Vatican.va")
        logger.info("[DRY RUN] Would fetch %d documents in Latin and Portuguese",
                     sum(1 for d in (document_sources or {}).values() if d.fetchable and d.source_url))
        logger.info("[DRY RUN] No changes made.")
        return

    # Run document multilingual fetcher
    if not args.skip_documents and document_sources:
        logger.info("=== Fetching multilingual documents (La/Pt) ===")
        from pipeline.src.fetch_documents_multilang import fetch_documents_multilang
        document_sources = fetch_documents_multilang(document_sources)
        state["document_sources"] = document_sources

    # Run CCC multilingual fetcher
    if not args.skip_ccc:
        logger.info("=== Fetching multilingual CCC (La/Pt) ===")
        from pipeline.src.fetch_ccc_multilang import fetch_ccc_multilang
        paragraphs = fetch_ccc_multilang(paragraphs)
        state["paragraphs"] = paragraphs

    # Summary
    para_langs: dict[str, int] = {}
    for p in paragraphs:
        if isinstance(p.text, dict):
            for lang in p.text:
                para_langs[lang] = para_langs.get(lang, 0) + 1
    logger.info("CCC paragraph languages: %s", {k: v for k, v in sorted(para_langs.items())})

    doc_langs: dict[str, int] = {}
    for doc in (document_sources or {}).values():
        for sec_text in doc.sections.values():
            if isinstance(sec_text, dict):
                for lang in sec_text:
                    doc_langs[lang] = doc_langs.get(lang, 0) + 1
    logger.info("Document section languages: %s", {k: v for k, v in sorted(doc_langs.items())})

    # Save updated checkpoint
    checkpoint_path = CHECKPOINTS_DIR / f"step_{step_num:02d}.pkl"
    with open(checkpoint_path, "wb") as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Updated checkpoint: %s", checkpoint_path.name)

    # Re-export all data
    logger.info("=== Re-exporting data ===")
    from pipeline.src.export import (
        export_graph,
        export_sources,
        export_documents_full,
        export_authors_full,
    )

    # Re-export graph + paragraphs (needed for multilingual paragraph text)
    G = state.get("G")
    positions = state.get("positions")
    if G is not None and positions:
        logger.info("Re-exporting graph and paragraphs...")
        export_graph(G, positions, paragraphs)

    export_sources(
        state["bible_sources"],
        state["document_sources"],
        state["author_sources"],
    )

    if document_sources:
        export_documents_full(document_sources)

    if state.get("author_sources") and state.get("patristic_works"):
        export_authors_full(state["author_sources"], state["patristic_works"])

    logger.info("=== Migration complete (%.1fs) ===", time.time() - t0)


if __name__ == "__main__":
    main()
