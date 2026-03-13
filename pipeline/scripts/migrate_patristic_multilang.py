#!/usr/bin/env python3
"""One-time migration: add Latin/Greek texts to existing patristic works.

Loads the latest checkpoint, runs the Latin and Greek fetchers on the
existing English-only patristic works, then re-exports source data.
No previously-fetched data is re-downloaded.

Usage:
    python -m pipeline.scripts.migrate_patristic_multilang [--dry-run]
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate patristic works to multilingual")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without fetching")
    args = parser.parse_args()

    t0 = time.time()
    step_num, state = _load_latest_checkpoint()

    patristic_works = state.get("patristic_works")
    if not patristic_works:
        logger.error("No patristic_works in checkpoint — nothing to migrate")
        return

    # Show current state
    total_works = sum(len(ws) for ws in patristic_works.values())
    logger.info("Found %d authors, %d total works (all English-only)", len(patristic_works), total_works)

    if args.dry_run:
        from pipeline.src.fetch_patristic_latin import _LATIN_CATALOG
        from pipeline.src.fetch_patristic_greek import _GREEK_CATALOG

        latin_authors = set(_LATIN_CATALOG.keys()) & set(patristic_works.keys())
        greek_authors = set(_GREEK_CATALOG.keys()) & set(patristic_works.keys())
        logger.info("[DRY RUN] Would fetch Latin for: %s", sorted(latin_authors))
        logger.info("[DRY RUN] Would fetch Greek for: %s", sorted(greek_authors))
        logger.info("[DRY RUN] No changes made.")
        return

    # Run Latin fetcher
    logger.info("=== Fetching Latin patristic texts ===")
    from pipeline.src.fetch_patristic_latin import fetch_patristic_latin
    patristic_works = fetch_patristic_latin(patristic_works)

    # Run Greek fetcher
    logger.info("=== Fetching Greek patristic texts ===")
    from pipeline.src.fetch_patristic_greek import fetch_patristic_greek
    patristic_works = fetch_patristic_greek(patristic_works)

    # Summary
    for author_id, works in patristic_works.items():
        lang_keys: set[str] = set()
        for w in works:
            for ch in w.chapters:
                for sec in ch.sections:
                    lang_keys.update(sec.text.keys())
        if len(lang_keys) > 1:
            logger.info("  %s: langs=%s", author_id, sorted(lang_keys))

    # Update state and save checkpoint
    state["patristic_works"] = patristic_works
    checkpoint_path = CHECKPOINTS_DIR / f"step_{step_num:02d}.pkl"
    with open(checkpoint_path, "wb") as f:
        pickle.dump(state, f)
    logger.info("Updated checkpoint: %s", checkpoint_path.name)

    # Re-export source data
    logger.info("=== Re-exporting source data ===")
    from pipeline.src.export import export_sources, export_authors_full, export_documents_full

    export_sources(
        state["bible_sources"],
        state["document_sources"],
        state["author_sources"],
    )

    if state.get("author_sources"):
        export_authors_full(state["author_sources"], patristic_works)

    if state.get("document_sources"):
        export_documents_full(state["document_sources"])

    logger.info("=== Migration complete (%.1fs) ===", time.time() - t0)


if __name__ == "__main__":
    main()
