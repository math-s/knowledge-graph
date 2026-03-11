"""Orchestrate the full data pipeline with checkpoint-based resumability.

Each step saves a checkpoint after completing. On failure, re-run with
--resume to pick up where you left off.

Usage:
    python pipeline/scripts/run_pipeline.py              # Run all steps
    python pipeline/scripts/run_pipeline.py --resume      # Resume from last checkpoint
    python pipeline/scripts/run_pipeline.py --from 5      # Resume from step 5
    python pipeline/scripts/run_pipeline.py --only 4      # Run only step 4
    python pipeline/scripts/run_pipeline.py --list        # Show step status
    python pipeline/scripts/run_pipeline.py --clean       # Delete all checkpoints
"""

import argparse
import logging
import pickle
import sys
import time
from pathlib import Path

# Ensure the project root is on the path so pipeline.src imports work
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.src.ingest import run as run_ingest
from pipeline.src.footnote_parser import parse_all_footnotes
from pipeline.src.themes import assign_themes
from pipeline.src.graph_builder import (
    build_graph,
    add_shared_theme_edges,
    add_source_nodes,
    add_bible_hierarchy,
    add_bible_crossref_edges,
    add_patristic_work_hierarchy,
)
from pipeline.src.layout import compute_layout
from pipeline.src.export import export_graph, export_sources, export_bible_full, export_authors_full
from pipeline.src.fetch_bible import fetch_bible_texts
from pipeline.src.fetch_documents import fetch_document_texts
from pipeline.src.fetch_patristic import fetch_patristic_texts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CHECKPOINT_DIR = Path(__file__).resolve().parent.parent / "data" / "checkpoints"

# ── Step definitions ────────────────────────────────────────────────────────
# Step 4 is split into 4a-4d so each fetch sub-step gets its own checkpoint.
# This means if patristic works fails, Bible data is still checkpointed.

STEPS = {
    1:  "Ingest CCC data",
    2:  "Parse footnotes",
    3:  "Assign themes",
    4:  "Fetch legacy sources (Bible KJV, documents, author metadata)",
    5:  "Fetch full Bible (4 languages + cross-refs)",
    6:  "Fetch patristic full-text works",
    7:  "Build base graph",
    8:  "Add shared-theme edges",
    9:  "Add source nodes + hierarchies",
    10: "Compute layout",
    11: "Export graph for web",
    12: "Export source data",
}


# ── Checkpoint helpers ──────────────────────────────────────────────────────

def _checkpoint_path(step: int) -> Path:
    return CHECKPOINT_DIR / f"step_{step:02d}.pkl"


def _save_checkpoint(step: int, state: dict) -> None:
    """Save pipeline state after a step completes."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    path = _checkpoint_path(step)
    with open(path, "wb") as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info("  Checkpoint saved: %s (%.1f MB)", path.name, size_mb)


def _load_checkpoint(step: int) -> dict | None:
    """Load checkpoint for a given step, or None if not found."""
    path = _checkpoint_path(step)
    if not path.exists():
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def _latest_checkpoint() -> int:
    """Return the highest completed step number, or 0."""
    if not CHECKPOINT_DIR.exists():
        return 0
    completed = []
    for p in CHECKPOINT_DIR.glob("step_*.pkl"):
        try:
            n = int(p.stem.split("_")[1])
            completed.append(n)
        except (ValueError, IndexError):
            pass
    return max(completed) if completed else 0


def _clean_checkpoints() -> None:
    """Remove all checkpoint files."""
    if not CHECKPOINT_DIR.exists():
        logger.info("No checkpoints to clean.")
        return
    count = 0
    for p in CHECKPOINT_DIR.glob("step_*.pkl"):
        p.unlink()
        count += 1
    logger.info("Removed %d checkpoint file(s).", count)


def _list_steps() -> None:
    """Print step status table."""
    latest = _latest_checkpoint()
    print("\n  Step  Status                          Description")
    print("  ----  ------------------------------  ----------------------------------------")
    for step, desc in STEPS.items():
        path = _checkpoint_path(step)
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(path.stat().st_mtime))
            status = f"done {ts} ({size_mb:.1f}MB)"
        else:
            status = "pending"
        marker = ">>>" if step == latest + 1 and latest > 0 else "   "
        print(f"  {marker} {step:2d}  {status:<30s}  {desc}")
    print()
    if latest > 0:
        print(f"  Last completed: step {latest}. Use --resume to continue from step {latest + 1}.")
    else:
        print("  No checkpoints found. Run the full pipeline or use --from N.")
    print()


# ── State management ────────────────────────────────────────────────────────

# All variables that flow between steps. Each checkpoint saves/restores these.
DEFAULT_STATE = {
    "paragraphs": [],
    "structures": [],
    "bible_sources": {},
    "document_sources": {},
    "author_sources": {},
    "bible_full": {},
    "crossrefs": {},
    "patristic_works": {},
    "G": None,
    "positions": {},
}


def _make_state(**overrides) -> dict:
    """Build a state dict with given overrides."""
    s = dict(DEFAULT_STATE)
    s.update(overrides)
    return s


# ── Fetch helpers ───────────────────────────────────────────────────────────

def _fetch_full_bible() -> dict:
    """Fetch full Bible in all available languages and merge."""
    from pipeline.src.fetch_bible_drb import fetch_full_bible_en
    from pipeline.src.fetch_bible_vulgate import fetch_full_bible_la
    from pipeline.src.fetch_bible_greek import fetch_full_bible_el
    from pipeline.src.fetch_bible_pt import fetch_full_bible_pt
    from pipeline.src.merge_languages import merge_bible_languages

    logger.info("--- Fetching English (Douay-Rheims) Bible ---")
    en_bible = fetch_full_bible_en()

    logger.info("--- Fetching Latin (Vulgate) Bible ---")
    la_bible = fetch_full_bible_la()

    logger.info("--- Fetching Greek Bible ---")
    el_bible = fetch_full_bible_el()

    logger.info("--- Fetching Portuguese Bible ---")
    pt_bible = fetch_full_bible_pt()

    logger.info("--- Merging Bible languages ---")
    merged = merge_bible_languages(en_bible, la_bible, el_bible, pt_bible)

    return merged


def _fetch_bible_crossrefs() -> dict:
    """Fetch Bible cross-references from TSK."""
    from pipeline.src.fetch_bible_crossrefs import fetch_bible_crossrefs
    return fetch_bible_crossrefs()


# ── Main pipeline ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the CCC knowledge graph pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  %(prog)s                  Run all steps from scratch
  %(prog)s --resume         Resume from last completed checkpoint
  %(prog)s --from 7         Resume from step 7 (load step 6 checkpoint)
  %(prog)s --only 5         Run only step 5 (load step 4 checkpoint)
  %(prog)s --list           Show which steps are done / pending
  %(prog)s --clean          Delete all checkpoints and start fresh
  %(prog)s --skip-fetch     Skip all network fetches (steps 4-6)

steps:
   1  Ingest CCC             5  Fetch full Bible      9  Add source nodes
   2  Parse footnotes         6  Fetch patristic      10  Compute layout
   3  Assign themes           7  Build graph          11  Export graph
   4  Fetch legacy sources    8  Theme edges          12  Export sources
        """,
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from the last completed checkpoint",
    )
    parser.add_argument(
        "--from", type=int, default=0, dest="from_step",
        help="Resume from a specific step (loads previous checkpoint)",
    )
    parser.add_argument(
        "--only", type=int, default=0,
        help="Run only a specific step (loads previous checkpoint)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Show step completion status and exit",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Delete all checkpoints and exit",
    )
    parser.add_argument(
        "--skip-fetch", action="store_true",
        help="Skip all network fetching (steps 4-6)",
    )
    parser.add_argument(
        "--skip-bible-full", action="store_true",
        help="Skip full Bible fetch (step 5)",
    )
    parser.add_argument(
        "--skip-patristic-works", action="store_true",
        help="Skip patristic full-text fetch (step 6)",
    )
    # Keep legacy --step flag for backward compat
    parser.add_argument("--step", type=int, default=0, help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Handle --list
    if args.list:
        _list_steps()
        return

    # Handle --clean
    if args.clean:
        _clean_checkpoints()
        return

    # Map legacy --step to --only
    if args.step > 0 and args.only == 0:
        args.only = args.step

    # Determine step range
    if args.resume:
        latest = _latest_checkpoint()
        if latest == 0:
            logger.info("No checkpoints found. Running full pipeline.")
            start_step = 1
        else:
            start_step = latest + 1
            logger.info("Resuming from step %d (checkpoint %d found).", start_step, latest)
    elif args.from_step > 0:
        start_step = args.from_step
    elif args.only > 0:
        start_step = args.only
    else:
        start_step = 1

    end_step = args.only if args.only > 0 else max(STEPS.keys())

    def should_run(step: int) -> bool:
        return start_step <= step <= end_step

    # ── Load state from checkpoint if resuming ──────────────────────────────

    state = dict(DEFAULT_STATE)

    if start_step > 1:
        # Load the checkpoint just before our start step
        load_from = start_step - 1
        logger.info("Loading checkpoint from step %d...", load_from)
        ckpt = _load_checkpoint(load_from)
        if ckpt is None:
            # Try to find any earlier checkpoint
            for s in range(load_from, 0, -1):
                ckpt = _load_checkpoint(s)
                if ckpt is not None:
                    logger.info("  Found checkpoint at step %d (not %d).", s, load_from)
                    load_from = s
                    break
        if ckpt is None:
            logger.warning("No checkpoint found before step %d. Running from step 1.", start_step)
            start_step = 1
        else:
            state.update(ckpt)
            logger.info("  State restored from step %d checkpoint.", load_from)

    # Unpack state variables
    paragraphs = state["paragraphs"]
    structures = state["structures"]
    bible_sources = state["bible_sources"]
    document_sources = state["document_sources"]
    author_sources = state["author_sources"]
    bible_full = state["bible_full"]
    crossrefs = state["crossrefs"]
    patristic_works = state["patristic_works"]
    G = state["G"]
    positions = state["positions"]

    if G is None:
        import networkx as nx
        G = nx.Graph()

    pipeline_start = time.time()

    # Helper to build the current full state dict for checkpoints
    def _current_state(**extra) -> dict:
        s = {
            "paragraphs": paragraphs,
            "structures": structures,
            "bible_sources": bible_sources,
            "document_sources": document_sources,
            "author_sources": author_sources,
            "bible_full": bible_full,
            "crossrefs": crossrefs,
            "patristic_works": patristic_works,
            "G": G,
            "positions": positions,
        }
        s.update(extra)
        return s

    # ── Step 1: Ingest CCC ──────────────────────────────────────────────────

    if should_run(1):
        t0 = time.time()
        logger.info("=== Step 1/12: Ingest CCC data ===")
        paragraphs, structures = run_ingest()
        logger.info("  Step 1 done in %.1fs (%d paragraphs)", time.time() - t0, len(paragraphs))
        _save_checkpoint(1, _current_state())
    elif not paragraphs:
        from pipeline.src.ingest import download_raw_data, parse_ccc
        raw_path = download_raw_data()
        paragraphs, structures = parse_ccc(raw_path)

    # ── Step 2: Parse footnotes ─────────────────────────────────────────────

    if should_run(2):
        t0 = time.time()
        logger.info("=== Step 2/12: Parse footnotes ===")
        paragraphs = parse_all_footnotes(paragraphs)
        logger.info("  Step 2 done in %.1fs", time.time() - t0)
        _save_checkpoint(2, _current_state())

    # ── Step 3: Assign themes ───────────────────────────────────────────────

    if should_run(3):
        t0 = time.time()
        logger.info("=== Step 3/12: Assign themes ===")
        paragraphs = assign_themes(paragraphs)
        logger.info("  Step 3 done in %.1fs", time.time() - t0)
        _save_checkpoint(3, _current_state())

    # ── Step 4: Fetch legacy sources ────────────────────────────────────────
    # Bible KJV, Vatican documents, author metadata.
    # Each fetcher caches raw downloads to disk, so re-runs skip cached items.

    if should_run(4):
        t0 = time.time()
        if args.skip_fetch:
            logger.info("=== Step 4/12: Fetch legacy sources (SKIPPED — --skip-fetch) ===")
        else:
            logger.info("=== Step 4/12: Fetch legacy sources ===")
            bible_sources = fetch_bible_texts(paragraphs)
            logger.info("  Fetched %d Bible book sources", len(bible_sources))
            document_sources = fetch_document_texts(paragraphs)
            logger.info("  Fetched %d document sources", len(document_sources))
            author_sources = fetch_patristic_texts(paragraphs)
            logger.info("  Fetched %d author sources", len(author_sources))
        logger.info("  Step 4 done in %.1fs", time.time() - t0)
        _save_checkpoint(4, _current_state())

    # ── Step 5: Fetch full Bible (4 languages + cross-refs) ─────────────────
    # Downloads DRB (en), Vulgate (la), Greek (el), Portuguese (pt).
    # Each language is cached as a single JSON file.

    if should_run(5):
        t0 = time.time()
        if args.skip_fetch or args.skip_bible_full:
            logger.info("=== Step 5/12: Fetch full Bible (SKIPPED) ===")
        else:
            logger.info("=== Step 5/12: Fetch full Bible (4 languages) ===")
            bible_full = _fetch_full_bible()

            logger.info("--- Fetching Bible cross-references (TSK) ---")
            crossrefs = _fetch_bible_crossrefs()

            # Propagate citing_paragraphs from CCC footnotes to Bible books
            for p in paragraphs:
                for pf in p.parsed_footnotes:
                    for br in pf.bible_refs:
                        if br.book in bible_full:
                            if p.id not in bible_full[br.book].citing_paragraphs:
                                bible_full[br.book].citing_paragraphs.append(p.id)
            for book in bible_full.values():
                book.citing_paragraphs.sort()

            logger.info("  Merged %d Bible books with cross-refs", len(bible_full))
        logger.info("  Step 5 done in %.1fs", time.time() - t0)
        _save_checkpoint(5, _current_state())

    # ── Step 6: Fetch patristic full-text works ─────────────────────────────
    # Crawls New Advent for full-text Church Father works.
    # Each chapter is cached individually — interrupting preserves progress.

    if should_run(6):
        t0 = time.time()
        if args.skip_fetch or args.skip_patristic_works:
            logger.info("=== Step 6/12: Fetch patristic works (SKIPPED) ===")
        elif author_sources:
            logger.info("=== Step 6/12: Fetch patristic full-text works ===")
            from pipeline.src.fetch_patristic_works import fetch_patristic_works
            patristic_works = fetch_patristic_works(author_sources)
            total_works = sum(len(ws) for ws in patristic_works.values())
            logger.info("  Fetched %d works across %d authors", total_works, len(patristic_works))
        else:
            logger.info("=== Step 6/12: Fetch patristic works (no author sources — skipped) ===")
        logger.info("  Step 6 done in %.1fs", time.time() - t0)
        _save_checkpoint(6, _current_state())

    # ── Step 7: Build base graph ────────────────────────────────────────────

    if should_run(7):
        t0 = time.time()
        logger.info("=== Step 7/12: Build base graph ===")
        G = build_graph(paragraphs, structures)
        logger.info("  Step 7 done in %.1fs (%d nodes, %d edges)", time.time() - t0, G.number_of_nodes(), G.number_of_edges())
        _save_checkpoint(7, _current_state())

    # ── Step 8: Add shared-theme edges ──────────────────────────────────────

    if should_run(8):
        t0 = time.time()
        logger.info("=== Step 8/12: Add shared-theme edges ===")
        G = add_shared_theme_edges(G, paragraphs)
        logger.info("  Step 8 done in %.1fs (%d edges total)", time.time() - t0, G.number_of_edges())
        _save_checkpoint(8, _current_state())

    # ── Step 9: Add source nodes + hierarchies ──────────────────────────────

    if should_run(9):
        t0 = time.time()
        logger.info("=== Step 9/12: Add source nodes ===")
        G = add_source_nodes(G, paragraphs)

        if bible_full:
            logger.info("--- Adding Bible hierarchy ---")
            G = add_bible_hierarchy(G, bible_full, paragraphs)
            logger.info("--- Adding Bible cross-reference edges ---")
            G = add_bible_crossref_edges(G, crossrefs)

        if patristic_works:
            logger.info("--- Adding patristic work hierarchy ---")
            G = add_patristic_work_hierarchy(G, patristic_works, paragraphs)

        logger.info("  Step 9 done in %.1fs (%d nodes, %d edges)", time.time() - t0, G.number_of_nodes(), G.number_of_edges())
        _save_checkpoint(9, _current_state())

    # ── Step 10: Compute layout ─────────────────────────────────────────────

    if should_run(10):
        t0 = time.time()
        logger.info("=== Step 10/12: Compute layout ===")
        positions = compute_layout(G)
        logger.info("  Step 10 done in %.1fs", time.time() - t0)
        _save_checkpoint(10, _current_state())

    # ── Step 11: Export graph for web ───────────────────────────────────────

    if should_run(11):
        t0 = time.time()
        logger.info("=== Step 11/12: Export graph for web ===")
        export_graph(G, positions, paragraphs)
        logger.info("  Step 11 done in %.1fs", time.time() - t0)
        _save_checkpoint(11, _current_state())

    # ── Step 12: Export source data ─────────────────────────────────────────

    if should_run(12):
        t0 = time.time()
        logger.info("=== Step 12/12: Export source data ===")
        export_sources(bible_sources, document_sources, author_sources)

        if bible_full:
            logger.info("--- Exporting full Bible (chunked per-book) ---")
            export_bible_full(bible_full)

        if patristic_works and author_sources:
            logger.info("--- Exporting patristic works (chunked per-author) ---")
            export_authors_full(author_sources, patristic_works)

        logger.info("  Step 12 done in %.1fs", time.time() - t0)

    # ── Summary ─────────────────────────────────────────────────────────────

    elapsed = time.time() - pipeline_start
    logger.info("=== Pipeline complete (%.1fs total) ===", elapsed)
    logger.info(
        "Final graph: %d nodes, %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )

    # Log node type breakdown
    type_counts: dict[str, int] = {}
    for n in G.nodes:
        ntype = G.nodes[n].get("node_type", "unknown")
        type_counts[ntype] = type_counts.get(ntype, 0) + 1
    for ntype, count in sorted(type_counts.items()):
        logger.info("  %s: %d nodes", ntype, count)

    # Log edge type breakdown
    etype_counts: dict[str, int] = {}
    for _, _, data in G.edges(data=True):
        etype = data.get("edge_type", "unknown")
        etype_counts[etype] = etype_counts.get(etype, 0) + 1
    for etype, count in sorted(etype_counts.items()):
        logger.info("  %s: %d edges", etype, count)


if __name__ == "__main__":
    main()
