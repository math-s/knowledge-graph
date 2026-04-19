"""Export a subgraph of knowledge-graph.db as Bend-readable source.

Bend's preferred shape for graph data is a compile-time constant — the HVM
runtime parses it into a tree once, then parallel folds/bends operate over
it. That only scales to a few thousand nodes before compile time gets silly,
so we select a meaningful subset rather than the whole 62k-node graph.

Default selection (≈ few thousand nodes):
  * all theme / entity / topic hub nodes
  * all CCC paragraphs
  * top N encyclopedia articles by degree

Output files:
  * bend/graph_data.bend  — adjacency as a Bend list constant
  * bend/labels.json      — { int_id: { "node_id": ..., "label": ..., "type": ... } }

Usage:
    uv run --project pipeline python -m pipeline.scripts.export_graph_for_bend
        [--top-ency N] [--include-fathers] [--include-summa]
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
BEND_DIR = PROJECT_ROOT / "bend"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("export_bend")


def select_subgraph(
    conn: sqlite3.Connection,
    top_ency: int,
    include_paragraphs: bool,
    include_fathers: bool,
    include_summa: bool,
    include_bible_books: bool,
) -> list[tuple[str, str, str]]:
    """Return list of (node_id, node_type, label) for the subgraph."""
    rows: list[tuple[str, str, str]] = []

    # All hub types
    for node_type in ("theme", "entity", "topic", "summa-part", "bible-testament"):
        for r in conn.execute(
            "SELECT id, node_type, label FROM graph_nodes WHERE node_type = ?",
            (node_type,),
        ):
            rows.append(tuple(r))

    # Structure nodes (CCC hierarchy) — always cheap, 395 nodes
    for r in conn.execute(
        "SELECT id, node_type, label FROM graph_nodes WHERE node_type = 'structure'"
    ):
        rows.append(tuple(r))

    if include_paragraphs:
        for r in conn.execute(
            "SELECT id, node_type, label FROM graph_nodes WHERE node_type = 'paragraph'"
        ):
            rows.append(tuple(r))

    # Top encyclopedia articles by degree
    for r in conn.execute(
        "SELECT id, node_type, label FROM graph_nodes "
        "WHERE node_type='encyclopedia' ORDER BY degree DESC LIMIT ?",
        (top_ency,),
    ):
        rows.append(tuple(r))

    # Bible books (73 — cheap, useful)
    if include_bible_books:
        for r in conn.execute(
            "SELECT id, node_type, label FROM graph_nodes WHERE node_type = 'bible-book'"
        ):
            rows.append(tuple(r))

    if include_fathers:
        for r in conn.execute(
            "SELECT id, node_type, label FROM graph_nodes WHERE node_type='fathers-page' ORDER BY degree DESC LIMIT 200"
        ):
            rows.append(tuple(r))

    if include_summa:
        # Top summa-articles by degree
        for r in conn.execute(
            "SELECT id, node_type, label FROM graph_nodes WHERE node_type='summa-article' ORDER BY degree DESC LIMIT 200"
        ):
            rows.append(tuple(r))
        # All summa-questions
        for r in conn.execute(
            "SELECT id, node_type, label FROM graph_nodes WHERE node_type='summa-question'"
        ):
            rows.append(tuple(r))

    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--top-ency", type=int, default=100,
                   help="How many top encyclopedia articles to include (by degree)")
    p.add_argument("--include-paragraphs", action="store_true",
                   help="Include all ~2,865 CCC paragraph nodes (heavier)")
    p.add_argument("--include-fathers", action="store_true")
    p.add_argument("--include-summa", action="store_true")
    p.add_argument("--no-bible-books", action="store_true")
    args = p.parse_args()

    BEND_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))

    node_rows = select_subgraph(
        conn,
        top_ency=args.top_ency,
        include_paragraphs=args.include_paragraphs,
        include_fathers=args.include_fathers,
        include_summa=args.include_summa,
        include_bible_books=not args.no_bible_books,
    )
    node_rows = list({r[0]: r for r in node_rows}.values())  # dedup by id
    node_rows.sort()  # deterministic order
    log.info("Selected %d nodes", len(node_rows))

    # Assign stable int ids
    id_to_int: dict[str, int] = {r[0]: i for i, r in enumerate(node_rows)}
    labels = {
        i: {"node_id": r[0], "type": r[1], "label": r[2]}
        for i, r in enumerate(node_rows)
    }

    # Induce edges: both endpoints must be in the selection
    placeholders = ",".join("?" for _ in id_to_int)
    id_list = list(id_to_int.keys())
    cur = conn.execute(
        f"SELECT source, target, edge_type FROM graph_edges "
        f"WHERE source IN ({placeholders}) AND target IN ({placeholders})",
        id_list + id_list,
    )
    edges: list[tuple[int, int, str]] = []
    for src, tgt, et in cur:
        edges.append((id_to_int[src], id_to_int[tgt], et))
    log.info("Induced %d edges", len(edges))

    # Bend-source output — one big list of pairs.
    # Write as a generated function that returns the edge list.
    # Bend's parser handles long lists fine; we keep ints small (u24-safe).
    graph_bend = BEND_DIR / "graph_data.bend"
    with graph_bend.open("w") as f:
        f.write(f"# AUTO-GENERATED from pipeline/scripts/export_graph_for_bend.py\n")
        f.write(f"# {len(node_rows)} nodes, {len(edges)} edges\n\n")
        f.write(f"def node_count() -> u24:\n  return {len(node_rows)}\n\n")
        f.write(f"def edge_count() -> u24:\n  return {len(edges)}\n\n")
        # Flat adjacency as (src, tgt) tuples packed into a single list.
        # We emit it as a chain of cons-cells so Bend folds it into a tree.
        f.write("def edges() -> List((u24, u24)):\n  return [\n")
        for src, tgt, _et in edges:
            f.write(f"    ({src}, {tgt}),\n")
        f.write("  ]\n")

    labels_path = BEND_DIR / "labels.json"
    with labels_path.open("w") as f:
        json.dump(labels, f, ensure_ascii=False, indent=None)

    log.info("Wrote %s (%d bytes)", graph_bend, graph_bend.stat().st_size)
    log.info("Wrote %s", labels_path)


if __name__ == "__main__":
    main()
