"""Promote patristic sections to first-class graph nodes.

Adds ``patristic-section:<id>`` nodes and links each to its parent
``patristic-work:<work_id>`` via ``child_of``. Leaves existing CCC→work
``cites`` edges alone (rewiring to section-level would need footnote-parser
changes upstream).

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_patristic_sections
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

SECTION_COLOR = "#B07AA1"  # matches patristic family
SIZE = 3.0

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("inject_patristic_sections")


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    empty_json = json.dumps([])

    # Map section -> work via chapters
    chapter_to_work = dict(
        conn.execute("SELECT id, work_id FROM patristic_chapters").fetchall()
    )
    work_nodes = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='patristic-work'"
        )
    }
    # Titles for work_ids that only live in patristic_chapters (different scraper naming)
    work_titles: dict[str, str] = {}
    for cid, wid in chapter_to_work.items():
        if f"patristic-work:{wid}" not in work_nodes and wid not in work_titles:
            title_row = conn.execute(
                "SELECT title FROM patristic_chapters WHERE work_id=? AND title IS NOT NULL LIMIT 1",
                (wid,),
            ).fetchone()
            if title_row and title_row[0]:
                work_titles[wid] = title_row[0]

    sections = conn.execute(
        "SELECT id, chapter_id, number FROM patristic_sections"
    ).fetchall()

    # Ensure a patristic-work node exists for every work referenced by a section
    new_work_rows: list[tuple] = []
    for wid, title in work_titles.items():
        work_node = f"patristic-work:{wid}"
        if work_node in work_nodes:
            continue
        new_work_rows.append((
            work_node, title, "patristic-work",
            0.0, 0.0, 6.0, "#B07AA1", "",
            0, 0, empty_json, empty_json, empty_json,
        ))
        work_nodes.add(work_node)
    if new_work_rows:
        cur.executemany(
            "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            new_work_rows,
        )
        conn.commit()
        log.info("Added %d missing patristic-work nodes from chapter metadata", len(new_work_rows))

    node_rows = []
    edge_rows: list[tuple[str, str, str]] = []
    orphans = 0

    for sid, chapter_id, number in sections:
        work_id = chapter_to_work.get(chapter_id)
        if not work_id:
            orphans += 1
            continue
        work_node = f"patristic-work:{work_id}"
        if work_node not in work_nodes:
            orphans += 1
            continue
        node_id = f"patristic-section:{sid}"
        label = f"§{number}" if number is not None else sid.split("/")[-1]
        node_rows.append((
            node_id, label, "patristic-section",
            0.0, 0.0, SIZE, SECTION_COLOR, "",
            0, 0,
            empty_json, empty_json, empty_json,
        ))
        edge_rows.append((node_id, work_node, "child_of"))

    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        edge_rows,
    )
    conn.commit()

    # Degree bump
    deg: dict[str, int] = defaultdict(int)
    for src, tgt, _ in edge_rows:
        deg[src] += 1
        deg[tgt] += 1
    cur.executemany(
        "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
        [(inc, nid) for nid, inc in deg.items()],
    )
    conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    sec_nodes = conn.execute(
        "SELECT COUNT(*) FROM graph_nodes WHERE node_type='patristic-section'"
    ).fetchone()[0]
    conn.close()

    log.info("Added %d patristic-section nodes (%d orphans skipped)", len(node_rows), orphans)
    log.info("Added %d child_of edges", len(edge_rows))
    log.info("DB now has %d nodes (patristic-section: %d), %d edges",
             total_nodes, sec_nodes, total_edges)


if __name__ == "__main__":
    main()
