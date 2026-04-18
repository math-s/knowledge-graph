"""Promote themes and topics to first-class graph nodes.

Mirrors inject_entity_nodes: each theme and topic becomes a hub node with
``has_theme`` / ``has_topic`` edges from every paragraph tagged with it.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_theme_topic_nodes
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

THEME_COLOR = "#4E79A7"
TOPIC_COLOR = "#59A14F"
SIZE = 10.0

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("inject_theme_topic")


def run() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    empty_json = json.dumps([])
    cur = conn.cursor()

    # ── Theme nodes ─────────────────────────────────────────────────────────
    theme_rows = conn.execute("SELECT id, label FROM themes").fetchall()
    node_rows = [
        (
            f"theme:{tid}",
            label,
            "theme",
            0.0, 0.0,
            SIZE,
            THEME_COLOR,
            "",
            0, 0,
            empty_json, empty_json, empty_json,
        )
        for tid, label in theme_rows
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    conn.commit()
    log.info("Added/ignored %d theme nodes", len(node_rows))

    # Theme mention edges
    pt_rows = conn.execute(
        "SELECT paragraph_id, theme_id FROM paragraph_themes"
    ).fetchall()
    theme_edges = [
        (f"p:{pid}", f"theme:{tid}", "has_theme") for pid, tid in pt_rows
    ]
    before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='has_theme'"
    ).fetchone()[0]
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)", theme_edges
    )
    conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='has_theme'"
    ).fetchone()[0]
    log.info("Inserted %d has_theme edges", after - before)

    # ── Topic nodes ─────────────────────────────────────────────────────────
    topic_rows = conn.execute("SELECT id, terms_json FROM topics").fetchall()
    t_nodes = []
    for tid, terms_json in topic_rows:
        try:
            terms = json.loads(terms_json or "[]")
        except Exception:
            terms = []
        label = ", ".join(terms[:4]) if terms else f"Topic {tid}"
        t_nodes.append((
            f"topic:{tid}",
            label,
            "topic",
            0.0, 0.0,
            SIZE,
            TOPIC_COLOR,
            "",
            0, 0,
            empty_json, empty_json, empty_json,
        ))
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        t_nodes,
    )
    conn.commit()
    log.info("Added/ignored %d topic nodes", len(t_nodes))

    # Topic mention edges
    pt_rows = conn.execute(
        "SELECT paragraph_id, topic_id FROM paragraph_topics"
    ).fetchall()
    topic_edges = [
        (f"p:{pid}", f"topic:{tid}", "has_topic") for pid, tid in pt_rows
    ]
    before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='has_topic'"
    ).fetchone()[0]
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)", topic_edges
    )
    conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='has_topic'"
    ).fetchone()[0]
    log.info("Inserted %d has_topic edges", after - before)

    # ── Bump degrees ────────────────────────────────────────────────────────
    deg: dict[str, int] = defaultdict(int)
    for src, tgt, _ in theme_edges + topic_edges:
        deg[src] += 1
        deg[tgt] += 1
    cur.executemany(
        "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
        [(inc, nid) for nid, inc in deg.items()],
    )
    conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    conn.close()
    log.info("")
    log.info("DB now has %d nodes, %d edges", total_nodes, total_edges)


if __name__ == "__main__":
    run()
