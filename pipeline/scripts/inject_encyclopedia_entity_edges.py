"""Extract entities from encyclopedia articles and add shared_entity edges.

Runs the regex entity extractor over each article's text, persists results to
``encyclopedia_entities`` (mirrors ``paragraph_entities``), then builds:

  * ency↔ency shared_entity edges where articles share >= min_shared entities
  * ency↔p    shared_entity edges where article and paragraph share >= min_shared

Mega-entities (appearing in more than ``max_group_size`` articles or paragraphs)
are dropped before pairing — otherwise terms like 'episcopate' or 'church'
create hairball fanout that drowns out real signal.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_encyclopedia_entity_edges
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from collections import defaultdict
from itertools import combinations
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.entity_extraction import extract_entities  # noqa: E402

MIN_SHARED = 3
MAX_GROUP_SIZE = 500  # entities mentioned in more items than this are skipped

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("inject_ency_edges")


def ensure_encyclopedia_entities_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS encyclopedia_entities (
            article_id TEXT NOT NULL,
            entity_id  TEXT NOT NULL,
            PRIMARY KEY (article_id, entity_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ency_ents_entity ON encyclopedia_entities(entity_id)"
    )
    conn.commit()


def extract_and_store(conn: sqlite3.Connection) -> int:
    """Populate encyclopedia_entities if empty. Returns row count."""
    existing = conn.execute("SELECT COUNT(*) FROM encyclopedia_entities").fetchone()[0]
    if existing:
        log.info("encyclopedia_entities already has %d rows — reusing", existing)
        return existing

    log.info("Extracting entities from encyclopedia articles...")
    t0 = time.time()
    articles = conn.execute(
        "SELECT id, title, text_en FROM encyclopedia"
    ).fetchall()

    rows: list[tuple[str, str]] = []
    with_any = 0
    for article_id, title, text in articles:
        blob = (title or "") + " " + (text or "")
        ents = extract_entities(blob)
        if ents:
            with_any += 1
            rows.extend((article_id, e) for e in ents)

    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.executemany(
        "INSERT OR IGNORE INTO encyclopedia_entities VALUES (?, ?)",
        rows,
    )
    conn.commit()
    log.info(
        "  extracted %d (article, entity) rows from %d/%d articles in %.1fs",
        len(rows), with_any, len(articles), time.time() - t0,
    )
    return len(rows)


def compute_edges(conn: sqlite3.Connection) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Return (ency_ency_edges, ency_para_edges) honoring MIN_SHARED + MAX_GROUP_SIZE."""
    ency_by_entity: dict[str, list[str]] = defaultdict(list)
    for aid, eid in conn.execute("SELECT article_id, entity_id FROM encyclopedia_entities"):
        ency_by_entity[eid].append(aid)

    para_by_entity: dict[str, list[int]] = defaultdict(list)
    for pid, eid in conn.execute("SELECT paragraph_id, entity_id FROM paragraph_entities"):
        para_by_entity[eid].append(pid)

    entity_ids = set(ency_by_entity) | set(para_by_entity)
    dropped = [
        e for e in entity_ids
        if len(ency_by_entity.get(e, [])) > MAX_GROUP_SIZE
        or len(para_by_entity.get(e, [])) > MAX_GROUP_SIZE
    ]
    log.info("Dropping %d mega-entities (>%d items): %s",
             len(dropped), MAX_GROUP_SIZE, sorted(dropped))

    ee_pairs: dict[tuple[str, str], int] = defaultdict(int)
    ep_pairs: dict[tuple[str, int], int] = defaultdict(int)

    for eid in entity_ids:
        if eid in dropped:
            continue
        a_list = ency_by_entity.get(eid, [])
        p_list = para_by_entity.get(eid, [])

        # ency↔ency
        for a, b in combinations(a_list, 2):
            key = (a, b) if a < b else (b, a)
            ee_pairs[key] += 1

        # ency↔paragraph (cross product)
        for a in a_list:
            for p in p_list:
                ep_pairs[(a, p)] += 1

    ency_ency_edges = [
        (f"ency:{a}", f"ency:{b}") for (a, b), count in ee_pairs.items() if count >= MIN_SHARED
    ]
    ency_para_edges = [
        (f"ency:{a}", f"p:{p}") for (a, p), count in ep_pairs.items() if count >= MIN_SHARED
    ]
    log.info("  pre-threshold: %d ency-ency pairs, %d ency-para pairs",
             len(ee_pairs), len(ep_pairs))
    log.info("  post-threshold (>=%d shared): %d ency-ency, %d ency-para",
             MIN_SHARED, len(ency_ency_edges), len(ency_para_edges))
    return ency_ency_edges, ency_para_edges


def insert_edges(conn: sqlite3.Connection, edges: list[tuple[str, str]]) -> int:
    rows = [(src, tgt, "shared_entity") for src, tgt in edges]
    cur = conn.cursor()
    before = conn.execute("SELECT COUNT(*) FROM graph_edges WHERE edge_type='shared_entity'").fetchone()[0]
    cur.executemany("INSERT OR IGNORE INTO graph_edges VALUES (?, ?, ?)", rows)
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM graph_edges WHERE edge_type='shared_entity'").fetchone()[0]
    return after - before


def bump_degrees(conn: sqlite3.Connection, edges: list[tuple[str, str]]) -> None:
    deg: dict[str, int] = defaultdict(int)
    for src, tgt in edges:
        deg[src] += 1
        deg[tgt] += 1
    cur = conn.cursor()
    cur.executemany(
        "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
        [(inc, nid) for nid, inc in deg.items()],
    )
    conn.commit()


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))

    ensure_encyclopedia_entities_table(conn)
    extract_and_store(conn)

    ency_ency, ency_para = compute_edges(conn)
    all_new_edges = ency_ency + ency_para

    added = insert_edges(conn, all_new_edges)
    bump_degrees(conn, all_new_edges)

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    conn.close()

    log.info("")
    log.info("Inserted %d new edges (ency-ency: %d, ency-para: %d)",
             added, len(ency_ency), len(ency_para))
    log.info("DB now has %d nodes, %d edges", total_nodes, total_edges)


if __name__ == "__main__":
    main()
