"""Rewrite graph_edges that still use the old ``library-doc:<lib_id>``
addressing for documents that now also live at ``document:<slug>``.

After the ``migrate_library_docs_to_documents`` migration, the same content
existed under two node ids (e.g. ``library-doc:docs_jp02ev`` and
``document:evangelium-vitae``). This script unifies those to the
``document:<slug>`` address so that downstream queries don't have to check
both namespaces.

Leaves ``library-doc:almanac_*`` and any ``docs_*`` without a document twin
alone (they're genuinely still library-only).

Usage:
    uv run --project pipeline python -m pipeline.scripts.unify_library_doc_namespace
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("unify_library_doc_namespace")

TITLE_YEAR_RE = re.compile(r"\s*\(\s*\d{3,4}\s*\)\s*$")


# ── node-id helpers (single source of truth for these prefixes) ──────────────

def lib_node(lib_id: str) -> str:
    return f"library-doc:{lib_id}"


def doc_node(slug: str) -> str:
    return f"document:{slug}"


# ── title slug ──────────────────────────────────────────────────────────────

def _slugify(text: str) -> str:
    t = text.lower()
    t = re.sub(r"[^\w\s-]", "", t)
    t = re.sub(r"[\s_]+", "-", t).strip("-")
    return re.sub(r"-+", "-", t)


# ── mapping construction ────────────────────────────────────────────────────

def build_mapping(cur: sqlite3.Cursor) -> dict[str, str]:
    """lib_id -> document_slug. Uses (a) source_url join, (b) title slugify
    as fallback for docs that overlap with existing vatican.va-sourced rows."""
    mapping: dict[str, str] = {}
    doc_ids = {r[0] for r in cur.execute("SELECT id FROM documents")}

    # Source-url mapping (covers the 175 freshly-migrated rows)
    for lib_id, slug in cur.execute("""
        SELECT ld.id, d.id
        FROM library_docs ld
        JOIN documents d
          ON d.source_url = 'https://www.newadvent.org/library/' || substr(ld.id, 6)
        WHERE ld.id LIKE 'docs_%'
    """):
        mapping[lib_id] = slug

    # Title-slug fallback for the ones that overlap with existing documents
    for lib_id, title in cur.execute(
        "SELECT id, title FROM library_docs WHERE id LIKE 'docs_%'"
    ):
        if lib_id in mapping:
            continue
        name = TITLE_YEAR_RE.sub("", title).strip()
        slug = _slugify(name)
        if slug and slug in doc_ids:
            mapping[lib_id] = slug
    return mapping


def build_node_rewrites(mapping: dict[str, str]) -> dict[str, str]:
    """lib-doc node-id -> document node-id."""
    return {lib_node(lib_id): doc_node(slug) for lib_id, slug in mapping.items()}


# ── edge rewriting ──────────────────────────────────────────────────────────

def rewrite_edges(cur: sqlite3.Cursor, node_rewrites: dict[str, str]) -> int:
    """For every graph_edges row whose source or target is a rewrite-key,
    insert the remapped row and delete the original. Handles the both-endpoints
    case in a single sweep, avoiding partial rewrites."""
    rows = cur.execute("""
        SELECT source, target, edge_type FROM graph_edges
        WHERE source LIKE 'library-doc:docs_%' OR target LIKE 'library-doc:docs_%'
    """).fetchall()

    inserts: list[tuple[str, str, str]] = []
    deletes: list[tuple[str, str, str]] = []
    for src, tgt, et in rows:
        new_src = node_rewrites.get(src, src)
        new_tgt = node_rewrites.get(tgt, tgt)
        if (new_src, new_tgt) == (src, tgt):
            continue
        inserts.append((new_src, new_tgt, et))
        deletes.append((src, tgt, et))

    cur.executemany("INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)", inserts)
    cur.executemany(
        "DELETE FROM graph_edges WHERE source=? AND target=? AND edge_type=?",
        deletes,
    )
    return len(inserts)


def prune_orphan_library_nodes(
    cur: sqlite3.Cursor, mapping: dict[str, str]
) -> int:
    """Drop library-doc: nodes whose lib_id is in the mapping AND have no
    remaining edges referencing them."""
    pruned = 0
    for lib_id in mapping:
        node_id = lib_node(lib_id)
        still_in_use = cur.execute(
            "SELECT 1 FROM graph_edges WHERE source=? OR target=? LIMIT 1",
            (node_id, node_id),
        ).fetchone()
        if not still_in_use:
            cur.execute("DELETE FROM graph_nodes WHERE id=?", (node_id,))
            pruned += cur.rowcount
    return pruned


# ── orchestration ───────────────────────────────────────────────────────────

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        mapping = build_mapping(cur)
        log.info("mapped %d library_docs -> documents", len(mapping))

        node_rewrites = build_node_rewrites(mapping)
        n_rewritten = rewrite_edges(cur, node_rewrites)
        log.info("rewrote %d edges (src-only + tgt-only + both-sides)", n_rewritten)

        n_pruned = prune_orphan_library_nodes(cur, mapping)
        log.info("pruned %d now-orphan library-doc: nodes", n_pruned)

        conn.commit()

        remaining = cur.execute("""
            SELECT COUNT(*) FROM graph_edges
            WHERE source LIKE 'library-doc:docs_%' OR target LIKE 'library-doc:docs_%'
        """).fetchone()[0]
        almanac_left = cur.execute("""
            SELECT COUNT(*) FROM graph_edges
            WHERE source LIKE 'library-doc:almanac_%' OR target LIKE 'library-doc:almanac_%'
        """).fetchone()[0]
        log.info("remaining docs_* library-doc edges: %d (target: 0)", remaining)
        log.info("remaining almanac_* library-doc edges: %d (expected, not migrated)", almanac_left)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
