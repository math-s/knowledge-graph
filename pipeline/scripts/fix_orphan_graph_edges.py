"""Fix the three classes of orphan graph_edges found during the data audit.

1. 74  contains_lemma → lemma-la:in
        Stale edges from an old lemmatization run where the Latin preposition
        was resolved to lemma_id='in'. The canonical ID is now 'in1'. Delete.

2. 26  cites → author:<id>
        Denzinger cites 7 authors (arnobius, bernard-clairvaux, hippolytus,
        vincent-lerins, cassiodorus, macarius-alexandria, novatian) that exist
        in the `authors` table but were never promoted to graph_nodes.
        Create the missing nodes.

3. 4   same_as → patristic-work:<id>
        Three fathers-page nodes link via same_as to patristic-work nodes that
        don't exist in graph_nodes. Create the missing work nodes.

Usage:
    python pipeline/scripts/fix_orphan_graph_edges.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

AUTHOR_COLOR = "#E07B54"
AUTHOR_SIZE = 6.0
WORK_COLOR = "#B07AA1"
WORK_SIZE = 6.0


def fix_stale_lemma_edges(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Delete stale contains_lemma edges pointing to lemma-la:in."""
    (count,) = conn.execute(
        "SELECT COUNT(*) FROM graph_edges "
        "WHERE edge_type='contains_lemma' AND target='lemma-la:in'"
    ).fetchone()
    print(f"Stale lemma-la:in edges: {count}")
    if not dry_run and count:
        conn.execute(
            "DELETE FROM graph_edges WHERE edge_type='contains_lemma' AND target='lemma-la:in'"
        )
    return count


def fix_missing_author_nodes(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Create graph nodes for authors that exist in `authors` but not in `graph_nodes`."""
    orphan_author_ids = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT SUBSTR(e.target, 8) AS author_id
            FROM graph_edges e
            WHERE e.edge_type = 'cites'
              AND e.target LIKE 'author:%'
              AND NOT EXISTS (SELECT 1 FROM graph_nodes n WHERE n.id = e.target)
            """
        )
    ]
    print(f"Missing author nodes: {orphan_author_ids}")
    if not orphan_author_ids:
        return 0

    empty_json = json.dumps([])
    node_rows = []
    for author_id in orphan_author_ids:
        row = conn.execute(
            "SELECT name, era, work_count FROM authors WHERE id = ?", (author_id,)
        ).fetchone()
        if not row:
            print(f"  WARNING: author '{author_id}' not in authors table — skipping")
            continue
        name, era, work_count = row
        node_rows.append((
            f"author:{author_id}",
            name or author_id,
            "author",
            0.0, 0.0, AUTHOR_SIZE, AUTHOR_COLOR, "",
            0, 0,
            empty_json, empty_json, empty_json,
        ))
        print(f"  + author:{author_id}  ({name})")

    if not dry_run and node_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            node_rows,
        )
    return len(node_rows)


def fix_missing_work_nodes(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Create graph nodes for patristic-work targets that are missing."""
    orphan_work_ids = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT e.target
            FROM graph_edges e
            WHERE e.edge_type = 'same_as'
              AND e.target LIKE 'patristic-work:%'
              AND NOT EXISTS (SELECT 1 FROM graph_nodes n WHERE n.id = e.target)
            """
        )
    ]
    print(f"Missing patristic-work nodes: {orphan_work_ids}")
    if not orphan_work_ids:
        return 0

    empty_json = json.dumps([])
    node_rows = []
    for node_id in orphan_work_ids:
        work_id = node_id.removeprefix("patristic-work:")
        # Try to get a title from author_works or patristic_chapters
        title_row = conn.execute(
            "SELECT title FROM author_works WHERE id = ? LIMIT 1", (work_id,)
        ).fetchone()
        label = title_row[0] if title_row and title_row[0] else work_id
        node_rows.append((
            node_id, label, "patristic-work",
            0.0, 0.0, WORK_SIZE, WORK_COLOR, "",
            0, 0,
            empty_json, empty_json, empty_json,
        ))
        print(f"  + {node_id}  ({label})")

    if not dry_run and node_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            node_rows,
        )
    return len(node_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        deleted = fix_stale_lemma_edges(conn, args.dry_run)
        authors_added = fix_missing_author_nodes(conn, args.dry_run)
        works_added = fix_missing_work_nodes(conn, args.dry_run)

        if not args.dry_run:
            conn.commit()
            # Verify
            (remaining,) = conn.execute(
                "SELECT COUNT(*) FROM graph_edges e "
                "WHERE NOT EXISTS (SELECT 1 FROM graph_nodes n WHERE n.id = e.target)"
            ).fetchone()
            print(
                f"\nDone: deleted={deleted} author_nodes_added={authors_added} "
                f"work_nodes_added={works_added} remaining_orphans={remaining}"
            )
        else:
            print("\n[dry-run] No changes written.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
