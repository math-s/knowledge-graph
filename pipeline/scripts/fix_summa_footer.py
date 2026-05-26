"""Strip New Advent book-edition footer from summa_articles.text.

Every article scraped from New Advent's Summa Theologica ends with the book
edition boilerplate:

    "The Summa Theologica of St. Thomas Aquinas Second and Revised Edition,
     1920 Literally translated by Fathers of the English Dominican Province
     Online Edition Copyright © 2009 by Kevin Knight Nihil Obstat..."

615 out of 3127 articles are affected.  The anchor phrase
" The Summa Theologica of St. Thomas Aquinas Second" is distinctive and does
not appear elsewhere in article text.

Usage:
    python pipeline/scripts/fix_summa_footer.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from pipeline.scripts.export_corpus_jsonl import sync as sync_corpus

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

FOOTER_ANCHOR = " The Summa Theologica of St. Thomas Aquinas Second"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, text FROM summa_articles WHERE text LIKE ?",
            (f"%{FOOTER_ANCHOR}%",),
        ).fetchall()
        print(f"Found {len(rows)} articles with footer contamination")

        updates: list[tuple[str, str]] = []
        for row in rows:
            anchor_pos = row["text"].find(FOOTER_ANCHOR)
            if anchor_pos == -1:
                continue
            clean = row["text"][:anchor_pos].rstrip()
            if clean != row["text"]:
                updates.append((clean, row["id"]))

        print(f"Will update {len(updates)} articles")

        if args.dry_run:
            for new_text, art_id in updates[:3]:
                orig = next(r["text"] for r in rows if r["id"] == art_id)
                print(f"  [{art_id}] stripped {len(orig) - len(new_text)} chars")
                print(f"    tail before: ...{orig[max(0,len(orig)-100):]!r}")
                print(f"    tail after:  ...{new_text[max(0,len(new_text)-100):]!r}")
            print("[dry-run] No changes written.")
            return

        conn.executemany(
            "UPDATE summa_articles SET text = ? WHERE id = ?",
            updates,
        )
        conn.commit()
        print(f"Updated {len(updates)} summa_articles.")
        if updates:
            sync_corpus("summa")
            print("synced corpus JSONL")

        (remaining,) = conn.execute(
            "SELECT COUNT(*) FROM summa_articles WHERE text LIKE ?",
            (f"%{FOOTER_ANCHOR}%",),
        ).fetchone()
        print(f"Remaining footer contamination: {remaining}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
