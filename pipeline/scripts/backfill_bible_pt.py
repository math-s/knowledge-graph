"""Backfill bible_verses.text_pt from the Portuguese Bible (ACF) JSON.

Reads existing (book_id, chapter, verse) rows from the DB and UPDATEs text_pt
where a PT match exists. INSERTs PT-only rows are NOT done here — only the
intersection with verses the DB already knows about is populated.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.fetch_bible_pt import fetch_full_bible_pt  # noqa: E402

DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    pt_books = fetch_full_bible_pt()
    if not pt_books:
        logger.error("No PT books returned; aborting.")
        return 1

    pt_verses: dict[tuple[str, int, int], str] = {}
    for book_id, book in pt_books.items():
        for ch_num, chapter in book.chapters.items():
            for v_idx, v_texts in chapter.verses.items():
                text = v_texts.get("pt") if isinstance(v_texts, dict) else None
                if text:
                    pt_verses[(book_id, ch_num, v_idx)] = text

    logger.info("PT source has %d verses", len(pt_verses))

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT book_id, chapter, verse FROM bible_verses")
        existing = {tuple(row) for row in cur.fetchall()}
        logger.info("DB has %d verses", len(existing))

        updates = [
            (text, book_id, ch, v)
            for (book_id, ch, v), text in pt_verses.items()
            if (book_id, ch, v) in existing
        ]
        logger.info("Will UPDATE text_pt on %d rows", len(updates))

        cur.executemany(
            "UPDATE bible_verses SET text_pt = ? WHERE book_id = ? AND chapter = ? AND verse = ?",
            updates,
        )
        conn.commit()

        cur.execute(
            "SELECT COUNT(*) FROM bible_verses WHERE text_pt IS NOT NULL AND text_pt <> ''"
        )
        pt_count = cur.fetchone()[0]
        logger.info("After backfill: %d verses have text_pt", pt_count)

        cur.execute(
            """SELECT book_id, COUNT(*) AS total,
                      SUM(CASE WHEN text_pt IS NOT NULL AND text_pt<>'' THEN 1 ELSE 0 END) AS pt
               FROM bible_verses GROUP BY book_id ORDER BY book_id"""
        )
        missing = [row for row in cur.fetchall() if row[2] == 0]
        if missing:
            logger.info("Books with ZERO PT coverage: %s", ", ".join(r[0] for r in missing))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
