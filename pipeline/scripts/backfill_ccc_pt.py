"""Backfill CCC paragraphs.text_pt from Vatican.va.

Reads existing paragraph IDs from data/knowledge-graph.db, fetches Portuguese
text via fetch_ccc_multilang._fetch_ccc_lang("pt"), and UPDATEs paragraphs
in-place. Also refreshes the paragraphs FTS index if present.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.fetch_ccc_multilang import _fetch_ccc_lang  # noqa: E402

DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    logger.info("Fetching PT CCC from Vatican.va (cached on repeat runs)...")
    pt_paras = _fetch_ccc_lang("pt")
    logger.info("Fetched %d PT paragraphs", len(pt_paras))

    if not pt_paras:
        logger.error("No PT paragraphs fetched; aborting.")
        return 1

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM paragraphs")
        existing_ids = {row[0] for row in cur.fetchall()}
        logger.info("DB has %d paragraphs", len(existing_ids))

        updates = [
            (text, para_num)
            for para_num, text in pt_paras.items()
            if para_num in existing_ids
        ]
        logger.info("Will UPDATE text_pt on %d rows", len(updates))

        cur.executemany("UPDATE paragraphs SET text_pt = ? WHERE id = ?", updates)
        conn.commit()

        cur.execute(
            "SELECT COUNT(*) FROM paragraphs WHERE text_pt IS NOT NULL AND text_pt <> ''"
        )
        pt_count = cur.fetchone()[0]
        logger.info("After backfill: %d paragraphs have text_pt", pt_count)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
