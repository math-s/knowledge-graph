"""Backfill document_sections.text_la / text_pt from Vatican.va.

For each document with fetchable=1 and a source_url:
  - derive the LA and PT URLs (same rules as fetch_documents_multilang)
  - download & parse numbered sections
  - UPDATE document_sections where (document_id, section_num) matches
Rows whose section_num only exists in LA/PT are INSERTed with NULL text_en.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.fetch_documents_multilang import (  # noqa: E402
    _download_document_lang,
    _generate_lang_url,
    _parse_sections,
)

DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, source_url, available_langs_json FROM documents "
            "WHERE fetchable = 1 AND source_url IS NOT NULL AND source_url <> ''"
        )
        docs = cur.fetchall()
        logger.info("Found %d fetchable documents with a source_url", len(docs))

        per_doc_langs: dict[str, set[str]] = {}
        total_updates = 0
        total_inserts = 0

        for doc_id, source_url, langs_json in docs:
            known_langs = set()
            try:
                known_langs = set(json.loads(langs_json or "[]"))
            except json.JSONDecodeError:
                pass

            for lang in ("la", "pt"):
                lang_url = _generate_lang_url(source_url, lang)
                if not lang_url:
                    continue

                html = _download_document_lang(doc_id, lang_url, lang)
                if not html:
                    continue

                sections = _parse_sections(html)
                if not sections:
                    logger.info("  %s (%s): 0 sections parsed", doc_id, lang)
                    continue

                col = f"text_{lang}"
                upd_count = 0
                ins_count = 0
                for sec_num, text in sections.items():
                    cur.execute(
                        f"UPDATE document_sections SET {col} = ? "
                        f"WHERE document_id = ? AND section_num = ?",
                        (text, doc_id, sec_num),
                    )
                    if cur.rowcount == 0:
                        cur.execute(
                            f"INSERT INTO document_sections (document_id, section_num, {col}) "
                            f"VALUES (?, ?, ?)",
                            (doc_id, sec_num, text),
                        )
                        ins_count += 1
                    else:
                        upd_count += 1
                known_langs.add(lang)
                per_doc_langs.setdefault(doc_id, set()).update([*known_langs])
                total_updates += upd_count
                total_inserts += ins_count
                logger.info(
                    "  %s (%s): updated=%d inserted=%d", doc_id, lang, upd_count, ins_count
                )

        for doc_id, langs in per_doc_langs.items():
            sorted_langs = sorted(langs)
            cur.execute(
                "UPDATE documents SET available_langs_json = ? WHERE id = ?",
                (json.dumps(sorted_langs), doc_id),
            )

        conn.commit()

        logger.info(
            "Done. Updated %d section rows, inserted %d new rows.",
            total_updates,
            total_inserts,
        )

        cur.execute(
            "SELECT COUNT(*) FROM document_sections WHERE text_la IS NOT NULL AND text_la <> ''"
        )
        la_count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM document_sections WHERE text_pt IS NOT NULL AND text_pt <> ''"
        )
        pt_count = cur.fetchone()[0]
        logger.info("Sections with LA: %d, with PT: %d", la_count, pt_count)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
