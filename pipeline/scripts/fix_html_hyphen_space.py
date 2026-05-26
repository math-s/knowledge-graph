"""Remove spurious spaces after hyphens in HTML-scraped English text.

When BeautifulSoup extracts text from New Advent HTML pages, a hyphen at the
end of a line followed by a newline becomes "word- nextword" (space inserted
between the two halves of the compound).  Unlike OCR PDFs where the hyphen
itself is an artifact, these are legitimate compound words — the fix is to
close the gap without removing the hyphen:

  "all- powerful"    →  "all-powerful"
  "pre- existence"   →  "pre-existence"
  "fellow- bishop"   →  "fellow-bishop"

Affects 83 rows across 47 documents in document_sections.text_en.
Idempotent.

Usage:
    python pipeline/scripts/fix_html_hyphen_space.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

from pipeline.scripts.export_corpus_jsonl import sync as sync_corpus

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

# Lowercase letter, hyphen, space, lowercase letter — HTML line-wrap artifact
_HYPHEN_SPACE_RE = re.compile(r'([a-z])- ([a-z])')


def _fix(text: str) -> str:
    return _HYPHEN_SPACE_RE.sub(r'\1-\2', text)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.create_function("REGEXP", 2, lambda pat, val: bool(re.search(pat, val or "")))
    try:
        rows = conn.execute(
            "SELECT document_id, section_num, text_en "
            "FROM document_sections "
            "WHERE text_en IS NOT NULL AND REGEXP('[a-z]- [a-z]', text_en)"
        ).fetchall()
        print(f"Found {len(rows)} rows")

        by_doc: dict[str, int] = {}
        updates: list[tuple[str, str, str]] = []
        for row in rows:
            fixed = _fix(row["text_en"])
            if fixed != row["text_en"]:
                updates.append((fixed, row["document_id"], row["section_num"]))
                by_doc[row["document_id"]] = by_doc.get(row["document_id"], 0) + 1

        print(f"Will update {len(updates)} rows:")
        for doc, cnt in sorted(by_doc.items(), key=lambda x: -x[1]):
            print(f"  {doc}: {cnt}")

        if args.dry_run:
            for new_text, doc_id, sec_num in updates[:4]:
                orig = next(r["text_en"] for r in rows if r["section_num"] == sec_num and r["document_id"] == doc_id)
                m = re.search(r'[a-z]- [a-z]', orig)
                if m:
                    start = max(0, m.start() - 20)
                    print(f"  [{doc_id}:{sec_num}] ...{orig[start:m.end()+20]!r} → ...{new_text[start:m.end()+19]!r}")
            print("[dry-run] No changes written.")
            return

        conn.executemany(
            "UPDATE document_sections SET text_en = ? WHERE document_id = ? AND section_num = ?",
            updates,
        )
        conn.commit()
        print(f"Updated {len(updates)} rows.")
        if updates:
            for doc_id in by_doc:
                sync_corpus("documents", doc_id=doc_id)
            print(f"synced corpus JSONL for {len(by_doc)} document(s)")

        (remaining,) = conn.execute(
            "SELECT COUNT(*) FROM document_sections "
            "WHERE text_en IS NOT NULL AND REGEXP('[a-z]- [a-z]', text_en)"
        ).fetchone()
        print(f"Remaining: {remaining}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
