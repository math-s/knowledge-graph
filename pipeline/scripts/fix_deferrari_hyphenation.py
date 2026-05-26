"""Join OCR line-break hyphens in Denzinger-Deferrari English text.

The PDF was scanned with hyphens at line ends, producing artifacts like:
  "acknowl- edged"  →  "acknowledged"
  "knowl- edge"     →  "knowledge"
  "propria- mente"  →  "propriamente"  (shouldn't appear but handled)

We fix only the lowercase-to-lowercase pattern to avoid joining legitimate
hyphens in compound words or proper nouns.  Idempotent.

Usage:
    python pipeline/scripts/fix_deferrari_hyphenation.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

_HYPHEN_RE = re.compile(r'([a-z])- ([a-z])')


def _fix_text(text: str) -> str:
    return _HYPHEN_RE.sub(r'\1\2', text)


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
            "WHERE document_id LIKE 'denzinger-deferrari%' "
            "  AND text_en IS NOT NULL "
            "  AND REGEXP('[a-z]- [a-z]', text_en)"
        ).fetchall()
        print(f"Found {len(rows)} rows with hyphenation artifacts")

        updates: list[tuple[str, str, str]] = []
        for row in rows:
            fixed = _fix_text(row["text_en"])
            if fixed != row["text_en"]:
                updates.append((fixed, row["document_id"], row["section_num"]))

        print(f"Will update {len(updates)} rows")
        if args.dry_run:
            for new_text, doc_id, sec_num in updates[:3]:
                orig = next(r["text_en"] for r in rows if r["section_num"] == sec_num)
                print(f"  [{sec_num}] before: {orig[:120]}")
                print(f"           after:  {new_text[:120]}")
            print("[dry-run] No changes written.")
            return

        conn.executemany(
            "UPDATE document_sections SET text_en = ? "
            "WHERE document_id = ? AND section_num = ?",
            updates,
        )
        conn.commit()
        print(f"Updated {len(updates)} Deferrari EN sections.")

        (remaining,) = conn.execute(
            "SELECT COUNT(*) FROM document_sections "
            "WHERE document_id LIKE 'denzinger-deferrari%' "
            "  AND REGEXP('[a-z]- [a-z]', text_en)"
        ).fetchone()
        print(f"Remaining hyphenation artifacts: {remaining}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
