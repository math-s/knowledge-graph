"""Join OCR line-break hyphens in Denzinger-Hünermann Portuguese text.

The PDF was scanned with hyphens at line ends, producing artifacts like:
  "Vir- gem Maria"  →  "Virgem Maria"
  "logo de- pois"   →  "logo depois"
  "propria- mente"  →  "propriamente"

We fix only the lowercase-to-lowercase pattern (letter + "- " + letter) to
avoid joining legitimate hyphens in proper nouns (e.g. "São João- Paulo").
Idempotent: a second run finds nothing to fix.

Usage:
    python pipeline/scripts/fix_denzinger_hyphenation.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

_HYPHEN_RE = re.compile(r'([a-záéíóúãõçàèìòùâêîôûäëïöü])- ([a-záéíóúãõçàèìòùâêîôûäëïöü])')


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
            "SELECT document_id, section_num, text_pt "
            "FROM document_sections "
            "WHERE document_id = 'denzinger-hunermann' "
            "  AND text_pt IS NOT NULL "
            "  AND text_pt REGEXP '[a-záéíóúãõç]- [a-záéíóúãõç]'"
        ).fetchall()
        print(f"Found {len(rows)} rows with hyphenation artifacts")

        updates: list[tuple[str, str, str]] = []
        for row in rows:
            fixed = _fix_text(row["text_pt"])
            if fixed != row["text_pt"]:
                updates.append((fixed, row["document_id"], row["section_num"]))

        print(f"Will update {len(updates)} rows")
        if args.dry_run:
            # Show a few examples
            for new_text, doc_id, sec_num in updates[:3]:
                orig = next(r["text_pt"] for r in rows if r["section_num"] == sec_num)
                print(f"  [{sec_num}] before: {orig[:100]}")
                print(f"          after:  {new_text[:100]}")
            print("[dry-run] No changes written.")
            return

        conn.executemany(
            "UPDATE document_sections SET text_pt = ? "
            "WHERE document_id = ? AND section_num = ?",
            updates,
        )
        conn.commit()
        print(f"Updated {len(updates)} Denzinger PT sections.")

        # Verify
        (remaining,) = conn.execute(
            "SELECT COUNT(*) FROM document_sections "
            "WHERE document_id = 'denzinger-hunermann' "
            "  AND REGEXP('[a-záéíóúãõç]- [a-záéíóúãõç]', text_pt)"
        ).fetchone()
        print(f"Remaining hyphenation artifacts: {remaining}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
