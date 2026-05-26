"""Join OCR line-break hyphens in Latin text columns (document_sections.text_la).

Same artifact as the Denzinger PT/Deferrari EN fixes: PDF scan inserted a hyphen
at every line break, producing artifacts like:
  "a mor- tuis"    →  "a mortuis"
  "eius- que"      →  "eiusque"
  "Diffini- mus"   →  "Diffinimus"

Affected documents:
  denzinger-hunermann (3291), vatican-ii (236), trent (218), vatican-i (127),
  constance (70), basel-florence (54), denzinger-deferrari-30 (46), others

Only fixes lowercase-to-lowercase pattern to avoid joining legitimate hyphens.

Usage:
    python pipeline/scripts/fix_latin_hyphenation.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

_HYPHEN_RE = re.compile(r'([a-z])- ([a-z])')


def _fix(text: str) -> str:
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
            "SELECT document_id, section_num, text_la "
            "FROM document_sections "
            "WHERE text_la IS NOT NULL AND REGEXP('[a-z]- [a-z]', text_la)"
        ).fetchall()
        print(f"Found {len(rows)} rows with Latin hyphenation artifacts")

        by_doc: dict[str, int] = {}
        updates: list[tuple[str, str, str]] = []
        for row in rows:
            fixed = _fix(row["text_la"])
            if fixed != row["text_la"]:
                updates.append((fixed, row["document_id"], row["section_num"]))
                by_doc[row["document_id"]] = by_doc.get(row["document_id"], 0) + 1

        print(f"Will update {len(updates)} rows:")
        for doc, cnt in sorted(by_doc.items(), key=lambda x: -x[1]):
            print(f"  {doc}: {cnt}")

        if args.dry_run:
            for new_text, doc_id, sec_num in updates[:3]:
                orig = next(r["text_la"] for r in rows if r["section_num"] == sec_num and r["document_id"] == doc_id)
                print(f"  [{doc_id}:{sec_num}] before: {orig[:120]}")
                print(f"                    after:  {new_text[:120]}")
            print("[dry-run] No changes written.")
            return

        conn.executemany(
            "UPDATE document_sections SET text_la = ? WHERE document_id = ? AND section_num = ?",
            updates,
        )
        conn.commit()
        print(f"Updated {len(updates)} rows.")

        (remaining,) = conn.execute(
            "SELECT COUNT(*) FROM document_sections "
            "WHERE text_la IS NOT NULL AND REGEXP('[a-z]- [a-z]', text_la)"
        ).fetchone()
        print(f"Remaining Latin hyphenation artifacts: {remaining}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
