"""Strip New Advent website navigation text contaminating patristic_sections.text_en.

Handles two contamination flavours:
  1. "CONTACT US | ADVERTISE WITH NEW ADVENT" — footer link text
  2. "Contact information. The editor of New Advent is Kevin Knight" — contact footer
  3. Pure TOC entries (Book I Book II ..., Homily N Homily N+1 ...) that have
     no sentence structure (no periods or commas outside abbreviations)

Steps:
  1. Strip each nav string in-place via SQL REPLACE
  2. NULL rows whose residual text is < 80 chars
  3. NULL rows that look like pure TOC (no '.' or ',' in text_en)
  4. Rebuild patristic_sections_fts

Usage:
    python pipeline/scripts/fix_patristic_nav.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from pipeline.scripts.export_corpus_jsonl import sync as sync_corpus

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

# Ordered from most to least specific so REPLACE chains work cleanly
NAV_STRINGS = [
    "CONTACT US | ADVERTISE WITH NEW ADVENT",
    "Contact information. The editor of New Advent is Kevin Knight",
    "Contact information.",
]
MIN_RESIDUAL = 80


def _strip_nav_strings(conn: sqlite3.Connection, col: str, dry_run: bool) -> int:
    """Strip all known nav strings from `col`. Returns number of rows touched."""
    total = 0
    for nav in NAV_STRINGS:
        (cnt,) = conn.execute(
            f"SELECT COUNT(*) FROM patristic_sections WHERE {col} LIKE ?",
            (f"%{nav}%",),
        ).fetchone()
        if cnt:
            print(f"  {col}: {cnt} rows contain '{nav[:50]}'")
            if not dry_run:
                conn.execute(
                    f"UPDATE patristic_sections "
                    f"SET {col} = TRIM(REPLACE({col}, ?, '')) "
                    f"WHERE {col} LIKE ?",
                    (nav, f"%{nav}%"),
                )
            total += cnt
    return total


def _null_short_and_toc(conn: sqlite3.Connection, col: str, dry_run: bool) -> int:
    """NULL rows that are short OR have no sentence punctuation (pure TOC)."""
    (cnt,) = conn.execute(
        f"""SELECT COUNT(*) FROM patristic_sections
            WHERE {col} IS NOT NULL AND (
              LENGTH(TRIM({col})) < ?
              OR (LENGTH({col}) < 500
                  AND {col} NOT LIKE '%.%'
                  AND {col} NOT LIKE '%,%')
            )""",
        (MIN_RESIDUAL,),
    ).fetchone()
    if cnt:
        print(f"  {col}: NULLing {cnt} short/TOC-only rows")
        if not dry_run:
            conn.execute(
                f"""UPDATE patristic_sections SET {col} = NULL
                    WHERE {col} IS NOT NULL AND (
                      LENGTH(TRIM({col})) < ?
                      OR (LENGTH({col}) < 500
                          AND {col} NOT LIKE '%.%'
                          AND {col} NOT LIKE '%,%')
                    )""",
                (MIN_RESIDUAL,),
            )
    return cnt


def _rebuild_fts(conn: sqlite3.Connection) -> None:
    print("Rebuilding patristic_sections_fts...")
    conn.execute("DELETE FROM patristic_sections_fts")
    conn.execute(
        "INSERT INTO patristic_sections_fts (id, chapter_id, text_en) "
        "SELECT id, chapter_id, text_en FROM patristic_sections"
    )
    conn.execute("INSERT INTO patristic_sections_fts(patristic_sections_fts) VALUES('optimize')")
    (count,) = conn.execute("SELECT COUNT(*) FROM patristic_sections_fts").fetchone()
    print(f"  FTS rebuilt: {count} indexed rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report only, make no changes")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        for col in ("text_en", "text_la", "text_el"):
            stripped = _strip_nav_strings(conn, col, args.dry_run)
            nulled = _null_short_and_toc(conn, col, args.dry_run)
            if stripped or nulled:
                print(f"  {col}: nav_stripped={stripped} toc_nulled={nulled}")

        if not args.dry_run:
            conn.commit()
            sync_corpus("patristic")
            print("synced corpus JSONL")
            _rebuild_fts(conn)
            conn.commit()
            print("Done.")
        else:
            print("[dry-run] No changes written.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
