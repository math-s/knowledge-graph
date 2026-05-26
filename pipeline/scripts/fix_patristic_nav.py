"""Strip New Advent website navigation text contaminating patristic_sections.text_en.

The scraper_patristic.py scraper missed the "CONTACT US | ADVERTISE WITH NEW ADVENT"
string that New Advent embeds in its content <p> tags. This script:

  1. Strips the nav string from all contaminated rows (in-place SQL REPLACE)
  2. NULLs rows whose residual text is < 80 chars (pure TOC entries like
     "Book I Book II Book III")
  3. Rebuilds the patristic_sections_fts index

Also applies the same strip to the latin text columns (text_la, text_el)
in case any scraper variant wrote the string there.

Usage:
    python pipeline/scripts/fix_patristic_nav.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

NAV_STRING = "CONTACT US | ADVERTISE WITH NEW ADVENT"
MIN_RESIDUAL = 80


def _strip_and_null(conn: sqlite3.Connection, col: str, dry_run: bool) -> tuple[int, int]:
    """Strip nav string from `col`, then NULL rows whose residual is too short.

    Returns (rows_stripped, rows_nulled).
    """
    # Count contaminated rows
    (contaminated,) = conn.execute(
        f"SELECT COUNT(*) FROM patristic_sections WHERE {col} LIKE ?",
        (f"%{NAV_STRING}%",),
    ).fetchone()

    if contaminated == 0:
        return 0, 0

    print(f"  {col}: {contaminated} contaminated rows")

    if not dry_run:
        # Strip the nav string
        conn.execute(
            f"UPDATE patristic_sections "
            f"SET {col} = TRIM(REPLACE({col}, ?, '')) "
            f"WHERE {col} LIKE ?",
            (NAV_STRING, f"%{NAV_STRING}%"),
        )

    # Count rows that are now too short (including already-stripped)
    (to_null,) = conn.execute(
        f"SELECT COUNT(*) FROM patristic_sections "
        f"WHERE {col} IS NOT NULL AND LENGTH(TRIM({col})) < ?",
        (MIN_RESIDUAL,),
    ).fetchone()

    if not dry_run and to_null:
        conn.execute(
            f"UPDATE patristic_sections SET {col} = NULL "
            f"WHERE {col} IS NOT NULL AND LENGTH(TRIM({col})) < ?",
            (MIN_RESIDUAL,),
        )

    return contaminated, to_null


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
            stripped, nulled = _strip_and_null(conn, col, args.dry_run)
            if stripped:
                print(f"  {col}: stripped={stripped} → nulled={nulled}")

        if not args.dry_run:
            conn.commit()
            _rebuild_fts(conn)
            conn.commit()
            print("Done.")
        else:
            print("[dry-run] No changes written.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
