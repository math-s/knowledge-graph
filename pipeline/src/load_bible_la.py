"""Download Latin Vulgate Bible text and populate bible_verses.text_la.

Source: https://raw.githubusercontent.com/emilekm2142/vulgate-bible-full-text/master/bible.json
73 books, structure: {"BookName": [[verse1_ch1, verse2_ch1, ...], [verse1_ch2, ...]]}

Usage:
    python -m pipeline.src.load_bible_la [--download] [--load] [--dry-run]
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import click
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "bible"
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

VULGATE_URL = "https://raw.githubusercontent.com/emilekm2142/vulgate-bible-full-text/master/bible.json"

# ---------------------------------------------------------------------------
# Book name mappings
# ---------------------------------------------------------------------------

# Vulgate JSON book name → DB book_id
VULGATE_TO_DB: dict[str, str] = {
    # Old Testament
    "Genesis": "genesis",
    "Exodus": "exodus",
    "Leviticus": "leviticus",
    "Numbers": "numbers",
    "Deuteronomy": "deuteronomy",
    "Joshua": "joshua",
    "Judges": "judges",
    "Ruth": "ruth",
    "1 Samuel": "1-samuel",
    "2 Samuel": "2-samuel",
    "1 Kings": "1-kings",
    "2 Kings": "2-kings",
    "1 Chronicles": "1-chronicles",
    "2 Chronicles": "2-chronicles",
    "Ezra": "ezra",
    "Nehemiah": "nehemiah",
    "Esther": "esther",
    "Job": "job",
    "Psalms": "psalms",
    "Proverbs": "proverbs",
    "Ecclesiastes": "ecclesiastes",
    "Song of Solomon": "song-of-solomon",
    "Isaiah": "isaiah",
    "Jeremiah": "jeremiah",
    "Lamentations": "lamentations",
    "Ezekiel": "ezekiel",
    "Daniel": "daniel",
    "Hosea": "hosea",
    "Joel": "joel",
    "Amos": "amos",
    "Obadiah": "obadiah",
    "Jonah": "jonah",
    "Micah": "micah",
    "Nahum": "nahum",
    "Habakkuk": "habakkuk",
    "Zephaniah": "zephaniah",
    "Haggai": "haggai",
    "Zechariah": "zechariah",
    "Malachi": "malachi",
    # Deuterocanonical books (Catholic canon)
    "Tobias": "tobit",  # Vulgate name for Tobit
    "Judith": "judith",
    "Wisdom": "wisdom",
    "Sirach": "sirach",
    "Baruch": "baruch",
    "1 Macabees": "1-maccabees",  # Note: source has "Macabees" not "Maccabees"
    "2 Macabees": "2-maccabees",
    # New Testament
    "Matthew": "matthew",
    "Mark": "mark",
    "Luke": "luke",
    "John": "john",
    "Acts": "acts",
    "Romans": "romans",
    "1 Corinthians": "1-corinthians",
    "2 Corinthians": "2-corinthians",
    "Galatians": "galatians",
    "Ephesians": "ephesians",
    "Philippians": "philippians",
    "Colossians": "colossians",
    "1 Thessalonians": "1-thessalonians",
    "2 Thessalonians": "2-thessalonians",
    "1 Timothy": "1-timothy",
    "2 Timothy": "2-timothy",
    "Titus": "titus",
    "Philemon": "philemon",
    "Hebrews": "hebrews",
    "James": "james",
    "1 Peter": "1-peter",
    "2 Peter": "2-peter",
    "1 John": "1-john",
    "2 John": "2-john",
    "3 John": "3-john",
    "Jude": "jude",
    "Revelation": "revelation",
}

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_vulgate(out_dir: Path) -> Path:
    """Download Vulgate Bible JSON."""
    dest = out_dir / "vulgate.json"
    if dest.exists():
        click.echo(f"  skip vulgate.json (exists)")
        return dest

    click.echo("  downloading Vulgate...")
    out_dir.mkdir(parents=True, exist_ok=True)
    resp = requests.get(VULGATE_URL, timeout=60)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    click.echo(f"Vulgate: saved to {dest}")
    return dest


# ---------------------------------------------------------------------------
# Load into DB
# ---------------------------------------------------------------------------

def load_vulgate_into_db(vulgate_path: Path, conn: sqlite3.Connection, dry_run: bool) -> int:
    """Load Vulgate text into bible_verses.text_la.

    The Vulgate JSON structure is:
    {
        "BookName": [
            [verse1_ch1, verse2_ch1, ...],  # chapter 1
            [verse1_ch2, verse2_ch2, ...],  # chapter 2
            ...
        ]
    }
    """
    data = json.loads(vulgate_path.read_text())
    total_updated = 0
    total_inserted = 0

    for vulgate_name, book_id in sorted(VULGATE_TO_DB.items()):
        if vulgate_name not in data:
            click.echo(f"  WARN: '{vulgate_name}' not found in Vulgate data")
            continue

        book_chapters = data[vulgate_name]

        # Check which verses already exist in DB for this book
        existing = set(
            (r[0], r[1]) for r in conn.execute(
                "SELECT chapter, verse FROM bible_verses WHERE book_id = ?",
                (book_id,),
            ).fetchall()
        )

        updated = 0
        inserted = 0

        for ch_idx, chapter_verses in enumerate(book_chapters, start=1):
            for vs_idx, text in enumerate(chapter_verses, start=1):
                text = text.strip()
                if not text:
                    continue

                if (ch_idx, vs_idx) in existing:
                    if not dry_run:
                        conn.execute(
                            "UPDATE bible_verses SET text_la = ? WHERE book_id = ? AND chapter = ? AND verse = ?",
                            (text, book_id, ch_idx, vs_idx),
                        )
                    updated += 1
                else:
                    if not dry_run:
                        conn.execute(
                            "INSERT INTO bible_verses (book_id, chapter, verse, text_la) VALUES (?, ?, ?, ?)",
                            (book_id, ch_idx, vs_idx, text),
                        )
                    inserted += 1

        total_updated += updated
        total_inserted += inserted
        suffix = " (dry-run)" if dry_run else ""
        if inserted:
            click.echo(f"  {book_id}: {updated} updated, {inserted} inserted{suffix}")
        else:
            click.echo(f"  {book_id}: {updated} updated{suffix}")

    return total_updated + total_inserted


# ---------------------------------------------------------------------------
# FTS rebuild
# ---------------------------------------------------------------------------

def rebuild_bible_fts(conn: sqlite3.Connection) -> None:
    """Rebuild the bible_verses_fts index after text updates.

    The FTS table is standalone (not content-synced), so we drop and
    repopulate it from the bible_verses table.
    """
    click.echo("Rebuilding bible_verses_fts...")
    conn.execute("DROP TABLE IF EXISTS bible_verses_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE bible_verses_fts USING fts5(
            book_id,
            chapter,
            verse,
            text_en,
            text_la,
            text_pt,
            text_el
        )
    """)
    conn.execute("""
        INSERT INTO bible_verses_fts (book_id, chapter, verse, text_en, text_la, text_pt, text_el)
        SELECT book_id, chapter, verse, text_en, text_la, text_pt, text_el
        FROM bible_verses
    """)
    count = conn.execute("SELECT COUNT(*) FROM bible_verses_fts").fetchone()[0]
    click.echo(f"  FTS rebuilt with {count} rows.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--download/--no-download", default=True, help="Download source files")
@click.option("--load/--no-load", default=True, help="Load into database")
@click.option("--dry-run", is_flag=True, help="Show what would be done without writing")
@click.option("--db", type=click.Path(), default=None, help="Path to database")
def main(download: bool, load: bool, dry_run: bool, db: str | None) -> None:
    """Download and load Latin Vulgate Bible text."""
    db_path = Path(db) if db else DB_PATH

    if download:
        click.echo("\n=== Downloading Vulgate source ===")
        vulgate_path = download_vulgate(DATA_DIR)
    else:
        vulgate_path = DATA_DIR / "vulgate.json"

    if load:
        click.echo(f"\n=== Loading into {db_path.name} ===")
        conn = sqlite3.connect(str(db_path))

        click.echo("\n--- Vulgate (73 books) ---")
        count = load_vulgate_into_db(vulgate_path, conn, dry_run)

        if not dry_run:
            conn.commit()
            rebuild_bible_fts(conn)
            conn.commit()

        conn.close()
        click.echo(f"\nTotal: {count} verses processed")
        if dry_run:
            click.echo("(dry-run — no changes written)")


if __name__ == "__main__":
    main()
