"""Download WEB + Douay-Rheims Bible text and populate bible_verses.text_en.

WEB (World English Bible) covers 66 Protestant-canon books.
Douay-Rheims covers all 73 Catholic-canon books (used for 7 deuterocanonical).

Usage:
    python -m pipeline.src.load_bible_en [--download] [--load] [--dry-run]
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

WEB_BASE_URL = "https://raw.githubusercontent.com/TehShrike/world-english-bible/master/json"
DR_URL = "https://raw.githubusercontent.com/xxruyle/Bible-DouayRheims/master/EntireBible-DR.json"

# ---------------------------------------------------------------------------
# Book name mappings
# ---------------------------------------------------------------------------

# WEB repo filename (no extension) → DB book_id
WEB_TO_DB: dict[str, str] = {
    "1chronicles": "1-chronicles",
    "1corinthians": "1-corinthians",
    "1john": "1-john",
    "1kings": "1-kings",
    "1peter": "1-peter",
    "1samuel": "1-samuel",
    "1thessalonians": "1-thessalonians",
    "1timothy": "1-timothy",
    "2chronicles": "2-chronicles",
    "2corinthians": "2-corinthians",
    "2john": "2-john",
    "2kings": "2-kings",
    "2peter": "2-peter",
    "2samuel": "2-samuel",
    "2thessalonians": "2-thessalonians",
    "2timothy": "2-timothy",
    "3john": "3-john",
    "acts": "acts",
    "amos": "amos",
    "colossians": "colossians",
    "daniel": "daniel",
    "deuteronomy": "deuteronomy",
    "ecclesiastes": "ecclesiastes",
    "ephesians": "ephesians",
    "esther": "esther",
    "exodus": "exodus",
    "ezekiel": "ezekiel",
    "ezra": "ezra",
    "galatians": "galatians",
    "genesis": "genesis",
    "habakkuk": "habakkuk",
    "haggai": "haggai",
    "hebrews": "hebrews",
    "hosea": "hosea",
    "isaiah": "isaiah",
    "james": "james",
    "jeremiah": "jeremiah",
    "job": "job",
    "joel": "joel",
    "john": "john",
    "jonah": "jonah",
    "joshua": "joshua",
    "jude": "jude",
    "judges": "judges",
    "lamentations": "lamentations",
    "leviticus": "leviticus",
    "luke": "luke",
    "malachi": "malachi",
    "mark": "mark",
    "matthew": "matthew",
    "micah": "micah",
    "nahum": "nahum",
    "nehemiah": "nehemiah",
    "numbers": "numbers",
    "obadiah": "obadiah",
    "philemon": "philemon",
    "philippians": "philippians",
    "proverbs": "proverbs",
    "psalms": "psalms",
    "revelation": "revelation",
    "romans": "romans",
    "ruth": "ruth",
    "songofsolomon": "song-of-solomon",
    "titus": "titus",
    "zechariah": "zechariah",
    "zephaniah": "zephaniah",
}

# Douay-Rheims book name → DB book_id (only deuterocanonical books)
DR_TO_DB: dict[str, str] = {
    "Tobias": "tobit",
    "Judith": "judith",
    "Wisdom": "wisdom",
    "Ecclesiasticus": "sirach",
    "Baruch": "baruch",
    "1 Machabees": "1-maccabees",
    "2 Machabees": "2-maccabees",
}

# ---------------------------------------------------------------------------
# WEB parsing
# ---------------------------------------------------------------------------

def parse_web_book(data: list[dict]) -> dict[tuple[int, int], str]:
    """Parse WEB JSON into {(chapter, verse): text} dict.

    WEB data is a list of paragraph/line elements. Multiple text entries
    may belong to the same verse — we concatenate them.
    """
    verses: dict[tuple[int, int], str] = {}
    for item in data:
        if item.get("type") not in ("paragraph text", "line text"):
            continue
        ch = item.get("chapterNumber")
        vs = item.get("verseNumber")
        val = item.get("value", "").strip()
        if ch is None or vs is None or not val:
            continue
        key = (int(ch), int(vs))
        if key in verses:
            verses[key] += " " + val
        else:
            verses[key] = val
    return verses


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_web(out_dir: Path) -> Path:
    """Download all 66 WEB book JSON files into out_dir/web/."""
    web_dir = out_dir / "web"
    web_dir.mkdir(parents=True, exist_ok=True)

    for filename in WEB_TO_DB:
        dest = web_dir / f"{filename}.json"
        if dest.exists():
            click.echo(f"  skip {filename} (exists)")
            continue
        url = f"{WEB_BASE_URL}/{filename}.json"
        click.echo(f"  downloading {filename}...")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        dest.write_bytes(resp.content)

    click.echo(f"WEB: {len(WEB_TO_DB)} books in {web_dir}")
    return web_dir


def download_dr(out_dir: Path) -> Path:
    """Download Douay-Rheims entire Bible JSON."""
    dest = out_dir / "douay-rheims.json"
    if dest.exists():
        click.echo(f"  skip douay-rheims.json (exists)")
        return dest
    click.echo("  downloading Douay-Rheims...")
    resp = requests.get(DR_URL, timeout=60)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    click.echo(f"DR: saved to {dest}")
    return dest


# ---------------------------------------------------------------------------
# Load into DB
# ---------------------------------------------------------------------------

def load_web_into_db(web_dir: Path, conn: sqlite3.Connection, dry_run: bool) -> int:
    """Load WEB text into bible_verses.text_en. Creates missing verse rows."""
    total_updated = 0
    total_inserted = 0

    for filename, book_id in sorted(WEB_TO_DB.items()):
        src = web_dir / f"{filename}.json"
        if not src.exists():
            click.echo(f"  WARN: {src} not found, skipping {book_id}")
            continue

        data = json.loads(src.read_text())
        verses = parse_web_book(data)

        if not verses:
            click.echo(f"  WARN: no verses parsed from {filename}")
            continue

        # Check which verses already exist in DB
        existing = set(
            (r[0], r[1]) for r in conn.execute(
                "SELECT chapter, verse FROM bible_verses WHERE book_id = ?",
                (book_id,),
            ).fetchall()
        )

        updated = 0
        inserted = 0
        for (ch, vs), text in sorted(verses.items()):
            if (ch, vs) in existing:
                if not dry_run:
                    conn.execute(
                        "UPDATE bible_verses SET text_en = ? WHERE book_id = ? AND chapter = ? AND verse = ?",
                        (text, book_id, ch, vs),
                    )
                updated += 1
            else:
                if not dry_run:
                    conn.execute(
                        "INSERT INTO bible_verses (book_id, chapter, verse, text_en) VALUES (?, ?, ?, ?)",
                        (book_id, ch, vs, text),
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


def load_dr_into_db(dr_path: Path, conn: sqlite3.Connection, dry_run: bool) -> int:
    """Load Douay-Rheims text for deuterocanonical books into bible_verses.text_en."""
    data = json.loads(dr_path.read_text())
    total = 0

    for dr_name, book_id in sorted(DR_TO_DB.items()):
        if dr_name not in data:
            click.echo(f"  WARN: '{dr_name}' not found in DR data")
            continue

        book_data = data[dr_name]
        existing = set(
            (r[0], r[1]) for r in conn.execute(
                "SELECT chapter, verse FROM bible_verses WHERE book_id = ?",
                (book_id,),
            ).fetchall()
        )

        updated = 0
        inserted = 0
        for ch_str, verses in book_data.items():
            ch = int(ch_str)
            for vs_str, text in verses.items():
                vs = int(vs_str)
                # Strip DR footnote markers (asterisks)
                clean_text = text.replace("*", "").strip()
                if (ch, vs) in existing:
                    if not dry_run:
                        conn.execute(
                            "UPDATE bible_verses SET text_en = ? WHERE book_id = ? AND chapter = ? AND verse = ?",
                            (clean_text, book_id, ch, vs),
                        )
                    updated += 1
                else:
                    if not dry_run:
                        conn.execute(
                            "INSERT INTO bible_verses (book_id, chapter, verse, text_en) VALUES (?, ?, ?, ?)",
                            (book_id, ch, vs, clean_text),
                        )
                    inserted += 1

        total += updated + inserted
        suffix = " (dry-run)" if dry_run else ""
        click.echo(f"  {book_id}: {updated} updated, {inserted} inserted{suffix}")

    return total


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
    """Download and load English Bible text (WEB + Douay-Rheims)."""
    db_path = Path(db) if db else DB_PATH

    if download:
        click.echo("\n=== Downloading Bible sources ===")
        web_dir = download_web(DATA_DIR)
        dr_path = download_dr(DATA_DIR)
    else:
        web_dir = DATA_DIR / "web"
        dr_path = DATA_DIR / "douay-rheims.json"

    if load:
        click.echo(f"\n=== Loading into {db_path.name} ===")
        conn = sqlite3.connect(str(db_path))

        click.echo("\n--- WEB (66 books) ---")
        web_count = load_web_into_db(web_dir, conn, dry_run)

        click.echo("\n--- Douay-Rheims (7 deuterocanonical) ---")
        dr_count = load_dr_into_db(dr_path, conn, dry_run)

        if not dry_run:
            conn.commit()
            rebuild_bible_fts(conn)
            conn.commit()

        conn.close()
        click.echo(f"\nTotal: {web_count + dr_count} verses processed")
        if dry_run:
            click.echo("(dry-run — no changes written)")


if __name__ == "__main__":
    main()
