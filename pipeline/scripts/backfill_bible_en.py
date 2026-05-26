"""Backfill bible_verses.text_en for rows that have a PT/LA/EL translation
but no English text.

Downloads the KJV JSON (same source as fetch_bible_drb.py), looks up each
missing (book, chapter, verse) triple, and UPDATEs text_en in place.
Rebuilds bible_verses_fts after the update.

Usage:
    python pipeline/scripts/backfill_bible_en.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "bible"
KJV_CACHE = RAW_DIR / "drb.json"

KJV_URL = "https://raw.githubusercontent.com/thiagobodruk/bible/master/json/en_kjv.json"

# Maps canonical book IDs to the JSON "name" field
_CANON_TO_JSON: dict[str, str] = {
    "genesis": "Genesis", "exodus": "Exodus", "leviticus": "Leviticus",
    "numbers": "Numbers", "deuteronomy": "Deuteronomy", "joshua": "Joshua",
    "judges": "Judges", "ruth": "Ruth", "1-samuel": "1 Samuel",
    "2-samuel": "2 Samuel", "1-kings": "1 Kings", "2-kings": "2 Kings",
    "1-chronicles": "1 Chronicles", "2-chronicles": "2 Chronicles",
    "ezra": "Ezra", "nehemiah": "Nehemiah", "esther": "Esther",
    "job": "Job", "psalms": "Psalms", "proverbs": "Proverbs",
    "ecclesiastes": "Ecclesiastes", "song-of-solomon": "Song of Solomon",
    "isaiah": "Isaiah", "jeremiah": "Jeremiah", "lamentations": "Lamentations",
    "ezekiel": "Ezekiel", "daniel": "Daniel", "hosea": "Hosea",
    "joel": "Joel", "amos": "Amos", "obadiah": "Obadiah", "jonah": "Jonah",
    "micah": "Micah", "nahum": "Nahum", "habakkuk": "Habakkuk",
    "zephaniah": "Zephaniah", "haggai": "Haggai", "zechariah": "Zechariah",
    "malachi": "Malachi", "matthew": "Matthew", "mark": "Mark",
    "luke": "Luke", "john": "John", "acts": "Acts", "romans": "Romans",
    "1-corinthians": "1 Corinthians", "2-corinthians": "2 Corinthians",
    "galatians": "Galatians", "ephesians": "Ephesians",
    "philippians": "Philippians", "colossians": "Colossians",
    "1-thessalonians": "1 Thessalonians", "2-thessalonians": "2 Thessalonians",
    "1-timothy": "1 Timothy", "2-timothy": "2 Timothy", "titus": "Titus",
    "philemon": "Philemon", "hebrews": "Hebrews", "james": "James",
    "1-peter": "1 Peter", "2-peter": "2 Peter", "1-john": "1 John",
    "2-john": "2 John", "3-john": "3 John", "jude": "Jude",
    "revelation": "Revelation",
    # Judith is deuterocanonical — not in standard KJV
    "judith": None,
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("backfill_bible_en")


def _load_kjv() -> dict[str, dict[int, list[str]]]:
    """Return {json_book_name: {chapter_num: [verse_text, ...]}}."""
    if not KJV_CACHE.exists():
        log.info("Downloading KJV JSON from %s", KJV_URL)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        resp = requests.get(KJV_URL, timeout=60)
        resp.raise_for_status()
        data = json.loads(resp.content.decode("utf-8-sig"))
        KJV_CACHE.write_text(json.dumps(data, ensure_ascii=False))
        log.info("Cached to %s", KJV_CACHE)
    else:
        log.info("Using cached KJV JSON: %s", KJV_CACHE)
        with open(KJV_CACHE, encoding="utf-8") as f:
            data = json.load(f)

    index: dict[str, dict[int, list[str]]] = {}
    for book in data:
        name = book.get("name", "")
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        index[name] = chapters
    return index


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    kjv = _load_kjv()
    log.info("KJV index: %d books", len(kjv))

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        # Find all rows with null text_en
        nulls = conn.execute(
            "SELECT book_id, chapter, verse FROM bible_verses WHERE text_en IS NULL"
        ).fetchall()
        log.info("Found %d verses with null text_en", len(nulls))

        updates: list[tuple[str, str, int, int]] = []
        not_in_kjv: list[tuple[str, int, int]] = []

        for row in nulls:
            book_id, chapter, verse = row["book_id"], row["chapter"], row["verse"]
            json_name = _CANON_TO_JSON.get(book_id)
            if json_name is None:
                # Deuterocanonical not in KJV (Judith etc.) — skip
                not_in_kjv.append((book_id, chapter, verse))
                continue
            book_chapters = kjv.get(json_name, {})
            verse_list = book_chapters.get(chapter, [])
            if verse < 1 or verse > len(verse_list):
                not_in_kjv.append((book_id, chapter, verse))
                continue
            text = verse_list[verse - 1].strip()
            if not text:
                not_in_kjv.append((book_id, chapter, verse))
                continue
            updates.append((text, book_id, chapter, verse))

        log.info(
            "Will update %d verses; %d not in KJV (deuterocanonical/out-of-range)",
            len(updates), len(not_in_kjv),
        )
        if not_in_kjv:
            books_skipped = {b for b, _, _ in not_in_kjv}
            log.info("  skipped books: %s", sorted(books_skipped))

        if args.dry_run:
            log.info("[dry-run] No changes written.")
            return

        conn.executemany(
            "UPDATE bible_verses SET text_en = ? WHERE book_id = ? AND chapter = ? AND verse = ?",
            updates,
        )
        conn.commit()
        log.info("Updated %d rows", len(updates))

        # Rebuild FTS for updated rows
        log.info("Rebuilding bible_verses_fts for updated rows...")
        conn.execute("DELETE FROM bible_verses_fts")
        conn.execute(
            "INSERT INTO bible_verses_fts (book_id, chapter, verse, text_en, text_la, text_pt, text_el) "
            "SELECT book_id, chapter, verse, text_en, text_la, text_pt, text_el FROM bible_verses"
        )
        conn.execute("INSERT INTO bible_verses_fts(bible_verses_fts) VALUES('optimize')")
        conn.commit()
        (fts_count,) = conn.execute("SELECT COUNT(*) FROM bible_verses_fts").fetchone()
        log.info("FTS rebuilt: %d rows", fts_count)

        # Final null count
        (remaining,) = conn.execute(
            "SELECT COUNT(*) FROM bible_verses WHERE text_en IS NULL"
        ).fetchone()
        log.info("Remaining null text_en: %d (all should be deuterocanonical)", remaining)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
