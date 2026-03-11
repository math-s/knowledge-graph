"""Fetch Bible verse texts for references cited in CCC footnotes.

Downloads a public-domain Bible JSON (World English Bible) and extracts
only the verses actually cited by CCC paragraphs.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import requests

from .models import BibleBookSource, Paragraph

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BIBLE_CACHE = RAW_DIR / "bible.json"

# World English Bible — public domain, JSON format
BIBLE_URL = "https://raw.githubusercontent.com/thiagobodruk/bible/master/json/en_kjv.json"

# Old Testament books (canonical IDs)
_OT_BOOKS = {
    "genesis", "exodus", "leviticus", "numbers", "deuteronomy",
    "joshua", "judges", "ruth", "1-samuel", "2-samuel",
    "1-kings", "2-kings", "1-chronicles", "2-chronicles",
    "ezra", "nehemiah", "tobit", "judith", "esther",
    "1-maccabees", "2-maccabees", "job", "psalms", "proverbs",
    "ecclesiastes", "song-of-solomon", "wisdom", "sirach",
    "isaiah", "jeremiah", "lamentations", "baruch", "ezekiel",
    "daniel", "hosea", "joel", "amos", "obadiah", "jonah",
    "micah", "nahum", "habakkuk", "zephaniah", "haggai",
    "zechariah", "malachi",
}

# Maps our canonical IDs to the KJV JSON book names
_CANONICAL_TO_KJV: dict[str, str] = {
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
}

# Display names and abbreviations
_BOOK_META: dict[str, tuple[str, str]] = {
    "genesis": ("Genesis", "Gen"), "exodus": ("Exodus", "Ex"),
    "leviticus": ("Leviticus", "Lev"), "numbers": ("Numbers", "Num"),
    "deuteronomy": ("Deuteronomy", "Deut"), "joshua": ("Joshua", "Josh"),
    "judges": ("Judges", "Judg"), "ruth": ("Ruth", "Ruth"),
    "1-samuel": ("1 Samuel", "1 Sam"), "2-samuel": ("2 Samuel", "2 Sam"),
    "1-kings": ("1 Kings", "1 Kings"), "2-kings": ("2 Kings", "2 Kings"),
    "1-chronicles": ("1 Chronicles", "1 Chron"),
    "2-chronicles": ("2 Chronicles", "2 Chron"),
    "ezra": ("Ezra", "Ezra"), "nehemiah": ("Nehemiah", "Neh"),
    "tobit": ("Tobit", "Tob"), "judith": ("Judith", "Jdt"),
    "esther": ("Esther", "Esth"),
    "1-maccabees": ("1 Maccabees", "1 Macc"),
    "2-maccabees": ("2 Maccabees", "2 Macc"),
    "job": ("Job", "Job"), "psalms": ("Psalms", "Ps"),
    "proverbs": ("Proverbs", "Prov"),
    "ecclesiastes": ("Ecclesiastes", "Eccl"),
    "song-of-solomon": ("Song of Solomon", "Song"),
    "wisdom": ("Wisdom", "Wis"), "sirach": ("Sirach", "Sir"),
    "isaiah": ("Isaiah", "Isa"), "jeremiah": ("Jeremiah", "Jer"),
    "lamentations": ("Lamentations", "Lam"), "baruch": ("Baruch", "Bar"),
    "ezekiel": ("Ezekiel", "Ezek"), "daniel": ("Daniel", "Dan"),
    "hosea": ("Hosea", "Hos"), "joel": ("Joel", "Joel"),
    "amos": ("Amos", "Amos"), "obadiah": ("Obadiah", "Obad"),
    "jonah": ("Jonah", "Jonah"), "micah": ("Micah", "Mic"),
    "nahum": ("Nahum", "Nah"), "habakkuk": ("Habakkuk", "Hab"),
    "zephaniah": ("Zephaniah", "Zeph"), "haggai": ("Haggai", "Hag"),
    "zechariah": ("Zechariah", "Zech"), "malachi": ("Malachi", "Mal"),
    "matthew": ("Matthew", "Mt"), "mark": ("Mark", "Mk"),
    "luke": ("Luke", "Lk"), "john": ("John", "Jn"),
    "acts": ("Acts", "Acts"), "romans": ("Romans", "Rom"),
    "1-corinthians": ("1 Corinthians", "1 Cor"),
    "2-corinthians": ("2 Corinthians", "2 Cor"),
    "galatians": ("Galatians", "Gal"), "ephesians": ("Ephesians", "Eph"),
    "philippians": ("Philippians", "Phil"),
    "colossians": ("Colossians", "Col"),
    "1-thessalonians": ("1 Thessalonians", "1 Thess"),
    "2-thessalonians": ("2 Thessalonians", "2 Thess"),
    "1-timothy": ("1 Timothy", "1 Tim"), "2-timothy": ("2 Timothy", "2 Tim"),
    "titus": ("Titus", "Titus"), "philemon": ("Philemon", "Phlm"),
    "hebrews": ("Hebrews", "Heb"), "james": ("James", "Jas"),
    "1-peter": ("1 Peter", "1 Pet"), "2-peter": ("2 Peter", "2 Pet"),
    "1-john": ("1 John", "1 Jn"), "2-john": ("2 John", "2 Jn"),
    "3-john": ("3 John", "3 Jn"), "jude": ("Jude", "Jude"),
    "revelation": ("Revelation", "Rev"),
}


def _download_bible() -> list[dict]:
    """Download and cache the KJV Bible JSON."""
    if BIBLE_CACHE.exists():
        logger.info("Using cached Bible JSON: %s", BIBLE_CACHE)
        with open(BIBLE_CACHE, encoding="utf-8") as f:
            return json.load(f)

    logger.info("Downloading Bible JSON from %s", BIBLE_URL)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(BIBLE_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    with open(BIBLE_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info("Cached Bible JSON: %s (%d books)", BIBLE_CACHE, len(data))
    return data


def _build_bible_index(bible_data: list[dict]) -> dict[str, dict[int, list[str]]]:
    """Build index: KJV book name -> {chapter_num: [verse_texts]}.

    The JSON format is a list of books, each with "name", "chapters" (list of
    lists of verse strings).
    """
    index: dict[str, dict[int, list[str]]] = {}
    for book in bible_data:
        name = book.get("name", "")
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        index[name] = chapters
    return index


def parse_reference(ref: str) -> list[tuple[int, int]]:
    """Parse a reference string like '5:1-12' or '3:16' into (chapter, verse) pairs.

    Returns a list of (chapter, verse) tuples for each individual verse in the range.
    Handles formats: '5:1', '5:1-12', '5:1,3', '5:1-3,7'.
    """
    results: list[tuple[int, int]] = []
    if not ref:
        return results

    # Split on semicolons for multiple chapter refs (rare in single ref strings)
    parts = ref.split(";")
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Match chapter:verse pattern
        m = re.match(r"(\d+):(.+)", part)
        if m:
            chapter = int(m.group(1))
            verse_part = m.group(2)
            # Parse verse ranges/lists: "1-12", "1,3", "1-3,7"
            for segment in verse_part.split(","):
                segment = segment.strip()
                range_m = re.match(r"(\d+)\s*-\s*(\d+)", segment)
                if range_m:
                    start = int(range_m.group(1))
                    end = int(range_m.group(2))
                    for v in range(start, end + 1):
                        results.append((chapter, v))
                elif re.fullmatch(r"\d+", segment):
                    results.append((chapter, int(segment)))
        else:
            # Chapter-only reference (e.g., just "5")
            m2 = re.match(r"(\d+)", part)
            if m2:
                # Store as chapter with verse 0 to indicate whole chapter
                results.append((int(m2.group(1)), 0))

    return results


def fetch_bible_texts(paragraphs: list[Paragraph]) -> dict[str, BibleBookSource]:
    """Fetch Bible texts for all references cited in the given paragraphs.

    Returns a dict keyed by canonical book ID.
    """
    # Collect all references per book, and citing paragraphs
    book_refs: dict[str, set[str]] = {}  # book_id -> set of reference strings
    book_citing: dict[str, set[int]] = {}  # book_id -> set of paragraph IDs

    for p in paragraphs:
        for pf in p.parsed_footnotes:
            for br in pf.bible_refs:
                book_refs.setdefault(br.book, set()).add(br.reference)
                book_citing.setdefault(br.book, set()).add(p.id)

    if not book_refs:
        logger.info("No Bible references found")
        return {}

    # Download and index Bible
    try:
        bible_data = _download_bible()
    except Exception:
        logger.warning("Failed to download Bible JSON, returning empty sources")
        return {}

    bible_index = _build_bible_index(bible_data)

    result: dict[str, BibleBookSource] = {}

    for book_id, refs in book_refs.items():
        name, abbreviation = _BOOK_META.get(book_id, (book_id.replace("-", " ").title(), book_id))
        testament = "old" if book_id in _OT_BOOKS else "new"
        kjv_name = _CANONICAL_TO_KJV.get(book_id, "")
        book_chapters = bible_index.get(kjv_name, {})

        verses: dict[str, str] = {}
        for ref in refs:
            if not ref:
                continue
            parsed = parse_reference(ref)
            for chapter, verse in parsed:
                if verse == 0:
                    # Whole chapter reference — include all verses
                    ch_verses = book_chapters.get(chapter, [])
                    for v_idx, v_text in enumerate(ch_verses, start=1):
                        key = f"{chapter}:{v_idx}"
                        if key not in verses:
                            verses[key] = v_text
                else:
                    ch_verses = book_chapters.get(chapter, [])
                    if 0 < verse <= len(ch_verses):
                        key = f"{chapter}:{verse}"
                        if key not in verses:
                            verses[key] = ch_verses[verse - 1]

        citing = sorted(book_citing.get(book_id, set()))
        result[book_id] = BibleBookSource(
            id=book_id,
            name=name,
            abbreviation=abbreviation,
            testament=testament,
            citing_paragraphs=citing,
            verses=verses,
        )

    # Include books without fetched text (e.g., deuterocanonical not in KJV)
    for book_id in book_refs:
        if book_id not in result:
            name, abbreviation = _BOOK_META.get(book_id, (book_id.replace("-", " ").title(), book_id))
            testament = "old" if book_id in _OT_BOOKS else "new"
            citing = sorted(book_citing.get(book_id, set()))
            result[book_id] = BibleBookSource(
                id=book_id,
                name=name,
                abbreviation=abbreviation,
                testament=testament,
                citing_paragraphs=citing,
                verses={},
            )

    logger.info(
        "Fetched Bible texts: %d books, %d total verses",
        len(result),
        sum(len(b.verses) for b in result.values()),
    )
    return result
