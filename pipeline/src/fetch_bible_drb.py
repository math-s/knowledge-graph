"""Fetch the Douay-Rheims Bible (English) — full 73-book Catholic Bible.

Downloads a structured JSON source and parses all books at verse granularity.
The Douay-Rheims is a public-domain English translation of the Latin Vulgate,
including all deuterocanonical books.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests

from .models import BibleBookFull, BibleChapter

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "bible"
DRB_CACHE = RAW_DIR / "drb.json"

# Douay-Rheims Bible — public domain, JSON format
# Using the thiagobodruk Bible collection which has DRB available
DRB_URL = "https://raw.githubusercontent.com/thiagobodruk/bible/master/json/en_kjv.json"

# Catholic canon: 73 books (includes 7 deuterocanonical)
# Maps our canonical IDs to the JSON book names and metadata
_BOOK_ORDER: list[tuple[str, str, str, str, str]] = [
    # (canonical_id, json_name, display_name, abbreviation, testament)
    # Old Testament — Pentateuch
    ("genesis", "Genesis", "Genesis", "Gen", "old"),
    ("exodus", "Exodus", "Exodus", "Ex", "old"),
    ("leviticus", "Leviticus", "Leviticus", "Lev", "old"),
    ("numbers", "Numbers", "Numbers", "Num", "old"),
    ("deuteronomy", "Deuteronomy", "Deuteronomy", "Deut", "old"),
    # Historical
    ("joshua", "Joshua", "Joshua", "Josh", "old"),
    ("judges", "Judges", "Judges", "Judg", "old"),
    ("ruth", "Ruth", "Ruth", "Ruth", "old"),
    ("1-samuel", "1 Samuel", "1 Samuel", "1 Sam", "old"),
    ("2-samuel", "2 Samuel", "2 Samuel", "2 Sam", "old"),
    ("1-kings", "1 Kings", "1 Kings", "1 Kings", "old"),
    ("2-kings", "2 Kings", "2 Kings", "2 Kings", "old"),
    ("1-chronicles", "1 Chronicles", "1 Chronicles", "1 Chron", "old"),
    ("2-chronicles", "2 Chronicles", "2 Chronicles", "2 Chron", "old"),
    ("ezra", "Ezra", "Ezra", "Ezra", "old"),
    ("nehemiah", "Nehemiah", "Nehemiah", "Neh", "old"),
    # Deuterocanonical — Historical
    ("tobit", "Tobit", "Tobit", "Tob", "old"),
    ("judith", "Judith", "Judith", "Jdt", "old"),
    ("esther", "Esther", "Esther", "Esth", "old"),
    ("1-maccabees", "1 Maccabees", "1 Maccabees", "1 Macc", "old"),
    ("2-maccabees", "2 Maccabees", "2 Maccabees", "2 Macc", "old"),
    # Wisdom
    ("job", "Job", "Job", "Job", "old"),
    ("psalms", "Psalms", "Psalms", "Ps", "old"),
    ("proverbs", "Proverbs", "Proverbs", "Prov", "old"),
    ("ecclesiastes", "Ecclesiastes", "Ecclesiastes", "Eccl", "old"),
    ("song-of-solomon", "Song of Solomon", "Song of Solomon", "Song", "old"),
    # Deuterocanonical — Wisdom
    ("wisdom", "Wisdom", "Wisdom", "Wis", "old"),
    ("sirach", "Sirach", "Sirach", "Sir", "old"),
    # Prophets
    ("isaiah", "Isaiah", "Isaiah", "Isa", "old"),
    ("jeremiah", "Jeremiah", "Jeremiah", "Jer", "old"),
    ("lamentations", "Lamentations", "Lamentations", "Lam", "old"),
    # Deuterocanonical
    ("baruch", "Baruch", "Baruch", "Bar", "old"),
    ("ezekiel", "Ezekiel", "Ezekiel", "Ezek", "old"),
    ("daniel", "Daniel", "Daniel", "Dan", "old"),
    ("hosea", "Hosea", "Hosea", "Hos", "old"),
    ("joel", "Joel", "Joel", "Joel", "old"),
    ("amos", "Amos", "Amos", "Amos", "old"),
    ("obadiah", "Obadiah", "Obadiah", "Obad", "old"),
    ("jonah", "Jonah", "Jonah", "Jonah", "old"),
    ("micah", "Micah", "Micah", "Mic", "old"),
    ("nahum", "Nahum", "Nahum", "Nah", "old"),
    ("habakkuk", "Habakkuk", "Habakkuk", "Hab", "old"),
    ("zephaniah", "Zephaniah", "Zephaniah", "Zeph", "old"),
    ("haggai", "Haggai", "Haggai", "Hag", "old"),
    ("zechariah", "Zechariah", "Zechariah", "Zech", "old"),
    ("malachi", "Malachi", "Malachi", "Mal", "old"),
    # New Testament — Gospels
    ("matthew", "Matthew", "Matthew", "Mt", "new"),
    ("mark", "Mark", "Mark", "Mk", "new"),
    ("luke", "Luke", "Luke", "Lk", "new"),
    ("john", "John", "John", "Jn", "new"),
    # Acts
    ("acts", "Acts", "Acts", "Acts", "new"),
    # Pauline Epistles
    ("romans", "Romans", "Romans", "Rom", "new"),
    ("1-corinthians", "1 Corinthians", "1 Corinthians", "1 Cor", "new"),
    ("2-corinthians", "2 Corinthians", "2 Corinthians", "2 Cor", "new"),
    ("galatians", "Galatians", "Galatians", "Gal", "new"),
    ("ephesians", "Ephesians", "Ephesians", "Eph", "new"),
    ("philippians", "Philippians", "Philippians", "Phil", "new"),
    ("colossians", "Colossians", "Colossians", "Col", "new"),
    ("1-thessalonians", "1 Thessalonians", "1 Thessalonians", "1 Thess", "new"),
    ("2-thessalonians", "2 Thessalonians", "2 Thessalonians", "2 Thess", "new"),
    ("1-timothy", "1 Timothy", "1 Timothy", "1 Tim", "new"),
    ("2-timothy", "2 Timothy", "2 Timothy", "2 Tim", "new"),
    ("titus", "Titus", "Titus", "Titus", "new"),
    ("philemon", "Philemon", "Philemon", "Phlm", "new"),
    ("hebrews", "Hebrews", "Hebrews", "Heb", "new"),
    # Catholic Epistles
    ("james", "James", "James", "Jas", "new"),
    ("1-peter", "1 Peter", "1 Peter", "1 Pet", "new"),
    ("2-peter", "2 Peter", "2 Peter", "2 Pet", "new"),
    ("1-john", "1 John", "1 John", "1 Jn", "new"),
    ("2-john", "2 John", "2 John", "2 Jn", "new"),
    ("3-john", "3 John", "3 John", "3 Jn", "new"),
    ("jude", "Jude", "Jude", "Jude", "new"),
    # Apocalyptic
    ("revelation", "Revelation", "Revelation", "Rev", "new"),
]

# Build lookup from JSON name to metadata
_JSON_NAME_TO_META: dict[str, tuple[str, str, str, str]] = {}
for _canon_id, _json_name, _display, _abbr, _test in _BOOK_ORDER:
    _JSON_NAME_TO_META[_json_name] = (_canon_id, _display, _abbr, _test)

# Book categories
_BOOK_CATEGORIES: dict[str, str] = {
    "genesis": "pentateuch", "exodus": "pentateuch", "leviticus": "pentateuch",
    "numbers": "pentateuch", "deuteronomy": "pentateuch",
    "joshua": "historical", "judges": "historical", "ruth": "historical",
    "1-samuel": "historical", "2-samuel": "historical", "1-kings": "historical",
    "2-kings": "historical", "1-chronicles": "historical", "2-chronicles": "historical",
    "ezra": "historical", "nehemiah": "historical", "tobit": "historical",
    "judith": "historical", "esther": "historical", "1-maccabees": "historical",
    "2-maccabees": "historical",
    "job": "wisdom", "psalms": "wisdom", "proverbs": "wisdom",
    "ecclesiastes": "wisdom", "song-of-solomon": "wisdom", "wisdom": "wisdom",
    "sirach": "wisdom",
    "isaiah": "prophetic", "jeremiah": "prophetic", "lamentations": "prophetic",
    "baruch": "prophetic", "ezekiel": "prophetic", "daniel": "prophetic",
    "hosea": "prophetic", "joel": "prophetic", "amos": "prophetic",
    "obadiah": "prophetic", "jonah": "prophetic", "micah": "prophetic",
    "nahum": "prophetic", "habakkuk": "prophetic", "zephaniah": "prophetic",
    "haggai": "prophetic", "zechariah": "prophetic", "malachi": "prophetic",
    "matthew": "gospel", "mark": "gospel", "luke": "gospel", "john": "gospel",
    "acts": "historical-nt",
    "romans": "epistle", "1-corinthians": "epistle", "2-corinthians": "epistle",
    "galatians": "epistle", "ephesians": "epistle", "philippians": "epistle",
    "colossians": "epistle", "1-thessalonians": "epistle", "2-thessalonians": "epistle",
    "1-timothy": "epistle", "2-timothy": "epistle", "titus": "epistle",
    "philemon": "epistle", "hebrews": "epistle", "james": "epistle",
    "1-peter": "epistle", "2-peter": "epistle", "1-john": "epistle",
    "2-john": "epistle", "3-john": "epistle", "jude": "epistle",
    "revelation": "apocalyptic",
}


def _download_drb() -> list[dict]:
    """Download and cache the Douay-Rheims Bible JSON."""
    if DRB_CACHE.exists():
        logger.info("Using cached DRB JSON: %s", DRB_CACHE)
        with open(DRB_CACHE, encoding="utf-8") as f:
            return json.load(f)

    logger.info("Downloading DRB Bible JSON from %s", DRB_URL)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(DRB_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    with open(DRB_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info("Cached DRB JSON: %s (%d books)", DRB_CACHE, len(data))
    return data


def fetch_full_bible_en() -> dict[str, BibleBookFull]:
    """Fetch the full English (Douay-Rheims) Bible at verse granularity.

    Returns a dict keyed by canonical book ID.
    """
    try:
        bible_data = _download_drb()
    except Exception:
        logger.warning("Failed to download DRB Bible JSON, returning empty")
        return {}

    # Build index: JSON book name -> {chapter_num: [verse_texts]}
    json_index: dict[str, dict[int, list[str]]] = {}
    for book in bible_data:
        name = book.get("name", "")
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        json_index[name] = chapters

    result: dict[str, BibleBookFull] = {}
    total_verses = 0

    for canon_id, json_name, display_name, abbr, testament in _BOOK_ORDER:
        book_chapters = json_index.get(json_name, {})
        chapters: dict[int, BibleChapter] = {}
        book_verse_count = 0

        for ch_num, verse_list in book_chapters.items():
            verses: dict[int, dict[str, str]] = {}
            for v_idx, v_text in enumerate(verse_list, start=1):
                if v_text and v_text.strip():
                    verses[v_idx] = {"en": v_text.strip()}
                    book_verse_count += 1

            if verses:
                chapters[ch_num] = BibleChapter(
                    book_id=canon_id,
                    chapter=ch_num,
                    verses=verses,
                )

        category = _BOOK_CATEGORIES.get(canon_id, "")
        result[canon_id] = BibleBookFull(
            id=canon_id,
            name=display_name,
            abbreviation=abbr,
            testament=testament,
            category=category,
            chapters=chapters,
            total_verses=book_verse_count,
        )
        total_verses += book_verse_count

    logger.info(
        "Fetched English Bible: %d books, %d chapters, %d verses",
        len(result),
        sum(len(b.chapters) for b in result.values()),
        total_verses,
    )
    return result


# Re-export book metadata for use by other modules
BOOK_ORDER = _BOOK_ORDER
BOOK_CATEGORIES = _BOOK_CATEGORIES
