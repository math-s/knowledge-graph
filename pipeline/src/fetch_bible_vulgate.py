"""Fetch the Latin Vulgate Bible.

Downloads a structured Vulgate source and parses all books at verse granularity.
The Vulgate is the authoritative Latin Bible of the Catholic Church, originally
translated by St. Jerome in the 4th century. Public domain.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests

from .models import BibleBookFull, BibleChapter
from .fetch_bible_drb import BOOK_ORDER, BOOK_CATEGORIES

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "bible"
VULGATE_CACHE = RAW_DIR / "vulgate.json"

# Latin Vulgate — public domain
VULGATE_URL = "https://raw.githubusercontent.com/thiagobodruk/bible/master/json/la_vulgate.json"

# Maps canonical IDs to Vulgate book names (may differ from English)
_CANONICAL_TO_VULGATE: dict[str, str] = {
    "genesis": "Genesis", "exodus": "Exodus", "leviticus": "Leviticus",
    "numbers": "Numeri", "deuteronomy": "Deuteronomium", "joshua": "Iosue",
    "judges": "Iudicum", "ruth": "Ruth", "1-samuel": "I Samuelis",
    "2-samuel": "II Samuelis", "1-kings": "I Regum", "2-kings": "II Regum",
    "1-chronicles": "I Paralipomenon", "2-chronicles": "II Paralipomenon",
    "ezra": "Esdrae", "nehemiah": "Nehemiae",
    "tobit": "Tobiae", "judith": "Iudith", "esther": "Esther",
    "1-maccabees": "I Maccabaeorum", "2-maccabees": "II Maccabaeorum",
    "job": "Iob", "psalms": "Psalmi", "proverbs": "Proverbia",
    "ecclesiastes": "Ecclesiastes", "song-of-solomon": "Canticum Canticorum",
    "wisdom": "Sapientia", "sirach": "Ecclesiasticus",
    "isaiah": "Isaias", "jeremiah": "Ieremias", "lamentations": "Lamentationes",
    "baruch": "Baruch", "ezekiel": "Ezechiel", "daniel": "Daniel",
    "hosea": "Osee", "joel": "Ioel", "amos": "Amos", "obadiah": "Abdias",
    "jonah": "Ionas", "micah": "Michaeas", "nahum": "Nahum",
    "habakkuk": "Habacuc", "zephaniah": "Sophonias", "haggai": "Aggaeus",
    "zechariah": "Zacharias", "malachi": "Malachias",
    "matthew": "Matthaeus", "mark": "Marcus", "luke": "Lucas",
    "john": "Ioannes", "acts": "Actus Apostolorum",
    "romans": "Romanos", "1-corinthians": "I Corinthios",
    "2-corinthians": "II Corinthios", "galatians": "Galatas",
    "ephesians": "Ephesios", "philippians": "Philippenses",
    "colossians": "Colossenses", "1-thessalonians": "I Thessalonicenses",
    "2-thessalonians": "II Thessalonicenses", "1-timothy": "I Timotheum",
    "2-timothy": "II Timotheum", "titus": "Titum", "philemon": "Philemonem",
    "hebrews": "Hebraeos", "james": "Iacobi", "1-peter": "I Petri",
    "2-peter": "II Petri", "1-john": "I Ioannis", "2-john": "II Ioannis",
    "3-john": "III Ioannis", "jude": "Iudae",
    "revelation": "Apocalypsis",
}

# Reverse lookup: Vulgate name -> canonical ID
_VULGATE_TO_CANONICAL = {v: k for k, v in _CANONICAL_TO_VULGATE.items()}


def _download_vulgate() -> list[dict]:
    """Download and cache the Vulgate Bible JSON."""
    if VULGATE_CACHE.exists():
        logger.info("Using cached Vulgate JSON: %s", VULGATE_CACHE)
        with open(VULGATE_CACHE, encoding="utf-8") as f:
            return json.load(f)

    logger.info("Downloading Vulgate JSON from %s", VULGATE_URL)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(VULGATE_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    with open(VULGATE_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info("Cached Vulgate JSON: %s (%d books)", VULGATE_CACHE, len(data))
    return data


def fetch_full_bible_la() -> dict[str, BibleBookFull]:
    """Fetch the full Latin (Vulgate) Bible at verse granularity.

    Returns a dict keyed by canonical book ID.
    """
    try:
        bible_data = _download_vulgate()
    except Exception:
        logger.warning("Failed to download Vulgate Bible JSON, returning empty")
        return {}

    # Build index: JSON book name -> {chapter_num: [verse_texts]}
    json_index: dict[str, dict[int, list[str]]] = {}
    for book in bible_data:
        name = book.get("name", "")
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        json_index[name] = chapters

    # Also try matching by position (some sources use different naming)
    positional_index: list[dict[int, list[str]]] = []
    for book in bible_data:
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        positional_index.append(chapters)

    result: dict[str, BibleBookFull] = {}
    total_verses = 0

    for book_idx, (canon_id, _json_name, display_name, abbr, testament) in enumerate(_BOOK_ORDER_WITH_META()):
        vulgate_name = _CANONICAL_TO_VULGATE.get(canon_id, "")
        book_chapters = json_index.get(vulgate_name, {})

        # Fallback: try by English name
        if not book_chapters:
            book_chapters = json_index.get(_json_name, {})

        # Fallback: try by position
        if not book_chapters and book_idx < len(positional_index):
            book_chapters = positional_index[book_idx]

        chapters: dict[int, BibleChapter] = {}
        book_verse_count = 0

        for ch_num, verse_list in book_chapters.items():
            verses: dict[int, dict[str, str]] = {}
            for v_idx, v_text in enumerate(verse_list, start=1):
                if v_text and v_text.strip():
                    verses[v_idx] = {"la": v_text.strip()}
                    book_verse_count += 1

            if verses:
                chapters[ch_num] = BibleChapter(
                    book_id=canon_id,
                    chapter=ch_num,
                    verses=verses,
                )

        category = BOOK_CATEGORIES.get(canon_id, "")
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
        "Fetched Latin (Vulgate) Bible: %d books, %d chapters, %d verses",
        len(result),
        sum(len(b.chapters) for b in result.values()),
        total_verses,
    )
    return result


def _BOOK_ORDER_WITH_META() -> list[tuple[str, str, str, str, str]]:
    """Return BOOK_ORDER data."""
    return list(BOOK_ORDER)
