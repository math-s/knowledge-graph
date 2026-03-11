"""Fetch the Greek Bible — Septuagint (OT) + Textus Receptus (NT).

Downloads structured Greek Bible sources and parses at verse granularity.
Both are public domain texts fundamental to Catholic biblical scholarship.
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
GREEK_CACHE = RAW_DIR / "greek.json"

# Greek Bible source — using available public domain sources
# Septuagint for OT, Textus Receptus for NT
GREEK_URL = "https://raw.githubusercontent.com/thiagobodruk/bible/master/json/el_greek.json"

# Maps canonical IDs to Greek book names
_CANONICAL_TO_GREEK: dict[str, str] = {
    "genesis": "ΓΕΝΕΣΙΣ", "exodus": "ΕΞΟΔΟΣ", "leviticus": "ΛΕΥΙΤΙΚΟΝ",
    "numbers": "ΑΡΙΘΜΟΙ", "deuteronomy": "ΔΕΥΤΕΡΟΝΟΜΙΟΝ",
    "joshua": "ΙΗΣΟΥΣ ΝΑΥΗ", "judges": "ΚΡΙΤΑΙ", "ruth": "ΡΟΥΘ",
    "1-samuel": "ΒΑΣΙΛΕΙΩΝ Α", "2-samuel": "ΒΑΣΙΛΕΙΩΝ Β",
    "1-kings": "ΒΑΣΙΛΕΙΩΝ Γ", "2-kings": "ΒΑΣΙΛΕΙΩΝ Δ",
    "1-chronicles": "ΠΑΡΑΛΕΙΠΟΜΕΝΩΝ Α", "2-chronicles": "ΠΑΡΑΛΕΙΠΟΜΕΝΩΝ Β",
    "ezra": "ΕΣΔΡΑΣ Β", "nehemiah": "ΝΕΕΜΙΑΣ",
    "tobit": "ΤΩΒΙΤ", "judith": "ΙΟΥΔΙΘ", "esther": "ΕΣΘΗΡ",
    "1-maccabees": "ΜΑΚΚΑΒΑΙΩΝ Α", "2-maccabees": "ΜΑΚΚΑΒΑΙΩΝ Β",
    "job": "ΙΩΒ", "psalms": "ΨΑΛΜΟΙ", "proverbs": "ΠΑΡΟΙΜΙΑΙ",
    "ecclesiastes": "ΕΚΚΛΗΣΙΑΣΤΗΣ", "song-of-solomon": "ΑΣΜΑ ΑΣΜΑΤΩΝ",
    "wisdom": "ΣΟΦΙΑ ΣΟΛΟΜΩΝΤΟΣ", "sirach": "ΣΟΦΙΑ ΣΕΙΡΑΧ",
    "isaiah": "ΗΣΑΙΑΣ", "jeremiah": "ΙΕΡΕΜΙΑΣ", "lamentations": "ΘΡΗΝΟΙ",
    "baruch": "ΒΑΡΟΥΧ", "ezekiel": "ΙΕΖΕΚΙΗΛ", "daniel": "ΔΑΝΙΗΛ",
    "hosea": "ΩΣΗΕ", "joel": "ΙΩΗΛ", "amos": "ΑΜΩΣ", "obadiah": "ΑΒΔΙΟΥ",
    "jonah": "ΙΩΝΑΣ", "micah": "ΜΙΧΑΙΑΣ", "nahum": "ΝΑΟΥΜ",
    "habakkuk": "ΑΜΒΑΚΟΥΜ", "zephaniah": "ΣΟΦΟΝΙΑΣ", "haggai": "ΑΓΓΑΙΟΣ",
    "zechariah": "ΖΑΧΑΡΙΑΣ", "malachi": "ΜΑΛΑΧΙΑΣ",
    # NT — standard Greek names
    "matthew": "ΚΑΤΑ ΜΑΤΘΑΙΟΝ", "mark": "ΚΑΤΑ ΜΑΡΚΟΝ",
    "luke": "ΚΑΤΑ ΛΟΥΚΑΝ", "john": "ΚΑΤΑ ΙΩΑΝΝΗΝ",
    "acts": "ΠΡΑΞΕΙΣ ΑΠΟΣΤΟΛΩΝ",
    "romans": "ΠΡΟΣ ΡΩΜΑΙΟΥΣ", "1-corinthians": "ΠΡΟΣ ΚΟΡΙΝΘΙΟΥΣ Α",
    "2-corinthians": "ΠΡΟΣ ΚΟΡΙΝΘΙΟΥΣ Β", "galatians": "ΠΡΟΣ ΓΑΛΑΤΑΣ",
    "ephesians": "ΠΡΟΣ ΕΦΕΣΙΟΥΣ", "philippians": "ΠΡΟΣ ΦΙΛΙΠΠΗΣΙΟΥΣ",
    "colossians": "ΠΡΟΣ ΚΟΛΟΣΣΑΕΙΣ",
    "1-thessalonians": "ΠΡΟΣ ΘΕΣΣΑΛΟΝΙΚΕΙΣ Α",
    "2-thessalonians": "ΠΡΟΣ ΘΕΣΣΑΛΟΝΙΚΕΙΣ Β",
    "1-timothy": "ΠΡΟΣ ΤΙΜΟΘΕΟΝ Α", "2-timothy": "ΠΡΟΣ ΤΙΜΟΘΕΟΝ Β",
    "titus": "ΠΡΟΣ ΤΙΤΟΝ", "philemon": "ΠΡΟΣ ΦΙΛΗΜΟΝΑ",
    "hebrews": "ΠΡΟΣ ΕΒΡΑΙΟΥΣ", "james": "ΙΑΚΩΒΟΥ",
    "1-peter": "ΠΕΤΡΟΥ Α", "2-peter": "ΠΕΤΡΟΥ Β",
    "1-john": "ΙΩΑΝΝΟΥ Α", "2-john": "ΙΩΑΝΝΟΥ Β", "3-john": "ΙΩΑΝΝΟΥ Γ",
    "jude": "ΙΟΥΔΑ", "revelation": "ΑΠΟΚΑΛΥΨΙΣ ΙΩΑΝΝΟΥ",
}


def _download_greek() -> list[dict]:
    """Download and cache the Greek Bible JSON."""
    if GREEK_CACHE.exists():
        logger.info("Using cached Greek Bible JSON: %s", GREEK_CACHE)
        with open(GREEK_CACHE, encoding="utf-8") as f:
            return json.load(f)

    logger.info("Downloading Greek Bible JSON from %s", GREEK_URL)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(GREEK_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    with open(GREEK_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info("Cached Greek Bible JSON: %s (%d books)", GREEK_CACHE, len(data))
    return data


def fetch_full_bible_el() -> dict[str, BibleBookFull]:
    """Fetch the full Greek Bible at verse granularity.

    Returns a dict keyed by canonical book ID.
    """
    try:
        bible_data = _download_greek()
    except Exception:
        logger.warning("Failed to download Greek Bible JSON, returning empty")
        return {}

    # Build index: position-based (most reliable for cross-language matching)
    positional_books: list[tuple[str, dict[int, list[str]]]] = []
    for book in bible_data:
        name = book.get("name", "")
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        positional_books.append((name, chapters))

    # Also build name index
    name_index: dict[str, dict[int, list[str]]] = {}
    for name, chapters in positional_books:
        name_index[name] = chapters

    result: dict[str, BibleBookFull] = {}
    total_verses = 0

    for book_idx, (canon_id, _json_name, display_name, abbr, testament) in enumerate(BOOK_ORDER):
        greek_name = _CANONICAL_TO_GREEK.get(canon_id, "")
        book_chapters = name_index.get(greek_name, {})

        # Fallback: try by position
        if not book_chapters and book_idx < len(positional_books):
            book_chapters = positional_books[book_idx][1]

        chapters: dict[int, BibleChapter] = {}
        book_verse_count = 0

        for ch_num, verse_list in book_chapters.items():
            verses: dict[int, dict[str, str]] = {}
            for v_idx, v_text in enumerate(verse_list, start=1):
                if v_text and v_text.strip():
                    verses[v_idx] = {"el": v_text.strip()}
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
        "Fetched Greek Bible: %d books, %d chapters, %d verses",
        len(result),
        sum(len(b.chapters) for b in result.values()),
        total_verses,
    )
    return result
