"""Fetch a Portuguese Bible translation.

Downloads a public-domain Portuguese Bible and parses at verse granularity.
Uses the Almeida Corrigida Fiel (ACF) or similar available source.
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
PT_CACHE = RAW_DIR / "pt_bible.json"

# Portuguese Bible source — Almeida Corrigida Fiel
PT_URL = "https://raw.githubusercontent.com/thiagobodruk/bible/master/json/pt_acf.json"

# Maps canonical IDs to Portuguese book names
_CANONICAL_TO_PT: dict[str, str] = {
    "genesis": "Gênesis", "exodus": "Êxodo", "leviticus": "Levítico",
    "numbers": "Números", "deuteronomy": "Deuteronômio", "joshua": "Josué",
    "judges": "Juízes", "ruth": "Rute", "1-samuel": "1 Samuel",
    "2-samuel": "2 Samuel", "1-kings": "1 Reis", "2-kings": "2 Reis",
    "1-chronicles": "1 Crônicas", "2-chronicles": "2 Crônicas",
    "ezra": "Esdras", "nehemiah": "Neemias",
    "tobit": "Tobias", "judith": "Judite", "esther": "Ester",
    "1-maccabees": "1 Macabeus", "2-maccabees": "2 Macabeus",
    "job": "Jó", "psalms": "Salmos", "proverbs": "Provérbios",
    "ecclesiastes": "Eclesiastes", "song-of-solomon": "Cânticos",
    "wisdom": "Sabedoria", "sirach": "Eclesiástico",
    "isaiah": "Isaías", "jeremiah": "Jeremias", "lamentations": "Lamentações",
    "baruch": "Baruc", "ezekiel": "Ezequiel", "daniel": "Daniel",
    "hosea": "Oséias", "joel": "Joel", "amos": "Amós", "obadiah": "Obadias",
    "jonah": "Jonas", "micah": "Miquéias", "nahum": "Naum",
    "habakkuk": "Habacuque", "zephaniah": "Sofonias", "haggai": "Ageu",
    "zechariah": "Zacarias", "malachi": "Malaquias",
    "matthew": "Mateus", "mark": "Marcos", "luke": "Lucas",
    "john": "João", "acts": "Atos",
    "romans": "Romanos", "1-corinthians": "1 Coríntios",
    "2-corinthians": "2 Coríntios", "galatians": "Gálatas",
    "ephesians": "Efésios", "philippians": "Filipenses",
    "colossians": "Colossenses", "1-thessalonians": "1 Tessalonicenses",
    "2-thessalonians": "2 Tessalonicenses", "1-timothy": "1 Timóteo",
    "2-timothy": "2 Timóteo", "titus": "Tito", "philemon": "Filemom",
    "hebrews": "Hebreus", "james": "Tiago", "1-peter": "1 Pedro",
    "2-peter": "2 Pedro", "1-john": "1 João", "2-john": "2 João",
    "3-john": "3 João", "jude": "Judas",
    "revelation": "Apocalipse",
}


def _download_pt_bible() -> list[dict]:
    """Download and cache the Portuguese Bible JSON."""
    if PT_CACHE.exists():
        logger.info("Using cached Portuguese Bible JSON: %s", PT_CACHE)
        with open(PT_CACHE, encoding="utf-8") as f:
            return json.load(f)

    logger.info("Downloading Portuguese Bible JSON from %s", PT_URL)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(PT_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    with open(PT_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info("Cached Portuguese Bible JSON: %s (%d books)", PT_CACHE, len(data))
    return data


def fetch_full_bible_pt() -> dict[str, BibleBookFull]:
    """Fetch the full Portuguese Bible at verse granularity.

    Returns a dict keyed by canonical book ID.
    """
    try:
        bible_data = _download_pt_bible()
    except Exception:
        logger.warning("Failed to download Portuguese Bible JSON, returning empty")
        return {}

    # Build positional and name indices
    positional_books: list[tuple[str, dict[int, list[str]]]] = []
    name_index: dict[str, dict[int, list[str]]] = {}
    for book in bible_data:
        name = book.get("name", "")
        chapters: dict[int, list[str]] = {}
        for ch_idx, verses in enumerate(book.get("chapters", []), start=1):
            chapters[ch_idx] = verses
        positional_books.append((name, chapters))
        name_index[name] = chapters

    result: dict[str, BibleBookFull] = {}
    total_verses = 0

    for book_idx, (canon_id, _json_name, display_name, abbr, testament) in enumerate(BOOK_ORDER):
        pt_name = _CANONICAL_TO_PT.get(canon_id, "")
        book_chapters = name_index.get(pt_name, {})

        # Fallback: try by position
        if not book_chapters and book_idx < len(positional_books):
            book_chapters = positional_books[book_idx][1]

        chapters: dict[int, BibleChapter] = {}
        book_verse_count = 0

        for ch_num, verse_list in book_chapters.items():
            verses: dict[int, dict[str, str]] = {}
            for v_idx, v_text in enumerate(verse_list, start=1):
                if v_text and v_text.strip():
                    verses[v_idx] = {"pt": v_text.strip()}
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
        "Fetched Portuguese Bible: %d books, %d chapters, %d verses",
        len(result),
        sum(len(b.chapters) for b in result.values()),
        total_verses,
    )
    return result
