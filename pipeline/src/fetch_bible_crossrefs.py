"""Fetch Treasury of Scripture Knowledge (TSK) cross-references.

The TSK is a public-domain Bible cross-reference database containing
~340,000 verse-to-verse cross-references. It is the most comprehensive
set of internal Bible cross-references available.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "bible"
TSK_CACHE = RAW_DIR / "tsk.json"

# TSK cross-references in JSON format
TSK_URL = "https://raw.githubusercontent.com/phanrahan/tsk/master/tsk.json"

# Maps TSK book names to our canonical IDs
_TSK_BOOK_TO_CANONICAL: dict[str, str] = {
    "Genesis": "genesis", "Exodus": "exodus", "Leviticus": "leviticus",
    "Numbers": "numbers", "Deuteronomy": "deuteronomy", "Joshua": "joshua",
    "Judges": "judges", "Ruth": "ruth", "1 Samuel": "1-samuel",
    "2 Samuel": "2-samuel", "1 Kings": "1-kings", "2 Kings": "2-kings",
    "1 Chronicles": "1-chronicles", "2 Chronicles": "2-chronicles",
    "Ezra": "ezra", "Nehemiah": "nehemiah", "Esther": "esther",
    "Job": "job", "Psalms": "psalms", "Psalm": "psalms",
    "Proverbs": "proverbs", "Ecclesiastes": "ecclesiastes",
    "Song of Solomon": "song-of-solomon", "Song of Songs": "song-of-solomon",
    "Isaiah": "isaiah", "Jeremiah": "jeremiah", "Lamentations": "lamentations",
    "Ezekiel": "ezekiel", "Daniel": "daniel", "Hosea": "hosea",
    "Joel": "joel", "Amos": "amos", "Obadiah": "obadiah", "Jonah": "jonah",
    "Micah": "micah", "Nahum": "nahum", "Habakkuk": "habakkuk",
    "Zephaniah": "zephaniah", "Haggai": "haggai", "Zechariah": "zechariah",
    "Malachi": "malachi", "Matthew": "matthew", "Mark": "mark",
    "Luke": "luke", "John": "john", "Acts": "acts", "Romans": "romans",
    "1 Corinthians": "1-corinthians", "2 Corinthians": "2-corinthians",
    "Galatians": "galatians", "Ephesians": "ephesians",
    "Philippians": "philippians", "Colossians": "colossians",
    "1 Thessalonians": "1-thessalonians", "2 Thessalonians": "2-thessalonians",
    "1 Timothy": "1-timothy", "2 Timothy": "2-timothy", "Titus": "titus",
    "Philemon": "philemon", "Hebrews": "hebrews", "James": "james",
    "1 Peter": "1-peter", "2 Peter": "2-peter", "1 John": "1-john",
    "2 John": "2-john", "3 John": "3-john", "Jude": "jude",
    "Revelation": "revelation",
}


def _make_verse_id(book_id: str, chapter: int, verse: int) -> str:
    """Create a canonical verse ID: 'book_id-ch:v'."""
    return f"{book_id}-{chapter}:{verse}"


def _parse_tsk_reference(ref_str: str) -> list[tuple[str, int, int]]:
    """Parse a TSK reference string into (book_id, chapter, verse) tuples.

    TSK references can be in various formats:
    - "Gen 1:1" -> [("genesis", 1, 1)]
    - "Gen 1:1,3,5" -> [("genesis", 1, 1), ("genesis", 1, 3), ("genesis", 1, 5)]
    - "Gen 1:1-3" -> [("genesis", 1, 1), ("genesis", 1, 2), ("genesis", 1, 3)]
    """
    results: list[tuple[str, int, int]] = []
    ref_str = ref_str.strip()
    if not ref_str:
        return results

    # Try to match "Book Chapter:Verse" pattern
    # Handle numbered books: "1 Samuel", "2 Kings", etc.
    m = re.match(r"^(\d?\s*[A-Za-z][A-Za-z\s]+?)\s+(\d+):(.+)$", ref_str)
    if not m:
        return results

    book_name = m.group(1).strip()
    chapter = int(m.group(2))
    verse_part = m.group(3).strip()

    book_id = _TSK_BOOK_TO_CANONICAL.get(book_name)
    if not book_id:
        return results

    # Parse verse ranges/lists
    for segment in verse_part.split(","):
        segment = segment.strip()
        range_m = re.match(r"(\d+)\s*-\s*(\d+)", segment)
        if range_m:
            start = int(range_m.group(1))
            end = int(range_m.group(2))
            for v in range(start, min(end + 1, start + 50)):  # cap at 50 to avoid runaway
                results.append((book_id, chapter, v))
        elif re.match(r"^\d+$", segment):
            results.append((book_id, chapter, int(segment)))

    return results


def fetch_bible_crossrefs() -> dict[str, list[str]]:
    """Fetch TSK Bible cross-references.

    Returns a dict mapping verse_id -> list[verse_id] of cross-referenced verses.
    Verse IDs use the format "book_id-chapter:verse" (e.g., "genesis-1:1").
    """
    # Try to load from cache
    if TSK_CACHE.exists():
        logger.info("Using cached TSK data: %s", TSK_CACHE)
        with open(TSK_CACHE, encoding="utf-8") as f:
            return json.load(f)

    logger.info("Downloading TSK cross-references from %s", TSK_URL)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    try:
        resp = requests.get(TSK_URL, timeout=120)
        resp.raise_for_status()
        raw_data = resp.json()
    except Exception:
        logger.warning("Failed to download TSK data, trying to build from local Bible data")
        return _build_fallback_crossrefs()

    crossrefs: dict[str, list[str]] = {}
    total_refs = 0

    # TSK JSON structure varies by source. Handle common formats.
    if isinstance(raw_data, dict):
        for book_name, chapters in raw_data.items():
            book_id = _TSK_BOOK_TO_CANONICAL.get(book_name)
            if not book_id:
                continue

            if isinstance(chapters, dict):
                for ch_str, verses in chapters.items():
                    ch_num = int(ch_str) if ch_str.isdigit() else 0
                    if not ch_num:
                        continue

                    if isinstance(verses, dict):
                        for v_str, refs in verses.items():
                            v_num = int(v_str) if v_str.isdigit() else 0
                            if not v_num:
                                continue

                            source_id = _make_verse_id(book_id, ch_num, v_num)
                            ref_list: list[str] = []

                            if isinstance(refs, list):
                                for ref in refs:
                                    if isinstance(ref, str):
                                        parsed = _parse_tsk_reference(ref)
                                        for rb, rc, rv in parsed:
                                            ref_list.append(_make_verse_id(rb, rc, rv))
                                    elif isinstance(ref, dict):
                                        # Some formats use {"book": ..., "chapter": ..., "verse": ...}
                                        rb = _TSK_BOOK_TO_CANONICAL.get(ref.get("book", ""), "")
                                        if rb:
                                            rc = ref.get("chapter", 0)
                                            rv = ref.get("verse", 0)
                                            if rc and rv:
                                                ref_list.append(_make_verse_id(rb, rc, rv))

                            if ref_list:
                                crossrefs[source_id] = ref_list
                                total_refs += len(ref_list)

    elif isinstance(raw_data, list):
        # Flat list format
        for entry in raw_data:
            if not isinstance(entry, dict):
                continue
            book_name = entry.get("book", "")
            book_id = _TSK_BOOK_TO_CANONICAL.get(book_name)
            if not book_id:
                continue

            ch_num = entry.get("chapter", 0)
            v_num = entry.get("verse", 0)
            refs = entry.get("references", entry.get("refs", []))
            if not ch_num or not v_num:
                continue

            source_id = _make_verse_id(book_id, ch_num, v_num)
            ref_list = []
            for ref in refs:
                if isinstance(ref, str):
                    parsed = _parse_tsk_reference(ref)
                    for rb, rc, rv in parsed:
                        ref_list.append(_make_verse_id(rb, rc, rv))

            if ref_list:
                crossrefs[source_id] = ref_list
                total_refs += len(ref_list)

    # Cache the processed data
    with open(TSK_CACHE, "w", encoding="utf-8") as f:
        json.dump(crossrefs, f, ensure_ascii=False)

    logger.info(
        "Fetched TSK cross-references: %d source verses, %d total refs",
        len(crossrefs),
        total_refs,
    )
    return crossrefs


def _build_fallback_crossrefs() -> dict[str, list[str]]:
    """Build a minimal cross-reference set from common well-known cross-references.

    Used as fallback when TSK download fails. Contains key theological cross-references.
    """
    logger.info("Building fallback cross-reference set")
    crossrefs: dict[str, list[str]] = {}

    # A small set of well-known cross-references for fallback
    known_refs: list[tuple[str, str]] = [
        # Creation accounts
        ("genesis-1:1", "john-1:1"),
        ("genesis-1:1", "hebrews-11:3"),
        # Key messianic prophecies -> fulfillment
        ("isaiah-7:14", "matthew-1:23"),
        ("isaiah-53:5", "1-peter-2:24"),
        ("micah-5:2", "matthew-2:6"),
        ("psalms-22:1", "matthew-27:46"),
        ("psalms-110:1", "matthew-22:44"),
        # Eucharistic connections
        ("exodus-12:1", "1-corinthians-5:7"),
        ("matthew-26:26", "1-corinthians-11:24"),
        # Baptism connections
        ("matthew-28:19", "acts-2:38"),
        # Great Commission
        ("matthew-28:19", "mark-16:15"),
    ]

    for source, target in known_refs:
        crossrefs.setdefault(source, []).append(target)
        crossrefs.setdefault(target, []).append(source)

    return crossrefs
