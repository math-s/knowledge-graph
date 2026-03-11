"""Merge multiple language-specific Bible data into unified MultiLangText per verse.

After all language-specific fetchers run, this module merges texts into
MultiLangText dictionaries keyed by canonical node IDs. Also used for
CCC, Fathers, and Documents in later phases.
"""

from __future__ import annotations

import logging

from .models import BibleBookFull, BibleChapter, MultiLangText

logger = logging.getLogger(__name__)


def merge_bible_languages(
    *bible_dicts: dict[str, BibleBookFull],
) -> dict[str, BibleBookFull]:
    """Merge multiple single-language Bible datasets into multi-language books.

    Each input dict is keyed by canonical book ID. Verses in each BibleBookFull
    have single-language MultiLangText (e.g., {"en": "..."} or {"la": "..."}).
    This function merges them so each verse has all available languages.

    Args:
        *bible_dicts: Variable number of language-specific Bible dicts.

    Returns:
        A single dict[str, BibleBookFull] with merged MultiLangText per verse.
    """
    if not bible_dicts:
        return {}

    # Use the first non-empty dict as the base (for book metadata)
    base: dict[str, BibleBookFull] | None = None
    for bd in bible_dicts:
        if bd:
            base = bd
            break
    if base is None:
        return {}

    # Collect all book IDs across all dicts
    all_book_ids: set[str] = set()
    for bd in bible_dicts:
        all_book_ids.update(bd.keys())

    result: dict[str, BibleBookFull] = {}
    total_verses = 0

    for book_id in sorted(all_book_ids):
        # Find the first dict that has this book (for metadata)
        meta_book: BibleBookFull | None = None
        for bd in bible_dicts:
            if book_id in bd:
                meta_book = bd[book_id]
                break
        if meta_book is None:
            continue

        # Collect all chapter numbers across all languages
        all_chapters: set[int] = set()
        for bd in bible_dicts:
            if book_id in bd:
                all_chapters.update(bd[book_id].chapters.keys())

        merged_chapters: dict[int, BibleChapter] = {}
        book_verse_count = 0

        for ch_num in sorted(all_chapters):
            # Collect all verse numbers across all languages for this chapter
            all_verses: set[int] = set()
            for bd in bible_dicts:
                if book_id in bd and ch_num in bd[book_id].chapters:
                    all_verses.update(bd[book_id].chapters[ch_num].verses.keys())

            merged_verses: dict[int, MultiLangText] = {}

            for v_num in sorted(all_verses):
                merged_text: MultiLangText = {}
                for bd in bible_dicts:
                    if book_id in bd and ch_num in bd[book_id].chapters:
                        ch = bd[book_id].chapters[ch_num]
                        if v_num in ch.verses:
                            merged_text.update(ch.verses[v_num])

                if merged_text:
                    merged_verses[v_num] = merged_text
                    book_verse_count += 1

            if merged_verses:
                merged_chapters[ch_num] = BibleChapter(
                    book_id=book_id,
                    chapter=ch_num,
                    verses=merged_verses,
                )

        # Merge citing_paragraphs from all sources
        all_citing: set[int] = set()
        for bd in bible_dicts:
            if book_id in bd:
                all_citing.update(bd[book_id].citing_paragraphs)

        result[book_id] = BibleBookFull(
            id=book_id,
            name=meta_book.name,
            abbreviation=meta_book.abbreviation,
            testament=meta_book.testament,
            category=meta_book.category,
            chapters=merged_chapters,
            total_verses=book_verse_count,
            citing_paragraphs=sorted(all_citing),
        )
        total_verses += book_verse_count

    # Log summary
    lang_counts: dict[str, int] = {}
    for book in result.values():
        for ch in book.chapters.values():
            for verse_text in ch.verses.values():
                for lang in verse_text:
                    lang_counts[lang] = lang_counts.get(lang, 0) + 1

    logger.info(
        "Merged Bible: %d books, %d total verse entries. Languages: %s",
        len(result),
        total_verses,
        ", ".join(f"{lang}={count}" for lang, count in sorted(lang_counts.items())),
    )
    return result


def merge_multilang_text(*texts: MultiLangText) -> MultiLangText:
    """Merge multiple MultiLangText dicts into one. Later dicts override earlier ones."""
    result: MultiLangText = {}
    for t in texts:
        result.update(t)
    return result
