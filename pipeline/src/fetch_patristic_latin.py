"""Fetch original Latin texts for Latin Church Fathers.

Downloads Latin source texts from The Latin Library (thelatinlibrary.com)
and other public domain sources, then merges them into existing
PatristicWork structures as the "la" language key.

Uses a curated URL catalog mapping canonical author/work IDs to source URLs.
Implements rate-limiting (2s between requests) and disk caching.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .models import PatristicChapter, PatristicSection, PatristicWork

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "patristic_latin"

# Rate-limit: delay between HTTP requests (seconds)
REQUEST_DELAY = 2.0

# ── Latin Fathers ────────────────────────────────────────────────────────────
# Authors who wrote primarily in Latin and whose original works we can source.

LATIN_FATHER_IDS = {
    "augustine",
    "thomas-aquinas",
    "jerome",
    "ambrose",
    "gregory-great",
    "leo-great",
    "hilary",
    "tertullian",
    "cyprian",
    "bonaventure",
    "anselm",
}

# ── URL Catalog ──────────────────────────────────────────────────────────────
# Maps (author_id, work_slug_pattern) to Latin Library index URLs.
# work_slug_pattern is a substring matched against the English work slug.
# Each entry is (index_url, is_single_page).
#
# The Latin Library organizes texts by author name. We map our canonical
# work IDs to their URLs. For multi-page works, the index page links to
# individual chapters/books.

_LATIN_CATALOG: dict[str, list[dict]] = {
    "augustine": [
        {
            "work_pattern": "confessions",
            "title": "Confessiones",
            "url": "https://www.thelatinlibrary.com/augustine/conf1.shtml",
            "chapter_url_template": "https://www.thelatinlibrary.com/augustine/conf{n}.shtml",
            "chapter_count": 13,
        },
        {
            "work_pattern": "city-of-god",
            "title": "De Civitate Dei",
            "url": "https://www.thelatinlibrary.com/augustine/civ1.shtml",
            "chapter_url_template": "https://www.thelatinlibrary.com/augustine/civ{n}.shtml",
            "chapter_count": 22,
        },
        {
            "work_pattern": "on-the-trinity",
            "title": "De Trinitate",
            "url": "https://www.thelatinlibrary.com/augustine/trin1.shtml",
            "chapter_url_template": "https://www.thelatinlibrary.com/augustine/trin{n}.shtml",
            "chapter_count": 15,
        },
        {
            "work_pattern": "enchiridion",
            "title": "Enchiridion",
            "url": "https://www.thelatinlibrary.com/augustine/ench.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "ambrose": [
        {
            "work_pattern": "on-the-duties",
            "title": "De Officiis",
            "url": "https://www.thelatinlibrary.com/ambrose/off1.shtml",
            "chapter_url_template": "https://www.thelatinlibrary.com/ambrose/off{n}.shtml",
            "chapter_count": 3,
        },
        {
            "work_pattern": "on-the-mysteries",
            "title": "De Mysteriis",
            "url": "https://www.thelatinlibrary.com/ambrose/myst.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "on-the-holy-spirit",
            "title": "De Spiritu Sancto",
            "url": "https://www.thelatinlibrary.com/ambrose/spiritu1.shtml",
            "chapter_url_template": "https://www.thelatinlibrary.com/ambrose/spiritu{n}.shtml",
            "chapter_count": 3,
        },
    ],
    "jerome": [
        {
            "work_pattern": "letter",
            "title": "Epistulae",
            "url": "https://www.thelatinlibrary.com/jerome.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "tertullian": [
        {
            "work_pattern": "apolog",
            "title": "Apologeticum",
            "url": "https://www.thelatinlibrary.com/tertullian/tertullian.apol.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "against-praxeas",
            "title": "Adversus Praxean",
            "url": "https://www.thelatinlibrary.com/tertullian/tertullian.adv.prax.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "prescription",
            "title": "De Praescriptione Haereticorum",
            "url": "https://www.thelatinlibrary.com/tertullian/tertullian.praescriptionibus.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "cyprian": [
        {
            "work_pattern": "unity",
            "title": "De Catholicae Ecclesiae Unitate",
            "url": "https://www.thelatinlibrary.com/cyprian/cyprian.unit.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "lord",
            "title": "De Dominica Oratione",
            "url": "https://www.thelatinlibrary.com/cyprian/cyprian.domin.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "anselm": [
        {
            "work_pattern": "proslog",
            "title": "Proslogion",
            "url": "https://www.thelatinlibrary.com/anselm/anselm.proslog.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "cur-deus",
            "title": "Cur Deus Homo",
            "url": "https://www.thelatinlibrary.com/anselm/anselm.curdeus.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "bonaventure": [
        {
            "work_pattern": "itinerary",
            "title": "Itinerarium Mentis in Deum",
            "url": "https://www.thelatinlibrary.com/bonaventura/bonaventura.itinerarium.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "leo-great": [
        {
            "work_pattern": "sermon",
            "title": "Sermones",
            "url": "https://www.thelatinlibrary.com/leo.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    "hilary": [
        {
            "work_pattern": "trinit",
            "title": "De Trinitate",
            "url": "https://www.thelatinlibrary.com/hilary.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
}


def _download_page(url: str, cache_path: Path) -> str | None:
    """Download a page with caching and rate limiting."""
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Downloading Latin text: %s", url)
    time.sleep(REQUEST_DELAY)

    try:
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "CatechismKnowledgeGraph/1.0"},
        )
        resp.raise_for_status()
        html = resp.text
        cache_path.write_text(html, encoding="utf-8")
        return html
    except Exception as e:
        logger.warning("Failed to download %s: %s", url, e)
        return None


def _extract_latin_text(html: str) -> str:
    """Extract Latin text content from a Latin Library HTML page.

    The Latin Library uses simple HTML with text in <p> tags within the body.
    Strips navigation, headers, and metadata.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content elements
    for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    body = soup.find("body")
    if not body:
        return ""

    paragraphs: list[str] = []
    for p in body.find_all("p"):
        text = p.get_text(strip=True)
        # Latin Library uses <p> for content paragraphs
        # Skip very short fragments (likely navigation or headers)
        if text and len(text) > 15:
            # Clean common artifacts
            text = re.sub(r"\s+", " ", text)
            paragraphs.append(text)

    return "\n\n".join(paragraphs)


def _match_work(work: PatristicWork, catalog_entry: dict) -> bool:
    """Check if a PatristicWork matches a catalog entry's pattern."""
    pattern = catalog_entry["work_pattern"].lower()
    work_id_lower = work.id.lower()
    work_title_lower = work.title.lower()
    # Also check with hyphens replaced by spaces (title uses spaces, pattern uses hyphens)
    pattern_spaced = pattern.replace("-", " ")
    return (
        pattern in work_id_lower
        or pattern in work_title_lower
        or pattern_spaced in work_title_lower
    )


def _fetch_latin_for_work(
    author_id: str,
    work: PatristicWork,
    catalog_entry: dict,
) -> dict[int, str]:
    """Fetch Latin text for a single work. Returns {chapter_number: latin_text}."""
    work_dir = RAW_DIR / author_id / work.id.split("/")[-1]
    result: dict[int, str] = {}

    template = catalog_entry.get("chapter_url_template")
    chapter_count = catalog_entry.get("chapter_count", 1)

    if template and chapter_count > 1:
        # Multi-page work: download each chapter
        for ch_num in range(1, chapter_count + 1):
            url = template.format(n=ch_num)
            cache_path = work_dir / f"chapter_{ch_num:03d}.html"
            html = _download_page(url, cache_path)
            if html:
                text = _extract_latin_text(html)
                if text and len(text) > 30:
                    result[ch_num] = text
    else:
        # Single-page work
        url = catalog_entry["url"]
        cache_path = work_dir / "full.html"
        html = _download_page(url, cache_path)
        if html:
            text = _extract_latin_text(html)
            if text and len(text) > 30:
                result[1] = text

    return result


def _merge_latin_into_work(
    work: PatristicWork,
    latin_chapters: dict[int, str],
) -> int:
    """Merge Latin text into an existing PatristicWork's sections.

    Matches by chapter number. If the work has N English chapters and
    the Latin source has M chapters, merges min(N, M) chapters.

    Returns the number of sections that received Latin text.
    """
    merged = 0

    for chapter in work.chapters:
        ch_num = chapter.number
        if ch_num not in latin_chapters:
            continue

        latin_text = latin_chapters[ch_num]

        # If the chapter has sections, add Latin to the first section
        # (Latin Library typically gives us one text blob per chapter)
        if chapter.sections:
            for section in chapter.sections:
                if "la" not in section.text:
                    section.text["la"] = latin_text
                    merged += 1
                    break  # Only add to first section per chapter
        # If no sections exist (unlikely), skip this chapter

    return merged


def _create_latin_work(
    author_id: str,
    entry: dict,
    latin_chapters: dict[int, str],
) -> PatristicWork:
    """Create a new PatristicWork from Latin-only text."""
    work_id = entry["work_pattern"]
    title = entry.get("title", work_id.replace("-", " ").title())

    chapters: list[PatristicChapter] = []
    for ch_num in sorted(latin_chapters):
        section = PatristicSection(
            id=f"{author_id}/{work_id}/{ch_num}/1",
            chapter_id=f"{author_id}/{work_id}/{ch_num}",
            number=1,
            text={"la": latin_chapters[ch_num]},
        )
        chapter = PatristicChapter(
            id=f"{author_id}/{work_id}/{ch_num}",
            work_id=f"{author_id}/{work_id}",
            number=ch_num,
            title=f"Book {ch_num}" if len(latin_chapters) > 1 else title,
            sections=[section],
        )
        chapters.append(chapter)

    return PatristicWork(
        id=f"{author_id}/{work_id}",
        author_id=author_id,
        title=title,
        source_url=entry["url"],
        chapters=chapters,
    )


def fetch_patristic_latin(
    patristic_works: dict[str, list[PatristicWork]],
) -> dict[str, list[PatristicWork]]:
    """Fetch Latin source texts and merge into existing patristic works.

    For each Latin Father, downloads the original Latin text and adds it
    as the "la" key in the PatristicSection.text MultiLangText dict.
    If no existing English work matches a catalog entry, a new Latin-only
    work is created and appended to the author's work list.

    Args:
        patristic_works: Existing works dict (author_id -> list[PatristicWork])
            with English text already populated.

    Returns:
        The same dict with Latin text merged/added where available.
    """
    total_works_merged = 0
    total_works_created = 0
    total_sections_merged = 0

    for author_id in LATIN_FATHER_IDS:
        catalog = _LATIN_CATALOG.get(author_id, [])
        if not catalog:
            continue

        works = patristic_works.get(author_id, [])
        matched_entries: set[int] = set()

        # Pass 1: try to merge into existing English works
        for work in works:
            for i, entry in enumerate(catalog):
                if i in matched_entries:
                    continue
                if not _match_work(work, entry):
                    continue

                logger.info(
                    "Fetching Latin text for %s / %s",
                    author_id,
                    work.title,
                )
                latin_chapters = _fetch_latin_for_work(author_id, work, entry)

                if latin_chapters:
                    merged = _merge_latin_into_work(work, latin_chapters)
                    if merged > 0:
                        total_works_merged += 1
                        total_sections_merged += merged
                        logger.info(
                            "  Merged %d Latin sections into %s",
                            merged,
                            work.title,
                        )
                matched_entries.add(i)
                break  # Only match first catalog entry per work

        # Pass 2: create new Latin-only works for unmatched catalog entries
        for i, entry in enumerate(catalog):
            if i in matched_entries:
                continue

            # Use a dummy work for _fetch_latin_for_work's cache path
            dummy = PatristicWork(
                id=f"{author_id}/{entry['work_pattern']}",
                author_id=author_id,
                title=entry.get("title", entry["work_pattern"]),
                source_url=entry["url"],
                chapters=[],
            )
            logger.info(
                "Fetching Latin text (new work) for %s / %s",
                author_id,
                entry.get("title", entry["work_pattern"]),
            )
            latin_chapters = _fetch_latin_for_work(author_id, dummy, entry)

            if latin_chapters:
                new_work = _create_latin_work(author_id, entry, latin_chapters)
                if author_id not in patristic_works:
                    patristic_works[author_id] = []
                patristic_works[author_id].append(new_work)
                total_works_created += 1
                total_sections_merged += sum(1 for _ in latin_chapters)
                logger.info(
                    "  Created Latin work '%s' with %d chapters",
                    new_work.title,
                    len(new_work.chapters),
                )

    logger.info(
        "Latin patristic: %d works merged, %d works created, %d sections total",
        total_works_merged,
        total_works_created,
        total_sections_merged,
    )
    return patristic_works
