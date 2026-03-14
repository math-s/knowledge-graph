"""Fetch original Greek texts for Greek Church Fathers.

Downloads Greek source texts from public domain repositories
(primarily Sacred Texts Archive and other digitized Patrologia Graeca sources),
then merges them into existing PatristicWork structures as the "el" language key.

Uses a curated URL catalog mapping canonical author/work IDs to source URLs.
Implements rate-limiting (2s between requests) and disk caching.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .models import PatristicChapter, PatristicSection, PatristicWork

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "patristic_greek"

# Rate-limit: delay between HTTP requests (seconds)
REQUEST_DELAY = 2.0

# ── Greek Fathers ────────────────────────────────────────────────────────────
# Authors who wrote primarily in Greek and whose original works we can source.

GREEK_FATHER_IDS = {
    "john-chrysostom",
    "basil",
    "gregory-nazianzen",
    "gregory-nyssa",
    "athanasius",
    "cyril-jerusalem",
    "cyril-alexandria",
    "john-damascene",
    "clement-alexandria",
    "justin-martyr",
    "ignatius-antioch",
    "polycarp",
    "irenaeus",
    "origen",
}

# ── URL Catalog ──────────────────────────────────────────────────────────────
# Maps author_id to a list of known Greek source URLs.
# Greek patristic texts are harder to find in structured, scrapable form
# than Latin texts. We use a mix of sources:
#   - Sacred Texts Archive (sacred-texts.com) — ECF volumes with Greek fragments
#   - Other digitized Patrologia Graeca sources
#
# WARNING: sacred-texts.com has been unreliable (DNS failures as of 2026-03).
# These entries will fail gracefully (cached if previously downloaded).
# A better long-term source would be First1KGreek (GitHub TEI XML files)
# or Perseus Digital Library, but those require different parsing.
#
# Each catalog entry maps a work_pattern (substring match) to URLs.

_GREEK_CATALOG: dict[str, list[dict]] = {
    "ignatius-antioch": [
        {
            "work_pattern": "ephesians",
            "title": "Πρὸς Ἐφεσίους (To the Ephesians)",
            "url": "https://www.sacred-texts.com/chr/ecf/001/0010030.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
        {
            "work_pattern": "romans",
            "title": "Πρὸς Ῥωμαίους (To the Romans)",
            "url": "https://www.sacred-texts.com/chr/ecf/001/0010034.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "clement-alexandria": [
        {
            "work_pattern": "stromata",
            "title": "Στρώματα (Stromata)",
            "url": "https://www.sacred-texts.com/chr/ecf/002/0020300.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "justin-martyr": [
        {
            "work_pattern": "apolog",
            "title": "Ἀπολογία (First Apology)",
            "url": "https://www.sacred-texts.com/chr/ecf/001/0010060.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "athanasius": [
        {
            "work_pattern": "incarnation",
            "title": "Περὶ τῆς Ἐνανθρωπήσεως (On the Incarnation)",
            "url": "https://www.sacred-texts.com/chr/ecf/204/2040058.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "basil": [
        {
            "work_pattern": "holy-spirit",
            "title": "Περὶ τοῦ Ἁγίου Πνεύματος (On the Holy Spirit)",
            "url": "https://www.sacred-texts.com/chr/ecf/208/2080131.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "john-chrysostom": [
        {
            "work_pattern": "matthew",
            "title": "Ὁμιλίαι εἰς τὸ κατὰ Ματθαῖον (Homilies on Matthew)",
            "url": "https://www.sacred-texts.com/chr/ecf/110/1100005.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "john-damascene": [
        {
            "work_pattern": "orthodox-faith",
            "title": "Ἔκδοσις Ἀκριβὴς τῆς Ὀρθοδόξου Πίστεως (Exact Exposition of the Orthodox Faith)",
            "url": "https://www.sacred-texts.com/chr/ecf/209/2090044.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "cyril-jerusalem": [
        {
            "work_pattern": "catechetical",
            "title": "Κατηχήσεις (Catechetical Lectures)",
            "url": "https://www.sacred-texts.com/chr/ecf/207/2070014.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
    "irenaeus": [
        {
            "work_pattern": "heresies",
            "title": "Ἔλεγχος (Against Heresies)",
            "url": "https://www.sacred-texts.com/chr/ecf/001/0010264.htm",
            "chapter_url_template": None,
            "chapter_count": 1,
            "extract_greek_only": True,
        },
    ],
}


def _download_page(url: str, cache_path: Path) -> str | None:
    """Download a page with caching and rate limiting."""
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Downloading Greek text: %s", url)
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


def _contains_greek(text: str) -> bool:
    """Check if a text contains Greek characters."""
    # Greek Unicode block: U+0370 to U+03FF (basic) + U+1F00 to U+1FFF (extended)
    return bool(re.search(r"[\u0370-\u03FF\u1F00-\u1FFF]", text))


def _extract_greek_text(html: str, greek_only: bool = True) -> str:
    """Extract Greek text from an HTML page.

    Many early Christian text archives include both Greek and English
    in the same page. When greek_only=True, we attempt to extract
    only paragraphs containing Greek characters.

    Args:
        html: Raw HTML content.
        greek_only: If True, only include paragraphs with Greek characters.

    Returns:
        Extracted text string.
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
        if not text or len(text) < 10:
            continue

        text = re.sub(r"\s+", " ", text)

        if greek_only:
            if _contains_greek(text):
                paragraphs.append(text)
        else:
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


def _fetch_greek_for_work(
    author_id: str,
    work: PatristicWork,
    catalog_entry: dict,
) -> dict[int, str]:
    """Fetch Greek text for a single work. Returns {chapter_number: greek_text}."""
    work_slug = work.id.split("/")[-1]
    work_dir = RAW_DIR / author_id / work_slug
    result: dict[int, str] = {}

    greek_only = catalog_entry.get("extract_greek_only", True)
    template = catalog_entry.get("chapter_url_template")
    chapter_count = catalog_entry.get("chapter_count", 1)

    if template and chapter_count > 1:
        # Multi-page work
        for ch_num in range(1, chapter_count + 1):
            url = template.format(n=ch_num)
            cache_path = work_dir / f"chapter_{ch_num:03d}.html"
            html = _download_page(url, cache_path)
            if html:
                text = _extract_greek_text(html, greek_only=greek_only)
                if text and len(text) > 20:
                    result[ch_num] = text
    else:
        # Single-page work
        url = catalog_entry["url"]
        cache_path = work_dir / "full.html"
        html = _download_page(url, cache_path)
        if html:
            text = _extract_greek_text(html, greek_only=greek_only)
            if text and len(text) > 20:
                result[1] = text

    return result


def _merge_greek_into_work(
    work: PatristicWork,
    greek_chapters: dict[int, str],
) -> int:
    """Merge Greek text into an existing PatristicWork's sections.

    Matches by chapter number. Adds "el" key to PatristicSection.text.

    Returns the number of sections that received Greek text.
    """
    merged = 0

    for chapter in work.chapters:
        ch_num = chapter.number
        if ch_num not in greek_chapters:
            continue

        greek_text = greek_chapters[ch_num]

        if chapter.sections:
            for section in chapter.sections:
                if "el" not in section.text:
                    section.text["el"] = greek_text
                    merged += 1
                    break  # Only add to first section per chapter

    return merged


def _create_greek_work(
    author_id: str,
    entry: dict,
    greek_chapters: dict[int, str],
) -> PatristicWork:
    """Create a new PatristicWork from Greek-only text."""
    work_id = entry["work_pattern"]
    title = entry.get("title", work_id.replace("-", " ").title())

    chapters: list[PatristicChapter] = []
    for ch_num in sorted(greek_chapters):
        section = PatristicSection(
            id=f"{author_id}/{work_id}/{ch_num}/1",
            chapter_id=f"{author_id}/{work_id}/{ch_num}",
            number=1,
            text={"el": greek_chapters[ch_num]},
        )
        chapter = PatristicChapter(
            id=f"{author_id}/{work_id}/{ch_num}",
            work_id=f"{author_id}/{work_id}",
            number=ch_num,
            title=f"Book {ch_num}" if len(greek_chapters) > 1 else title,
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


def fetch_patristic_greek(
    patristic_works: dict[str, list[PatristicWork]],
) -> dict[str, list[PatristicWork]]:
    """Fetch Greek source texts and merge into existing patristic works.

    For each Greek Father, downloads the original Greek text and adds it
    as the "el" key in the PatristicSection.text MultiLangText dict.
    If no existing English work matches a catalog entry, a new Greek-only
    work is created and appended to the author's work list.

    Args:
        patristic_works: Existing works dict (author_id -> list[PatristicWork])
            with English text already populated.

    Returns:
        The same dict with Greek text merged/added where available.
    """
    total_works_merged = 0
    total_works_created = 0
    total_sections_merged = 0

    for author_id in GREEK_FATHER_IDS:
        catalog = _GREEK_CATALOG.get(author_id, [])
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
                    "Fetching Greek text for %s / %s",
                    author_id,
                    work.title,
                )
                greek_chapters = _fetch_greek_for_work(author_id, work, entry)

                if greek_chapters:
                    merged = _merge_greek_into_work(work, greek_chapters)
                    if merged > 0:
                        total_works_merged += 1
                        total_sections_merged += merged
                        logger.info(
                            "  Merged %d Greek sections into %s",
                            merged,
                            work.title,
                        )
                matched_entries.add(i)
                break  # Only match first catalog entry per work

        # Pass 2: create new Greek-only works for unmatched catalog entries
        for i, entry in enumerate(catalog):
            if i in matched_entries:
                continue

            dummy = PatristicWork(
                id=f"{author_id}/{entry['work_pattern']}",
                author_id=author_id,
                title=entry.get("title", entry["work_pattern"]),
                source_url=entry["url"],
                chapters=[],
            )
            logger.info(
                "Fetching Greek text (new work) for %s / %s",
                author_id,
                entry.get("title", entry["work_pattern"]),
            )
            greek_chapters = _fetch_greek_for_work(author_id, dummy, entry)

            if greek_chapters:
                new_work = _create_greek_work(author_id, entry, greek_chapters)
                if author_id not in patristic_works:
                    patristic_works[author_id] = []
                patristic_works[author_id].append(new_work)
                total_works_created += 1
                total_sections_merged += sum(1 for _ in greek_chapters)
                logger.info(
                    "  Created Greek work '%s' with %d chapters",
                    new_work.title,
                    len(new_work.chapters),
                )

    logger.info(
        "Greek patristic: %d works merged, %d works created, %d sections total",
        total_works_merged,
        total_works_created,
        total_sections_merged,
    )
    return patristic_works
