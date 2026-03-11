"""Fetch full-text patristic works from New Advent.

Crawls author index pages, discovers works and chapter links,
downloads chapter HTML, parses text content into structured data.

Implements rate-limiting (1.5s between requests) and disk caching.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .models import (
    AuthorSource,
    MultiLangText,
    PatristicChapter,
    PatristicSection,
    PatristicWork,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "patristic"

# Rate-limit: delay between HTTP requests (seconds)
REQUEST_DELAY = 1.5

# Max chapters to download per work (safety cap)
MAX_CHAPTERS_PER_WORK = 60

# Max works to download per author (safety cap)
MAX_WORKS_PER_AUTHOR = 20


def _slugify(title: str) -> str:
    """Convert a title to a URL-friendly slug."""
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug[:60].rstrip("-")


def _download_page(url: str, cache_path: Path) -> str | None:
    """Download a page with caching and rate limiting."""
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Downloading %s", url)
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


def _extract_text_from_html(html: str) -> str:
    """Extract clean text from a New Advent HTML page.

    Skips navigation, headers, and extracts paragraph text.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove navigation and non-content elements
    for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    body = soup.find("body")
    if not body:
        return ""

    # Extract text from paragraphs
    paragraphs: list[str] = []
    for p in body.find_all("p"):
        text = p.get_text(strip=True)
        if text and len(text) > 20:  # Skip tiny fragments
            paragraphs.append(text)

    return "\n\n".join(paragraphs)


def _is_chapter_link(href: str, base_url: str) -> bool:
    """Check if a link looks like a chapter page relative to a work index.

    New Advent chapter URLs typically follow patterns like:
    - /fathers/XXXX.htm (index) -> /fathers/XXXXNN.htm (chapter)
    - /fathers/XXXX.htm (index) -> /fathers/XXYYXX.htm (sub-chapter)
    """
    if not href:
        return False

    base_parsed = urlparse(base_url)
    href_parsed = urlparse(href if href.startswith("http") else urljoin(base_url, href))

    # Must be on the same host
    if href_parsed.netloc and href_parsed.netloc != base_parsed.netloc:
        return False

    # Must be under /fathers/ or /summa/
    path = href_parsed.path
    if not (path.startswith("/fathers/") or path.startswith("/summa/")):
        return False

    # Should end with .htm
    if not path.endswith(".htm"):
        return False

    # Must not be the same page
    if path == base_parsed.path:
        return False

    return True


def _discover_chapter_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Discover chapter links from an index page.

    Returns list of (title, url) tuples for chapter pages.
    """
    soup = BeautifulSoup(html, "html.parser")
    chapters: list[tuple[str, str]] = []
    seen_urls: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        title = a_tag.get_text(strip=True)

        if not title or len(title) < 3:
            continue

        # Skip obvious navigation
        if title.lower() in (
            "home", "encyclopedia", "fathers", "summa", "bible",
            "encyclopedia", "catechism", "new advent",
        ):
            continue

        if not _is_chapter_link(href, base_url):
            continue

        # Build absolute URL
        full_url = urljoin(base_url, href)

        if full_url in seen_urls:
            continue

        seen_urls.add(full_url)
        chapters.append((title, full_url))

    return chapters[:MAX_CHAPTERS_PER_WORK]


def _fetch_single_work(
    author_id: str,
    work_id: str,
    work_title: str,
    work_url: str,
) -> PatristicWork | None:
    """Fetch a single work's full text from New Advent.

    Downloads the index page, discovers chapter links, downloads each chapter,
    and parses the text into structured sections.
    """
    work_dir = RAW_DIR / author_id / work_id

    # Download index page
    index_cache = work_dir / "index.html"
    index_html = _download_page(work_url, index_cache)
    if not index_html:
        return None

    # Discover chapter links
    chapter_links = _discover_chapter_links(index_html, work_url)

    if not chapter_links:
        # Single-page work: extract text directly from index page
        text = _extract_text_from_html(index_html)
        if not text or len(text) < 50:
            return None

        section = PatristicSection(
            id=f"{author_id}/{work_id}/1/1",
            chapter_id=f"{author_id}/{work_id}/1",
            number=1,
            text={"en": text},
        )
        chapter = PatristicChapter(
            id=f"{author_id}/{work_id}/1",
            work_id=f"{author_id}/{work_id}",
            number=1,
            title=work_title,
            sections=[section],
        )
        return PatristicWork(
            id=f"{author_id}/{work_id}",
            author_id=author_id,
            title=work_title,
            source_url=work_url,
            chapters=[chapter],
        )

    # Multi-chapter work: download each chapter
    chapters: list[PatristicChapter] = []
    for ch_num, (ch_title, ch_url) in enumerate(chapter_links, start=1):
        ch_cache = work_dir / f"chapter_{ch_num:03d}.html"
        ch_html = _download_page(ch_url, ch_cache)
        if not ch_html:
            continue

        text = _extract_text_from_html(ch_html)
        if not text or len(text) < 30:
            continue

        section = PatristicSection(
            id=f"{author_id}/{work_id}/{ch_num}/1",
            chapter_id=f"{author_id}/{work_id}/{ch_num}",
            number=1,
            text={"en": text},
        )
        chapter = PatristicChapter(
            id=f"{author_id}/{work_id}/{ch_num}",
            work_id=f"{author_id}/{work_id}",
            number=ch_num,
            title=ch_title,
            sections=[section],
        )
        chapters.append(chapter)

    if not chapters:
        return None

    logger.info(
        "  Fetched %s: %d chapters",
        work_title,
        len(chapters),
    )

    return PatristicWork(
        id=f"{author_id}/{work_id}",
        author_id=author_id,
        title=work_title,
        source_url=work_url,
        chapters=chapters,
    )


# ── Extra works not discoverable from index pages ────────────────────────────

# Some authors have works that require explicit URL definitions because
# the index page doesn't link to them, or they use a different URL structure.
_EXTRA_WORKS: dict[str, list[dict]] = {
    "thomas-aquinas": [
        {
            "id": "summa-theologica",
            "title": "Summa Theologica",
            "url": "https://www.newadvent.org/summa/",
            "metadata_only": True,  # Too large to download; use metadata reference only
        },
    ],
}


def _is_valid_work_url(url: str) -> bool:
    """Check whether a URL points to an actual work page on New Advent."""
    parsed = urlparse(url)
    if parsed.netloc and "newadvent.org" not in parsed.netloc:
        return False
    path = parsed.path
    # Must be under /fathers/ or /summa/
    if not (path.startswith("/fathers/") or path.startswith("/summa/")):
        return False
    # Must end in .htm
    if not path.endswith(".htm"):
        return False
    return True


def fetch_patristic_works(
    author_sources: dict[str, AuthorSource],
) -> dict[str, list[PatristicWork]]:
    """Fetch full-text works for all known patristic authors.

    Uses the works list from author_sources (populated by fetch_patristic_texts)
    to discover and download work content.

    Args:
        author_sources: Dict[author_id, AuthorSource] with metadata and works lists.

    Returns:
        Dict[author_id, list[PatristicWork]] with full text content.
    """
    result: dict[str, list[PatristicWork]] = {}
    total_works = 0
    total_chapters = 0

    for author_id, author in author_sources.items():
        works: list[PatristicWork] = []

        # Process works from the author's metadata (discovered from index pages)
        work_count = 0
        for work_meta in author.works:
            if work_count >= MAX_WORKS_PER_AUTHOR:
                logger.info(
                    "  Reached max works (%d) for %s",
                    MAX_WORKS_PER_AUTHOR,
                    author_id,
                )
                break

            title = work_meta.get("title", "")
            url = work_meta.get("url", "")

            if not url or not title:
                continue

            # Skip URLs that don't look like work pages
            if not url.startswith("http"):
                continue

            # Validate the URL points to an actual work page
            if not _is_valid_work_url(url):
                logger.debug("Skipping non-work URL for %s: %s", author_id, url)
                continue

            work_id = _slugify(title)
            if not work_id:
                continue

            logger.info("Fetching work: %s / %s", author_id, title)
            work = _fetch_single_work(author_id, work_id, title, url)
            if work:
                works.append(work)
                total_chapters += len(work.chapters)
                work_count += 1

        # Add extra works (e.g., Summa for Aquinas)
        for extra in _EXTRA_WORKS.get(author_id, []):
            if extra.get("metadata_only"):
                # Create metadata-only work (no downloaded text)
                work = PatristicWork(
                    id=f"{author_id}/{extra['id']}",
                    author_id=author_id,
                    title=extra["title"],
                    source_url=extra.get("url", ""),
                    chapters=[],
                )
                works.append(work)
            else:
                work_id = extra["id"]
                title = extra["title"]
                url = extra.get("url", "")
                if url:
                    logger.info("Fetching extra work: %s / %s", author_id, title)
                    work = _fetch_single_work(author_id, work_id, title, url)
                    if work:
                        works.append(work)
                        total_chapters += len(work.chapters)

        if works:
            result[author_id] = works
            total_works += len(works)

    logger.info(
        "Fetched patristic works: %d authors, %d works, %d chapters",
        len(result),
        total_works,
        total_chapters,
    )
    return result
