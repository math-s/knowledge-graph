"""Fetch multilingual CCC paragraphs from Vatican.va.

Scrapes the Vatican.va CCC archive for Latin and Portuguese editions.
CCC paragraph numbering (1-2865) is consistent across all language editions.

Vatican.va CCC archive structure:
- Index page links to section pages
- Section pages contain paragraphs with bold numbers: <b>123</b> Text...
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .models import Paragraph

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "ccc"

# Rate limiting
_REQUEST_DELAY = 1.0  # seconds between requests

# Known CCC archive base URLs on Vatican.va
# These may need adjustment if Vatican.va restructures
_CCC_ARCHIVES: dict[str, str] = {
    "la": "https://www.vatican.va/archive/catechismus_lt/",
    "pt": "https://www.vatican.va/archive/compendium_ccc/documents/",
}

# Alternative patterns to try if primary fails
_CCC_ARCHIVE_ALTERNATIVES: dict[str, list[str]] = {
    "la": [
        "https://www.vatican.va/archive/catechismus_lt/",
    ],
    "pt": [
        "https://www.vatican.va/archive/compendium_ccc/documents/",
        "https://www.vatican.va/archive/cathchism_po/",
        "https://www.vatican.va/archive/catechismus_po/",
    ],
}


def _download_page(url: str, cache_path: Path) -> str | None:
    """Download a page with caching."""
    if cache_path.exists():
        content = cache_path.read_text(encoding="utf-8", errors="replace")
        if content:
            return content
        return None

    logger.info("  Downloading %s", url)
    try:
        time.sleep(_REQUEST_DELAY)
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "CatechismKnowledgeGraph/1.0"},
        )
        if resp.status_code == 404:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("")  # Cache 404
            return None
        resp.raise_for_status()
        html = resp.text
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html
    except Exception as e:
        logger.warning("  Failed to download %s: %s", url, e)
        return None


def _extract_paragraphs_from_html(html: str) -> dict[int, str]:
    """Extract CCC paragraph numbers and text from a Vatican.va HTML page.

    Vatican.va CCC pages use various patterns:
    - <b>123</b> or <strong>123</strong> followed by paragraph text
    - Paragraphs in <p> tags with bold number at start
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    paragraphs: dict[int, str] = {}

    # Strategy 1: Look for <p> tags where text starts with a bold number
    for p_tag in soup.find_all("p"):
        # Check if first child is <b> or <strong> containing a number
        first_bold = p_tag.find(["b", "strong"])
        if first_bold:
            bold_text = first_bold.get_text(strip=True)
            # Match CCC paragraph numbers (1-2865)
            m = re.match(r"^(\d{1,4})\.?$", bold_text)
            if m:
                para_num = int(m.group(1))
                if 1 <= para_num <= 2865:
                    # Get full paragraph text (excluding the number itself)
                    full_text = p_tag.get_text(strip=True)
                    # Remove the leading number
                    text = re.sub(r"^\d{1,4}\.?\s*", "", full_text).strip()
                    if text and len(text) > 10:
                        paragraphs[para_num] = text

    # Strategy 2: Look for text patterns "NNN. Text" in <p> tags
    if not paragraphs:
        for p_tag in soup.find_all("p"):
            text = p_tag.get_text(strip=True)
            if not text:
                continue
            m = re.match(r"^(\d{1,4})\.\s+(.+)", text)
            if m:
                para_num = int(m.group(1))
                if 1 <= para_num <= 2865:
                    para_text = m.group(2).strip()
                    if para_text and len(para_text) > 10:
                        paragraphs[para_num] = para_text

    return paragraphs


def _discover_section_pages(index_html: str, base_url: str) -> list[str]:
    """Discover section page URLs from a CCC index page."""
    soup = BeautifulSoup(index_html, "html.parser")
    urls: list[str] = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Look for relative links to section/part pages
        if href.endswith(".htm") or href.endswith(".html"):
            if href.startswith("http"):
                urls.append(href)
            else:
                # Resolve relative URL
                if base_url.endswith("/"):
                    urls.append(base_url + href)
                else:
                    urls.append(base_url.rsplit("/", 1)[0] + "/" + href)

    return urls


def _fetch_ccc_lang(lang: str) -> dict[int, str]:
    """Fetch all CCC paragraphs for a given language.

    Tries the known archive URLs, discovers section pages from the index,
    then downloads and parses each section page.

    Returns dict mapping paragraph number -> text.
    """
    cache_dir = RAW_DIR / lang
    cache_dir.mkdir(parents=True, exist_ok=True)

    all_paragraphs: dict[int, str] = {}

    # Try each known base URL
    base_urls = _CCC_ARCHIVE_ALTERNATIVES.get(lang, [])
    if not base_urls:
        logger.warning("No known CCC archive URLs for language: %s", lang)
        return {}

    for base_url in base_urls:
        index_cache = cache_dir / "index.html"
        index_html = _download_page(base_url, index_cache)
        if not index_html:
            # Try with index.htm suffix
            alt_url = base_url.rstrip("/") + "/index.htm"
            index_cache_alt = cache_dir / "index_alt.html"
            index_html = _download_page(alt_url, index_cache_alt)

        if not index_html:
            logger.info("  Could not access CCC index for %s at %s", lang, base_url)
            continue

        # Discover section pages
        section_urls = _discover_section_pages(index_html, base_url)
        logger.info("  Found %d section pages for CCC (%s)", len(section_urls), lang)

        # Download and parse each section page
        for i, section_url in enumerate(section_urls):
            # Generate a cache filename from the URL
            page_slug = re.sub(r"[^a-zA-Z0-9]", "_", section_url.split("/")[-1])
            page_cache = cache_dir / f"page_{page_slug}.html"

            section_html = _download_page(section_url, page_cache)
            if not section_html:
                continue

            page_paras = _extract_paragraphs_from_html(section_html)
            if page_paras:
                all_paragraphs.update(page_paras)
                logger.debug(
                    "    Page %d: extracted %d paragraphs (%d-%d)",
                    i + 1, len(page_paras),
                    min(page_paras.keys()), max(page_paras.keys()),
                )

        if all_paragraphs:
            break  # Found content, no need to try alternative URLs

    return all_paragraphs


def fetch_ccc_multilang(
    paragraphs: list[Paragraph],
    languages: tuple[str, ...] = ("la", "pt"),
) -> list[Paragraph]:
    """Fetch multilingual CCC text and merge into existing paragraphs.

    For each language, downloads the CCC edition from Vatican.va and merges
    paragraph text into the existing MultiLangText dictionaries.

    Args:
        paragraphs: Existing paragraphs (with English text).
        languages: Language codes to fetch.

    Returns:
        Updated paragraphs list with multilingual text.
    """
    para_lookup = {p.id: p for p in paragraphs}

    for lang in languages:
        logger.info("--- Fetching CCC (%s) ---", lang)
        lang_paras = _fetch_ccc_lang(lang)

        if not lang_paras:
            logger.info("  No paragraphs found for CCC (%s)", lang)
            continue

        # Merge into existing paragraphs
        merged_count = 0
        for para_num, text in lang_paras.items():
            p = para_lookup.get(para_num)
            if p:
                p.text[lang] = text
                merged_count += 1

        logger.info(
            "  CCC (%s): merged %d paragraphs (of %d found, %d total)",
            lang, merged_count, len(lang_paras), len(paragraphs),
        )

    return paragraphs
