"""Fetch multilingual editions of ecclesiastical documents from Vatican.va.

Downloads Latin and Portuguese versions of already-fetched English documents,
then merges the section texts into the existing MultiLangText dictionaries.

Vatican.va URL patterns:
- Vatican II (/archive/): ..._en.html -> ..._lt.html (Latin), ..._po.html (Portuguese)
- Papal (/content/):       .../en/... -> .../la/... (Latin), .../pt/... (Portuguese)
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .models import DocumentSource

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "documents"

# Rate limiting
_REQUEST_DELAY = 1.0  # seconds between requests


def _generate_lang_url(english_url: str, lang: str) -> str | None:
    """Generate a Vatican.va URL for a different language from the English URL.

    Returns None if URL pattern is not recognized.
    """
    if not english_url:
        return None

    # Pattern 1: Vatican II archive URLs
    # https://www.vatican.va/archive/.../..._en.html -> ..._lt.html or ..._po.html
    if "/archive/" in english_url:
        lang_code = {"la": "lt", "pt": "po"}.get(lang)
        if not lang_code:
            return None
        return re.sub(r"_en\.html$", f"_{lang_code}.html", english_url)

    # Pattern 2: Papal /content/ URLs
    # https://www.vatican.va/content/.../en/... -> .../la/... or .../pt/...
    if "/content/" in english_url:
        lang_code = {"la": "la", "pt": "pt"}.get(lang)
        if not lang_code:
            return None
        return re.sub(r"/en/", f"/{lang_code}/", english_url)

    return None


def _download_document_lang(doc_id: str, url: str, lang: str) -> str | None:
    """Download and cache a document in a specific language.

    Returns HTML content or None if download fails (404, network error, etc.).
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_DIR / f"{doc_id}_{lang}.html"

    if cache_path.exists():
        logger.debug("Using cached %s (%s): %s", doc_id, lang, cache_path)
        with open(cache_path, encoding="utf-8", errors="replace") as f:
            return f.read()

    logger.info("Downloading %s (%s) from %s", doc_id, lang, url)
    try:
        time.sleep(_REQUEST_DELAY)
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "CatechismKnowledgeGraph/1.0"},
        )
        if resp.status_code == 404:
            logger.info("  Not found (404): %s (%s)", doc_id, lang)
            # Cache the 404 as empty file to avoid re-fetching
            cache_path.write_text("")
            return None
        resp.raise_for_status()
        html = resp.text
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html
    except Exception as e:
        logger.warning("Failed to download %s (%s): %s", doc_id, lang, e)
        return None


def _parse_sections(html: str) -> dict[str, str]:
    """Parse numbered sections from a Vatican document HTML page.

    Same logic as fetch_documents._parse_sections.
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    sections: dict[str, str] = {}

    for p_tag in soup.find_all("p"):
        text = p_tag.get_text(strip=True)
        if not text:
            continue
        m = re.match(r"^(\d{1,4})\.\s*(.+)", text)
        if m:
            section_num = m.group(1)
            section_text = m.group(2).strip()
            if section_text and len(section_text) > 20:
                sections[section_num] = section_text[:2000]

    return sections


def fetch_documents_multilang(
    document_sources: dict[str, DocumentSource],
    languages: tuple[str, ...] = ("la", "pt"),
) -> dict[str, DocumentSource]:
    """Fetch multilingual editions and merge into existing document sources.

    For each document that has a known English URL, attempts to download
    Latin and Portuguese editions. Merges section text into the existing
    MultiLangText dictionaries.

    Args:
        document_sources: Existing document sources (with English sections).
        languages: Language codes to fetch (default: Latin, Portuguese).

    Returns:
        Updated document_sources dict with multilingual sections.
    """
    total_fetched = 0
    total_sections = 0

    for doc_id, doc in document_sources.items():
        if not doc.source_url or not doc.fetchable:
            continue

        for lang in languages:
            lang_url = _generate_lang_url(doc.source_url, lang)
            if not lang_url:
                continue

            html = _download_document_lang(doc_id, lang_url, lang)
            if not html:
                continue

            lang_sections = _parse_sections(html)
            if not lang_sections:
                continue

            total_fetched += 1

            # Merge language text into existing sections
            merged_count = 0
            for sec_num, lang_text in lang_sections.items():
                if sec_num in doc.sections:
                    # Add language to existing MultiLangText
                    doc.sections[sec_num][lang] = lang_text
                    merged_count += 1
                else:
                    # Section exists in other language but not in English
                    doc.sections[sec_num] = {lang: lang_text}
                    merged_count += 1

            total_sections += merged_count
            logger.info(
                "  %s (%s): merged %d sections",
                doc_id, lang, merged_count,
            )

    logger.info(
        "Multilingual documents: fetched %d language editions, merged %d sections total",
        total_fetched, total_sections,
    )
    return document_sources
