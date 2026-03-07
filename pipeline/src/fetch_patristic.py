"""Fetch patristic author metadata from New Advent.

Downloads author index pages and extracts works lists with URLs.
Does not download full work texts since CCC footnotes rarely give
precise locations within works.
"""

from __future__ import annotations

import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .models import AuthorSource, Paragraph

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "patristic"

# Maps canonical author IDs to metadata and New Advent URLs.
_AUTHOR_META: dict[str, dict] = {
    "augustine": {
        "name": "St. Augustine of Hippo",
        "era": "354-430 AD",
        "url": "https://www.newadvent.org/fathers/1701.htm",
    },
    "thomas-aquinas": {
        "name": "St. Thomas Aquinas",
        "era": "1225-1274 AD",
        "url": "https://www.newadvent.org/summa/",
    },
    "john-chrysostom": {
        "name": "St. John Chrysostom",
        "era": "347-407 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "irenaeus": {
        "name": "St. Irenaeus of Lyon",
        "era": "c. 130-202 AD",
        "url": "https://www.newadvent.org/fathers/0103.htm",
    },
    "ambrose": {
        "name": "St. Ambrose of Milan",
        "era": "c. 340-397 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "jerome": {
        "name": "St. Jerome",
        "era": "c. 347-420 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "athanasius": {
        "name": "St. Athanasius of Alexandria",
        "era": "c. 296-373 AD",
        "url": "https://www.newadvent.org/fathers/2802.htm",
    },
    "basil": {
        "name": "St. Basil the Great",
        "era": "c. 330-379 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "gregory-nazianzen": {
        "name": "St. Gregory of Nazianzus",
        "era": "c. 329-390 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "gregory-nyssa": {
        "name": "St. Gregory of Nyssa",
        "era": "c. 335-395 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "gregory-great": {
        "name": "St. Gregory the Great",
        "era": "c. 540-604 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "cyril-jerusalem": {
        "name": "St. Cyril of Jerusalem",
        "era": "c. 313-386 AD",
        "url": "https://www.newadvent.org/fathers/3101.htm",
    },
    "cyril-alexandria": {
        "name": "St. Cyril of Alexandria",
        "era": "c. 376-444 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "john-damascene": {
        "name": "St. John of Damascus",
        "era": "c. 676-749 AD",
        "url": "https://www.newadvent.org/fathers/3304.htm",
    },
    "leo-great": {
        "name": "St. Leo the Great",
        "era": "c. 400-461 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "hilary": {
        "name": "St. Hilary of Poitiers",
        "era": "c. 310-367 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "cyprian": {
        "name": "St. Cyprian of Carthage",
        "era": "c. 210-258 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "clement-rome": {
        "name": "St. Clement of Rome",
        "era": "c. 35-99 AD",
        "url": "https://www.newadvent.org/fathers/1010.htm",
    },
    "clement-alexandria": {
        "name": "St. Clement of Alexandria",
        "era": "c. 150-215 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "justin-martyr": {
        "name": "St. Justin Martyr",
        "era": "c. 100-165 AD",
        "url": "https://www.newadvent.org/fathers/0126.htm",
    },
    "ignatius-antioch": {
        "name": "St. Ignatius of Antioch",
        "era": "c. 35-108 AD",
        "url": "https://www.newadvent.org/fathers/0104.htm",
    },
    "polycarp": {
        "name": "St. Polycarp of Smyrna",
        "era": "c. 69-155 AD",
        "url": "https://www.newadvent.org/fathers/0136.htm",
    },
    "tertullian": {
        "name": "Tertullian",
        "era": "c. 155-240 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "origen": {
        "name": "Origen",
        "era": "c. 184-253 AD",
        "url": "https://www.newadvent.org/fathers/",
    },
    "bonaventure": {
        "name": "St. Bonaventure",
        "era": "1221-1274 AD",
        "url": "",
    },
    "anselm": {
        "name": "St. Anselm of Canterbury",
        "era": "c. 1033-1109 AD",
        "url": "",
    },
}


def _download_author_page(author_id: str, url: str) -> str | None:
    """Download and cache an author's index page."""
    cache_dir = RAW_DIR / author_id
    cache_path = cache_dir / "index.html"

    if cache_path.exists():
        logger.debug("Using cached author page: %s", cache_path)
        with open(cache_path, encoding="utf-8", errors="replace") as f:
            return f.read()

    if not url or url.endswith("/fathers/"):
        # Generic URL — can't fetch a specific index
        return None

    logger.info("Downloading author page %s from %s", author_id, url)
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "CatechismKnowledgeGraph/1.0"})
        resp.raise_for_status()
        html = resp.text
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html
    except Exception as e:
        logger.warning("Failed to download author page %s: %s", author_id, e)
        return None


def _parse_works_list(html: str, base_url: str) -> list[dict]:
    """Parse a New Advent page to extract a list of works with URLs."""
    soup = BeautifulSoup(html, "html.parser")
    works: list[dict] = []
    seen_titles: set[str] = set()

    # Look for links that point to other /fathers/ pages (works)
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        title = a_tag.get_text(strip=True)

        if not title or len(title) < 3 or len(title) > 200:
            continue

        # Skip navigation links
        if title.lower() in ("home", "encyclopedia", "fathers", "summa", "bible"):
            continue

        # Build absolute URL
        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            full_url = "https://www.newadvent.org" + href
        else:
            # Relative URL
            base = base_url.rsplit("/", 1)[0]
            full_url = base + "/" + href

        if title not in seen_titles:
            seen_titles.add(title)
            works.append({"title": title, "url": full_url})

    return works[:50]  # Cap at 50 works


def fetch_patristic_texts(paragraphs: list[Paragraph]) -> dict[str, AuthorSource]:
    """Fetch patristic author metadata for all authors cited in the given paragraphs.

    Returns a dict keyed by canonical author ID.
    """
    # Collect citing paragraphs per author
    author_citing: dict[str, set[int]] = {}

    for p in paragraphs:
        for pf in p.parsed_footnotes:
            for ar in pf.author_refs:
                author_citing.setdefault(ar.author, set()).add(p.id)

    result: dict[str, AuthorSource] = {}

    for author_id in author_citing:
        meta = _AUTHOR_META.get(author_id, {})
        name = meta.get("name", author_id.replace("-", " ").title())
        era = meta.get("era", "")
        url = meta.get("url", "")

        works: list[dict] = []
        if url and not url.endswith("/fathers/"):
            html = _download_author_page(author_id, url)
            if html:
                works = _parse_works_list(html, url)

        citing = sorted(author_citing.get(author_id, set()))
        result[author_id] = AuthorSource(
            id=author_id,
            name=name,
            era=era,
            works=works,
            citing_paragraphs=citing,
        )

    logger.info(
        "Fetched patristic metadata: %d authors, %d with works lists",
        len(result),
        sum(1 for a in result.values() if a.works),
    )
    return result
