"""Scrape Church Fathers texts from New Advent and populate patristic tables.

New Advent structure:
  /fathers/         -> index page listing all authors and works
  /fathers/XXXX.htm -> work index page listing all chapters
  /fathers/XXXXXXX.htm -> individual chapter page with actual text

Usage:
    python -m pipeline.src.scraper_patristic [--download] [--load] [--dry-run]
    python -m pipeline.src.scraper_patristic --author augustine
    python -m pipeline.src.scraper_patristic --list-authors
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin

import click
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "patristic_v2"
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

BASE_URL = "https://www.newadvent.org/fathers/"

_HTTP_HEADERS = {
    "User-Agent": "knowledge-graph-scraper/1.0 (CCC patristic pipeline)",
}

# Delay between requests to be respectful
REQUEST_DELAY = 1.0

# ---------------------------------------------------------------------------
# Authors cited in CCC (priority order by citation count)
# ---------------------------------------------------------------------------

# Map: display name -> (slug for DB, [work index URLs])
# We'll discover work URLs from the fathers index page dynamically.

CCC_CITED_AUTHORS: dict[str, str] = {
    "Augustine of Hippo": "augustine",
    "Irenaeus of Lyons": "irenaeus",
    "John Chrysostom": "john-chrysostom",
    "Ambrose": "ambrose",
    "Ignatius of Antioch": "ignatius-antioch",
    "Gregory Nazianzen": "gregory-nazianzen",
    "Gregory of Nyssa": "gregory-nyssa",
    "Cyprian of Carthage": "cyprian",
    "Justin Martyr": "justin-martyr",
    "Basil the Great": "basil",
    "Cyril of Jerusalem": "cyril-jerusalem",
    "John of Damascus": "john-damascene",
    "Leo the Great": "leo-great",
    "Hippolytus": "hippolytus",
    "Gregory the Great": "gregory-great",
    "Jerome": "jerome",
    "Clement of Rome": "clement-rome",
    "Athanasius": "athanasius",
    "Tertullian": "tertullian",
    "Origen": "origen",
    "Clement of Alexandria": "clement-alexandria",
    "Hilary of Poitiers": "hilary-poitiers",
    "Polycarp": "polycarp",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Work:
    """A patristic work (e.g., Augustine's Confessions)."""
    author_slug: str
    code: str          # e.g., "1701" (from URL /fathers/1701.htm)
    title: str
    chapter_urls: list[str] = field(default_factory=list)


@dataclass
class Chapter:
    """A single chapter of a patristic work."""
    work_code: str
    number: int
    url: str
    title: str
    text: str


# ---------------------------------------------------------------------------
# Fetching helpers
# ---------------------------------------------------------------------------

def _fetch(url: str, delay: float = REQUEST_DELAY) -> str | None:
    """Fetch a URL with polite delay. Returns HTML or None on error."""
    try:
        resp = requests.get(url, headers=_HTTP_HEADERS, timeout=30)
        resp.raise_for_status()
        time.sleep(delay)
        return resp.text
    except requests.RequestException as e:
        click.echo(f"    WARN: failed to fetch {url}: {e}")
        return None


def _cached_fetch(url: str, cache_path: Path, delay: float = REQUEST_DELAY) -> str | None:
    """Fetch a URL, caching to local file."""
    if cache_path.exists():
        return cache_path.read_text(errors="replace")
    html = _fetch(url, delay=delay)
    if html:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(html)
    return html


# ---------------------------------------------------------------------------
# Parsing: fathers index
# ---------------------------------------------------------------------------

def parse_fathers_index(html: str) -> dict[str, list[str]]:
    """Parse the /fathers/ index page to get author -> work URLs.

    HTML structure: each author is in a <p> with <strong>Name</strong>,
    followed by <a href="../fathers/XXXX.htm">Work Title</a> links.

    Returns: {"Author Name": ["https://.../fathers/XXXX.htm", ...]}
    """
    soup = BeautifulSoup(html, "lxml")
    authors: dict[str, list[str]] = {}

    # Each author block is a <p> containing a <strong> with the author name
    # and <a> links to work index pages (4-digit codes)
    work_re = re.compile(r"fathers/(\d{4})\.htm")

    for p in soup.find_all("p"):
        strong = p.find("strong")
        if not strong:
            continue
        author_name = strong.get_text().strip()
        if not author_name or len(author_name) < 3:
            continue

        work_urls = []
        for a in p.find_all("a", href=True):
            m = work_re.search(a["href"])
            if m:
                work_urls.append(f"{BASE_URL}{m.group(1)}.htm")

        if work_urls:
            authors[author_name] = work_urls

    return authors


def discover_author_works(author_display: str, fathers_index: dict[str, list[str]]) -> list[str]:
    """Find work URLs for an author from the fathers index.

    Handles name variations: "Ambrose" matches "Ambrose (340-397)",
    "John Chrysostom" matches "John Chrysostom (347-407)", etc.
    """
    display_lower = author_display.lower()
    # Try exact match
    if author_display in fathers_index:
        return fathers_index[author_display]
    # Try: index name starts with our display name (handles "(date)" suffixes)
    for name, urls in fathers_index.items():
        name_lower = name.lower()
        if name_lower.startswith(display_lower) or display_lower.startswith(name_lower.split("(")[0].strip()):
            return urls
    # Try: any significant overlap
    for name, urls in fathers_index.items():
        # Compare key words
        display_words = set(display_lower.split())
        name_words = set(name.lower().replace("(", "").replace(")", "").split())
        if len(display_words & name_words) >= 2 or (len(display_words) == 1 and display_words <= name_words):
            return urls
    return []


# ---------------------------------------------------------------------------
# Parsing: work index page
# ---------------------------------------------------------------------------

def parse_work_index(html: str, work_url: str) -> Work | None:
    """Parse a work index page (e.g., /fathers/1701.htm) to get chapter URLs.

    Returns a Work with title and chapter_urls populated.
    """
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    title = title_tag.get_text().strip() if title_tag else ""
    # Clean title: "CHURCH FATHERS: Tractates on John (Augustine)" -> "Tractates on John"
    title = re.sub(r"^CHURCH FATHERS:\s*", "", title).strip()

    # Extract the work code from URL: /fathers/1701.htm -> "1701"
    code_match = re.search(r"/(\d{4})\.htm$", work_url)
    if not code_match:
        return None
    code = code_match.group(1)

    # Find chapter links: they match /fathers/XXXXXXX.htm (7 digits = 4-digit code + 3-digit chapter)
    chapter_pattern = re.compile(rf"fathers/{code}\d{{3}}\.htm")
    chapter_urls = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if chapter_pattern.search(href):
            full_url = urljoin(work_url, href)
            if full_url not in seen:
                seen.add(full_url)
                chapter_urls.append(full_url)

    # If no chapter links found, this is a single-page work —
    # the content is on this page itself. Use the work URL as the sole chapter.
    if not chapter_urls:
        chapter_urls = [work_url]

    return Work(
        author_slug="",  # filled in by caller
        code=code,
        title=title,
        chapter_urls=chapter_urls,
    )


# ---------------------------------------------------------------------------
# Parsing: chapter page
# ---------------------------------------------------------------------------

def parse_chapter_page(html: str, url: str) -> Chapter | None:
    """Parse an individual chapter page to extract the patristic text."""
    soup = BeautifulSoup(html, "lxml")

    # Remove script, style, nav
    for tag in soup.find_all(["script", "style", "nav"]):
        tag.decompose()

    title_tag = soup.find("title")
    title = title_tag.get_text().strip() if title_tag else ""
    title = re.sub(r"^CHURCH FATHERS:\s*", "", title).strip()

    # Extract chapter number from URL:
    #   /fathers/1701003.htm -> work 1701, chapter 3
    #   /fathers/0104.htm    -> work 0104, chapter 1 (single-page work)
    code_match = re.search(r"/(\d{4})(\d{3})\.htm$", url)
    if code_match:
        work_code = code_match.group(1)
        chapter_num = int(code_match.group(2))
    else:
        single_match = re.search(r"/(\d{4})\.htm$", url)
        if not single_match:
            return None
        work_code = single_match.group(1)
        chapter_num = 1

    # Find main content - New Advent uses <p> tags within body
    # The content is between the navigation header and the footer
    body = soup.find("body")
    if not body:
        return None

    # Extract all <p> tags that contain actual content
    paragraphs = []
    in_content = False
    for p in body.find_all("p"):
        text = p.get_text(separator=" ", strip=True)

        # Skip navigation and header text
        if any(skip in text for skip in [
            "Submit Search", "Home Encyclopedia Summa",
            "Please help support the mission of New Advent",
            "Copyright ©", "New Advent is maintained by",
            "Kevin Knight", "Home >",
        ]):
            continue

        # Skip very short or empty paragraphs
        if len(text) < 10:
            continue

        # Skip breadcrumb-like content
        if text.startswith("Home") and ">" in text[:30]:
            continue

        paragraphs.append(text)
        in_content = True

    if not paragraphs:
        return None

    full_text = "\n\n".join(paragraphs)

    # Clean up
    full_text = re.sub(r"\s+", " ", full_text).strip()
    # But preserve paragraph breaks
    full_text = "\n\n".join(p.strip() for p in full_text.split("\n\n") if p.strip())

    if len(full_text) < 50:
        return None

    return Chapter(
        work_code=work_code,
        number=chapter_num,
        url=url,
        title=title,
        text=full_text,
    )


# ---------------------------------------------------------------------------
# Download pipeline
# ---------------------------------------------------------------------------

def download_author(
    author_display: str,
    author_slug: str,
    work_urls: list[str],
    out_dir: Path,
) -> list[Work]:
    """Download all works and chapters for an author."""
    click.echo(f"\n  {author_display} ({len(work_urls)} works)")
    works = []

    for work_url in work_urls:
        code_match = re.search(r"/(\d{4})\.htm$", work_url)
        if not code_match:
            continue
        code = code_match.group(1)

        # Fetch work index
        work_cache = out_dir / author_slug / f"{code}_index.html"
        html = _cached_fetch(work_url, work_cache)
        if not html:
            continue

        work = parse_work_index(html, work_url)
        if not work or not work.chapter_urls:
            click.echo(f"    {code}: no chapters found")
            continue

        work.author_slug = author_slug
        click.echo(f"    {code} ({work.title[:50]}): {len(work.chapter_urls)} chapters")

        # Download each chapter
        for ch_url in work.chapter_urls:
            ch_match = re.search(r"/(\d{4,7})\.htm$", ch_url)
            if not ch_match:
                continue
            ch_file = out_dir / author_slug / f"{ch_match.group(1)}.html"
            _cached_fetch(ch_url, ch_file)

        works.append(work)

    return works


# ---------------------------------------------------------------------------
# Load into DB
# ---------------------------------------------------------------------------

def load_into_db(out_dir: Path, conn: sqlite3.Connection, dry_run: bool) -> dict[str, int]:
    """Parse downloaded HTML and load into patristic_chapters + patristic_sections."""
    stats = {"chapters": 0, "sections": 0, "authors": 0}

    # Find all author directories
    author_dirs = sorted(d for d in out_dir.iterdir() if d.is_dir())

    for author_dir in author_dirs:
        author_slug = author_dir.name
        work_indices = sorted(author_dir.glob("*_index.html"))

        if not work_indices:
            continue

        author_sections = 0
        for idx_file in work_indices:
            code = idx_file.name.replace("_index.html", "")
            html = idx_file.read_text(errors="replace")
            work_url = f"{BASE_URL}{code}.htm"

            work = parse_work_index(html, work_url)
            if not work:
                continue

            work.author_slug = author_slug

            # Process each chapter file
            for ch_url in work.chapter_urls:
                ch_match = re.search(r"/(\d{4,7})\.htm$", ch_url)
                if not ch_match:
                    continue

                ch_file = author_dir / f"{ch_match.group(1)}.html"
                if not ch_file.exists():
                    continue

                ch_html = ch_file.read_text(errors="replace")
                chapter = parse_chapter_page(ch_html, ch_url)
                if not chapter:
                    continue

                # Build IDs matching existing schema
                work_id = f"{author_slug}/{code}"
                chapter_id = f"{author_slug}/{code}/{chapter.number}"
                section_id = f"{author_slug}/{code}/{chapter.number}/1"

                if not dry_run:
                    conn.execute("""
                        INSERT OR REPLACE INTO patristic_chapters (id, work_id, number, title)
                        VALUES (?, ?, ?, ?)
                    """, (chapter_id, work_id, chapter.number, chapter.title))

                    conn.execute("""
                        INSERT OR REPLACE INTO patristic_sections (id, chapter_id, number, text_en)
                        VALUES (?, ?, ?, ?)
                    """, (section_id, chapter_id, 1, chapter.text))

                stats["chapters"] += 1
                stats["sections"] += 1
                author_sections += 1

        if author_sections > 0:
            stats["authors"] += 1
            click.echo(f"  {author_slug}: {author_sections} sections")

    return stats


def rebuild_patristic_fts(conn: sqlite3.Connection) -> None:
    """Rebuild the patristic_sections_fts index."""
    click.echo("Rebuilding patristic_sections_fts...")
    conn.execute("DROP TABLE IF EXISTS patristic_sections_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE patristic_sections_fts USING fts5(
            id, chapter_id, text_en
        )
    """)
    conn.execute("""
        INSERT INTO patristic_sections_fts (id, chapter_id, text_en)
        SELECT id, chapter_id, text_en FROM patristic_sections
    """)
    count = conn.execute("SELECT COUNT(*) FROM patristic_sections_fts").fetchone()[0]
    click.echo(f"  FTS rebuilt with {count} rows.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--download/--no-download", default=True, help="Download from New Advent")
@click.option("--load/--no-load", default=True, help="Load into database")
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.option("--author", default=None, help="Only process this author slug")
@click.option("--list-authors", is_flag=True, help="List available authors and exit")
@click.option("--db", type=click.Path(), default=None, help="Path to database")
@click.option("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
def main(
    download: bool,
    load: bool,
    dry_run: bool,
    author: str | None,
    list_authors: bool,
    db: str | None,
    delay: float,
) -> None:
    """Scrape Church Fathers texts from New Advent."""
    global REQUEST_DELAY
    REQUEST_DELAY = delay
    db_path = Path(db) if db else DB_PATH

    if list_authors:
        click.echo("CCC-cited Church Fathers (priority order):")
        for display, slug in CCC_CITED_AUTHORS.items():
            click.echo(f"  {slug:25s} {display}")
        return

    # Filter authors if --author is specified
    if author:
        targets = {k: v for k, v in CCC_CITED_AUTHORS.items() if v == author}
        if not targets:
            raise click.ClickException(f"Unknown author slug: {author}")
    else:
        targets = CCC_CITED_AUTHORS

    if download:
        click.echo("=== Fetching fathers index ===")
        idx_cache = RAW_DIR / "fathers_index.html"
        idx_html = _cached_fetch(BASE_URL, idx_cache, delay=0.5)
        if not idx_html:
            raise click.ClickException("Failed to fetch fathers index page")

        fathers_index = parse_fathers_index(idx_html)
        click.echo(f"Found {len(fathers_index)} authors on New Advent")

        # For each CCC-cited author, we already have the work URLs from WebFetch
        # But let's also try dynamic discovery as fallback
        click.echo(f"\n=== Downloading {len(targets)} authors ===")

        for display_name, slug in targets.items():
            work_urls = discover_author_works(display_name, fathers_index)
            if not work_urls:
                click.echo(f"\n  {display_name}: no works found in index, skipping")
                continue
            download_author(display_name, slug, work_urls, RAW_DIR)

    if load:
        click.echo(f"\n=== Loading into {db_path.name} ===")
        conn = sqlite3.connect(str(db_path))

        # Clear old patristic data (keep Thomas Aquinas from Summa if not re-downloading)
        if not author:
            click.echo("Clearing old patristic data...")
            if not dry_run:
                conn.execute("DELETE FROM patristic_sections")
                conn.execute("DELETE FROM patristic_chapters")

        stats = load_into_db(RAW_DIR, conn, dry_run)

        # Also re-load the old v1 data for Thomas Aquinas (Summa) if it exists
        old_dir = PROJECT_ROOT / "pipeline" / "data" / "raw" / "patristic"
        if (old_dir / "thomas-aquinas").exists() and (not author or author == "thomas-aquinas"):
            click.echo("\n  Re-loading Thomas Aquinas (Summa) from v1 data...")
            _reload_aquinas_v1(old_dir / "thomas-aquinas", conn, dry_run)

        if not dry_run:
            conn.commit()
            rebuild_patristic_fts(conn)
            conn.commit()

        conn.close()

        suffix = " (dry-run)" if dry_run else ""
        click.echo(f"\nDone: {stats['authors']} authors, {stats['chapters']} chapters, "
                    f"{stats['sections']} sections{suffix}")


def _reload_aquinas_v1(aquinas_dir: Path, conn: sqlite3.Connection, dry_run: bool) -> None:
    """Re-load Thomas Aquinas from the v1 raw data (already has good content)."""
    count = 0
    for work_dir in sorted(aquinas_dir.iterdir()):
        if not work_dir.is_dir() or work_dir.name in ("index.html",):
            continue
        work_name = work_dir.name
        if work_name.startswith("tractate") or work_name == "tractates-on-the-gospel-of-john-augustine":
            continue  # These were Augustine, not Aquinas

        for ch_file in sorted(work_dir.glob("chapter_*.html")):
            ch_num_match = re.search(r"chapter_(\d+)", ch_file.name)
            if not ch_num_match:
                continue
            ch_num = int(ch_num_match.group(1))

            html = ch_file.read_text(errors="replace")
            soup = BeautifulSoup(html, "lxml")

            # Remove non-content elements
            for tag in soup.find_all(["script", "style", "nav"]):
                tag.decompose()

            body = soup.find("body")
            if not body:
                continue

            # Extract text from <p> tags
            paragraphs = []
            for p in body.find_all("p"):
                text = p.get_text(separator=" ", strip=True)
                if any(skip in text for skip in [
                    "Submit Search", "Home Encyclopedia",
                    "Please help support", "Copyright ©",
                    "New Advent is maintained", "Kevin Knight",
                ]):
                    continue
                if len(text) < 10:
                    continue
                if text.startswith("Home") and ">" in text[:30]:
                    continue
                paragraphs.append(text)

            if not paragraphs:
                continue

            full_text = " ".join(paragraphs)
            full_text = re.sub(r"\s+", " ", full_text).strip()

            if len(full_text) < 50:
                continue

            chapter_id = f"thomas-aquinas/{work_name}/{ch_num}"
            section_id = f"thomas-aquinas/{work_name}/{ch_num}/1"
            work_id = f"thomas-aquinas/{work_name}"

            if not dry_run:
                conn.execute("""
                    INSERT OR REPLACE INTO patristic_chapters (id, work_id, number, title)
                    VALUES (?, ?, ?, ?)
                """, (chapter_id, work_id, ch_num, work_name))
                conn.execute("""
                    INSERT OR REPLACE INTO patristic_sections (id, chapter_id, number, text_en)
                    VALUES (?, ?, ?, ?)
                """, (section_id, chapter_id, 1, full_text))

            count += 1

    click.echo(f"    Thomas Aquinas (v1): {count} sections")


if __name__ == "__main__":
    main()
