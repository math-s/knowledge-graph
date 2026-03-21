"""Scrape the Catholic Encyclopedia from New Advent and load into SQLite.

Source: https://www.newadvent.org/cathen/
~11,500 articles, public domain (1907-1913).

Usage:
    python -m pipeline.src.scraper_cathen [--download] [--load] [--dry-run]
    python -m pipeline.src.scraper_cathen --delay 0.3
"""

from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

import click
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "cathen"
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
_ENCYCLOPEDIA_DB_PATH = PROJECT_ROOT / "data" / "encyclopedia.db"  # standalone, used during initial scrape

BASE_URL = "https://www.newadvent.org/cathen/"

_HTTP_HEADERS = {
    "User-Agent": "knowledge-graph-scraper/1.0 (Catholic Encyclopedia pipeline)",
}

REQUEST_DELAY = 0.5


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class Article:
    id: str        # e.g. "05649a"
    title: str
    summary: str
    url: str
    text_en: str = ""


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _fetch(url: str, delay: float = REQUEST_DELAY) -> bytes | None:
    """Fetch a URL, returning raw bytes (to handle encoding ourselves)."""
    try:
        resp = requests.get(url, headers=_HTTP_HEADERS, timeout=30)
        resp.raise_for_status()
        time.sleep(delay)
        return resp.content
    except requests.RequestException as e:
        click.echo(f"  WARN: failed to fetch {url}: {e}")
        return None


def _cached_fetch(url: str, cache_path: Path, delay: float = REQUEST_DELAY) -> str | None:
    """Fetch a URL with local file caching. Returns decoded HTML string."""
    if cache_path.exists():
        return cache_path.read_text(errors="replace")
    raw = _fetch(url, delay=delay)
    if raw is None:
        return None
    # Detect encoding from meta tag or fall back to latin-1
    html = raw.decode("latin-1", errors="replace")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")
    return html


# ---------------------------------------------------------------------------
# Parsing: index pages
# ---------------------------------------------------------------------------

def parse_index_page(html: str, letter: str) -> list[Article]:
    """Parse a letter index page and return list of articles (without text_en)."""
    soup = BeautifulSoup(html, "lxml")
    articles: list[Article] = []

    # Links to articles look like: <a href="../cathen/XXXXX.htm">Title</a>
    article_re = re.compile(r"\.\./cathen/([^.]+)\.htm", re.IGNORECASE)

    for a in soup.find_all("a", href=True):
        m = article_re.match(a["href"])
        if not m:
            continue
        article_id = m.group(1)
        title = a.get_text(strip=True)
        if not title:
            continue

        # Summary is the text after the closing </a> until the next <br> or <a>
        summary = ""
        for sib in a.next_siblings:
            if hasattr(sib, "name"):
                break  # hit a tag
            text = str(sib).strip(" \t\n\r-–")
            if text:
                summary = text
                break

        url = f"{BASE_URL}{article_id}.htm"
        articles.append(Article(id=article_id, title=title, summary=summary, url=url))

    return articles


# ---------------------------------------------------------------------------
# Parsing: article pages
# ---------------------------------------------------------------------------

def parse_article(html: str) -> str:
    """Extract plain text from a Catholic Encyclopedia article page."""
    soup = BeautifulSoup(html, "lxml")

    # Find the main content div
    content = soup.find("div", id="springfield2")
    if not content:
        # Fallback: use body
        content = soup.find("body")
    if not content:
        return ""

    # Remove donation prompt: <p><em><a href="...gumroad...">Please help support...</a></em></p>
    for p in content.find_all("p"):
        em = p.find("em")
        if em and em.find("a", href=re.compile(r"gumroad|support")):
            p.decompose()
            continue
        # Also match the text-only variant
        text = p.get_text()
        if "Please help support the mission of New Advent" in text:
            p.decompose()

    # Remove everything from <h2>About this page</h2> onward
    about_h2 = content.find("h2", string=re.compile(r"About this page", re.IGNORECASE))
    if about_h2:
        for sib in list(about_h2.next_siblings):
            if hasattr(sib, "decompose"):
                sib.decompose()
        about_h2.decompose()

    # Remove ad divs
    for div in content.find_all("div", class_=re.compile(r"catholicadnet")):
        div.decompose()

    # Remove scripts and styles
    for tag in content.find_all(["script", "style"]):
        tag.decompose()

    text = content.get_text(separator=" ", strip=True)
    # Collapse whitespace
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Download pipeline
# ---------------------------------------------------------------------------

def scrape_index(delay: float) -> list[Article]:
    """Fetch all 26 letter index pages and return complete article list."""
    all_articles: list[Article] = []
    letters = "abcdefghijklmnopqrstuvwxyz"

    click.echo("=== Scraping index pages ===")
    for letter in letters:
        url = f"{BASE_URL}{letter}-ce.htm"
        cache = RAW_DIR / "index" / f"{letter}-ce.htm"
        html = _cached_fetch(url, cache, delay=delay)
        if not html:
            click.echo(f"  {letter}: FAILED")
            continue
        arts = parse_index_page(html, letter)
        click.echo(f"  {letter}: {len(arts)} articles")
        all_articles.extend(arts)

    # Deduplicate by id (some articles appear under multiple letters)
    seen: set[str] = set()
    unique: list[Article] = []
    for a in all_articles:
        if a.id not in seen:
            seen.add(a.id)
            unique.append(a)

    click.echo(f"Total: {len(unique)} unique articles")
    return unique


def download_articles(articles: list[Article], delay: float) -> None:
    """Download article HTML pages to the cache directory."""
    click.echo(f"\n=== Downloading {len(articles)} articles ===")
    total = len(articles)
    downloaded = 0
    skipped = 0

    for i, art in enumerate(articles):
        cache = RAW_DIR / "articles" / f"{art.id}.htm"
        if cache.exists():
            skipped += 1
        else:
            html = _cached_fetch(art.url, cache, delay=delay)
            if html:
                downloaded += 1

        if (i + 1) % 100 == 0:
            click.echo(f"  {i + 1}/{total} ({downloaded} downloaded, {skipped} cached)")

    click.echo(f"Done: {downloaded} downloaded, {skipped} already cached")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS encyclopedia (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            summary TEXT,
            text_en TEXT,
            url TEXT
        )
    """)
    conn.commit()


def load_articles(articles: list[Article], conn: sqlite3.Connection, dry_run: bool) -> int:
    """Parse cached HTML and load articles into the encyclopedia table."""
    click.echo(f"\n=== Loading {len(articles)} articles into DB ===")
    loaded = 0
    missing = 0

    for i, art in enumerate(articles):
        cache = RAW_DIR / "articles" / f"{art.id}.htm"
        if cache.exists():
            html = cache.read_text(errors="replace")
            art.text_en = parse_article(html)
        else:
            missing += 1

        if not dry_run:
            conn.execute("""
                INSERT OR REPLACE INTO encyclopedia (id, title, summary, text_en, url)
                VALUES (?, ?, ?, ?, ?)
            """, (art.id, art.title, art.summary, art.text_en, art.url))

        loaded += 1
        if (i + 1) % 500 == 0:
            if not dry_run:
                conn.commit()
            click.echo(f"  {i + 1}/{len(articles)} loaded ({missing} without HTML so far)")

    if not dry_run:
        conn.commit()

    click.echo(f"Loaded {loaded} articles ({missing} without downloaded HTML)")
    return loaded


def rebuild_fts(conn: sqlite3.Connection) -> None:
    click.echo("Rebuilding encyclopedia_fts...")
    conn.execute("DROP TABLE IF EXISTS encyclopedia_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE encyclopedia_fts USING fts5(
            id, title, summary, text_en,
            content=encyclopedia, content_rowid=rowid
        )
    """)
    conn.execute("""
        INSERT INTO encyclopedia_fts(encyclopedia_fts) VALUES('rebuild')
    """)
    count = conn.execute("SELECT COUNT(*) FROM encyclopedia_fts").fetchone()[0]
    conn.commit()
    click.echo(f"  FTS rebuilt with {count} rows.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--download/--no-download", default=True, help="Download from New Advent")
@click.option("--load/--no-load", default=True, help="Load into database")
@click.option("--dry-run", is_flag=True, help="Show what would be done without writing")
@click.option("--delay", type=float, default=0.5, help="Seconds between HTTP requests")
@click.option("--db", type=click.Path(), default=None, help="Path to SQLite database")
def main(
    download: bool,
    load: bool,
    dry_run: bool,
    delay: float,
    db: str | None,
) -> None:
    """Scrape the Catholic Encyclopedia from New Advent and load into SQLite."""
    global REQUEST_DELAY
    REQUEST_DELAY = delay
    db_path = Path(db) if db else DB_PATH

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / "index").mkdir(exist_ok=True)
    (RAW_DIR / "articles").mkdir(exist_ok=True)

    # Step 1 & 2: Scrape indexes and download articles
    if download:
        articles = scrape_index(delay)
        download_articles(articles, delay)
    else:
        # Load article list from cached index pages
        click.echo("=== Reading cached index pages ===")
        articles = scrape_index(0)  # no network, reads from cache

    if not articles:
        raise click.ClickException("No articles found. Run with --download first.")

    # Step 3 & 4: Parse and load into DB
    if load:
        suffix = " (dry-run)" if dry_run else ""
        click.echo(f"\nConnecting to {db_path.name}{suffix}")
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        if not dry_run:
            create_tables(conn)

        loaded = load_articles(articles, conn, dry_run)

        if not dry_run:
            rebuild_fts(conn)

        conn.close()
        click.echo(f"\nDone: {loaded} articles{suffix}")


if __name__ == "__main__":
    main()
