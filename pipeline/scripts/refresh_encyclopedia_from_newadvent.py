"""Refresh the encyclopedia table from the licensed New Advent HTML bundle.

Reads every ``pipeline/data/raw/newadvent/cathen/*.htm`` file, extracts
title + summary + body text, and populates:

  * ``encyclopedia`` — replaces the scraped version
  * ``encyclopedia_fts`` — rebuilt
  * ``encyclopedia_cross_refs`` — new table, ``(source_id, target_id)`` of
    internal article-to-article <a href> links, for graph-edge injection

The bundle is the canonical source (more complete than our scrape) so we
treat it as authoritative and overwrite.

Usage:
    uv run --project pipeline python -m pipeline.scripts.refresh_encyclopedia_from_newadvent
"""

from __future__ import annotations

import logging
import re
import sqlite3
import sys
from pathlib import Path

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
CATHEN_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent" / "cathen"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("refresh_encyclopedia")

ARTICLE_ID_RE = re.compile(r"^(\d+[a-z])\.htm$")
# Internal article link: href="../cathen/NNNNN.htm" or "cathen/NNNNN.htm"
CROSSREF_RE = re.compile(r'href="(?:\.\./)?cathen/(\d+[a-z])\.htm(?:#[^"]*)?"')


def parse_article(html: str) -> tuple[str, str, str]:
    """Return (title, summary, text)."""
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    meta_desc = soup.find("meta", attrs={"name": "description"})
    summary = meta_desc.get("content", "").strip() if meta_desc else ""

    content = soup.find("div", id="springfield2")
    if not content:
        return title, summary, ""

    # Drop donation prompts
    for p in content.find_all("p"):
        em = p.find("em")
        if em and em.find("a", href=re.compile(r"gumroad|support")):
            p.decompose()
            continue
        text = p.get_text()
        if "Please help support the mission of New Advent" in text:
            p.decompose()

    # Drop "About this page" and everything after
    about_h2 = content.find(
        ["h2", "h3"], string=re.compile(r"About this page", re.IGNORECASE)
    )
    if about_h2:
        for sib in list(about_h2.next_siblings):
            if hasattr(sib, "decompose"):
                sib.decompose()
        about_h2.decompose()

    # Drop ads and scripts/styles
    for tag in content.find_all(["script", "style"]):
        tag.decompose()
    for div in content.find_all("div", class_=re.compile(r"catholicadnet")):
        div.decompose()

    # Drop the <h1> itself (we've already captured the title)
    if content.find("h1"):
        content.find("h1").decompose()

    text = content.get_text(separator=" ", strip=True)
    text = re.sub(r" {2,}", " ", text)
    return title, summary, text.strip()


def extract_cross_refs(html: str, own_id: str) -> set[str]:
    """Return the set of article IDs referenced by <a href> links, excluding self."""
    refs = set(CROSSREF_RE.findall(html))
    refs.discard(own_id)
    # Drop references to index letter pages (already excluded by the regex
    # requiring a digit, but just in case)
    return refs


def main() -> None:
    if not CATHEN_DIR.exists():
        raise SystemExit(f"Cathen dir not found: {CATHEN_DIR}")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    files = sorted(p for p in CATHEN_DIR.iterdir() if ARTICLE_ID_RE.match(p.name))
    log.info("Found %d cathen article files", len(files))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    # Ensure cross-ref table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS encyclopedia_cross_refs (
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            PRIMARY KEY (source_id, target_id)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_ecr_target ON encyclopedia_cross_refs(target_id)"
    )

    # Wipe stale data
    cur.execute("DELETE FROM encyclopedia")
    cur.execute("DELETE FROM encyclopedia_cross_refs")
    # encyclopedia_fts uses content=encyclopedia — rebuild after
    conn.commit()

    article_rows: list[tuple[str, str, str, str, str]] = []
    crossref_rows: list[tuple[str, str]] = []
    empty_title = 0
    empty_body = 0

    for i, path in enumerate(files):
        m = ARTICLE_ID_RE.match(path.name)
        if not m:
            continue
        article_id = m.group(1)
        html = path.read_text(encoding="utf-8", errors="replace")

        title, summary, text = parse_article(html)
        if not title:
            empty_title += 1
        if not text:
            empty_body += 1

        url = f"https://www.newadvent.org/cathen/{article_id}.htm"
        article_rows.append((article_id, title, summary, text, url))

        for target in extract_cross_refs(html, article_id):
            crossref_rows.append((article_id, target))

        if (i + 1) % 1000 == 0:
            log.info("  parsed %d / %d", i + 1, len(files))

    log.info(
        "Parsed %d articles (%d missing title, %d missing body), %d cross-refs",
        len(article_rows), empty_title, empty_body, len(crossref_rows),
    )

    cur.executemany(
        "INSERT OR REPLACE INTO encyclopedia (id, title, summary, text_en, url) VALUES (?,?,?,?,?)",
        article_rows,
    )
    conn.commit()

    cur.executemany(
        "INSERT OR IGNORE INTO encyclopedia_cross_refs VALUES (?,?)",
        crossref_rows,
    )
    conn.commit()
    distinct_cr = conn.execute("SELECT COUNT(*) FROM encyclopedia_cross_refs").fetchone()[0]
    log.info("Stored %d distinct cross-ref rows", distinct_cr)

    # Rebuild FTS
    log.info("Rebuilding encyclopedia_fts...")
    cur.execute("INSERT INTO encyclopedia_fts(encyclopedia_fts) VALUES('rebuild')")
    conn.commit()

    total_articles = conn.execute("SELECT COUNT(*) FROM encyclopedia").fetchone()[0]
    conn.close()

    log.info("")
    log.info("Encyclopedia refreshed: %d articles, %d cross-refs", total_articles, distinct_cr)


if __name__ == "__main__":
    main()
