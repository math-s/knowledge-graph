"""Parse the Church Fathers HTML bundle into structured tables + graph nodes.

Source: ``pipeline/data/raw/newadvent/fathers/*.htm`` — 3,728 files ranging
from whole-work pages down to chapter-level sections across 38 author groups.

The filename scheme is positional but irregular (``0103`` = Against Heresies
root, ``0103100`` = Book I of Against Heresies, ``0101`` = Epistle to Diognetus
root). Rather than reverse-engineer the full hierarchy, we treat every file as
a ``fathers-page`` node and derive parent links by longest-matching-prefix
against the set of siblings. Pages without a longer-prefix parent become
top-level works.

Cross-corpus:
  * cites → ency:<id>, bible-verse:*, summa-article:*
  * mentions → entity:<id>
  * has_theme → theme:<id>

This is additive — existing ``patristic-work`` / ``patristic-section`` nodes
are left alone (different id schema, covered by a different scraper).

Usage:
    uv run --project pipeline python -m pipeline.scripts.refresh_fathers_from_newadvent
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
FATHERS_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent" / "fathers"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.entity_extraction import extract_entities  # noqa: E402
from pipeline.src.themes import THEME_DEFINITIONS  # noqa: E402

FATHERS_PAGE_COLOR = "#B07AA1"  # same family as patristic
ROOT_COLOR = "#8E6289"

CATHEN_HREF = re.compile(r'^(?:\.\./)?cathen/(\d+[a-z])\.htm')
BIBLE_HREF = re.compile(r'^(?:\.\./)?bible/([a-z0-9]+)\.htm(?:#vrs?(\d+))?', re.IGNORECASE)
SUMMA_HREF = re.compile(r'^(?:\.\./)?summa/(\d{4}).htm(?:#article(\d+))?', re.IGNORECASE)
FATHERS_HREF = re.compile(r'^(?:\.\./)?fathers/(\d+).htm', re.IGNORECASE)
FILE_RE = re.compile(r"^(\d+)\.htm$")

BIBLE_PREFIX_TO_BOOK: dict[str, str] = {
    "gen": "genesis", "exo": "exodus", "lev": "leviticus",
    "num": "numbers", "deu": "deuteronomy", "jos": "joshua",
    "jdg": "judges", "rut": "ruth",
    "1sa": "1-samuel", "2sa": "2-samuel",
    "1ki": "1-kings", "2ki": "2-kings",
    "1ch": "1-chronicles", "2ch": "2-chronicles",
    "ezr": "ezra", "neh": "nehemiah", "est": "esther",
    "job": "job", "psa": "psalms", "pro": "proverbs",
    "ecc": "ecclesiastes", "son": "song-of-solomon",
    "isa": "isaiah", "jer": "jeremiah", "lam": "lamentations",
    "eze": "ezekiel", "dan": "daniel",
    "hos": "hosea", "joe": "joel", "amo": "amos", "oba": "obadiah",
    "jon": "jonah", "mic": "micah", "nah": "nahum", "hab": "habakkuk",
    "zep": "zephaniah", "hag": "haggai", "zec": "zechariah", "mal": "malachi",
    "mat": "matthew", "mar": "mark", "luk": "luke", "joh": "john",
    "act": "acts", "rom": "romans",
    "1co": "1-corinthians", "2co": "2-corinthians",
    "gal": "galatians", "eph": "ephesians",
    "phi": "philippians", "col": "colossians",
    "1th": "1-thessalonians", "2th": "2-thessalonians",
    "1ti": "1-timothy", "2ti": "2-timothy", "tit": "titus",
    "phl": "philemon", "heb": "hebrews", "jam": "james",
    "1pe": "1-peter", "2pe": "2-peter",
    "1jo": "1-john", "2jo": "2-john", "3jo": "3-john",
    "jud": "jude", "apo": "revelation", "rev": "revelation",
    "psm": "psalms", "can": "song-of-solomon",
    "sir": "sirach", "wis": "wisdom", "tob": "tobit",
    "jdt": "judith", "bar": "baruch",
    "1ma": "1-maccabees", "2ma": "2-maccabees",
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("refresh_fathers")


def parse_bible_href(href: str) -> str | None:
    m = BIBLE_HREF.match(href)
    if not m:
        return None
    prefix = m.group(1)[:3].lower()
    book = BIBLE_PREFIX_TO_BOOK.get(prefix)
    if not book:
        return None
    num_match = re.search(r"(\d+)$", m.group(1))
    if not num_match:
        return None
    chapter = int(num_match.group(1))
    verse = int(m.group(2)) if m.group(2) else None
    if verse is None:
        return None
    return f"bible-verse:{book}-{chapter}:{verse}"


def find_parent(page_id: str, id_set: set[str]) -> str | None:
    """Longest-prefix parent within the id set."""
    for length in range(len(page_id) - 1, 3, -1):
        candidate = page_id[:length]
        if candidate in id_set and candidate != page_id:
            return candidate
    return None


def parse_page(html: str) -> tuple[str, str, list[tuple[str, str]]]:
    """Return (title, text, cites). cites is a list of (kind, target_node_id)."""
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    raw_title = title_tag.get_text(strip=True) if title_tag else ""
    # Strip leading "CHURCH FATHERS: " prefix if present
    title = re.sub(r"^CHURCH FATHERS:\s*", "", raw_title)

    content = soup.find("div", id="springfield2")
    if not content:
        return title, "", []

    # Drop "About this page" onward
    about = content.find(["h2", "h3"], string=re.compile(r"About this page", re.IGNORECASE))
    if about:
        for sib in list(about.next_siblings):
            if hasattr(sib, "decompose"):
                sib.decompose()
        about.decompose()

    # Drop scripts/styles/ads
    for tag in content.find_all(["script", "style"]):
        tag.decompose()
    for div in content.find_all("div", class_=re.compile(r"catholicadnet")):
        div.decompose()

    cites: list[tuple[str, str]] = []
    for a in content.find_all("a", href=True):
        href = a["href"]
        m = CATHEN_HREF.match(href)
        if m:
            cites.append(("ency", f"ency:{m.group(1)}"))
            continue
        bv = parse_bible_href(href)
        if bv:
            cites.append(("bible", bv))
            continue
        ms = SUMMA_HREF.match(href)
        if ms:
            q = ms.group(1)
            art = ms.group(2)
            if art:
                cites.append(("summa", f"summa-article:{q}:{int(art)}"))
            else:
                cites.append(("summa", f"summa-question:{q}"))
            continue
        mf = FATHERS_HREF.match(href)
        if mf:
            cites.append(("fathers", f"fathers-page:{mf.group(1)}"))

    text = content.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return title, text, cites


def main() -> None:
    if not FATHERS_DIR.exists():
        raise SystemExit(f"Fathers dir not found: {FATHERS_DIR}")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    files = []
    for p in FATHERS_DIR.iterdir():
        m = FILE_RE.match(p.name)
        if not m:
            continue
        files.append((m.group(1), p))
    files.sort(key=lambda x: x[0])
    log.info("Found %d fathers HTML files", len(files))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS fathers_pages (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            title TEXT,
            text TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fp_parent ON fathers_pages(parent_id);
        """
    )
    cur.execute("DELETE FROM fathers_pages")
    conn.commit()

    id_set = {pid for pid, _ in files}

    # Parse all pages
    page_rows: list[tuple[str, str | None, str, str]] = []
    all_cites: dict[str, list[tuple[str, str]]] = {}
    for i, (page_id, path) in enumerate(files):
        html = path.read_text(encoding="utf-8", errors="replace")
        title, text, cites = parse_page(html)
        parent = find_parent(page_id, id_set)
        page_rows.append((page_id, parent, title, text))
        all_cites[page_id] = cites
        if (i + 1) % 500 == 0:
            log.info("  parsed %d / %d", i + 1, len(files))

    cur.executemany(
        "INSERT INTO fathers_pages VALUES (?,?,?,?)", page_rows
    )
    conn.commit()
    log.info("Inserted %d fathers pages", len(page_rows))

    # --- Graph nodes ---
    empty_json = json.dumps([])
    node_rows = []
    for page_id, parent_id, title, _text in page_rows:
        color = FATHERS_PAGE_COLOR if parent_id else ROOT_COLOR
        size = 3.0 if parent_id else 6.0
        node_rows.append((
            f"fathers-page:{page_id}",
            title or page_id,
            "fathers-page",
            0.0, 0.0, size, color, "",
            0, 0, empty_json, empty_json, empty_json,
        ))
    cur.executemany(
        "INSERT OR REPLACE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    conn.commit()
    log.info("Inserted %d fathers-page nodes", len(node_rows))

    # --- Edges ---
    existing = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes "
            "WHERE node_type IN ('encyclopedia','bible-verse','summa-article','summa-question','fathers-page')"
        )
    }
    entity_ids = {
        row[0].removeprefix("entity:")
        for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='entity'"
        )
    }
    theme_ids = {
        row[0].removeprefix("theme:")
        for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='theme'"
        )
    }
    theme_keywords = [
        (td.id, [kw.lower() for kw in td.keywords])
        for td in THEME_DEFINITIONS
        if td.id in theme_ids
    ]

    edge_rows: list[tuple[str, str, str]] = []

    # child_of — parent linkage
    child_of = 0
    for page_id, parent_id, *_ in page_rows:
        if parent_id:
            edge_rows.append((f"fathers-page:{page_id}", f"fathers-page:{parent_id}", "child_of"))
            child_of += 1

    # cites — from parsed hrefs (skipping targets we don't have)
    cites = 0
    cites_skipped = 0
    seen_cite: set[tuple[str, str]] = set()
    for page_id, targets in all_cites.items():
        src = f"fathers-page:{page_id}"
        for _kind, tgt in targets:
            if tgt == src:
                continue
            if tgt not in existing:
                cites_skipped += 1
                continue
            pair = (src, tgt)
            if pair in seen_cite:
                continue
            seen_cite.add(pair)
            edge_rows.append((src, tgt, "cites"))
            cites += 1

    # Cross-corpus entities + themes from body text
    mention_rows = 0
    has_theme_rows = 0
    for page_id, _parent, _title, text in page_rows:
        if not text:
            continue
        src = f"fathers-page:{page_id}"
        lowered = text.lower()
        for eid in extract_entities(text):
            if eid in entity_ids:
                edge_rows.append((src, f"entity:{eid}", "mentions"))
                mention_rows += 1
        for tid, kws in theme_keywords:
            if any(kw in lowered for kw in kws):
                edge_rows.append((src, f"theme:{tid}", "has_theme"))
                has_theme_rows += 1

    log.info(
        "Edges: %d child_of, %d cites (%d skipped), %d mentions, %d has_theme",
        child_of, cites, cites_skipped, mention_rows, has_theme_rows,
    )

    CHUNK = 50_000
    for i in range(0, len(edge_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows[i : i + CHUNK],
        )
        conn.commit()

    # --- Rebuild degree ---
    log.info("Rebuilding degree column...")
    deg: dict[str, int] = defaultdict(int)
    for src, tgt in conn.execute("SELECT source, target FROM graph_edges"):
        deg[src] += 1
        deg[tgt] += 1
    cur.execute("UPDATE graph_nodes SET degree = 0")
    deg_items = list(deg.items())
    for i in range(0, len(deg_items), CHUNK):
        cur.executemany(
            "UPDATE graph_nodes SET degree = ? WHERE id = ?",
            [(d, nid) for nid, d in deg_items[i : i + CHUNK]],
        )
    conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    avg_deg = conn.execute("SELECT AVG(degree) FROM graph_nodes").fetchone()[0]
    conn.close()
    log.info("DB now has %d nodes, %d edges (avg degree %.1f)",
             total_nodes, total_edges, avg_deg)


if __name__ == "__main__":
    main()
