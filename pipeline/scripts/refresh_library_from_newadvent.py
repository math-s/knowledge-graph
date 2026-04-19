"""Parse the New Advent "Library" bundle — papal docs, encyclicals, declarations.

Source: ``pipeline/data/raw/newadvent/library/*.htm`` — 187 ``docs_*`` files
plus a handful of ``almanac_*`` miscellany. The docs set is the substantive
one: Unam Sanctam (1302), A Quo Primum (1751), Declaration on Euthanasia
(1980), Humanae Vitae, etc.

Adds a ``library-doc`` node type with cross-corpus edges to cathen, bible,
summa, fathers, plus entity/theme mentions.

Tables:
  * library_docs(id, title, year, category, text)

Usage:
    uv run --project pipeline python -m pipeline.scripts.refresh_library_from_newadvent
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
LIBRARY_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent" / "library"

sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.entity_extraction import extract_entities  # noqa: E402
from pipeline.src.themes import THEME_DEFINITIONS  # noqa: E402

DOCS_COLOR = "#D4A94E"      # warm gold for papal/magisterial docs
ALMANAC_COLOR = "#A89970"
SIZE = 5.0

CATHEN_HREF = re.compile(r'^(?:\.\./)?cathen/(\d+[a-z])\.htm')
BIBLE_HREF = re.compile(r'^(?:\.\./)?bible/([a-z0-9]+)\.htm(?:#(?:verse|vrs)(\d+))?', re.IGNORECASE)
SUMMA_HREF = re.compile(r'^(?:\.\./)?summa/(\d{4})\.htm(?:#article(\d+))?', re.IGNORECASE)
FATHERS_HREF = re.compile(r'^(?:\.\./)?fathers/(\d+)\.htm', re.IGNORECASE)
LIBRARY_HREF = re.compile(r'^(?:\.\./)?library/([a-zA-Z0-9_]+)\.htm', re.IGNORECASE)

YEAR_RE = re.compile(r"\((\d{3,4})\)")
FILENAME_RE = re.compile(r"^(docs_[a-z0-9]+|almanac_[a-z0-9]+)\.htm?l?$", re.IGNORECASE)

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
log = logging.getLogger("refresh_library")


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


def parse_doc(html: str) -> tuple[str, int | None, str, list[tuple[str, str]]]:
    """Return (title, year, text, cites)."""
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    raw_title = title_tag.get_text(strip=True) if title_tag else ""
    title = re.sub(r"^CATHOLIC LIBRARY:\s*", "", raw_title)

    year: int | None = None
    year_m = YEAR_RE.search(title)
    if year_m:
        try:
            year = int(year_m.group(1))
        except ValueError:
            year = None

    content = soup.find("div", id="springfield2")
    if not content:
        return title, year, "", []

    about = content.find(["h2", "h3"], string=re.compile(r"About this page", re.IGNORECASE))
    if about:
        for sib in list(about.next_siblings):
            if hasattr(sib, "decompose"):
                sib.decompose()
        about.decompose()

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
            continue
        ml = LIBRARY_HREF.match(href)
        if ml and ml.group(1) != "index":
            cites.append(("library", f"library-doc:{ml.group(1)}"))

    text = content.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return title, year, text, cites


def main() -> None:
    if not LIBRARY_DIR.exists():
        raise SystemExit(f"Library dir not found: {LIBRARY_DIR}")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    files = []
    for p in LIBRARY_DIR.iterdir():
        m = FILENAME_RE.match(p.name)
        if not m:
            continue
        doc_id = m.group(1)
        files.append((doc_id, p))
    files.sort(key=lambda x: x[0])
    log.info("Found %d library HTML files", len(files))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS library_docs (
            id TEXT PRIMARY KEY,
            category TEXT,
            title TEXT,
            year INTEGER,
            text TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_libdoc_year ON library_docs(year);
        """
    )
    cur.execute("DELETE FROM library_docs")
    conn.commit()

    rows: list[tuple[str, str, str, int | None, str]] = []
    all_cites: dict[str, list[tuple[str, str]]] = {}
    for doc_id, path in files:
        html = path.read_text(encoding="utf-8", errors="replace")
        title, year, text, cites = parse_doc(html)
        category = "docs" if doc_id.startswith("docs_") else "almanac"
        rows.append((doc_id, category, title, year, text))
        all_cites[doc_id] = cites

    cur.executemany("INSERT INTO library_docs VALUES (?,?,?,?,?)", rows)
    conn.commit()
    log.info("Inserted %d library rows", len(rows))

    # Graph nodes
    empty_json = json.dumps([])
    node_rows = []
    for doc_id, category, title, year, _text in rows:
        color = DOCS_COLOR if category == "docs" else ALMANAC_COLOR
        # Store category in `part` column for later styling
        node_rows.append((
            f"library-doc:{doc_id}",
            title or doc_id,
            "library-doc",
            0.0, 0.0, SIZE, color, category,
            0, 0, empty_json, empty_json, empty_json,
        ))
    cur.executemany(
        "INSERT OR REPLACE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    conn.commit()
    log.info("Inserted %d library-doc nodes", len(node_rows))

    # Cross-corpus edges
    existing = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes "
            "WHERE node_type IN ('encyclopedia','bible-verse','summa-article',"
            "'summa-question','fathers-page','library-doc')"
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
    cites_added = 0
    cites_skipped = 0
    seen: set[tuple[str, str]] = set()

    for doc_id, targets in all_cites.items():
        src = f"library-doc:{doc_id}"
        for _kind, tgt in targets:
            if tgt == src:
                continue
            if tgt not in existing:
                cites_skipped += 1
                continue
            pair = (src, tgt)
            if pair in seen:
                continue
            seen.add(pair)
            edge_rows.append((src, tgt, "cites"))
            cites_added += 1

    mention_count = 0
    theme_count = 0
    for doc_id, _cat, _title, _year, text in rows:
        if not text:
            continue
        src = f"library-doc:{doc_id}"
        lowered = text.lower()
        for eid in extract_entities(text):
            if eid in entity_ids:
                edge_rows.append((src, f"entity:{eid}", "mentions"))
                mention_count += 1
        for tid, kws in theme_keywords:
            if any(kw in lowered for kw in kws):
                edge_rows.append((src, f"theme:{tid}", "has_theme"))
                theme_count += 1

    log.info(
        "Edges: %d cites (%d skipped), %d mentions, %d has_theme",
        cites_added, cites_skipped, mention_count, theme_count,
    )

    CHUNK = 50_000
    for i in range(0, len(edge_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows[i : i + CHUNK],
        )
        conn.commit()

    # Rebuild degrees
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
