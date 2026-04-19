"""Backfill cross-corpus ``cites`` edges across all newadvent HTML.

The first-pass refresh scripts (cathen, summa, fathers, library, bible) each
extracted outbound links, but inconsistently:

  * ``refresh_encyclopedia_from_newadvent`` only captured cathen→cathen.
  * Summa/fathers/library scripts used a bible-href regex that matched
    ``#vrsN`` anchors but newadvent's actual anchors are ``#verseN`` —
    which meant every bible citation from those corpora was silently
    dropped.

This script re-parses every ``cathen``, ``summa``, ``fathers``, ``library``
HTML file and inserts any *missing* ``cites`` edges into ``graph_edges``.
Idempotent via ``INSERT OR IGNORE``.

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_cross_corpus_cites
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
NEWADVENT = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent"

# Internal link regexes — accept both `#verseN` and `#vrsN`
CATHEN_HREF = re.compile(r'^(?:\.\./)?cathen/(\d+[a-z])\.htm', re.IGNORECASE)
BIBLE_HREF = re.compile(
    r'^(?:\.\./)?bible/([a-z0-9]+?)(\d{3})\.htm(?:#(?:verse|vrs)(\d+))?',
    re.IGNORECASE,
)
SUMMA_HREF = re.compile(r'^(?:\.\./)?summa/(\d{4})\.htm(?:#article(\d+))?', re.IGNORECASE)
FATHERS_HREF = re.compile(r'^(?:\.\./)?fathers/(\d+)\.htm', re.IGNORECASE)
LIBRARY_HREF = re.compile(r'^(?:\.\./)?library/([a-zA-Z0-9_]+)\.htm', re.IGNORECASE)

BOOK_PREFIX_TO_ID: dict[str, str] = {
    "gen": "genesis", "exo": "exodus", "lev": "leviticus",
    "num": "numbers", "deu": "deuteronomy",
    "jos": "joshua", "jdg": "judges", "rut": "ruth",
    "1sa": "1-samuel", "2sa": "2-samuel",
    "1ki": "1-kings", "2ki": "2-kings",
    "1ch": "1-chronicles", "2ch": "2-chronicles",
    "ezr": "ezra", "neh": "nehemiah",
    "tob": "tobit", "jth": "judith", "est": "esther",
    "1ma": "1-maccabees", "2ma": "2-maccabees",
    "job": "job", "psa": "psalms", "pro": "proverbs",
    "ecc": "ecclesiastes", "son": "song-of-solomon",
    "wis": "wisdom", "sir": "sirach",
    "isa": "isaiah", "jer": "jeremiah", "lam": "lamentations",
    "bar": "baruch", "eze": "ezekiel", "dan": "daniel",
    "hos": "hosea", "joe": "joel", "amo": "amos", "oba": "obadiah",
    "jon": "jonah", "mic": "micah", "nah": "nahum", "hab": "habakkuk",
    "zep": "zephaniah", "hag": "haggai", "zec": "zechariah", "mal": "malachi",
    "mat": "matthew", "mar": "mark", "luk": "luke", "joh": "john",
    "act": "acts",
    "rom": "romans", "1co": "1-corinthians", "2co": "2-corinthians",
    "gal": "galatians", "eph": "ephesians",
    "phi": "philippians", "col": "colossians",
    "1th": "1-thessalonians", "2th": "2-thessalonians",
    "1ti": "1-timothy", "2ti": "2-timothy", "tit": "titus",
    "phm": "philemon", "heb": "hebrews",
    "jam": "james", "1pe": "1-peter", "2pe": "2-peter",
    "1jo": "1-john", "2jo": "2-john", "3jo": "3-john",
    "jud": "jude", "rev": "revelation",
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("backfill_cites")


def parse_bible_href(href: str) -> str | None:
    m = BIBLE_HREF.match(href)
    if not m:
        return None
    prefix = m.group(1)[:3].lower()
    book = BOOK_PREFIX_TO_ID.get(prefix)
    if not book:
        return None
    chapter = int(m.group(2))
    if not m.group(3):
        return f"bible-chapter:{book}-{chapter}"
    verse = int(m.group(3))
    return f"bible-verse:{book}-{chapter}:{verse}"


def parse_href(href: str) -> str | None:
    """Map any internal newadvent href to a graph node id (or None)."""
    m = CATHEN_HREF.match(href)
    if m:
        return f"ency:{m.group(1)}"
    bv = parse_bible_href(href)
    if bv:
        return bv
    m = SUMMA_HREF.match(href)
    if m:
        q = m.group(1)
        art = m.group(2)
        if art:
            return f"summa-article:{q}:{int(art)}"
        return f"summa-question:{q}"
    m = FATHERS_HREF.match(href)
    if m:
        return f"fathers-page:{m.group(1)}"
    m = LIBRARY_HREF.match(href)
    if m and m.group(1) != "index":
        return f"library-doc:{m.group(1)}"
    return None


def src_node_for(subdir: str, path: Path) -> str | None:
    """Map an HTML file to its graph node id based on its directory."""
    stem = path.stem
    if subdir == "cathen":
        if re.match(r"^\d+[a-z]$", stem):
            return f"ency:{stem}"
    elif subdir == "summa":
        m = re.match(r"^(\d)(\d{3})$", stem)
        if m:
            return f"summa-question:{m.group(1)}{m.group(2)}"
    elif subdir == "fathers":
        if stem.isdigit():
            return f"fathers-page:{stem}"
    elif subdir == "library":
        if stem.startswith(("docs_", "almanac_")):
            return f"library-doc:{stem}"
    return None


def scan_dir(subdir: str) -> dict[str, set[str]]:
    """Return {src_node: set(target_nodes)} for every outbound internal href
    in every HTML file of subdir."""
    dir_path = NEWADVENT / subdir
    if not dir_path.exists():
        log.warning("Skipping %s — dir missing", subdir)
        return {}
    out: dict[str, set[str]] = {}
    files = [p for p in dir_path.iterdir() if p.suffix.lower() == ".htm"]
    log.info("  %s: %d files", subdir, len(files))
    for i, path in enumerate(files):
        src = src_node_for(subdir, path)
        if not src:
            continue
        html = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            tgt = parse_href(a["href"])
            if tgt and tgt != src:
                out.setdefault(src, set()).add(tgt)
        if (i + 1) % 1000 == 0:
            log.info("    parsed %d / %d", i + 1, len(files))
    return out


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    log.info("Scanning newadvent HTML for all outbound cites...")
    all_cites: dict[str, set[str]] = {}
    for sub in ("cathen", "summa", "fathers", "library"):
        log.info("Scanning %s/", sub)
        for k, v in scan_dir(sub).items():
            all_cites.setdefault(k, set()).update(v)

    total_candidate = sum(len(v) for v in all_cites.values())
    log.info("Total candidate (src, tgt) pairs: %d", total_candidate)

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    # Accept any node id that's present in graph_nodes (so we don't add dangling edges)
    all_nodes = {
        row[0] for row in conn.execute("SELECT id FROM graph_nodes")
    }
    log.info("Graph has %d nodes total", len(all_nodes))

    # Also skip targets that resolve to a bible-chapter but no such chapter exists
    # (chapter-level links may point to chapters outside our canon)
    edge_rows: list[tuple[str, str, str]] = []
    skipped_missing = 0
    skipped_self = 0
    for src, targets in all_cites.items():
        if src not in all_nodes:
            continue
        for tgt in targets:
            if tgt == src:
                skipped_self += 1
                continue
            if tgt not in all_nodes:
                skipped_missing += 1
                continue
            edge_rows.append((src, tgt, "cites"))

    log.info(
        "Candidates after filter: %d (skipped %d missing, %d self)",
        len(edge_rows), skipped_missing, skipped_self,
    )

    # Summarize by (src_type, tgt_type) for visibility
    type_counts: dict[tuple[str, str], int] = defaultdict(int)
    for src, tgt, _ in edge_rows:
        src_t = src.split(":", 1)[0]
        tgt_t = tgt.split(":", 1)[0]
        type_counts[(src_t, tgt_t)] += 1
    for (s, t), n in sorted(type_counts.items(), key=lambda x: -x[1]):
        log.info("  %-18s → %-20s %7d", s, t, n)

    before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='cites'"
    ).fetchone()[0]
    CHUNK = 50_000
    for i in range(0, len(edge_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows[i : i + CHUNK],
        )
        conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='cites'"
    ).fetchone()[0]
    added = after - before

    # Rebuild degrees
    log.info("Rebuilding degree column...")
    deg: dict[str, int] = defaultdict(int)
    for src, tgt in conn.execute("SELECT source, target FROM graph_edges"):
        deg[src] += 1
        deg[tgt] += 1
    cur.execute("UPDATE graph_nodes SET degree = 0")
    items = list(deg.items())
    for i in range(0, len(items), CHUNK):
        cur.executemany(
            "UPDATE graph_nodes SET degree = ? WHERE id = ?",
            [(d, nid) for nid, d in items[i : i + CHUNK]],
        )
    conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    avg_deg = conn.execute("SELECT AVG(degree) FROM graph_nodes").fetchone()[0]
    conn.close()

    log.info("")
    log.info("Inserted %d new cites edges", added)
    log.info("DB now has %d nodes, %d edges (avg degree %.1f)",
             total_nodes, total_edges, avg_deg)


if __name__ == "__main__":
    main()
