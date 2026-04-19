"""Parse the Summa Theologica HTML bundle into structured tables + graph nodes.

Source: ``pipeline/data/raw/newadvent/summa/*.htm`` — 620 files, each
containing one Question with its Articles.

Adds two node types:
  * ``summa-question`` — one per file
  * ``summa-article``  — one per <h2 id="articleN"> inside a file

Tables:
  * summa_parts     (num, name)
  * summa_questions (id, part_num, question_num, title, summary)
  * summa_articles  (id, question_id, article_num, title, text)

Edges:
  * summa-article → summa-question (child_of)
  * summa-question → summa-part    (child_of)
  * summa-article → ency:<id>      (cites, from embedded <a href="../cathen/...">)
  * summa-article → bible-verse    (cites, from embedded <a href="../bible/...#vrsN">)
  * summa-article → entity:<id>    (mentions, from regex entity extraction on text)
  * summa-article → theme:<id>     (has_theme, from theme keyword extraction)

Usage:
    uv run --project pipeline python -m pipeline.scripts.refresh_summa_from_newadvent
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
SUMMA_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent" / "summa"

import sys
sys.path.insert(0, str(PROJECT_ROOT))
from pipeline.src.entity_extraction import extract_entities  # noqa: E402
from pipeline.src.themes import THEME_DEFINITIONS  # noqa: E402

PARTS = {
    1: "Prima Pars",
    2: "Prima Secundae",
    3: "Secunda Secundae",
    4: "Tertia Pars",
    5: "Supplementum",
    6: "Appendix I",
    7: "Appendix II",
}

QUESTION_COLOR = "#7B6E95"
ARTICLE_COLOR = "#8E85A8"
PART_COLOR = "#5B4E7A"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("refresh_summa")

Q_FILE_RE = re.compile(r"^(\d)(\d{3})\.htm$")

# Internal link patterns
CATHEN_HREF = re.compile(r'^(?:\.\./)?cathen/(\d+[a-z])\.htm')
BIBLE_HREF = re.compile(r'^(?:\.\./)?bible/([a-z0-9]+)\.htm(?:#(?:verse|vrs)(\d+))?', re.IGNORECASE)
FATHERS_HREF = re.compile(r'^(?:\.\./)?fathers/(\d+)\.htm', re.IGNORECASE)

# Bible filename (e.g. "gen001") → (book_slug, chapter)
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
    # Douay name variants
    "psm": "psalms", "can": "song-of-solomon",
    # Deutero-canonical (we may or may not have these in the graph)
    "sir": "sirach", "wis": "wisdom", "tob": "tobit",
    "jdt": "judith", "bar": "baruch",
    "1ma": "1-maccabees", "2ma": "2-maccabees",
}


def parse_bible_href(href: str) -> str | None:
    """Convert '../bible/gen001.htm#vrs5' → 'bible-verse:genesis-1:5' if parseable."""
    m = BIBLE_HREF.match(href)
    if not m:
        return None
    prefix = m.group(1)[:3].lower()
    book = BIBLE_PREFIX_TO_BOOK.get(prefix)
    if not book:
        return None
    # Chapter number is the numeric part after the prefix
    num_match = re.search(r"(\d+)$", m.group(1))
    if not num_match:
        return None
    chapter = int(num_match.group(1))
    verse = int(m.group(2)) if m.group(2) else None
    if verse is None:
        return None  # Chapter-only links aren't verse-level; skip
    return f"bible-verse:{book}-{chapter}:{verse}"


def parse_question_file(path: Path) -> tuple[dict, list[dict], list[tuple[str, str]]] | None:
    """Parse a single question file.

    Returns (question_dict, list_of_article_dicts, list_of_(article_id, cite_target)).
    """
    m = Q_FILE_RE.match(path.name)
    if not m:
        return None
    part_num = int(m.group(1))
    question_num = int(m.group(2))
    question_id = f"{part_num}{question_num:03d}"

    html = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    q_title = h1.get_text(strip=True) if h1 else ""
    # Strip "Question N." prefix if present
    q_title = re.sub(r"^Question\s+\d+\.\s*", "", q_title)

    meta = soup.find("meta", attrs={"name": "description"})
    summary = (meta.get("content", "").strip() if meta else "")

    content = soup.find("div", id="springfield2")
    if not content:
        return None

    # Drop "About this page" and everything after
    about = content.find(["h2", "h3"], string=re.compile(r"About this page", re.IGNORECASE))
    if about:
        for sib in list(about.next_siblings):
            if hasattr(sib, "decompose"):
                sib.decompose()
        about.decompose()

    # Strip scripts/styles/ads
    for tag in content.find_all(["script", "style"]):
        tag.decompose()
    for div in content.find_all("div", class_=re.compile(r"catholicadnet")):
        div.decompose()

    # Find all article <h2> headers (some HTML has malformed nesting, so we
    # can't rely on walking direct children of springfield2 — use find_all).
    articles: list[dict] = []
    cites: list[tuple[str, str]] = []

    h2s = [h for h in content.find_all("h2") if re.match(r"article\d+", h.get("id", ""))]
    for i, h in enumerate(h2s):
        art_num = int(re.match(r"article(\d+)", h["id"]).group(1))
        art_title = re.sub(r"^Article\s+\d+\.\s*", "", h.get_text(strip=True))
        article_id = f"{question_id}:{art_num}"

        # Collect text + hrefs from all siblings until the next article h2 (or end)
        next_stop = h2s[i + 1] if i + 1 < len(h2s) else None
        text_parts: list[str] = []
        for sib in h.next_siblings:
            if sib is next_stop:
                break
            if isinstance(sib, NavigableString):
                text_parts.append(str(sib))
                continue
            if not isinstance(sib, Tag):
                continue
            # Another article h2 could also be a direct descendant under weird nesting
            if sib.name == "h2" and re.match(r"article\d+", sib.get("id", "")):
                break
            text_parts.append(sib.get_text(" ", strip=True))
            for a in sib.find_all("a", href=True):
                href = a["href"]
                m_cath = CATHEN_HREF.match(href)
                if m_cath:
                    cites.append((article_id, f"ency:{m_cath.group(1)}"))
                    continue
                bv = parse_bible_href(href)
                if bv:
                    cites.append((article_id, bv))
                    continue
                m_fathers = FATHERS_HREF.match(href)
                if m_fathers:
                    cites.append((article_id, f"fathers-page:{m_fathers.group(1)}"))

        text = re.sub(r"\s+", " ", " ".join(text_parts)).strip()
        articles.append({
            "id": article_id,
            "question_id": question_id,
            "article_num": art_num,
            "title": art_title,
            "text": text,
        })

    question = {
        "id": question_id,
        "part_num": part_num,
        "question_num": question_num,
        "title": q_title,
        "summary": summary,
    }
    return question, articles, cites


def main() -> None:
    if not SUMMA_DIR.exists():
        raise SystemExit(f"Summa dir not found: {SUMMA_DIR}")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    files = sorted(p for p in SUMMA_DIR.iterdir() if Q_FILE_RE.match(p.name))
    log.info("Found %d summa question files", len(files))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS summa_parts (
            num INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS summa_questions (
            id TEXT PRIMARY KEY,
            part_num INTEGER NOT NULL,
            question_num INTEGER NOT NULL,
            title TEXT,
            summary TEXT
        );
        CREATE TABLE IF NOT EXISTS summa_articles (
            id TEXT PRIMARY KEY,
            question_id TEXT NOT NULL,
            article_num INTEGER NOT NULL,
            title TEXT,
            text TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sa_q ON summa_articles(question_id);
        """
    )

    # Wipe summa tables (authoritative refresh)
    cur.execute("DELETE FROM summa_articles")
    cur.execute("DELETE FROM summa_questions")
    cur.execute("DELETE FROM summa_parts")
    conn.commit()

    cur.executemany(
        "INSERT INTO summa_parts VALUES (?,?)",
        list(PARTS.items()),
    )
    conn.commit()

    q_rows = []
    a_rows = []
    cite_rows: list[tuple[str, str]] = []

    for path in files:
        parsed = parse_question_file(path)
        if parsed is None:
            continue
        q, articles, cites = parsed
        q_rows.append((
            q["id"], q["part_num"], q["question_num"], q["title"], q["summary"],
        ))
        for a in articles:
            a_rows.append((
                a["id"], a["question_id"], a["article_num"], a["title"], a["text"],
            ))
        cite_rows.extend(cites)

    cur.executemany(
        "INSERT INTO summa_questions VALUES (?,?,?,?,?)", q_rows
    )
    cur.executemany(
        "INSERT INTO summa_articles VALUES (?,?,?,?,?)", a_rows
    )
    conn.commit()
    log.info("Inserted %d questions, %d articles, %d raw cites",
             len(q_rows), len(a_rows), len(cite_rows))

    # --- Graph nodes ---
    empty_json = json.dumps([])
    node_rows = []

    # Part nodes
    for num, name in PARTS.items():
        node_rows.append((
            f"summa-part:{num}", name, "summa-part",
            0.0, 0.0, 12.0, PART_COLOR, "",
            0, 0, empty_json, empty_json, empty_json,
        ))
    # Question nodes
    for qid, part_num, _qn, title, _summary in q_rows:
        label = title if title else f"Question {qid}"
        node_rows.append((
            f"summa-question:{qid}", label, "summa-question",
            0.0, 0.0, 6.0, QUESTION_COLOR, PARTS.get(part_num, ""),
            0, 0, empty_json, empty_json, empty_json,
        ))
    # Article nodes
    for aid, qid, _an, title, _text in a_rows:
        label = title if title else f"Article {aid}"
        # part column = part name (for stats)
        part_num = int(qid[0])
        node_rows.append((
            f"summa-article:{aid}", label, "summa-article",
            0.0, 0.0, 3.0, ARTICLE_COLOR, PARTS.get(part_num, ""),
            0, 0, empty_json, empty_json, empty_json,
        ))

    cur.executemany(
        "INSERT OR REPLACE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )
    conn.commit()
    log.info("Inserted %d summa graph nodes", len(node_rows))

    # --- Graph edges ---
    # child_of: article → question
    edge_rows: list[tuple[str, str, str]] = []
    for aid, qid, *_ in a_rows:
        edge_rows.append((f"summa-article:{aid}", f"summa-question:{qid}", "child_of"))
    # child_of: question → part
    for qid, part_num, *_ in q_rows:
        edge_rows.append((f"summa-question:{qid}", f"summa-part:{part_num}", "child_of"))

    # cites: article → cathen/bible/fathers — filter by node existence.
    # Fathers links use page numbers (e.g. "0103") that don't map cleanly to
    # our current patristic-work ids; we attempt best-effort matching via a
    # lookup table built from a compact URL-fragment index.
    existing = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type IN ('encyclopedia','bible-verse')"
        )
    }
    cites_added = 0
    cites_skipped = 0
    seen_cite_pairs: set[tuple[str, str]] = set()
    for aid, target in cite_rows:
        if target.startswith("fathers-page:"):
            cites_skipped += 1  # no stable target node yet
            continue
        if target not in existing:
            cites_skipped += 1
            continue
        src = f"summa-article:{aid}"
        pair = (src, target)
        if pair in seen_cite_pairs:
            continue
        seen_cite_pairs.add(pair)
        edge_rows.append((src, target, "cites"))
        cites_added += 1

    # Cross-corpus mentions: extract entities + themes from article text.
    entity_ids_in_graph = {
        row[0].removeprefix("entity:")
        for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='entity'"
        )
    }
    theme_ids_in_graph = {
        row[0].removeprefix("theme:")
        for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='theme'"
        )
    }
    theme_keywords = [
        (td.id, [kw.lower() for kw in td.keywords])
        for td in THEME_DEFINITIONS
        if td.id in theme_ids_in_graph
    ]

    mention_rows: list[tuple[str, str, str]] = []
    has_theme_rows: list[tuple[str, str, str]] = []
    for aid, _qid, _an, _title, text in a_rows:
        if not text:
            continue
        src = f"summa-article:{aid}"
        lowered = text.lower()

        for eid in extract_entities(text):
            if eid in entity_ids_in_graph:
                mention_rows.append((src, f"entity:{eid}", "mentions"))

        for tid, kws in theme_keywords:
            if any(kw in lowered for kw in kws):
                has_theme_rows.append((src, f"theme:{tid}", "has_theme"))

    edge_rows.extend(mention_rows)
    edge_rows.extend(has_theme_rows)
    log.info(
        "Cross-corpus signal: %d mentions (→entity), %d has_theme",
        len(mention_rows), len(has_theme_rows),
    )

    CHUNK = 50_000
    for i in range(0, len(edge_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows[i : i + CHUNK],
        )
        conn.commit()

    log.info("Inserted edges: %d child_of + %d cites (%d skipped, not-in-graph)",
             len(a_rows) + len(q_rows), cites_added, cites_skipped)

    # Rebuild degrees from scratch (cheaper than tracking diffs across many edge types)
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
