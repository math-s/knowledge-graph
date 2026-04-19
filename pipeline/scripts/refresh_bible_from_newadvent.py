"""Rebuild bible_verses from the newadvent bible/ three-column HTML.

Each ``bible/XXXNNN.htm`` file contains one chapter, laid out in a 3-column
table:
  * bibletd1 = Greek  (Septuagint for OT, Byzantine Textform for NT)
  * bibletd2 = English (Douay-Rheims)
  * bibletd3 = Latin  (Vulgate)

Existing bible_verses has ~49k rows but is badly corrupted (Habakkuk spans
20+ chapters, etc.) — this script wipes it and rebuilds cleanly. Existing
text_pt values are preserved where the (book, ch, v) key still matches.

Also keeps bible-verse / bible-chapter / bible-book graph nodes in sync
with the refreshed verse set so no verse is orphaned.

Usage:
    uv run --project pipeline python -m pipeline.scripts.refresh_bible_from_newadvent
"""

from __future__ import annotations

import html
import json
import logging
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
BIBLE_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent" / "bible"

FILE_RE = re.compile(r"^([a-z0-9]+?)(\d{3})\.htm$", re.IGNORECASE)
CATHEN_HREF = re.compile(r'^(?:\.\./)?cathen/(\d+[a-z])\.htm', re.IGNORECASE)

BOOK_PREFIX_TO_ID: dict[str, str] = {
    # Pentateuch
    "gen": "genesis", "exo": "exodus", "lev": "leviticus",
    "num": "numbers", "deu": "deuteronomy",
    # Historical
    "jos": "joshua", "jdg": "judges", "rut": "ruth",
    "1sa": "1-samuel", "2sa": "2-samuel",
    "1ki": "1-kings", "2ki": "2-kings",
    "1ch": "1-chronicles", "2ch": "2-chronicles",
    "ezr": "ezra", "neh": "nehemiah",
    "tob": "tobit", "jth": "judith", "est": "esther",
    "1ma": "1-maccabees", "2ma": "2-maccabees",
    # Wisdom
    "job": "job", "psa": "psalms", "pro": "proverbs",
    "ecc": "ecclesiastes", "son": "song-of-solomon",
    "wis": "wisdom", "sir": "sirach",
    # Prophets
    "isa": "isaiah", "jer": "jeremiah", "lam": "lamentations",
    "bar": "baruch", "eze": "ezekiel", "dan": "daniel",
    "hos": "hosea", "joe": "joel", "amo": "amos", "oba": "obadiah",
    "jon": "jonah", "mic": "micah", "nah": "nahum", "hab": "habakkuk",
    "zep": "zephaniah", "hag": "haggai", "zec": "zechariah", "mal": "malachi",
    # Gospels + Acts
    "mat": "matthew", "mar": "mark", "luk": "luke", "joh": "john",
    "act": "acts",
    # Pauline
    "rom": "romans", "1co": "1-corinthians", "2co": "2-corinthians",
    "gal": "galatians", "eph": "ephesians",
    "phi": "philippians", "col": "colossians",
    "1th": "1-thessalonians", "2th": "2-thessalonians",
    "1ti": "1-timothy", "2ti": "2-timothy", "tit": "titus",
    "phm": "philemon", "heb": "hebrews",
    # Catholic epistles
    "jam": "james", "1pe": "1-peter", "2pe": "2-peter",
    "1jo": "1-john", "2jo": "2-john", "3jo": "3-john",
    "jud": "jude", "rev": "revelation",
}

BIBLE_VERSE_COLOR = "#4E79A7"
BIBLE_CHAPTER_COLOR = "#3F6785"
BIBLE_BOOK_COLOR = "#305776"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("refresh_bible")


def extract_verses(td: Tag, capture_cathen: bool = False) -> tuple[dict[int, str], dict[int, set[str]]]:
    """Walk a <td> extracting verse_number → text, plus (if requested) the
    set of cathen article ids linked from each verse. Skips footnotes."""
    verses: dict[int, str] = {}
    cathen_per_verse: dict[int, set[str]] = {}
    current_num: int | None = None
    buffer: list[str] = []

    def flush():
        if current_num is None:
            return
        txt = " ".join(buffer)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt:
            verses[current_num] = txt

    def walk(node: Tag, skip_stiki: bool = True):
        nonlocal current_num, buffer
        for child in node.children:
            if isinstance(child, NavigableString):
                buffer.append(str(child))
                continue
            if not isinstance(child, Tag):
                continue
            if skip_stiki and child.name == "span" and "stiki" in (child.get("class") or []):
                continue
            if child.name == "span" and "verse" in (child.get("class") or []):
                flush()
                try:
                    current_num = int(child.get_text(strip=True))
                except ValueError:
                    current_num = None
                buffer = []
                continue
            if child.name == "p" and "initial" in (child.get("class") or []):
                # Drop-cap opening paragraph marks the start of verse 1 even
                # without an explicit <span class="verse">1</span> marker.
                if current_num is None:
                    current_num = 1
                    buffer = []
                for img in child.find_all("img"):
                    alt = img.get("alt", "")
                    if alt:
                        buffer.append(alt)
                    img.decompose()
                walk(child)
                continue
            # Capture cathen cross-references
            if capture_cathen and child.name == "a" and child.get("href"):
                m = CATHEN_HREF.match(child["href"])
                if m and current_num is not None:
                    cathen_per_verse.setdefault(current_num, set()).add(m.group(1))
            walk(child)

    walk(td)
    flush()
    return verses, cathen_per_verse


def parse_chapter_file(
    path: Path,
) -> tuple[str, int, dict[str, dict[int, str]], dict[int, set[str]]]:
    """Return (book_prefix, chapter_num, {lang: {verse: text}}, {verse: {cathen_ids}})."""
    m = FILE_RE.match(path.name)
    if not m:
        return "", 0, {}, {}
    prefix = m.group(1).lower()
    chapter = int(m.group(2))

    html_text = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html_text, "html.parser")

    lang_by_col = {"bibletd1": "el", "bibletd2": "en", "bibletd3": "la"}
    out: dict[str, dict[int, str]] = {"el": {}, "en": {}, "la": {}}
    cathen_links: dict[int, set[str]] = {}

    for td in soup.find_all("td"):
        cls = td.get("class") or []
        for col_name, lang in lang_by_col.items():
            if col_name in cls:
                verses, links = extract_verses(td, capture_cathen=(lang == "en"))
                for v, t in verses.items():
                    out[lang][v] = t
                for v, ids in links.items():
                    cathen_links.setdefault(v, set()).update(ids)
                break

    return prefix, chapter, out, cathen_links


def main() -> None:
    if not BIBLE_DIR.exists():
        raise SystemExit(f"Bible dir not found: {BIBLE_DIR}")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    files: list[Path] = []
    for p in BIBLE_DIR.iterdir():
        m = FILE_RE.match(p.name)
        if not m:
            continue
        prefix = m.group(1).lower()
        if prefix not in BOOK_PREFIX_TO_ID:
            continue
        # Skip chapter 0 (these are title pages with no verses)
        if int(m.group(2)) == 0:
            continue
        files.append(p)
    files.sort()
    log.info("Found %d bible chapter files to parse", len(files))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    # Preserve existing Portuguese translations
    existing_pt: dict[tuple[str, int, int], str] = {}
    try:
        for row in conn.execute(
            "SELECT book_id, chapter, verse, text_pt FROM bible_verses WHERE text_pt IS NOT NULL AND text_pt != ''"
        ):
            existing_pt[(row[0], row[1], row[2])] = row[3]
        log.info("Preserved %d existing Portuguese verses", len(existing_pt))
    except sqlite3.OperationalError as e:
        log.warning("Could not read existing text_pt: %s", e)

    # Wipe old verses
    cur.execute("DELETE FROM bible_verses")
    conn.commit()

    new_rows: list[tuple] = []
    per_book = defaultdict(int)
    # (verse_node_id, cathen_id) pairs to be added as cites edges
    cathen_cites: list[tuple[str, str]] = []

    for i, path in enumerate(files):
        prefix, chapter, langs, cathen_links = parse_chapter_file(path)
        book_id = BOOK_PREFIX_TO_ID[prefix]
        all_verse_nums = set(langs["en"]) | set(langs["la"]) | set(langs["el"])
        for v in sorted(all_verse_nums):
            text_en = html.unescape(langs["en"].get(v, "")) or None
            text_la = html.unescape(langs["la"].get(v, "")) or None
            text_el = html.unescape(langs["el"].get(v, "")) or None
            text_pt = existing_pt.get((book_id, chapter, v))
            new_rows.append((book_id, chapter, v, text_en, text_la, text_pt, text_el))
            per_book[book_id] += 1
            verse_node = f"bible-verse:{book_id}-{chapter}:{v}"
            for aid in cathen_links.get(v, ()):
                cathen_cites.append((verse_node, f"ency:{aid}"))
        if (i + 1) % 200 == 0:
            log.info("  parsed %d / %d chapters", i + 1, len(files))

    cur.executemany(
        "INSERT OR REPLACE INTO bible_verses (book_id, chapter, verse, text_en, text_la, text_pt, text_el) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        new_rows,
    )
    conn.commit()
    log.info("Inserted %d verse rows across %d books", len(new_rows), len(per_book))

    # Rebuild FTS if present
    fts_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='bible_verses_fts'"
    ).fetchone()
    if fts_exists:
        log.info("Rebuilding bible_verses_fts...")
        cur.execute("INSERT INTO bible_verses_fts(bible_verses_fts) VALUES('rebuild')")
        conn.commit()

    # --- Sync graph nodes -----------------------------------------------------
    log.info("Syncing graph_nodes for bible-verse / bible-chapter / bible-book...")
    empty_json = json.dumps([])

    # Existing node ids
    existing_verses = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='bible-verse'"
        )
    }
    existing_chapters = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='bible-chapter'"
        )
    }
    existing_books = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='bible-book'"
        )
    }

    needed_verses: set[str] = set()
    needed_chapters: set[str] = set()
    needed_books: set[str] = set()
    chapter_parent: dict[str, str] = {}  # chapter_node -> book_node
    verse_parent: dict[str, str] = {}    # verse_node  -> chapter_node

    for book_id, chapter, v, *_ in new_rows:
        verse_id = f"bible-verse:{book_id}-{chapter}:{v}"
        chapter_id = f"bible-chapter:{book_id}-{chapter}"
        book_node_id = f"bible-book:{book_id}"
        needed_verses.add(verse_id)
        needed_chapters.add(chapter_id)
        needed_books.add(book_node_id)
        verse_parent[verse_id] = chapter_id
        chapter_parent[chapter_id] = book_node_id

    # Insert missing nodes
    new_verse_nodes = []
    for nid in needed_verses - existing_verses:
        # label: chapter:verse
        _, tail = nid.split(":", 1)  # tail = "<book>-<ch>:<v>"
        new_verse_nodes.append((
            nid, tail, "bible-verse",
            0.0, 0.0, 1.0, BIBLE_VERSE_COLOR, "",
            0, 0, empty_json, empty_json, empty_json,
        ))
    new_chapter_nodes = []
    for nid in needed_chapters - existing_chapters:
        _, tail = nid.split(":", 1)
        new_chapter_nodes.append((
            nid, tail, "bible-chapter",
            0.0, 0.0, 3.0, BIBLE_CHAPTER_COLOR, "",
            0, 0, empty_json, empty_json, empty_json,
        ))
    new_book_nodes = []
    for nid in needed_books - existing_books:
        _, tail = nid.split(":", 1)
        label = tail.replace("-", " ").title()
        new_book_nodes.append((
            nid, label, "bible-book",
            0.0, 0.0, 6.0, BIBLE_BOOK_COLOR, "",
            0, 0, empty_json, empty_json, empty_json,
        ))
    for rows in (new_book_nodes, new_chapter_nodes, new_verse_nodes):
        if rows:
            cur.executemany(
                "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
    conn.commit()
    log.info(
        "Added nodes: %d bible-verse, %d bible-chapter, %d bible-book",
        len(new_verse_nodes), len(new_chapter_nodes), len(new_book_nodes),
    )

    # Delete orphan verse nodes (verses no longer in bible_verses)
    orphan_verses = existing_verses - needed_verses
    orphan_chapters = existing_chapters - needed_chapters
    if orphan_verses:
        cur.executemany(
            "DELETE FROM graph_edges WHERE source = ? OR target = ?",
            [(nid, nid) for nid in orphan_verses],
        )
        cur.executemany(
            "DELETE FROM graph_nodes WHERE id = ?",
            [(nid,) for nid in orphan_verses],
        )
        conn.commit()
        log.info("Removed %d orphan bible-verse nodes", len(orphan_verses))
    if orphan_chapters:
        cur.executemany(
            "DELETE FROM graph_edges WHERE source = ? OR target = ?",
            [(nid, nid) for nid in orphan_chapters],
        )
        cur.executemany(
            "DELETE FROM graph_nodes WHERE id = ?",
            [(nid,) for nid in orphan_chapters],
        )
        conn.commit()
        log.info("Removed %d orphan bible-chapter nodes", len(orphan_chapters))

    # Add missing child_of hierarchy edges
    hierarchy_edges: list[tuple[str, str, str]] = []
    for v, p in verse_parent.items():
        hierarchy_edges.append((v, p, "child_of"))
    for c, p in chapter_parent.items():
        hierarchy_edges.append((c, p, "child_of"))
    CHUNK = 50_000
    for i in range(0, len(hierarchy_edges), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            hierarchy_edges[i : i + CHUNK],
        )
    conn.commit()
    log.info("Ensured %d child_of edges in bible hierarchy", len(hierarchy_edges))

    # Inject bible-verse → encyclopedia cites (from <a href="../cathen/..."> links
    # embedded in the DR English column). Deduplicate and filter to existing ency nodes.
    ency_ids = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='encyclopedia'"
        )
    }
    seen_cite: set[tuple[str, str]] = set()
    cite_rows: list[tuple[str, str, str]] = []
    for verse_node, ency_node in cathen_cites:
        if ency_node not in ency_ids:
            continue
        pair = (verse_node, ency_node)
        if pair in seen_cite:
            continue
        seen_cite.add(pair)
        cite_rows.append((verse_node, ency_node, "cites"))
    for i in range(0, len(cite_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            cite_rows[i : i + CHUNK],
        )
    conn.commit()
    log.info("Added %d bible-verse → encyclopedia cites edges", len(cite_rows))

    # Recompute degrees
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

    total_verses = conn.execute("SELECT COUNT(*) FROM bible_verses").fetchone()[0]
    total_verses_with_en = conn.execute("SELECT COUNT(*) FROM bible_verses WHERE text_en IS NOT NULL").fetchone()[0]
    total_verses_with_la = conn.execute("SELECT COUNT(*) FROM bible_verses WHERE text_la IS NOT NULL").fetchone()[0]
    total_verses_with_el = conn.execute("SELECT COUNT(*) FROM bible_verses WHERE text_el IS NOT NULL").fetchone()[0]
    total_verses_with_pt = conn.execute("SELECT COUNT(*) FROM bible_verses WHERE text_pt IS NOT NULL").fetchone()[0]
    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    avg_deg = conn.execute("SELECT AVG(degree) FROM graph_nodes").fetchone()[0]
    conn.close()

    log.info("")
    log.info("bible_verses: %d rows (en=%d, la=%d, el=%d, pt=%d)",
             total_verses, total_verses_with_en, total_verses_with_la,
             total_verses_with_el, total_verses_with_pt)
    log.info("DB now has %d nodes, %d edges (avg degree %.1f)",
             total_nodes, total_edges, avg_deg)


if __name__ == "__main__":
    main()
