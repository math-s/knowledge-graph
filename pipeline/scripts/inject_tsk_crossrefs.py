"""Fetch OpenBible.info cross-references (TSK) and inject verse-to-verse edges.

Downloads the canonical OpenBible.info cross-reference ZIP (~2 MB, ~340k
references), parses its TSV, and inserts ``bible_cross_reference`` edges
between existing bible-verse nodes. Cached locally on first run.

Usage:
    uv run --project pipeline python -m pipeline.scripts.inject_tsk_crossrefs
"""

from __future__ import annotations

import io
import logging
import re
import sqlite3
import sys
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
CACHE = PROJECT_ROOT / "pipeline" / "data" / "raw" / "openbible-cross-references.tsv"
URL = "https://a.openbible.info/data/cross-references.zip"

# OSIS book abbreviations → canonical verse-node slugs used in graph_nodes
OSIS_TO_SLUG: dict[str, str] = {
    "Gen": "genesis", "Exod": "exodus", "Lev": "leviticus",
    "Num": "numbers", "Deut": "deuteronomy", "Josh": "joshua",
    "Judg": "judges", "Ruth": "ruth", "1Sam": "1-samuel",
    "2Sam": "2-samuel", "1Kgs": "1-kings", "2Kgs": "2-kings",
    "1Chr": "1-chronicles", "2Chr": "2-chronicles",
    "Ezra": "ezra", "Neh": "nehemiah", "Esth": "esther",
    "Job": "job", "Ps": "psalms",
    "Prov": "proverbs", "Eccl": "ecclesiastes",
    "Song": "song-of-solomon",
    "Isa": "isaiah", "Jer": "jeremiah", "Lam": "lamentations",
    "Ezek": "ezekiel", "Dan": "daniel", "Hos": "hosea",
    "Joel": "joel", "Amos": "amos", "Obad": "obadiah", "Jonah": "jonah",
    "Mic": "micah", "Nah": "nahum", "Hab": "habakkuk",
    "Zeph": "zephaniah", "Hag": "haggai", "Zech": "zechariah",
    "Mal": "malachi",
    "Matt": "matthew", "Mark": "mark", "Luke": "luke", "John": "john",
    "Acts": "acts", "Rom": "romans",
    "1Cor": "1-corinthians", "2Cor": "2-corinthians",
    "Gal": "galatians", "Eph": "ephesians",
    "Phil": "philippians", "Col": "colossians",
    "1Thess": "1-thessalonians", "2Thess": "2-thessalonians",
    "1Tim": "1-timothy", "2Tim": "2-timothy", "Titus": "titus",
    "Phlm": "philemon", "Heb": "hebrews", "Jas": "james",
    "1Pet": "1-peter", "2Pet": "2-peter",
    "1John": "1-john", "2John": "2-john", "3John": "3-john",
    "Jude": "jude", "Rev": "revelation",
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("inject_tsk")

_osis_re = re.compile(r"^([1-3]?[A-Za-z]+)\.(\d+)\.(\d+)$")


def fetch_tsv() -> str:
    if CACHE.exists():
        log.info("Using cached TSV: %s", CACHE)
        return CACHE.read_text(encoding="utf-8")
    log.info("Downloading TSK data from %s", URL)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(URL, timeout=120) as resp:
        raw = resp.read()
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        # Pick the largest .txt file in the archive
        names = [n for n in zf.namelist() if n.endswith(".txt")]
        if not names:
            raise SystemExit(f"No .txt in zip: {zf.namelist()}")
        name = max(names, key=lambda n: zf.getinfo(n).file_size)
        log.info("Extracting %s", name)
        tsv = zf.read(name).decode("utf-8")
    CACHE.write_text(tsv, encoding="utf-8")
    return tsv


def osis_to_slug(ref: str) -> str | None:
    """Convert 'Gen.1.1' → 'genesis-1:1', or 'Gen.1.1-Gen.1.3' → None (drop ranges)."""
    if "-" in ref:
        ref = ref.split("-", 1)[0]
    m = _osis_re.match(ref.strip())
    if not m:
        return None
    book = OSIS_TO_SLUG.get(m.group(1))
    if not book:
        return None
    return f"{book}-{int(m.group(2))}:{int(m.group(3))}"


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    tsv = fetch_tsv()

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    verse_ids = {
        row[0] for row in conn.execute(
            "SELECT id FROM graph_nodes WHERE node_type='bible-verse'"
        )
    }
    log.info("Graph has %d bible-verse nodes", len(verse_ids))

    edge_set: set[tuple[str, str]] = set()
    parsed_pairs = 0
    skipped_unparseable = 0
    skipped_missing = 0

    for raw_line in tsv.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("From"):
            continue
        cols = line.split("\t")
        if len(cols) < 2:
            continue
        src_slug = osis_to_slug(cols[0])
        tgt_slug = osis_to_slug(cols[1])
        if not src_slug or not tgt_slug:
            skipped_unparseable += 1
            continue
        parsed_pairs += 1
        src = f"bible-verse:{src_slug}"
        tgt = f"bible-verse:{tgt_slug}"
        if src not in verse_ids or tgt not in verse_ids:
            skipped_missing += 1
            continue
        if src == tgt:
            continue
        a, b = (src, tgt) if src < tgt else (tgt, src)
        edge_set.add((a, b))

    log.info(
        "Parsed %d verse pairs, %d unique edges "
        "(skipped %d unparseable, %d not-in-graph)",
        parsed_pairs, len(edge_set), skipped_unparseable, skipped_missing,
    )

    edge_rows = [(a, b, "bible_cross_reference") for a, b in edge_set]

    before = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='bible_cross_reference'"
    ).fetchone()[0]
    CHUNK = 50_000
    for i in range(0, len(edge_rows), CHUNK):
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows[i : i + CHUNK],
        )
        conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE edge_type='bible_cross_reference'"
    ).fetchone()[0]
    added = after - before

    deg: dict[str, int] = defaultdict(int)
    for a, b in edge_set:
        deg[a] += 1
        deg[b] += 1
    deg_items = list(deg.items())
    for i in range(0, len(deg_items), CHUNK):
        cur.executemany(
            "UPDATE graph_nodes SET degree = degree + ? WHERE id = ?",
            [(inc, nid) for nid, inc in deg_items[i : i + CHUNK]],
        )
        conn.commit()

    total_nodes = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    conn.close()

    log.info("")
    log.info("Inserted %d new bible_cross_reference edges", added)
    log.info("DB now has %d nodes, %d edges", total_nodes, total_edges)


if __name__ == "__main__":
    main()
