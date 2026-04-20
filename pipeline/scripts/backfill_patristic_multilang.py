"""Backfill patristic_sections.text_la / text_el for patristic works.

Reconstructs the in-memory PatristicWork tree from the DB, invokes
fetch_patristic_latin and fetch_patristic_greek (which merge in-place and
may append new works), then writes the tree back — UPDATE for existing
section IDs, INSERT for anything new (authors, works, chapters, sections).
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.fetch_patristic_greek import fetch_patristic_greek  # noqa: E402
from pipeline.src.fetch_patristic_latin import fetch_patristic_latin  # noqa: E402
from pipeline.src.models import (  # noqa: E402
    PatristicChapter,
    PatristicSection,
    PatristicWork,
)

DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

logger = logging.getLogger(__name__)

DEFAULT_ERAS = {
    # Latin-only authors (subset not in base `authors` table yet).
    "arnobius": "c. 255-330 AD",
    "commodianus": "3rd century AD",
    "lactantius": "c. 250-325 AD",
    "novatian": "c. 200-258 AD",
    "benedict": "c. 480-547 AD",
    "cassiodorus": "c. 485-585 AD",
    "eucherius": "c. 380-449 AD",
    "macarius-alexandria": "c. 300-395 AD",
    "vincent-lerins": "d. c. 445 AD",
    "bede": "c. 672-735 AD",
    "bernard-clairvaux": "1090-1153 AD",
    "hugo-st-victor": "c. 1096-1141 AD",
    "innocent-iii": "1160-1216 AD",
    "isidore-seville": "c. 560-636 AD",
}

DEFAULT_NAMES = {
    "arnobius": "Arnobius of Sicca",
    "commodianus": "Commodianus",
    "lactantius": "Lactantius",
    "novatian": "Novatian",
    "benedict": "St. Benedict of Nursia",
    "cassiodorus": "Cassiodorus",
    "eucherius": "St. Eucherius of Lyon",
    "macarius-alexandria": "St. Macarius of Alexandria",
    "vincent-lerins": "St. Vincent of Lérins",
    "bede": "St. Bede the Venerable",
    "bernard-clairvaux": "St. Bernard of Clairvaux",
    "hugo-st-victor": "Hugh of St. Victor",
    "innocent-iii": "Pope Innocent III",
    "isidore-seville": "St. Isidore of Seville",
}


def _load_tree(conn: sqlite3.Connection) -> dict[str, list[PatristicWork]]:
    """Load all author_works/chapters/sections from the DB into the
    nested tree shape fetch_patristic_latin / _greek expect."""
    cur = conn.cursor()

    cur.execute("SELECT id, author_id, title, source_url FROM author_works")
    work_rows = cur.fetchall()

    cur.execute("SELECT id, work_id, number, title FROM patristic_chapters")
    chapter_rows = cur.fetchall()

    cur.execute(
        "SELECT id, chapter_id, number, text_en, text_la, text_el "
        "FROM patristic_sections"
    )
    section_rows = cur.fetchall()

    sections_by_chapter: dict[str, list[PatristicSection]] = defaultdict(list)
    for sid, chapter_id, number, text_en, text_la, text_el in section_rows:
        text: dict[str, str] = {}
        if text_en:
            text["en"] = text_en
        if text_la:
            text["la"] = text_la
        if text_el:
            text["el"] = text_el
        sections_by_chapter[chapter_id].append(
            PatristicSection(
                id=sid,
                chapter_id=chapter_id,
                number=number or 0,
                text=text,
            )
        )

    chapters_by_work: dict[str, list[PatristicChapter]] = defaultdict(list)
    for cid, work_id, number, title in chapter_rows:
        chapters_by_work[work_id].append(
            PatristicChapter(
                id=cid,
                work_id=work_id,
                number=number or 0,
                title=title or "",
                sections=sections_by_chapter.get(cid, []),
            )
        )

    # Order chapters by number for deterministic merge behavior
    for chapters in chapters_by_work.values():
        chapters.sort(key=lambda c: (c.number, c.id))

    works_by_author: dict[str, list[PatristicWork]] = defaultdict(list)
    for wid, author_id, title, source_url in work_rows:
        works_by_author[author_id].append(
            PatristicWork(
                id=wid,
                author_id=author_id,
                title=title or "",
                source_url=source_url or "",
                chapters=chapters_by_work.get(wid, []),
            )
        )

    return dict(works_by_author)


def _snapshot_section_ids(tree: dict[str, list[PatristicWork]]) -> set[str]:
    return {
        sec.id
        for works in tree.values()
        for work in works
        for chap in work.chapters
        for sec in chap.sections
    }


def _ensure_authors(conn: sqlite3.Connection, author_ids: set[str]) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM authors")
    existing = {row[0] for row in cur.fetchall()}
    to_add = author_ids - existing
    inserted = 0
    for aid in to_add:
        cur.execute(
            "INSERT INTO authors (id, name, era, citing_paragraphs_json, work_count) "
            "VALUES (?, ?, ?, '[]', 0)",
            (aid, DEFAULT_NAMES.get(aid, aid.replace("-", " ").title()),
             DEFAULT_ERAS.get(aid, "")),
        )
        inserted += 1
    return inserted


def _write_tree(
    conn: sqlite3.Connection,
    tree: dict[str, list[PatristicWork]],
    preexisting_section_ids: set[str],
) -> dict[str, int]:
    """UPSERT every work/chapter/section in the tree. Returns counters."""
    cur = conn.cursor()

    cur.execute("SELECT id FROM author_works")
    existing_work_ids = {row[0] for row in cur.fetchall()}
    cur.execute("SELECT id FROM patristic_chapters")
    existing_chapter_ids = {row[0] for row in cur.fetchall()}

    stats = {
        "authors_added": 0,
        "works_added": 0,
        "chapters_added": 0,
        "sections_added": 0,
        "sections_updated": 0,
    }

    # Make sure every author_id referenced in the tree exists.
    author_ids = set(tree.keys())
    for works in tree.values():
        for w in works:
            author_ids.add(w.author_id)
    stats["authors_added"] = _ensure_authors(conn, author_ids)

    for author_id, works in tree.items():
        for work in works:
            if work.id in existing_work_ids:
                cur.execute(
                    "UPDATE author_works SET author_id = ?, title = ?, "
                    "source_url = ?, chapter_count = ? WHERE id = ?",
                    (author_id, work.title, work.source_url or None,
                     len(work.chapters), work.id),
                )
            else:
                cur.execute(
                    "INSERT INTO author_works (id, author_id, title, "
                    "source_url, chapter_count) VALUES (?, ?, ?, ?, ?)",
                    (work.id, author_id, work.title,
                     work.source_url or None, len(work.chapters)),
                )
                existing_work_ids.add(work.id)
                stats["works_added"] += 1

            for chap in work.chapters:
                if chap.id in existing_chapter_ids:
                    cur.execute(
                        "UPDATE patristic_chapters SET work_id = ?, number = ?, "
                        "title = ? WHERE id = ?",
                        (chap.work_id, chap.number, chap.title, chap.id),
                    )
                else:
                    cur.execute(
                        "INSERT INTO patristic_chapters (id, work_id, number, title) "
                        "VALUES (?, ?, ?, ?)",
                        (chap.id, chap.work_id, chap.number, chap.title),
                    )
                    existing_chapter_ids.add(chap.id)
                    stats["chapters_added"] += 1

                for sec in chap.sections:
                    text_en = sec.text.get("en")
                    text_la = sec.text.get("la")
                    text_el = sec.text.get("el")
                    if sec.id in preexisting_section_ids:
                        # Only set columns we actually have values for, so we
                        # don't clobber pre-existing language columns with NULL.
                        fields = []
                        vals: list[object] = []
                        if text_en is not None:
                            fields.append("text_en = ?")
                            vals.append(text_en)
                        if text_la is not None:
                            fields.append("text_la = ?")
                            vals.append(text_la)
                        if text_el is not None:
                            fields.append("text_el = ?")
                            vals.append(text_el)
                        if fields:
                            vals.append(sec.id)
                            cur.execute(
                                f"UPDATE patristic_sections SET {', '.join(fields)} "
                                f"WHERE id = ?",
                                vals,
                            )
                            stats["sections_updated"] += 1
                    else:
                        cur.execute(
                            "INSERT INTO patristic_sections "
                            "(id, chapter_id, number, text_en, text_la, text_el) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (sec.id, sec.chapter_id, sec.number,
                             text_en, text_la, text_el),
                        )
                        stats["sections_added"] += 1

    return stats


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        logger.info("Loading patristic tree from DB...")
        tree = _load_tree(conn)
        logger.info(
            "Loaded: %d authors, %d works, %d chapters, %d sections",
            len(tree),
            sum(len(ws) for ws in tree.values()),
            sum(len(w.chapters) for ws in tree.values() for w in ws),
            sum(len(c.sections) for ws in tree.values() for w in ws for c in w.chapters),
        )

        preexisting = _snapshot_section_ids(tree)

        logger.info("=== Running fetch_patristic_latin ===")
        tree = fetch_patristic_latin(tree)
        logger.info("=== Running fetch_patristic_greek ===")
        tree = fetch_patristic_greek(tree)

        la_count = sum(
            1
            for ws in tree.values() for w in ws for c in w.chapters for s in c.sections
            if s.text.get("la")
        )
        el_count = sum(
            1
            for ws in tree.values() for w in ws for c in w.chapters for s in c.sections
            if s.text.get("el")
        )
        logger.info(
            "Tree after fetch: sections with LA=%d, EL=%d", la_count, el_count,
        )

        logger.info("Writing tree back to DB...")
        stats = _write_tree(conn, tree, preexisting)

        # Recompute authors.work_count
        conn.execute(
            "UPDATE authors SET work_count = ("
            "SELECT COUNT(*) FROM author_works aw WHERE aw.author_id = authors.id"
            ")"
        )
        conn.commit()

        logger.info(
            "Write stats: authors_added=%(authors_added)d works_added=%(works_added)d "
            "chapters_added=%(chapters_added)d sections_added=%(sections_added)d "
            "sections_updated=%(sections_updated)d",
            stats,
        )

        cur = conn.cursor()
        cur.execute(
            "SELECT "
            "SUM(CASE WHEN text_la IS NOT NULL AND text_la<>'' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN text_el IS NOT NULL AND text_el<>'' THEN 1 ELSE 0 END), "
            "COUNT(*) FROM patristic_sections"
        )
        la, el, total = cur.fetchone()
        logger.info(
            "Post-backfill DB: %d sections total, text_la=%d, text_el=%d",
            total, la, el,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
