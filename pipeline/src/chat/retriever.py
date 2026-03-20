"""Retrieve data from the CCC knowledge graph SQLite database.

Provides search, paragraph lookup, theme browsing, citation expansion,
and graph traversal — all returning structured data for LLM consumption.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Paragraph:
    id: int
    text: str
    part: str
    section: str
    chapter: str
    article: str
    themes: list[str]
    entities: list[str] = field(default_factory=list)
    footnotes: str = ""
    source: str = "direct"  # "direct", "cross-ref", "theme", "entity"

    @property
    def location(self) -> str:
        return " > ".join(p for p in [self.part, self.section, self.chapter, self.article] if p)


@dataclass
class BibleCitation:
    book: str
    reference: str
    text: str = ""


@dataclass
class DocumentCitation:
    document_id: str
    section_num: str
    text: str = ""


@dataclass
class PatristicText:
    id: str
    chapter_id: str
    text: str


# ---------------------------------------------------------------------------
# FTS helpers
# ---------------------------------------------------------------------------

_STOP_WORDS = frozenset({
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "must", "can", "could", "am", "it", "its",
    "he", "she", "we", "they", "you", "me", "him", "her", "us", "them",
    "my", "your", "his", "our", "their", "this", "that", "these", "those",
    "what", "which", "who", "whom", "when", "where", "why", "how",
    "if", "then", "than", "but", "and", "or", "not", "no", "nor",
    "of", "in", "on", "at", "to", "for", "with", "by", "from", "as",
    "about", "into", "through", "during", "before", "after", "above",
    "below", "between", "under", "again", "further", "once", "here",
    "there", "all", "each", "every", "both", "few", "more", "most",
    "other", "some", "such", "only", "own", "same", "so", "very",
    "just", "because", "until", "while", "also", "does",
})


def sanitize_fts(query: str) -> str:
    """Sanitize a user query for FTS5 MATCH syntax.

    Strips FTS5 operators, removes stop words, returns cleaned query.
    """
    cleaned = re.sub(r'[*?"(){}^~:+\-]', " ", query)
    words = [w for w in cleaned.split() if len(w) >= 2 and w.lower() not in _STOP_WORDS]
    if not words:
        return '""'
    return " ".join(words)


# ---------------------------------------------------------------------------
# Retriever
# ---------------------------------------------------------------------------

class Retriever:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _parse_paragraph(self, row: sqlite3.Row, source: str = "direct") -> Paragraph:
        themes = json.loads(row["themes_json"]) if row["themes_json"] else []
        return Paragraph(
            id=row["id"],
            text=row["text_en"] or "",
            part=row["part"] or "",
            section=row["section"] or "",
            chapter=row["chapter"] or "",
            article=row["article"] or "",
            themes=themes,
            footnotes=row["footnotes_json"] or "",
            source=source,
        )

    # -- Search --------------------------------------------------------------

    def search(self, query: str, limit: int = 10) -> list[Paragraph]:
        """Full-text search across CCC paragraphs."""
        fts_q = sanitize_fts(query)
        rows = self.conn.execute("""
            SELECT p.id, p.text_en, p.part, p.section, p.chapter, p.article,
                   p.themes_json, p.footnotes_json
            FROM search_fts s
            JOIN paragraphs p ON p.id = CAST(s.entry_id AS INTEGER)
            WHERE search_fts MATCH ?
              AND s.entry_type = 'paragraph'
            ORDER BY s.rank
            LIMIT ?
        """, (fts_q, limit)).fetchall()
        return [self._parse_paragraph(r) for r in rows]

    def search_bible(self, query: str, limit: int = 10) -> list[BibleCitation]:
        """Full-text search across Bible verses."""
        fts_q = sanitize_fts(query)
        rows = self.conn.execute("""
            SELECT bv.book_id, bv.chapter, bv.verse, bv.text_en
            FROM bible_verses_fts f
            JOIN bible_verses bv ON bv.rowid = f.rowid
            WHERE bible_verses_fts MATCH ?
            ORDER BY f.rank
            LIMIT ?
        """, (fts_q, limit)).fetchall()
        return [BibleCitation(
            book=r["book_id"],
            reference=f"{r['chapter']}:{r['verse']}",
            text=r["text_en"] or "",
        ) for r in rows]

    def search_patristic(self, query: str, limit: int = 10) -> list[PatristicText]:
        """Full-text search across patristic texts."""
        fts_q = sanitize_fts(query)
        rows = self.conn.execute("""
            SELECT ps.id, ps.chapter_id, ps.text_en
            FROM patristic_sections_fts f
            JOIN patristic_sections ps ON ps.rowid = f.rowid
            WHERE patristic_sections_fts MATCH ?
            ORDER BY f.rank
            LIMIT ?
        """, (fts_q, limit)).fetchall()
        return [PatristicText(
            id=r["id"], chapter_id=r["chapter_id"], text=r["text_en"] or "",
        ) for r in rows]

    # -- Paragraph lookup ----------------------------------------------------

    def get_paragraph(self, pid: int) -> Paragraph | None:
        """Get a single CCC paragraph by ID."""
        r = self.conn.execute("""
            SELECT id, text_en, part, section, chapter, article,
                   themes_json, footnotes_json
            FROM paragraphs WHERE id = ?
        """, (pid,)).fetchone()
        if not r:
            return None
        p = self._parse_paragraph(r)
        # Also fetch entities
        ents = self.conn.execute(
            "SELECT entity_id FROM paragraph_entities WHERE paragraph_id = ?",
            (pid,),
        ).fetchall()
        p.entities = [e["entity_id"] for e in ents]
        return p

    def get_paragraphs(self, pids: list[int]) -> list[Paragraph]:
        """Get multiple paragraphs by ID."""
        if not pids:
            return []
        placeholders = ",".join("?" for _ in pids)
        rows = self.conn.execute(f"""
            SELECT id, text_en, part, section, chapter, article,
                   themes_json, footnotes_json
            FROM paragraphs WHERE id IN ({placeholders})
            ORDER BY id
        """, pids).fetchall()
        return [self._parse_paragraph(r) for r in rows]

    # -- Cross references ----------------------------------------------------

    def get_cross_refs(self, pid: int) -> list[int]:
        """Get paragraph IDs cross-referenced by a given paragraph."""
        rows = self.conn.execute(
            "SELECT target_id FROM paragraph_cross_refs WHERE paragraph_id = ?",
            (pid,),
        ).fetchall()
        return [r["target_id"] for r in rows]

    def get_cross_ref_paragraphs(self, pid: int) -> list[Paragraph]:
        """Get full paragraphs cross-referenced by a given paragraph."""
        ref_ids = self.get_cross_refs(pid)
        if not ref_ids:
            return []
        paragraphs = self.get_paragraphs(ref_ids)
        for p in paragraphs:
            p.source = "cross-ref"
        return paragraphs

    # -- Citations -----------------------------------------------------------

    def get_bible_citations(self, pid: int) -> list[BibleCitation]:
        """Get Bible citations for a paragraph."""
        rows = self.conn.execute("""
            SELECT book, reference
            FROM paragraph_bible_citations
            WHERE paragraph_id = ?
        """, (pid,)).fetchall()
        return [BibleCitation(
            book=r["book"] or "",
            reference=r["reference"] or "",
        ) for r in rows]

    def get_document_citations(self, pid: int) -> list[DocumentCitation]:
        """Get ecclesiastical document citations for a paragraph."""
        rows = self.conn.execute("""
            SELECT pdc.document, pdc.section,
                   ds.text_en
            FROM paragraph_document_citations pdc
            LEFT JOIN document_sections ds
              ON ds.document_id = pdc.document
              AND ds.section_num = pdc.section
            WHERE pdc.paragraph_id = ?
        """, (pid,)).fetchall()
        return [DocumentCitation(
            document_id=r["document"] or "",
            section_num=r["section"] or "",
            text=r["text_en"] or "",
        ) for r in rows]

    # -- Theme browsing ------------------------------------------------------

    def get_themes(self) -> list[dict]:
        """List all themes with paragraph counts."""
        rows = self.conn.execute("""
            SELECT t.id, t.label, COUNT(pt.paragraph_id) as count
            FROM themes t
            LEFT JOIN paragraph_themes pt ON pt.theme_id = t.id
            GROUP BY t.id
            ORDER BY count DESC
        """).fetchall()
        return [{"id": r["id"], "label": r["label"], "count": r["count"]} for r in rows]

    def get_paragraphs_by_theme(self, theme_id: str, limit: int = 20) -> list[Paragraph]:
        """Get paragraphs belonging to a theme."""
        rows = self.conn.execute("""
            SELECT p.id, p.text_en, p.part, p.section, p.chapter, p.article,
                   p.themes_json, p.footnotes_json
            FROM paragraph_themes pt
            JOIN paragraphs p ON p.id = pt.paragraph_id
            WHERE pt.theme_id = ?
            ORDER BY p.id
            LIMIT ?
        """, (theme_id, limit)).fetchall()
        return [self._parse_paragraph(r, source="theme") for r in rows]

    # -- Entity browsing -----------------------------------------------------

    def get_entities(self) -> list[dict]:
        """List all entities with paragraph counts."""
        rows = self.conn.execute("""
            SELECT e.id, e.label, e.category, COUNT(pe.paragraph_id) as count
            FROM entities e
            LEFT JOIN paragraph_entities pe ON pe.entity_id = e.id
            GROUP BY e.id
            ORDER BY count DESC
        """).fetchall()
        return [{"id": r["id"], "label": r["label"],
                 "category": r["category"], "count": r["count"]} for r in rows]

    def get_paragraphs_by_entity(self, entity_id: str, limit: int = 20) -> list[Paragraph]:
        """Get paragraphs mentioning an entity."""
        rows = self.conn.execute("""
            SELECT p.id, p.text_en, p.part, p.section, p.chapter, p.article,
                   p.themes_json, p.footnotes_json
            FROM paragraph_entities pe
            JOIN paragraphs p ON p.id = pe.paragraph_id
            WHERE pe.entity_id = ?
            ORDER BY p.id
            LIMIT ?
        """, (entity_id, limit)).fetchall()
        return [self._parse_paragraph(r, source="entity") for r in rows]

    # -- Document lookup -----------------------------------------------------

    def get_document_section(self, doc_id: str, section: str) -> DocumentCitation | None:
        """Get a specific document section."""
        r = self.conn.execute("""
            SELECT document_id, section_num, text_en
            FROM document_sections
            WHERE document_id = ? AND section_num = ?
        """, (doc_id, section)).fetchone()
        if not r:
            return None
        return DocumentCitation(
            document_id=r["document_id"],
            section_num=r["section_num"],
            text=r["text_en"] or "",
        )

    def list_documents(self) -> list[dict]:
        """List all ecclesiastical documents."""
        rows = self.conn.execute("""
            SELECT d.id, d.name, d.abbreviation, d.category,
                   COUNT(ds.section_num) as section_count
            FROM documents d
            LEFT JOIN document_sections ds ON ds.document_id = d.id
            GROUP BY d.id
            ORDER BY d.name
        """).fetchall()
        return [{"id": r["id"], "name": r["name"],
                 "abbreviation": r["abbreviation"],
                 "category": r["category"],
                 "sections": r["section_count"]} for r in rows]
