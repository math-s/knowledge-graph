"""Export pipeline data to a SQLite database for the API server.

Produces a single knowledge-graph.db file with all tables, indexes,
and FTS5 full-text search indexes needed by the read-only API.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

import networkx as nx

from .models import (
    AuthorSource,
    BibleBookFull,
    BibleBookSource,
    DocumentSource,
    Paragraph,
    PatristicWork,
    resolve_lang,
)
from .themes import THEME_DEFINITIONS

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """\
-- Core CCC paragraphs
CREATE TABLE paragraphs (
    id          INTEGER PRIMARY KEY,
    text_en     TEXT,
    text_la     TEXT,
    text_pt     TEXT,
    part        TEXT,
    section     TEXT,
    chapter     TEXT,
    article     TEXT,
    themes_json TEXT,
    footnotes_json TEXT
);

CREATE TABLE paragraph_cross_refs (
    paragraph_id INTEGER NOT NULL,
    target_id    INTEGER NOT NULL,
    PRIMARY KEY (paragraph_id, target_id)
);

CREATE TABLE paragraph_themes (
    paragraph_id INTEGER NOT NULL,
    theme_id     TEXT    NOT NULL,
    PRIMARY KEY (paragraph_id, theme_id)
);
CREATE INDEX idx_paragraph_themes_theme ON paragraph_themes(theme_id);

CREATE TABLE paragraph_entities (
    paragraph_id INTEGER NOT NULL,
    entity_id    TEXT    NOT NULL,
    PRIMARY KEY (paragraph_id, entity_id)
);
CREATE INDEX idx_paragraph_entities_entity ON paragraph_entities(entity_id);

CREATE TABLE paragraph_topics (
    paragraph_id INTEGER NOT NULL,
    topic_id     INTEGER NOT NULL,
    PRIMARY KEY (paragraph_id, topic_id)
);
CREATE INDEX idx_paragraph_topics_topic ON paragraph_topics(topic_id);

CREATE TABLE paragraph_bible_citations (
    paragraph_id INTEGER NOT NULL,
    book         TEXT    NOT NULL,
    reference    TEXT
);
CREATE INDEX idx_pbc_paragraph ON paragraph_bible_citations(paragraph_id);
CREATE INDEX idx_pbc_book      ON paragraph_bible_citations(book);

CREATE TABLE paragraph_document_citations (
    paragraph_id INTEGER NOT NULL,
    document     TEXT    NOT NULL,
    section      TEXT
);
CREATE INDEX idx_pdc_paragraph ON paragraph_document_citations(paragraph_id);
CREATE INDEX idx_pdc_document  ON paragraph_document_citations(document);

CREATE TABLE paragraph_author_citations (
    paragraph_id INTEGER NOT NULL,
    author       TEXT    NOT NULL
);
CREATE INDEX idx_pac_paragraph ON paragraph_author_citations(paragraph_id);
CREATE INDEX idx_pac_author    ON paragraph_author_citations(author);

-- Graph (pre-computed layout)
CREATE TABLE graph_nodes (
    id            TEXT PRIMARY KEY,
    label         TEXT,
    node_type     TEXT,
    x             REAL,
    y             REAL,
    size          REAL,
    color         TEXT,
    part          TEXT,
    degree        INTEGER,
    community     INTEGER,
    themes_json   TEXT,
    entities_json TEXT,
    topics_json   TEXT
);
CREATE INDEX idx_graph_nodes_type      ON graph_nodes(node_type);
CREATE INDEX idx_graph_nodes_community ON graph_nodes(community);

CREATE TABLE graph_edges (
    source    TEXT NOT NULL,
    target    TEXT NOT NULL,
    edge_type TEXT NOT NULL
);
CREATE INDEX idx_graph_edges_source    ON graph_edges(source);
CREATE INDEX idx_graph_edges_target    ON graph_edges(target);
CREATE INDEX idx_graph_edges_type      ON graph_edges(edge_type);

-- Bible
CREATE TABLE bible_books (
    id                     TEXT PRIMARY KEY,
    name                   TEXT,
    abbreviation           TEXT,
    testament              TEXT,
    category               TEXT,
    total_verses           INTEGER,
    total_chapters         INTEGER,
    citing_paragraphs_json TEXT
);

CREATE TABLE bible_verses (
    book_id TEXT    NOT NULL,
    chapter INTEGER NOT NULL,
    verse   INTEGER NOT NULL,
    text_en TEXT,
    text_la TEXT,
    text_pt TEXT,
    text_el TEXT,
    PRIMARY KEY (book_id, chapter, verse)
);

-- Documents
CREATE TABLE documents (
    id                     TEXT PRIMARY KEY,
    name                   TEXT,
    abbreviation           TEXT,
    category               TEXT,
    source_url             TEXT,
    fetchable              INTEGER,
    citing_paragraphs_json TEXT,
    section_count          INTEGER,
    available_langs_json   TEXT
);

CREATE TABLE document_sections (
    document_id TEXT NOT NULL,
    section_num TEXT NOT NULL,
    text_en     TEXT,
    text_la     TEXT,
    text_pt     TEXT,
    PRIMARY KEY (document_id, section_num)
);

-- Authors & patristic works
CREATE TABLE authors (
    id                     TEXT PRIMARY KEY,
    name                   TEXT,
    era                    TEXT,
    citing_paragraphs_json TEXT,
    work_count             INTEGER
);

CREATE TABLE author_works (
    id            TEXT PRIMARY KEY,
    author_id     TEXT NOT NULL,
    title         TEXT,
    source_url    TEXT,
    chapter_count INTEGER
);
CREATE INDEX idx_author_works_author ON author_works(author_id);

CREATE TABLE patristic_chapters (
    id      TEXT PRIMARY KEY,
    work_id TEXT NOT NULL,
    number  INTEGER,
    title   TEXT
);
CREATE INDEX idx_patristic_chapters_work ON patristic_chapters(work_id);

CREATE TABLE patristic_sections (
    id         TEXT PRIMARY KEY,
    chapter_id TEXT NOT NULL,
    number     INTEGER,
    text_en    TEXT,
    text_la    TEXT,
    text_el    TEXT
);
CREATE INDEX idx_patristic_sections_chapter ON patristic_sections(chapter_id);

-- Metadata
CREATE TABLE themes (
    id    TEXT PRIMARY KEY,
    label TEXT,
    count INTEGER
);

CREATE TABLE entities (
    id       TEXT PRIMARY KEY,
    label    TEXT,
    category TEXT,
    count    INTEGER
);

CREATE TABLE topics (
    id         INTEGER PRIMARY KEY,
    terms_json TEXT
);

-- FTS5 full-text search
CREATE VIRTUAL TABLE search_fts USING fts5(
    entry_id,
    entry_type,
    text_en,
    text_la,
    text_pt,
    themes
);

CREATE VIRTUAL TABLE bible_verses_fts USING fts5(
    book_id,
    chapter,
    verse,
    text_en,
    text_la,
    text_pt,
    text_el
);

CREATE VIRTUAL TABLE patristic_sections_fts USING fts5(
    section_id,
    work_id,
    author_id,
    text_en,
    text_la,
    text_el
);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lang(text, lang: str) -> str:
    """Extract a language from a MultiLangText or plain string."""
    if isinstance(text, str):
        return text if lang == "en" else ""
    if isinstance(text, dict):
        return text.get(lang, "")
    return ""


def _normalize_doc_sections(doc: DocumentSource) -> None:
    doc.sections = {
        k: {"en": v} if isinstance(v, str) else v
        for k, v in doc.sections.items()
    }


# ---------------------------------------------------------------------------
# Population functions
# ---------------------------------------------------------------------------

def _populate_paragraphs(cur: sqlite3.Cursor, paragraphs: list[Paragraph]) -> None:
    logger.info("Populating paragraphs (%d)...", len(paragraphs))

    para_rows = []
    cross_ref_rows = []
    theme_rows = []
    entity_rows = []
    topic_rows = []
    bible_cite_rows = []
    doc_cite_rows = []
    author_cite_rows = []

    for p in paragraphs:
        para_rows.append((
            p.id,
            _lang(p.text, "en"),
            _lang(p.text, "la"),
            _lang(p.text, "pt"),
            p.part,
            p.section,
            p.chapter,
            p.article,
            json.dumps(p.themes),
            json.dumps(p.footnotes),
        ))

        for target in p.cross_references:
            cross_ref_rows.append((p.id, target))

        for theme in p.themes:
            theme_rows.append((p.id, theme))

        for eid in p.entities:
            entity_rows.append((p.id, eid))

        for tid_pair in p.topics:
            topic_rows.append((p.id, tid_pair[0]))

        seen_bible: set[tuple[str, str]] = set()
        seen_doc: set[tuple[str, str]] = set()
        seen_author: set[str] = set()
        for pf in p.parsed_footnotes:
            for br in pf.bible_refs:
                key = (br.book, br.reference)
                if key not in seen_bible:
                    seen_bible.add(key)
                    bible_cite_rows.append((p.id, br.book, br.reference))
            for dr in pf.document_refs:
                key = (dr.document, dr.section)
                if key not in seen_doc:
                    seen_doc.add(key)
                    doc_cite_rows.append((p.id, dr.document, dr.section))
            for ar in pf.author_refs:
                if ar.author not in seen_author:
                    seen_author.add(ar.author)
                    author_cite_rows.append((p.id, ar.author))

    cur.executemany(
        "INSERT INTO paragraphs VALUES (?,?,?,?,?,?,?,?,?,?)",
        para_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO paragraph_cross_refs VALUES (?,?)",
        cross_ref_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO paragraph_themes VALUES (?,?)",
        theme_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO paragraph_entities VALUES (?,?)",
        entity_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO paragraph_topics VALUES (?,?)",
        topic_rows,
    )
    cur.executemany(
        "INSERT INTO paragraph_bible_citations VALUES (?,?,?)",
        bible_cite_rows,
    )
    cur.executemany(
        "INSERT INTO paragraph_document_citations VALUES (?,?,?)",
        doc_cite_rows,
    )
    cur.executemany(
        "INSERT INTO paragraph_author_citations VALUES (?,?)",
        author_cite_rows,
    )

    logger.info(
        "  %d paragraphs, %d cross-refs, %d themes, %d entities, %d topics, %d bible cites, %d doc cites, %d author cites",
        len(para_rows), len(cross_ref_rows), len(theme_rows),
        len(entity_rows), len(topic_rows),
        len(bible_cite_rows), len(doc_cite_rows), len(author_cite_rows),
    )


def _populate_graph(
    cur: sqlite3.Cursor,
    G: nx.Graph,
    positions: dict[str, tuple[float, float]],
    paragraphs: list[Paragraph],
) -> None:
    from .export import compute_communities

    logger.info("Populating graph (%d nodes, %d edges)...", G.number_of_nodes(), G.number_of_edges())

    para_lookup = {p.id: p for p in paragraphs}
    communities = compute_communities(G)

    node_rows = []
    for node_id in G.nodes:
        data = G.nodes[node_id]
        x, y = positions.get(node_id, (0.0, 0.0))
        degree = G.degree(node_id)
        node_type = data.get("node_type", "paragraph")

        # Size: same logic as export.py
        if node_type == "structure":
            size = 8.0
        elif node_type == "bible-testament":
            size = 20.0
        elif node_type in ("bible", "bible-book"):
            size = max(4.0, min(25.0, 4.0 + degree * 0.04))
        elif node_type == "bible-chapter":
            size = max(2.0, min(10.0, 2.0 + degree * 0.02))
        elif node_type == "bible-verse":
            size = max(1.0, min(5.0, 1.0 + degree * 0.3))
        elif node_type == "patristic-work":
            size = max(3.0, min(12.0, 3.0 + degree * 0.1))
        elif node_type == "document-section":
            size = max(2.0, min(8.0, 2.0 + degree * 0.15))
        elif node_type in ("author", "document"):
            size = max(4.0, min(25.0, 4.0 + degree * 0.04))
        else:
            size = max(2.0, min(15.0, 2.0 + degree * 0.5))

        themes: list[str] = []
        entities: list[str] = []
        topics: list[int] = []
        if node_type == "paragraph" and node_id.startswith("p:"):
            pid = int(node_id[2:])
            para = para_lookup.get(pid)
            if para:
                themes = para.themes
                entities = para.entities
                topics = [t[0] for t in para.topics]

        node_rows.append((
            node_id,
            data.get("label", node_id),
            node_type,
            x, y, size,
            data.get("color", "#666666"),
            data.get("part", ""),
            degree,
            communities.get(node_id, 0),
            json.dumps(themes),
            json.dumps(entities),
            json.dumps(topics),
        ))

    cur.executemany(
        "INSERT INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )

    edge_rows = []
    for source, target, data in G.edges(data=True):
        edge_rows.append((source, target, data.get("edge_type", "cross_reference")))

    cur.executemany(
        "INSERT INTO graph_edges VALUES (?,?,?)",
        edge_rows,
    )

    logger.info("  %d nodes, %d edges written", len(node_rows), len(edge_rows))


def _populate_bible(
    cur: sqlite3.Cursor,
    bible_full: dict[str, BibleBookFull],
) -> None:
    logger.info("Populating Bible (%d books)...", len(bible_full))

    book_rows = []
    verse_rows = []

    for book_id, book in bible_full.items():
        book_rows.append((
            book.id,
            book.name,
            book.abbreviation,
            book.testament,
            book.category,
            book.total_verses,
            len(book.chapters),
            json.dumps(book.citing_paragraphs),
        ))

        for ch_num, ch in book.chapters.items():
            for v_num, v_text in ch.verses.items():
                verse_rows.append((
                    book_id, ch_num, v_num,
                    v_text.get("en", ""),
                    v_text.get("la", ""),
                    v_text.get("pt", ""),
                    v_text.get("el", ""),
                ))

    cur.executemany(
        "INSERT INTO bible_books VALUES (?,?,?,?,?,?,?,?)",
        book_rows,
    )
    cur.executemany(
        "INSERT INTO bible_verses VALUES (?,?,?,?,?,?,?)",
        verse_rows,
    )

    logger.info("  %d books, %d verses written", len(book_rows), len(verse_rows))


def _populate_documents(
    cur: sqlite3.Cursor,
    document_sources: dict[str, DocumentSource],
) -> None:
    logger.info("Populating documents (%d)...", len(document_sources))

    doc_rows = []
    section_rows = []

    for doc_id, doc in document_sources.items():
        _normalize_doc_sections(doc)

        available_langs: list[str] = []
        for sec_text in doc.sections.values():
            for lang in sec_text:
                if lang not in available_langs:
                    available_langs.append(lang)

        doc_rows.append((
            doc.id,
            doc.name,
            doc.abbreviation,
            doc.category,
            doc.source_url,
            int(doc.fetchable),
            json.dumps(doc.citing_paragraphs),
            len(doc.sections),
            json.dumps(available_langs),
        ))

        for sec_num, sec_text in doc.sections.items():
            section_rows.append((
                doc_id, sec_num,
                sec_text.get("en", "") if isinstance(sec_text, dict) else sec_text,
                sec_text.get("la", "") if isinstance(sec_text, dict) else "",
                sec_text.get("pt", "") if isinstance(sec_text, dict) else "",
            ))

    cur.executemany(
        "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?)",
        doc_rows,
    )
    cur.executemany(
        "INSERT INTO document_sections VALUES (?,?,?,?,?)",
        section_rows,
    )

    logger.info("  %d documents, %d sections written", len(doc_rows), len(section_rows))


def _populate_authors(
    cur: sqlite3.Cursor,
    author_sources: dict[str, AuthorSource],
    patristic_works: dict[str, list[PatristicWork]],
) -> None:
    logger.info("Populating authors (%d)...", len(author_sources))

    author_rows = []
    work_rows = []
    chapter_rows = []
    section_rows = []

    for author_id, author in author_sources.items():
        works = patristic_works.get(author_id, [])
        author_rows.append((
            author.id,
            author.name,
            author.era,
            json.dumps(author.citing_paragraphs),
            len(works),
        ))

        for work in works:
            work_rows.append((
                work.id,
                author_id,
                work.title,
                work.source_url,
                len(work.chapters),
            ))

            for ch in work.chapters:
                chapter_rows.append((
                    ch.id,
                    work.id,
                    ch.number,
                    ch.title,
                ))

                for sec in ch.sections:
                    section_rows.append((
                        sec.id,
                        ch.id,
                        sec.number,
                        _lang(sec.text, "en"),
                        _lang(sec.text, "la"),
                        _lang(sec.text, "el"),
                    ))

    cur.executemany("INSERT INTO authors VALUES (?,?,?,?,?)", author_rows)
    cur.executemany("INSERT INTO author_works VALUES (?,?,?,?,?)", work_rows)
    cur.executemany("INSERT INTO patristic_chapters VALUES (?,?,?,?)", chapter_rows)
    cur.executemany("INSERT INTO patristic_sections VALUES (?,?,?,?,?,?)", section_rows)

    logger.info(
        "  %d authors, %d works, %d chapters, %d sections written",
        len(author_rows), len(work_rows), len(chapter_rows), len(section_rows),
    )


def _populate_metadata(
    cur: sqlite3.Cursor,
    paragraphs: list[Paragraph],
    topic_terms: list[list[str]],
) -> None:
    logger.info("Populating metadata...")

    # Themes
    theme_counts: dict[str, int] = {}
    for p in paragraphs:
        for t in p.themes:
            theme_counts[t] = theme_counts.get(t, 0) + 1

    theme_rows = []
    for td in THEME_DEFINITIONS:
        theme_rows.append((td.id, td.label, theme_counts.get(td.id, 0)))
    cur.executemany("INSERT INTO themes VALUES (?,?,?)", theme_rows)

    # Entities
    from .entity_extraction import ENTITY_DEFINITIONS

    entity_counts: dict[str, int] = {}
    for p in paragraphs:
        for eid in p.entities:
            entity_counts[eid] = entity_counts.get(eid, 0) + 1

    entity_rows = []
    for edef in ENTITY_DEFINITIONS:
        if edef.id in entity_counts:
            entity_rows.append((edef.id, edef.label, edef.category, entity_counts[edef.id]))
    cur.executemany("INSERT INTO entities VALUES (?,?,?,?)", entity_rows)

    # Topics
    topic_rows = []
    for topic_id, terms in enumerate(topic_terms or []):
        topic_rows.append((topic_id, json.dumps(terms)))
    cur.executemany("INSERT INTO topics VALUES (?,?)", topic_rows)

    logger.info("  %d themes, %d entities, %d topics", len(theme_rows), len(entity_rows), len(topic_rows))


# ---------------------------------------------------------------------------
# FTS population
# ---------------------------------------------------------------------------

def _populate_fts(
    cur: sqlite3.Cursor,
    paragraphs: list[Paragraph],
    G: nx.Graph,
    bible_full: dict[str, BibleBookFull],
    patristic_works: dict[str, list[PatristicWork]],
) -> None:
    logger.info("Populating FTS indexes...")

    # Unified search index (paragraphs + source nodes)
    search_rows = []
    for p in paragraphs:
        search_rows.append((
            str(p.id),
            "paragraph",
            _lang(p.text, "en")[:500],
            _lang(p.text, "la")[:500],
            _lang(p.text, "pt")[:500],
            " ".join(p.themes),
        ))

    for node_id in G.nodes:
        ndata = G.nodes[node_id]
        ntype = ndata.get("node_type", "")
        if ntype in ("bible", "bible-book", "author", "patristic-work", "document", "document-section"):
            search_rows.append((
                node_id,
                ntype,
                ndata.get("label", node_id),
                "", "", "",
            ))

    cur.executemany("INSERT INTO search_fts VALUES (?,?,?,?,?,?)", search_rows)
    logger.info("  search_fts: %d entries", len(search_rows))

    # Bible verse FTS
    verse_fts_rows = []
    for book_id, book in bible_full.items():
        for ch_num, ch in book.chapters.items():
            for v_num, v_text in ch.verses.items():
                verse_fts_rows.append((
                    book_id, str(ch_num), str(v_num),
                    v_text.get("en", ""),
                    v_text.get("la", ""),
                    v_text.get("pt", ""),
                    v_text.get("el", ""),
                ))

    cur.executemany("INSERT INTO bible_verses_fts VALUES (?,?,?,?,?,?,?)", verse_fts_rows)
    logger.info("  bible_verses_fts: %d entries", len(verse_fts_rows))

    # Patristic section FTS
    patristic_fts_rows = []
    for author_id, works in patristic_works.items():
        for work in works:
            for ch in work.chapters:
                for sec in ch.sections:
                    en = _lang(sec.text, "en")
                    la = _lang(sec.text, "la")
                    el = _lang(sec.text, "el")
                    if en or la or el:
                        patristic_fts_rows.append((
                            sec.id, work.id, author_id,
                            en, la, el,
                        ))

    cur.executemany("INSERT INTO patristic_sections_fts VALUES (?,?,?,?,?,?)", patristic_fts_rows)
    logger.info("  patristic_sections_fts: %d entries", len(patristic_fts_rows))


# ---------------------------------------------------------------------------
# Main export
# ---------------------------------------------------------------------------

def export_sqlite(
    G: nx.Graph,
    positions: dict[str, tuple[float, float]],
    paragraphs: list[Paragraph],
    bible_sources: dict[str, BibleBookSource],
    document_sources: dict[str, DocumentSource],
    author_sources: dict[str, AuthorSource],
    bible_full: dict[str, BibleBookFull] | None = None,
    patristic_works: dict[str, list[PatristicWork]] | None = None,
    topic_terms: list[list[str]] | None = None,
    db_path: Path | None = None,
) -> Path:
    """Export all pipeline data to a single SQLite database.

    Returns the path to the created database file.
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing DB to start fresh
    if db_path.exists():
        db_path.unlink()
        logger.info("Removed existing database: %s", db_path)

    logger.info("Creating SQLite database: %s", db_path)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Enable WAL mode for better read performance
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")

    # Create schema
    cur.executescript(SCHEMA)

    # Populate tables
    _populate_paragraphs(cur, paragraphs)
    conn.commit()

    _populate_graph(cur, G, positions, paragraphs)
    conn.commit()

    if bible_full:
        _populate_bible(cur, bible_full)
        conn.commit()

    _populate_documents(cur, document_sources)
    conn.commit()

    _populate_authors(cur, author_sources, patristic_works or {})
    conn.commit()

    _populate_metadata(cur, paragraphs, topic_terms or [])
    conn.commit()

    # FTS indexes
    _populate_fts(
        cur, paragraphs, G,
        bible_full or {},
        patristic_works or {},
    )
    conn.commit()

    # Optimize FTS indexes
    cur.execute("INSERT INTO search_fts(search_fts) VALUES('optimize')")
    cur.execute("INSERT INTO bible_verses_fts(bible_verses_fts) VALUES('optimize')")
    cur.execute("INSERT INTO patristic_sections_fts(patristic_sections_fts) VALUES('optimize')")
    conn.commit()

    # Analyze for query planner
    cur.execute("ANALYZE")
    conn.commit()

    conn.close()

    size_mb = db_path.stat().st_size / (1024 * 1024)
    logger.info("SQLite export complete: %s (%.1f MB)", db_path, size_mb)
    return db_path
