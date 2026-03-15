"""Export graph data to JSON files for the web UI."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import community as community_louvain
import networkx as nx

from .models import (
    AuthorSource,
    BibleBookFull,
    BibleBookSource,
    DocumentSource,
    GraphData,
    GraphEdge,
    GraphNode,
    Paragraph,
    PatristicWork,
    resolve_lang,
)
from .themes import THEME_DEFINITIONS

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DATA_DIR = PROJECT_ROOT / "web" / "public" / "data"


def _normalize_doc_sections(doc: DocumentSource) -> None:
    """Wrap any plain-string section values as {"en": text} in place.

    Old pickled checkpoints may store sections as dict[str, str] instead of
    dict[str, MultiLangText].  This ensures consistent typing before export.
    """
    doc.sections = {
        k: {"en": v} if isinstance(v, str) else v
        for k, v in doc.sections.items()
    }


def compute_communities(G: nx.Graph) -> dict[str, int]:
    """Detect communities using Louvain method."""
    # Only use paragraph nodes for community detection
    paragraph_subgraph = G.subgraph(
        [n for n in G.nodes if G.nodes[n].get("node_type") == "paragraph"]
    )
    if paragraph_subgraph.number_of_nodes() == 0:
        return {}

    partition = community_louvain.best_partition(paragraph_subgraph)
    logger.info("Detected %d communities", len(set(partition.values())))
    return partition


def export_graph(
    G: nx.Graph,
    positions: dict[str, tuple[float, float]],
    paragraphs: list[Paragraph],
) -> None:
    """Export graph to JSON files for the web UI."""
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Build paragraph lookup for themes
    para_lookup = {p.id: p for p in paragraphs}

    # Compute communities
    communities = compute_communities(G)

    # Build node list
    nodes: list[GraphNode] = []
    for node_id in G.nodes:
        data = G.nodes[node_id]
        x, y = positions.get(node_id, (0.0, 0.0))
        degree = G.degree(node_id)
        node_type = data.get("node_type", "paragraph")

        # Size: type-dependent sizing
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

        # Get themes/entities/topics for paragraph nodes
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

        nodes.append(GraphNode(
            id=node_id,
            label=data.get("label", node_id),
            node_type=node_type,
            x=x,
            y=y,
            size=size,
            color=data.get("color", "#666666"),
            part=data.get("part", ""),
            degree=degree,
            community=communities.get(node_id, 0),
            themes=themes,
            entities=entities,
            topics=topics,
        ))

    # Build edge list
    edges: list[GraphEdge] = []
    for source, target, data in G.edges(data=True):
        edges.append(GraphEdge(
            source=source,
            target=target,
            edge_type=data.get("edge_type", "cross_reference"),
        ))

    # Export graph.json
    graph_data = GraphData(nodes=nodes, edges=edges)
    graph_path = WEB_DATA_DIR / "graph.json"
    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump(graph_data.model_dump(), f, ensure_ascii=False)
    logger.info("Exported graph.json: %d nodes, %d edges", len(nodes), len(edges))

    # Export paragraphs.json (full text for lazy loading)
    paragraphs_data = []
    for p in paragraphs:
        # Collect unique Bible, author, and document citations
        bible_citations: list[str] = []
        author_citations: list[str] = []
        document_citations: list[str] = []
        bible_citation_details: list[dict] = []
        document_citation_details: list[dict] = []
        seen_bible: set[str] = set()
        seen_author: set[str] = set()
        seen_document: set[str] = set()
        seen_bible_detail: set[tuple[str, str]] = set()
        seen_doc_detail: set[tuple[str, str]] = set()
        for pf in p.parsed_footnotes:
            for br in pf.bible_refs:
                if br.book not in seen_bible:
                    seen_bible.add(br.book)
                    bible_citations.append(br.book)
                detail_key = (br.book, br.reference)
                if detail_key not in seen_bible_detail and br.reference:
                    seen_bible_detail.add(detail_key)
                    bible_citation_details.append({"book": br.book, "reference": br.reference})
            for ar in pf.author_refs:
                if ar.author not in seen_author:
                    seen_author.add(ar.author)
                    author_citations.append(ar.author)
            for dr in pf.document_refs:
                if dr.document not in seen_document:
                    seen_document.add(dr.document)
                    document_citations.append(dr.document)
                detail_key_doc = (dr.document, dr.section)
                if detail_key_doc not in seen_doc_detail and dr.section:
                    seen_doc_detail.add(detail_key_doc)
                    document_citation_details.append({"document": dr.document, "section": dr.section})

        paragraphs_data.append({
            "id": p.id,
            "text": p.text,
            "footnotes": p.footnotes,
            "cross_references": p.cross_references,
            "bible_citations": bible_citations,
            "author_citations": author_citations,
            "document_citations": document_citations,
            "bible_citation_details": bible_citation_details,
            "document_citation_details": document_citation_details,
            "themes": p.themes,
            "entities": getattr(p, "entities", []),
            "topics": [t[0] for t in getattr(p, "topics", [])],
            "part": p.part,
            "section": p.section,
            "chapter": p.chapter,
            "article": p.article,
        })
    paragraphs_path = WEB_DATA_DIR / "paragraphs.json"
    with open(paragraphs_path, "w", encoding="utf-8") as f:
        json.dump(paragraphs_data, f, ensure_ascii=False)
    logger.info("Exported paragraphs.json: %d paragraphs", len(paragraphs_data))

    # Export search-index.json (lightweight for Fuse.js)
    search_data: list[dict] = []
    for p in paragraphs:
        en_text = resolve_lang(p.text, "en")
        search_data.append({
            "id": p.id,
            "text": en_text[:300],  # Truncate for search index (English only)
            "themes": " ".join(p.themes),
            "part": p.part,
            "section": p.section,
            "chapter": p.chapter,
            "article": p.article,
        })
    # Also add source nodes to search index
    for node_id in G.nodes:
        ndata = G.nodes[node_id]
        ntype = ndata.get("node_type", "")
        if ntype in ("bible", "bible-book", "author", "patristic-work", "document", "document-section"):
            search_data.append({
                "id": node_id,
                "text": ndata.get("label", node_id),
                "themes": "",
                "part": "",
                "section": "",
                "chapter": "",
                "article": "",
            })
    search_path = WEB_DATA_DIR / "search-index.json"
    with open(search_path, "w", encoding="utf-8") as f:
        json.dump(search_data, f, ensure_ascii=False)
    logger.info("Exported search-index.json: %d entries", len(search_data))

    # Export themes.json metadata
    theme_counts: dict[str, int] = {}
    for p in paragraphs:
        for t in p.themes:
            theme_counts[t] = theme_counts.get(t, 0) + 1

    themes_meta: dict[str, dict] = {}
    for td in THEME_DEFINITIONS:
        themes_meta[td.id] = {
            "label": td.label,
            "count": theme_counts.get(td.id, 0),
        }
    themes_path = WEB_DATA_DIR / "themes.json"
    with open(themes_path, "w", encoding="utf-8") as f:
        json.dump(themes_meta, f, ensure_ascii=False)
    logger.info("Exported themes.json: %d themes", len(themes_meta))


def export_sources(
    bible_sources: dict[str, BibleBookSource],
    document_sources: dict[str, DocumentSource],
    author_sources: dict[str, AuthorSource],
) -> None:
    """Export source data to JSON files for the web UI (legacy format)."""
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Export sources-bible.json
    bible_data = {k: v.model_dump() for k, v in bible_sources.items()}
    bible_path = WEB_DATA_DIR / "sources-bible.json"
    with open(bible_path, "w", encoding="utf-8") as f:
        json.dump(bible_data, f, ensure_ascii=False)
    logger.info("Exported sources-bible.json: %d books", len(bible_data))

    # Export sources-documents.json (legacy: flatten MultiLangText to English strings)
    doc_data = {}
    for k, v in document_sources.items():
        _normalize_doc_sections(v)
        d = v.model_dump()
        # Flatten sections from MultiLangText to plain English strings for legacy compat
        d["sections"] = {
            sec_num: resolve_lang(sec_text, "en") if isinstance(sec_text, dict) else sec_text
            for sec_num, sec_text in d.get("sections", {}).items()
        }
        doc_data[k] = d
    doc_path = WEB_DATA_DIR / "sources-documents.json"
    with open(doc_path, "w", encoding="utf-8") as f:
        json.dump(doc_data, f, ensure_ascii=False)
    logger.info("Exported sources-documents.json: %d documents", len(doc_data))

    # Export sources-authors.json
    author_data = {k: v.model_dump() for k, v in author_sources.items()}
    author_path = WEB_DATA_DIR / "sources-authors.json"
    with open(author_path, "w", encoding="utf-8") as f:
        json.dump(author_data, f, ensure_ascii=False)
    logger.info("Exported sources-authors.json: %d authors", len(author_data))


def export_bible_full(
    bible_books: dict[str, BibleBookFull],
) -> None:
    """Export full Bible data with chunked per-book verse files.

    Creates:
    - sources-bible-meta.json: Lightweight metadata for all 73 books
    - sources-bible-verses/{book_id}.json: Per-book verse data (lazy-loaded)
    """
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)
    verses_dir = WEB_DATA_DIR / "sources-bible-verses"
    verses_dir.mkdir(parents=True, exist_ok=True)

    # Export metadata (lightweight)
    meta: dict[str, dict] = {}
    for book_id, book in bible_books.items():
        meta[book_id] = {
            "id": book.id,
            "name": book.name,
            "abbreviation": book.abbreviation,
            "testament": book.testament,
            "category": book.category,
            "total_verses": book.total_verses,
            "total_chapters": len(book.chapters),
            "citing_paragraphs": book.citing_paragraphs,
        }

    meta_path = WEB_DATA_DIR / "sources-bible-meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    logger.info("Exported sources-bible-meta.json: %d books", len(meta))

    # Export per-book verse data
    total_files = 0
    for book_id, book in bible_books.items():
        chapters_data: list[dict] = []
        for ch_num in sorted(book.chapters.keys()):
            ch = book.chapters[ch_num]
            verses_data: dict[str, dict[str, str]] = {}
            for v_num in sorted(ch.verses.keys()):
                verses_data[str(v_num)] = ch.verses[v_num]

            chapters_data.append({
                "book_id": book_id,
                "chapter": ch_num,
                "verses": verses_data,
            })

        if chapters_data:
            book_path = verses_dir / f"{book_id}.json"
            with open(book_path, "w", encoding="utf-8") as f:
                json.dump(chapters_data, f, ensure_ascii=False)
            total_files += 1

    logger.info("Exported %d per-book Bible verse files", total_files)

    # Also export legacy sources-bible.json for backward compatibility
    legacy_data: dict[str, dict] = {}
    for book_id, book in bible_books.items():
        # Only include English text in legacy format, with cited verses only
        legacy_verses: dict[str, str] = {}
        for ch_num, ch in book.chapters.items():
            for v_num, v_text in ch.verses.items():
                key = f"{ch_num}:{v_num}"
                # Use English text for legacy format
                if "en" in v_text:
                    legacy_verses[key] = v_text["en"]

        legacy_data[book_id] = {
            "id": book_id,
            "name": book.name,
            "abbreviation": book.abbreviation,
            "testament": book.testament,
            "citing_paragraphs": book.citing_paragraphs,
            "verses": legacy_verses,
        }

    legacy_path = WEB_DATA_DIR / "sources-bible.json"
    with open(legacy_path, "w", encoding="utf-8") as f:
        json.dump(legacy_data, f, ensure_ascii=False)
    logger.info("Exported sources-bible.json (legacy): %d books", len(legacy_data))


def export_authors_full(
    author_sources: dict[str, AuthorSource],
    patristic_works: dict[str, list[PatristicWork]],
) -> None:
    """Export author data with per-author chunked work text files.

    Creates:
    - sources-authors-meta.json: Lightweight metadata for all authors
    - sources-authors-works/{author_id}.json: Per-author work data (lazy-loaded)
    - sources-authors.json: Legacy format (backward compat)
    """
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)
    works_dir = WEB_DATA_DIR / "sources-authors-works"
    works_dir.mkdir(parents=True, exist_ok=True)

    # Export metadata (lightweight)
    meta: dict[str, dict] = {}
    for author_id, author in author_sources.items():
        works = patristic_works.get(author_id, [])
        meta[author_id] = {
            "id": author.id,
            "name": author.name,
            "era": author.era,
            "citing_paragraphs": author.citing_paragraphs,
            "work_count": len(works),
            "work_titles": [w.title for w in works],
        }

    meta_path = WEB_DATA_DIR / "sources-authors-meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    logger.info("Exported sources-authors-meta.json: %d authors", len(meta))

    # Export per-author work data (lazy-loaded)
    total_files = 0
    for author_id, works in patristic_works.items():
        if not works:
            continue

        works_data: list[dict] = []
        for work in works:
            chapters_data: list[dict] = []
            for ch in work.chapters:
                sections_data: list[dict] = []
                for sec in ch.sections:
                    sections_data.append({
                        "id": sec.id,
                        "number": sec.number,
                        "text": sec.text,
                    })
                chapters_data.append({
                    "id": ch.id,
                    "number": ch.number,
                    "title": ch.title,
                    "sections": sections_data,
                })

            works_data.append({
                "id": work.id,
                "title": work.title,
                "source_url": work.source_url,
                "chapter_count": len(work.chapters),
                "chapters": chapters_data,
            })

        author_path = works_dir / f"{author_id}.json"
        with open(author_path, "w", encoding="utf-8") as f:
            json.dump(works_data, f, ensure_ascii=False)
        total_files += 1

    logger.info("Exported %d per-author work files", total_files)


def export_documents_full(
    document_sources: dict[str, DocumentSource],
) -> None:
    """Export document data with per-document chunked section files.

    Creates:
    - sources-documents-meta.json: Lightweight metadata for all documents
    - sources-documents-sections/{doc_id}.json: Per-document section data (lazy-loaded)
    - sources-documents.json: Legacy format (English only, backward compat)
    """
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)
    sections_dir = WEB_DATA_DIR / "sources-documents-sections"
    sections_dir.mkdir(parents=True, exist_ok=True)

    # Normalize sections from old checkpoints (plain str -> {"en": text})
    for doc in document_sources.values():
        _normalize_doc_sections(doc)

    # Export metadata (lightweight)
    meta: dict[str, dict] = {}
    for doc_id, doc in document_sources.items():
        # Determine which languages are available
        available_langs: list[str] = []
        for sec_text in doc.sections.values():
            for lang in sec_text:
                if lang not in available_langs:
                    available_langs.append(lang)

        meta[doc_id] = {
            "id": doc.id,
            "name": doc.name,
            "abbreviation": doc.abbreviation,
            "category": doc.category,
            "source_url": doc.source_url,
            "fetchable": doc.fetchable,
            "citing_paragraphs": doc.citing_paragraphs,
            "section_count": len(doc.sections),
            "available_langs": available_langs,
        }

    meta_path = WEB_DATA_DIR / "sources-documents-meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    logger.info("Exported sources-documents-meta.json: %d documents", len(meta))

    # Export per-document section data (lazy-loaded)
    total_files = 0
    for doc_id, doc in document_sources.items():
        if not doc.sections:
            continue

        sections_data: dict[str, dict[str, str]] = {}
        for sec_num in sorted(doc.sections.keys(), key=lambda x: int(x) if x.isdigit() else 0):
            sections_data[sec_num] = doc.sections[sec_num]

        doc_path = sections_dir / f"{doc_id}.json"
        with open(doc_path, "w", encoding="utf-8") as f:
            json.dump(sections_data, f, ensure_ascii=False)
        total_files += 1

    logger.info("Exported %d per-document section files", total_files)


def export_topics(topic_terms: list[list[str]]) -> None:
    """Export topic metadata to topics.json.

    Each topic is an object with its ID and top-10 terms.
    """
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    topics_data: list[dict] = []
    for topic_id, terms in enumerate(topic_terms):
        topics_data.append({
            "id": topic_id,
            "terms": terms,
        })

    topics_path = WEB_DATA_DIR / "topics.json"
    with open(topics_path, "w", encoding="utf-8") as f:
        json.dump(topics_data, f, ensure_ascii=False)
    logger.info("Exported topics.json: %d topics", len(topics_data))


def export_entities(paragraphs: list[Paragraph]) -> None:
    """Export entity metadata to entities.json.

    Counts how many paragraphs contain each entity and includes the definition.
    """
    from .entity_extraction import ENTITY_DEFINITIONS

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Count entity occurrences
    entity_counts: dict[str, int] = {}
    for p in paragraphs:
        for eid in p.entities:
            entity_counts[eid] = entity_counts.get(eid, 0) + 1

    entities_data: list[dict] = []
    for edef in ENTITY_DEFINITIONS:
        if edef.id in entity_counts:
            entities_data.append({
                "id": edef.id,
                "label": edef.label,
                "category": edef.category,
                "count": entity_counts[edef.id],
            })

    # Sort by count descending
    entities_data.sort(key=lambda x: -x["count"])

    entities_path = WEB_DATA_DIR / "entities.json"
    with open(entities_path, "w", encoding="utf-8") as f:
        json.dump(entities_data, f, ensure_ascii=False)
    logger.info("Exported entities.json: %d entities", len(entities_data))
