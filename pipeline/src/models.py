"""Pydantic data models for the Catechism Knowledge Graph."""

from __future__ import annotations

from pydantic import BaseModel

# ── Multi-language support ────────────────────────────────────────────────────

# Text stored as dict[lang_code, str] where available.
# Not all texts exist in all languages.
MultiLangText = dict[str, str]  # {"en": "...", "la": "...", "pt": "...", "el": "..."}

SUPPORTED_LANGS = ("la", "en", "pt", "el")
FALLBACK_ORDER = ("la", "en", "pt", "el")  # when preferred lang unavailable


def resolve_lang(text: MultiLangText, preferred: str = "en") -> str:
    """Resolve a MultiLangText to a single string using fallback chain."""
    if preferred in text and text[preferred]:
        return text[preferred]
    for lang in FALLBACK_ORDER:
        if lang in text and text[lang]:
            return text[lang]
    # Return first available
    for v in text.values():
        if v:
            return v
    return ""


# ── Reference models (from footnote parsing) ─────────────────────────────────


class BibleReference(BaseModel):
    """A parsed Bible reference from a footnote."""

    book: str  # Canonical ID: "matthew", "1-corinthians"
    abbreviation: str  # As found: "Mt", "1 Cor"
    reference: str = ""  # Chapter:verse e.g. "28:19-20"


class PatristicReference(BaseModel):
    """A parsed patristic author reference from a footnote."""

    author: str  # Canonical ID: "augustine", "thomas-aquinas"
    work: str = ""  # Canonical work ID if resolved
    location: str = ""  # Specific location within work


class DocumentReference(BaseModel):
    """A parsed ecclesiastical document reference from a footnote."""

    document: str  # Canonical ID: "lumen-gentium", "cic"
    abbreviation: str  # As found: "LG", "CIC"
    section: str = ""  # Section number e.g. "12"


class ParsedFootnote(BaseModel):
    """A footnote parsed into structured references."""

    raw: str
    bible_refs: list[BibleReference] = []
    author_refs: list[PatristicReference] = []
    document_refs: list[DocumentReference] = []


# ── CCC models ────────────────────────────────────────────────────────────────


class Paragraph(BaseModel):
    """A CCC paragraph with its text and cross-references."""

    id: int
    text: str
    cross_references: list[int] = []
    footnotes: list[str] = []
    parsed_footnotes: list[ParsedFootnote] = []
    themes: list[str] = []
    part: str = ""
    section: str = ""
    chapter: str = ""
    article: str = ""


class StructuralNode(BaseModel):
    """A node in the CCC structural hierarchy."""

    id: str  # e.g. "part-1", "part-1-section-2"
    label: str
    level: str  # "part", "section", "chapter", "article"
    parent_id: str | None = None
    paragraph_ids: list[int] = []


# ── Bible models ──────────────────────────────────────────────────────────────


class BibleVerse(BaseModel):
    """A single Bible verse with its text."""

    book_id: str  # "matthew"
    chapter: int
    verse: int
    text: MultiLangText  # {"en": "Blessed are...", "la": "Beati pauperes..."}


class BibleChapter(BaseModel):
    """A chapter in a Bible book."""

    book_id: str
    chapter: int
    verses: dict[int, MultiLangText] = {}  # verse_num -> MultiLangText


class BibleBookFull(BaseModel):
    """Full Bible book with all chapters and verses."""

    id: str
    name: str
    abbreviation: str
    testament: str  # "old" or "new"
    category: str = ""  # "pentateuch", "historical", "wisdom", "prophetic", "gospel", "epistle", "apocalyptic"
    chapters: dict[int, BibleChapter] = {}
    total_verses: int = 0
    citing_paragraphs: list[int] = []


class BibleBookSource(BaseModel):
    """Source data for a Bible book cited by the CCC (legacy, for backward compat)."""

    id: str
    name: str
    abbreviation: str
    testament: str  # "old" or "new"
    citing_paragraphs: list[int] = []
    verses: dict[str, str] = {}  # "5:1" -> verse text (only cited verses)


# ── Document models ───────────────────────────────────────────────────────────


class DocumentSection(BaseModel):
    """A numbered section from an ecclesiastical document."""

    document_id: str
    section_num: int
    text: MultiLangText  # {"en": "...", "la": "...", "pt": "..."}


class DocumentSource(BaseModel):
    """Source data for an ecclesiastical document cited by the CCC."""

    id: str
    name: str
    abbreviation: str
    category: str  # "vatican-ii", "encyclical", "canon-law", "reference"
    source_url: str = ""
    fetchable: bool = True
    citing_paragraphs: list[int] = []
    sections: dict[str, str] = {}  # "12" -> section text (only cited sections)


# ── Patristic models ─────────────────────────────────────────────────────────


class PatristicPassage(BaseModel):
    """A passage from a Church Father's work."""

    author_id: str
    work: str
    location: str
    text: str
    source_url: str = ""
    citing_paragraphs: list[int] = []


class PatristicSection(BaseModel):
    """A section within a patristic chapter."""

    id: str
    chapter_id: str
    number: int
    text: MultiLangText  # {"en": "...", "la": "..."} or {"en": "...", "el": "..."}


class PatristicChapter(BaseModel):
    """A chapter within a patristic work."""

    id: str
    work_id: str
    number: int
    title: str = ""
    sections: list[PatristicSection] = []


class PatristicWork(BaseModel):
    """A complete work by a Church Father."""

    id: str
    author_id: str
    title: str
    source_url: str = ""
    chapters: list[PatristicChapter] = []


class AuthorSource(BaseModel):
    """Source data for a patristic author cited by the CCC."""

    id: str
    name: str
    era: str = ""
    works: list[dict] = []  # [{"title": "...", "url": "..."}]
    citing_paragraphs: list[int] = []


# ── Graph export models ───────────────────────────────────────────────────────


class GraphNode(BaseModel):
    """A node in the exported graph."""

    id: str
    label: str
    node_type: str  # "paragraph", "structure", "bible", "bible-testament", "bible-book", "bible-chapter", "bible-verse", "author", "patristic-work", "document"
    x: float = 0.0
    y: float = 0.0
    size: float = 1.0
    color: str = "#666666"
    part: str = ""
    degree: int = 0
    community: int = 0
    themes: list[str] = []


class GraphEdge(BaseModel):
    """An edge in the exported graph."""

    source: str
    target: str
    edge_type: str  # "cross_reference", "belongs_to", "child_of", "cites", "shared_theme", "bible_cross_reference"


class GraphData(BaseModel):
    """Complete graph data for export."""

    nodes: list[GraphNode]
    edges: list[GraphEdge]
