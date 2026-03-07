"""Pydantic data models for the Catechism Knowledge Graph."""

from __future__ import annotations

from pydantic import BaseModel


class BibleReference(BaseModel):
    """A parsed Bible reference from a footnote."""

    book: str  # Canonical ID: "matthew", "1-corinthians"
    abbreviation: str  # As found: "Mt", "1 Cor"
    reference: str = ""  # Chapter:verse e.g. "28:19-20"


class PatristicReference(BaseModel):
    """A parsed patristic author reference from a footnote."""

    author: str  # Canonical ID: "augustine", "thomas-aquinas"


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


class BibleVerse(BaseModel):
    """A single Bible verse with its text."""

    book_id: str  # "matthew"
    chapter: int
    verse: int
    text: str


class DocumentSection(BaseModel):
    """A numbered section from an ecclesiastical document."""

    document_id: str
    section_num: int
    text: str


class PatristicPassage(BaseModel):
    """A passage from a Church Father's work."""

    author_id: str
    work: str
    location: str
    text: str
    source_url: str = ""
    citing_paragraphs: list[int] = []


class BibleBookSource(BaseModel):
    """Source data for a Bible book cited by the CCC."""

    id: str
    name: str
    abbreviation: str
    testament: str  # "old" or "new"
    citing_paragraphs: list[int] = []
    verses: dict[str, str] = {}  # "5:1" -> verse text (only cited verses)


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


class AuthorSource(BaseModel):
    """Source data for a patristic author cited by the CCC."""

    id: str
    name: str
    era: str = ""
    works: list[dict] = []  # [{"title": "...", "url": "..."}]
    citing_paragraphs: list[int] = []


class GraphNode(BaseModel):
    """A node in the exported graph."""

    id: str
    label: str
    node_type: str  # "paragraph", "structure", "bible", or "author"
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
    edge_type: str  # "cross_reference", "belongs_to", "child_of"


class GraphData(BaseModel):
    """Complete graph data for export."""

    nodes: list[GraphNode]
    edges: list[GraphEdge]
