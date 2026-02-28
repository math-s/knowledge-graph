"""Pydantic data models for the Catechism Knowledge Graph."""

from __future__ import annotations

from pydantic import BaseModel


class BibleReference(BaseModel):
    """A parsed Bible reference from a footnote."""

    book: str  # Canonical ID: "matthew", "1-corinthians"
    abbreviation: str  # As found: "Mt", "1 Cor"


class PatristicReference(BaseModel):
    """A parsed patristic author reference from a footnote."""

    author: str  # Canonical ID: "augustine", "thomas-aquinas"


class DocumentReference(BaseModel):
    """A parsed ecclesiastical document reference from a footnote."""

    document: str  # Canonical ID: "lumen-gentium", "cic"
    abbreviation: str  # As found: "LG", "CIC"


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
