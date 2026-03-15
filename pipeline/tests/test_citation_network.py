"""Tests for citation network module."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import networkx as nx

from pipeline.src.citation_network import add_shared_citation_edges
from pipeline.src.models import (
    BibleReference,
    DocumentReference,
    Paragraph,
    ParsedFootnote,
    PatristicReference,
)


def _make_graph(para_ids: list[int]) -> nx.Graph:
    """Create a graph with paragraph nodes."""
    G = nx.Graph()
    for pid in para_ids:
        G.add_node(f"p:{pid}", node_type="paragraph")
    return G


class TestSharedBibleCitations:
    """Test shared citation edges from Bible references."""

    def test_shared_bible_verse(self):
        """Two paragraphs citing the same Bible verse should be linked."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
            Paragraph(
                id=2,
                text={"en": "Text 2"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    ),
                    ParsedFootnote(
                        raw="Jn 3:16",
                        bible_refs=[BibleReference(book="john", abbreviation="Jn", reference="3:16")],
                    ),
                ],
            ),
        ]
        G = _make_graph([1, 2])
        # min_shared=1 to detect single shared citation
        G = add_shared_citation_edges(G, paras, min_shared=1)
        assert G.has_edge("p:1", "p:2")
        assert G.edges["p:1", "p:2"]["edge_type"] == "shared_citation"

    def test_threshold_filters(self):
        """Pairs below the threshold should not get an edge."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
            Paragraph(
                id=2,
                text={"en": "Text 2"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
        ]
        G = _make_graph([1, 2])
        # With min_shared=2, only 1 shared citation should not create an edge
        G = add_shared_citation_edges(G, paras, min_shared=2)
        assert not G.has_edge("p:1", "p:2")

    def test_multiple_shared_citations_meet_threshold(self):
        """Pairs sharing 2+ citation targets should get an edge with min_shared=2."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19; Jn 3:16",
                        bible_refs=[
                            BibleReference(book="matthew", abbreviation="Mt", reference="28:19"),
                            BibleReference(book="john", abbreviation="Jn", reference="3:16"),
                        ],
                    )
                ],
            ),
            Paragraph(
                id=2,
                text={"en": "Text 2"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19; Jn 3:16",
                        bible_refs=[
                            BibleReference(book="matthew", abbreviation="Mt", reference="28:19"),
                            BibleReference(book="john", abbreviation="Jn", reference="3:16"),
                        ],
                    )
                ],
            ),
        ]
        G = _make_graph([1, 2])
        G = add_shared_citation_edges(G, paras, min_shared=2)
        assert G.has_edge("p:1", "p:2")
        assert G.edges["p:1", "p:2"]["shared_count"] == 2


class TestSharedDocumentCitations:
    """Test shared citation edges from document references."""

    def test_shared_document_section(self):
        """Two paragraphs citing the same document section should be linked."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="LG 12",
                        document_refs=[
                            DocumentReference(document="lumen-gentium", abbreviation="LG", section="12")
                        ],
                    )
                ],
            ),
            Paragraph(
                id=2,
                text={"en": "Text 2"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="LG 12",
                        document_refs=[
                            DocumentReference(document="lumen-gentium", abbreviation="LG", section="12")
                        ],
                    )
                ],
            ),
        ]
        G = _make_graph([1, 2])
        G = add_shared_citation_edges(G, paras, min_shared=1)
        assert G.has_edge("p:1", "p:2")


class TestSharedAuthorCitations:
    """Test shared citation edges from patristic references."""

    def test_shared_author_work(self):
        """Two paragraphs citing the same author work should be linked."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Aug. Conf.",
                        author_refs=[
                            PatristicReference(author="augustine", work="confessions")
                        ],
                    )
                ],
            ),
            Paragraph(
                id=2,
                text={"en": "Text 2"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Aug. Conf.",
                        author_refs=[
                            PatristicReference(author="augustine", work="confessions")
                        ],
                    )
                ],
            ),
        ]
        G = _make_graph([1, 2])
        G = add_shared_citation_edges(G, paras, min_shared=1)
        assert G.has_edge("p:1", "p:2")


class TestMissingNodes:
    """Test behavior when graph nodes are missing."""

    def test_missing_node_skipped(self):
        """Paragraphs not in the graph should not cause errors."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
            Paragraph(
                id=999,
                text={"en": "Text 999"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
        ]
        # Only paragraph 1 is in the graph
        G = _make_graph([1])
        G = add_shared_citation_edges(G, paras, min_shared=1)
        # No edge should be created since p:999 is not in graph
        assert G.number_of_edges() == 0

    def test_no_footnotes(self):
        """Paragraphs without footnotes should be handled gracefully."""
        paras = [
            Paragraph(id=1, text={"en": "No citations here."}),
            Paragraph(id=2, text={"en": "No citations here either."}),
        ]
        G = _make_graph([1, 2])
        G = add_shared_citation_edges(G, paras, min_shared=1)
        assert G.number_of_edges() == 0


class TestExistingEdgesPreserved:
    """Test that existing edges are not overwritten."""

    def test_existing_edge_not_duplicated(self):
        """If two paragraphs already have an edge, no shared_citation edge should be added."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "Text 1"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
            Paragraph(
                id=2,
                text={"en": "Text 2"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="Mt 28:19",
                        bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="28:19")],
                    )
                ],
            ),
        ]
        G = _make_graph([1, 2])
        G.add_edge("p:1", "p:2", edge_type="cross_reference")
        G = add_shared_citation_edges(G, paras, min_shared=1)
        # Edge should still be cross_reference, not overwritten
        assert G.edges["p:1", "p:2"]["edge_type"] == "cross_reference"
