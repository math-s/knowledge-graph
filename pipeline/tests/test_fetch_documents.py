"""Tests for fetch_documents.py."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_documents import (
    _DOCUMENT_META,
    _parse_sections,
    fetch_document_texts,
)
from pipeline.src.models import DocumentReference, Paragraph, ParsedFootnote


class TestDocumentMeta:
    """Tests for the document metadata mapping."""

    def test_all_footnote_parser_docs_have_meta(self):
        """Every document in footnote_parser should have metadata."""
        from pipeline.src.footnote_parser import _DOCUMENTS as FP_DOCS
        for doc_id, _name, _forms in FP_DOCS:
            assert doc_id in _DOCUMENT_META, f"Missing metadata for {doc_id}"

    def test_reference_collections_not_fetchable(self):
        """DS, PL, PG, SCh, AAS should be marked as reference collections."""
        for doc_id in ["denzinger-schonmetzer", "patrologia-latina", "patrologia-graeca",
                       "sources-chretiennes", "acta-apostolicae-sedis"]:
            assert _DOCUMENT_META[doc_id]["category"] == "reference"

    def test_vatican_ii_docs_have_urls(self):
        """Vatican II documents should have non-empty URLs."""
        for doc_id, meta in _DOCUMENT_META.items():
            if meta["category"] == "vatican-ii":
                assert meta["url"], f"Missing URL for Vatican II doc {doc_id}"


class TestParseSections:
    """Tests for the HTML section parser."""

    def test_parses_numbered_paragraphs(self):
        html = """
        <html><body>
        <p>1. This is the first section of the document with enough text to pass the length check.</p>
        <p>2. This is the second section of the document with enough text to pass the length check.</p>
        <p>Some unnumbered text that should not be captured.</p>
        <p>3. This is the third section of the document with enough text to pass the length check here.</p>
        </body></html>
        """
        sections = _parse_sections(html)
        assert "1" in sections
        assert "2" in sections
        assert "3" in sections
        assert len(sections) == 3

    def test_empty_html(self):
        sections = _parse_sections("<html><body></body></html>")
        assert sections == {}

    def test_short_text_not_captured(self):
        """Sections shorter than 20 chars should be excluded."""
        html = "<html><body><p>1. Too short.</p></body></html>"
        sections = _parse_sections(html)
        assert "1" not in sections


class TestFetchDocumentTexts:
    """Tests for fetch_document_texts with mock data."""

    def _make_paragraphs(self, refs: list[tuple[str, str, str]]) -> list[Paragraph]:
        """Create test paragraphs with given doc refs: [(doc_id, abbrev, section)]."""
        paragraphs = []
        for i, (doc_id, abbrev, section) in enumerate(refs, start=1):
            p = Paragraph(
                id=i,
                text={"en": f"Test paragraph {i}"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="test",
                        document_refs=[DocumentReference(document=doc_id, abbreviation=abbrev, section=section)],
                    )
                ],
            )
            paragraphs.append(p)
        return paragraphs

    def test_no_refs_returns_empty(self):
        paras = [Paragraph(id=1, text={"en": "No refs"})]
        result = fetch_document_texts(paras)
        assert result == {}

    def test_collects_citing_paragraphs(self):
        paras = self._make_paragraphs([
            ("lumen-gentium", "LG", "12"),
            ("lumen-gentium", "LG", "16"),
            ("gaudium-et-spes", "GS", "22"),
        ])
        result = fetch_document_texts(paras)
        assert isinstance(result, dict)
        if result:
            assert "lumen-gentium" in result
            assert result["lumen-gentium"].citing_paragraphs == [1, 2]

    def test_unfetchable_document(self):
        """Reference collection documents should have fetchable=False."""
        paras = self._make_paragraphs([
            ("denzinger-schonmetzer", "DS", "150"),
        ])
        result = fetch_document_texts(paras)
        assert "denzinger-schonmetzer" in result
        assert result["denzinger-schonmetzer"].fetchable is False
        assert result["denzinger-schonmetzer"].sections == {}

    def test_sections_are_multilang_text(self):
        """Sections from fetch_document_texts should be MultiLangText dicts."""
        paras = self._make_paragraphs([
            ("lumen-gentium", "LG", "12"),
        ])
        result = fetch_document_texts(paras)
        if result and result["lumen-gentium"].sections:
            for sec_num, sec_text in result["lumen-gentium"].sections.items():
                assert isinstance(sec_text, dict), f"Section {sec_num} should be a dict (MultiLangText)"
                assert "en" in sec_text, f"Section {sec_num} should have 'en' key"
