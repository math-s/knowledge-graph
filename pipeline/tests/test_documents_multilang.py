"""Tests for fetch_documents_multilang.py."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_documents_multilang import (
    _generate_lang_url,
    _parse_sections,
    fetch_documents_multilang,
)
from pipeline.src.models import DocumentSource


class TestGenerateLangUrl:
    """Tests for URL generation from English to other languages."""

    def test_vatican_ii_latin(self):
        en_url = "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19641121_lumen-gentium_en.html"
        result = _generate_lang_url(en_url, "la")
        assert result == "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19641121_lumen-gentium_lt.html"

    def test_vatican_ii_portuguese(self):
        en_url = "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19641121_lumen-gentium_en.html"
        result = _generate_lang_url(en_url, "pt")
        assert result == "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19641121_lumen-gentium_po.html"

    def test_papal_content_latin(self):
        en_url = "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_01051991_centesimus-annus.html"
        result = _generate_lang_url(en_url, "la")
        assert result == "https://www.vatican.va/content/john-paul-ii/la/encyclicals/documents/hf_jp-ii_enc_01051991_centesimus-annus.html"

    def test_papal_content_portuguese(self):
        en_url = "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_01051991_centesimus-annus.html"
        result = _generate_lang_url(en_url, "pt")
        assert result == "https://www.vatican.va/content/john-paul-ii/pt/encyclicals/documents/hf_jp-ii_enc_01051991_centesimus-annus.html"

    def test_empty_url(self):
        assert _generate_lang_url("", "la") is None

    def test_unknown_url_pattern(self):
        assert _generate_lang_url("https://example.com/doc.html", "la") is None

    def test_unsupported_language(self):
        en_url = "https://www.vatican.va/archive/test_en.html"
        assert _generate_lang_url(en_url, "el") is None


class TestParseSections:
    """Tests for section parsing (should match fetch_documents behavior)."""

    def test_parses_numbered_paragraphs(self):
        html = """
        <html><body>
        <p>1. This is the first section with enough text to pass the minimum length check here.</p>
        <p>2. Second section also has enough text to pass the length check here in this test.</p>
        </body></html>
        """
        sections = _parse_sections(html)
        assert "1" in sections
        assert "2" in sections
        assert len(sections) == 2

    def test_empty_html(self):
        sections = _parse_sections("<html><body></body></html>")
        assert sections == {}

    def test_short_text_excluded(self):
        html = "<html><body><p>1. Too short.</p></body></html>"
        sections = _parse_sections(html)
        assert "1" not in sections


class TestFetchDocumentsMultilang:
    """Tests for the merge function logic."""

    def test_no_documents_returns_empty(self):
        result = fetch_documents_multilang({})
        assert result == {}

    def test_unfetchable_skipped(self):
        docs = {
            "ds": DocumentSource(
                id="ds",
                name="Denzinger",
                abbreviation="DS",
                category="reference",
                fetchable=False,
                sections={},
            ),
        }
        result = fetch_documents_multilang(docs)
        assert "ds" in result
        # Unfetchable documents should not have new languages added
        assert result["ds"].sections == {}

    def test_document_without_url_skipped(self):
        docs = {
            "test": DocumentSource(
                id="test",
                name="Test",
                abbreviation="T",
                category="encyclical",
                source_url="",
                fetchable=True,
                sections={"1": {"en": "English text here for section one."}},
            ),
        }
        result = fetch_documents_multilang(docs)
        # No URL, so no languages should be fetched
        assert result["test"].sections["1"] == {"en": "English text here for section one."}
