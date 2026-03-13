"""Tests for fetch_ccc_multilang.py."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_ccc_multilang import (
    _extract_paragraphs_from_html,
    _discover_section_pages,
)
from pipeline.src.models import Paragraph


class TestExtractParagraphsFromHtml:
    """Tests for paragraph extraction from Vatican.va HTML."""

    def test_bold_number_pattern(self):
        html = """
        <html><body>
        <p><b>100.</b> This is a test paragraph with enough text to pass the minimum length check.</p>
        <p><b>101.</b> Another paragraph also with enough text to pass the minimum length requirement.</p>
        </body></html>
        """
        result = _extract_paragraphs_from_html(html)
        assert 100 in result
        assert 101 in result
        assert "This is a test paragraph" in result[100]

    def test_strong_number_pattern(self):
        html = """
        <html><body>
        <p><strong>200</strong> Paragraph text here with enough length to be valid content for this test.</p>
        </body></html>
        """
        result = _extract_paragraphs_from_html(html)
        assert 200 in result

    def test_plain_number_dot_pattern(self):
        html = """
        <html><body>
        <p>300. Plain numbered paragraph with enough text length to pass the minimum check.</p>
        </body></html>
        """
        result = _extract_paragraphs_from_html(html)
        assert 300 in result

    def test_out_of_range_excluded(self):
        html = """
        <html><body>
        <p><b>0.</b> Zero is not a valid CCC paragraph number in the actual catechism.</p>
        <p><b>3000.</b> This number is above the maximum CCC paragraph 2865 range.</p>
        </body></html>
        """
        result = _extract_paragraphs_from_html(html)
        assert 0 not in result
        assert 3000 not in result

    def test_short_text_excluded(self):
        html = """
        <html><body>
        <p><b>500.</b> Too short.</p>
        </body></html>
        """
        result = _extract_paragraphs_from_html(html)
        assert 500 not in result

    def test_empty_html(self):
        result = _extract_paragraphs_from_html("<html><body></body></html>")
        assert result == {}


class TestDiscoverSectionPages:
    """Tests for section page URL discovery."""

    def test_discovers_relative_links(self):
        html = """
        <html><body>
        <a href="part1.htm">Part 1</a>
        <a href="part2.htm">Part 2</a>
        </body></html>
        """
        urls = _discover_section_pages(html, "https://www.vatican.va/archive/ccc/")
        assert len(urls) == 2
        assert "https://www.vatican.va/archive/ccc/part1.htm" in urls
        assert "https://www.vatican.va/archive/ccc/part2.htm" in urls

    def test_discovers_absolute_links(self):
        html = """
        <html><body>
        <a href="https://www.vatican.va/archive/ccc/full.html">Full</a>
        </body></html>
        """
        urls = _discover_section_pages(html, "https://www.vatican.va/archive/ccc/")
        assert len(urls) == 1
        assert "https://www.vatican.va/archive/ccc/full.html" in urls

    def test_ignores_non_html_links(self):
        html = """
        <html><body>
        <a href="image.jpg">Image</a>
        <a href="section.htm">Section</a>
        </body></html>
        """
        urls = _discover_section_pages(html, "https://example.com/")
        assert len(urls) == 1


class TestFetchCccMultilang:
    """Tests for CCC multilang merge logic."""

    def test_merge_preserves_english(self):
        """Merging should not overwrite existing English text."""
        paras = [
            Paragraph(id=1, text={"en": "English text for testing."}),
        ]
        # Simulate: no languages will actually be fetched without network
        # but the function should return paragraphs unchanged
        from pipeline.src.fetch_ccc_multilang import fetch_ccc_multilang
        result = fetch_ccc_multilang(paras, languages=())  # empty tuple = no fetching
        assert result[0].text["en"] == "English text for testing."
