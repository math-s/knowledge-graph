"""Tests for fetch_patristic.py."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_patristic import (
    _AUTHOR_META,
    _parse_works_list,
    fetch_patristic_texts,
)
from pipeline.src.models import Paragraph, ParsedFootnote, PatristicReference


class TestAuthorMeta:
    """Tests for the author metadata mapping."""

    def test_all_footnote_parser_authors_have_meta(self):
        """Every author in footnote_parser should have metadata."""
        from pipeline.src.footnote_parser import _PATRISTIC_AUTHORS
        for author_id, _names in _PATRISTIC_AUTHORS:
            assert author_id in _AUTHOR_META, f"Missing metadata for {author_id}"

    def test_all_authors_have_era(self):
        """Every author should have an era string."""
        for author_id, meta in _AUTHOR_META.items():
            assert meta["era"], f"Missing era for {author_id}"


class TestParseWorksList:
    """Tests for the HTML works list parser."""

    def test_extracts_links(self):
        html = """
        <html><body>
        <a href="/fathers/0103.htm">Against Heresies (Book I)</a>
        <a href="/fathers/0104.htm">Against Heresies (Book II)</a>
        </body></html>
        """
        works = _parse_works_list(html, "https://www.newadvent.org/fathers/0103.htm")
        assert len(works) == 2
        assert works[0]["title"] == "Against Heresies (Book I)"
        assert works[0]["url"].startswith("https://")

    def test_skips_nav_links(self):
        html = """
        <html><body>
        <a href="/">Home</a>
        <a href="/encyclopedia/">Encyclopedia</a>
        <a href="/fathers/0103.htm">Against Heresies (Book I)</a>
        </body></html>
        """
        works = _parse_works_list(html, "https://www.newadvent.org/fathers/0103.htm")
        assert len(works) == 1
        assert works[0]["title"] == "Against Heresies (Book I)"

    def test_empty_html(self):
        works = _parse_works_list("<html><body></body></html>", "https://example.com")
        assert works == []


class TestFetchPatristicTexts:
    """Tests for fetch_patristic_texts."""

    def _make_paragraphs(self, refs: list[str]) -> list[Paragraph]:
        """Create test paragraphs with given author IDs."""
        paragraphs = []
        for i, author_id in enumerate(refs, start=1):
            p = Paragraph(
                id=i,
                text=f"Test paragraph {i}",
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="test",
                        author_refs=[PatristicReference(author=author_id)],
                    )
                ],
            )
            paragraphs.append(p)
        return paragraphs

    def test_no_refs_returns_empty(self):
        paras = [Paragraph(id=1, text="No refs")]
        result = fetch_patristic_texts(paras)
        assert result == {}

    def test_collects_citing_paragraphs(self):
        paras = self._make_paragraphs(["augustine", "augustine", "irenaeus"])
        result = fetch_patristic_texts(paras)
        assert isinstance(result, dict)
        assert "augustine" in result
        assert result["augustine"].citing_paragraphs == [1, 2]
        assert "irenaeus" in result
        assert result["irenaeus"].citing_paragraphs == [3]

    def test_author_has_name_and_era(self):
        paras = self._make_paragraphs(["augustine"])
        result = fetch_patristic_texts(paras)
        assert result["augustine"].name == "St. Augustine of Hippo"
        assert "354" in result["augustine"].era
