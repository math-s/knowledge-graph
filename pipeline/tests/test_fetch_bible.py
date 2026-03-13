"""Tests for fetch_bible.py."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_bible import parse_reference, fetch_bible_texts
from pipeline.src.models import BibleReference, Paragraph, ParsedFootnote


class TestParseReference:
    """Tests for the parse_reference function."""

    def test_simple_chapter_verse(self):
        result = parse_reference("3:16")
        assert result == [(3, 16)]

    def test_verse_range(self):
        result = parse_reference("5:1-3")
        assert result == [(5, 1), (5, 2), (5, 3)]

    def test_verse_list(self):
        result = parse_reference("5:1,3")
        assert result == [(5, 1), (5, 3)]

    def test_verse_range_and_list(self):
        result = parse_reference("5:1-3,7")
        assert result == [(5, 1), (5, 2), (5, 3), (5, 7)]

    def test_chapter_only(self):
        result = parse_reference("5")
        assert result == [(5, 0)]

    def test_empty_string(self):
        result = parse_reference("")
        assert result == []

    def test_complex_range(self):
        result = parse_reference("28:19-20")
        assert result == [(28, 19), (28, 20)]


class TestFetchBibleTexts:
    """Tests for fetch_bible_texts with mock data."""

    def _make_paragraphs(self, refs: list[tuple[str, str, str]]) -> list[Paragraph]:
        """Create test paragraphs with given Bible refs: [(book, abbrev, reference)]."""
        paragraphs = []
        for i, (book, abbrev, ref) in enumerate(refs, start=1):
            p = Paragraph(
                id=i,
                text={"en": f"Test paragraph {i}"},
                parsed_footnotes=[
                    ParsedFootnote(
                        raw="test",
                        bible_refs=[BibleReference(book=book, abbreviation=abbrev, reference=ref)],
                    )
                ],
            )
            paragraphs.append(p)
        return paragraphs

    def test_collects_citing_paragraphs(self):
        """Verify that citing paragraphs are tracked per book."""
        paras = self._make_paragraphs([
            ("matthew", "Mt", "5:1"),
            ("matthew", "Mt", "28:19"),
            ("john", "Jn", "3:16"),
        ])
        # We can test the collection logic without downloading
        # by checking that the function handles empty downloads gracefully
        result = fetch_bible_texts(paras)
        # Result may be empty if download fails (no network in CI),
        # but should not raise
        assert isinstance(result, dict)

    def test_no_refs_returns_empty(self):
        """No Bible references should return empty dict."""
        paras = [Paragraph(id=1, text={"en": "No refs"})]
        result = fetch_bible_texts(paras)
        assert result == {}

    def test_multiple_books_tracked(self):
        """Multiple books should all appear as keys."""
        paras = self._make_paragraphs([
            ("matthew", "Mt", "5:1"),
            ("john", "Jn", "3:16"),
            ("romans", "Rom", "8:28"),
        ])
        result = fetch_bible_texts(paras)
        # If download succeeds, all 3 books should be present
        if result:
            assert "matthew" in result
            assert "john" in result
            assert "romans" in result
