"""Tests for the Greek patristic text fetcher."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_patristic_greek import (
    GREEK_FATHER_IDS,
    _GREEK_CATALOG,
    _contains_greek,
    _extract_greek_text,
    _match_work,
    _merge_greek_into_work,
)
from pipeline.src.models import (
    PatristicChapter,
    PatristicSection,
    PatristicWork,
)


# ── Constants tests ──────────────────────────────────────────────────────────


class TestGreekFatherIds:
    """Tests for the GREEK_FATHER_IDS set."""

    def test_chrysostom_is_greek(self):
        assert "john-chrysostom" in GREEK_FATHER_IDS

    def test_basil_is_greek(self):
        assert "basil" in GREEK_FATHER_IDS

    def test_athanasius_is_greek(self):
        assert "athanasius" in GREEK_FATHER_IDS

    def test_irenaeus_is_greek(self):
        assert "irenaeus" in GREEK_FATHER_IDS

    def test_ignatius_is_greek(self):
        assert "ignatius-antioch" in GREEK_FATHER_IDS

    def test_augustine_is_not_greek(self):
        assert "augustine" not in GREEK_FATHER_IDS

    def test_thomas_aquinas_is_not_greek(self):
        assert "thomas-aquinas" not in GREEK_FATHER_IDS


class TestGreekCatalog:
    """Tests for the _GREEK_CATALOG mapping."""

    def test_has_some_entries(self):
        assert len(_GREEK_CATALOG) > 0

    def test_all_entries_have_required_keys(self):
        for author_id, entries in _GREEK_CATALOG.items():
            for entry in entries:
                assert "work_pattern" in entry, f"Missing work_pattern in {author_id}"
                assert "url" in entry, f"Missing url in {author_id}"
                assert "chapter_count" in entry, f"Missing chapter_count in {author_id}"

    def test_urls_are_valid(self):
        for author_id, entries in _GREEK_CATALOG.items():
            for entry in entries:
                url = entry["url"]
                assert url.startswith("https://"), f"Invalid URL for {author_id}: {url}"

    def test_all_catalog_authors_are_greek_fathers(self):
        for author_id in _GREEK_CATALOG:
            assert author_id in GREEK_FATHER_IDS, (
                f"Catalog author {author_id} not in GREEK_FATHER_IDS"
            )


# ── Greek detection tests ────────────────────────────────────────────────────


class TestContainsGreek:
    """Tests for the _contains_greek function."""

    def test_basic_greek(self):
        assert _contains_greek("Ἐν ἀρχῇ ἦν ὁ Λόγος")

    def test_extended_greek(self):
        assert _contains_greek("ἠγάπησεν ὁ θεὸς τὸν κόσμον")

    def test_english_only(self):
        assert not _contains_greek("In the beginning was the Word")

    def test_latin_only(self):
        assert not _contains_greek("In principio erat Verbum")

    def test_empty_string(self):
        assert not _contains_greek("")

    def test_mixed_greek_and_english(self):
        assert _contains_greek("The word Λόγος means Word")


# ── Text extraction tests ────────────────────────────────────────────────────


class TestExtractGreekText:
    """Tests for the _extract_greek_text function."""

    def test_extracts_greek_paragraphs(self):
        html = """
        <html><body>
        <p>This is English text that should be excluded from output.</p>
        <p>Ἐν ἀρχῇ ἦν ὁ Λόγος, καὶ ὁ Λόγος ἦν πρὸς τὸν Θεόν.</p>
        </body></html>
        """
        text = _extract_greek_text(html, greek_only=True)
        assert "Ἐν ἀρχῇ" in text
        assert "English text" not in text

    def test_includes_all_when_greek_only_false(self):
        html = """
        <html><body>
        <p>This is English text that should be included here.</p>
        <p>Ἐν ἀρχῇ ἦν ὁ Λόγος, καὶ ὁ Λόγος ἦν πρὸς τὸν Θεόν.</p>
        </body></html>
        """
        text = _extract_greek_text(html, greek_only=False)
        assert "English text" in text
        assert "Ἐν ἀρχῇ" in text

    def test_skips_scripts(self):
        html = """
        <html><body>
        <script>var x = 1;</script>
        <p>Ἐν ἀρχῇ ἦν ὁ Λόγος, καὶ ὁ Λόγος ἦν πρὸς τὸν Θεόν.</p>
        </body></html>
        """
        text = _extract_greek_text(html, greek_only=True)
        assert "var x" not in text
        assert "Ἐν ἀρχῇ" in text

    def test_empty_html(self):
        text = _extract_greek_text("<html><body></body></html>")
        assert text == ""

    def test_no_body(self):
        text = _extract_greek_text("<html></html>")
        assert text == ""

    def test_skips_short_fragments(self):
        html = """
        <html><body>
        <p>Θεός</p>
        <p>Ἐν ἀρχῇ ἦν ὁ Λόγος, καὶ ὁ Λόγος ἦν πρὸς τὸν Θεόν.</p>
        </body></html>
        """
        text = _extract_greek_text(html, greek_only=True)
        # Short fragment "Θεός" alone (< 10 chars) should be skipped
        lines = [line for line in text.split("\n\n") if line.strip()]
        assert len(lines) == 1


# ── Work matching tests ──────────────────────────────────────────────────────


class TestMatchWork:
    """Tests for the _match_work function."""

    def test_matches_by_id(self):
        work = PatristicWork(
            id="irenaeus/against-heresies",
            author_id="irenaeus",
            title="Against Heresies",
        )
        entry = {"work_pattern": "heresies", "url": "http://example.com", "chapter_count": 1}
        assert _match_work(work, entry)

    def test_matches_by_title(self):
        work = PatristicWork(
            id="basil/some-slug",
            author_id="basil",
            title="On the Holy Spirit",
        )
        entry = {"work_pattern": "holy-spirit", "url": "http://example.com", "chapter_count": 1}
        assert _match_work(work, entry)

    def test_no_match(self):
        work = PatristicWork(
            id="basil/on-the-hexaemeron",
            author_id="basil",
            title="On the Hexaemeron",
        )
        entry = {"work_pattern": "holy-spirit", "url": "http://example.com", "chapter_count": 1}
        assert not _match_work(work, entry)


# ── Merge tests ──────────────────────────────────────────────────────────────


class TestMergeGreekIntoWork:
    """Tests for the _merge_greek_into_work function."""

    def _make_work(self, chapters: int = 3) -> PatristicWork:
        """Create a test work with English-only sections."""
        chs = []
        for i in range(1, chapters + 1):
            sec = PatristicSection(
                id=f"test/work/{i}/1",
                chapter_id=f"test/work/{i}",
                number=1,
                text={"en": f"English text for chapter {i}."},
            )
            ch = PatristicChapter(
                id=f"test/work/{i}",
                work_id="test/work",
                number=i,
                title=f"Chapter {i}",
                sections=[sec],
            )
            chs.append(ch)
        return PatristicWork(
            id="test/work",
            author_id="test",
            title="Test Work",
            chapters=chs,
        )

    def test_merges_matching_chapters(self):
        work = self._make_work(3)
        greek = {1: "Ἐν ἀρχῇ chapter 1", 2: "Ἐν ἀρχῇ chapter 2"}
        merged = _merge_greek_into_work(work, greek)
        assert merged == 2
        assert work.chapters[0].sections[0].text["el"] == "Ἐν ἀρχῇ chapter 1"
        assert work.chapters[1].sections[0].text["el"] == "Ἐν ἀρχῇ chapter 2"
        assert "el" not in work.chapters[2].sections[0].text

    def test_preserves_english(self):
        work = self._make_work(1)
        greek = {1: "Ἐν ἀρχῇ"}
        _merge_greek_into_work(work, greek)
        assert work.chapters[0].sections[0].text["en"] == "English text for chapter 1."
        assert work.chapters[0].sections[0].text["el"] == "Ἐν ἀρχῇ"

    def test_empty_greek_dict(self):
        work = self._make_work(2)
        merged = _merge_greek_into_work(work, {})
        assert merged == 0

    def test_no_chapters(self):
        work = PatristicWork(
            id="test/work",
            author_id="test",
            title="Empty",
            chapters=[],
        )
        merged = _merge_greek_into_work(work, {1: "Ἐν ἀρχῇ"})
        assert merged == 0

    def test_no_double_merge(self):
        """Merging twice should not overwrite existing Greek text."""
        work = self._make_work(1)
        work.chapters[0].sections[0].text["el"] = "Already present"
        greek = {1: "New Greek text"}
        merged = _merge_greek_into_work(work, greek)
        assert merged == 0
        assert work.chapters[0].sections[0].text["el"] == "Already present"

    def test_coexists_with_latin(self):
        """Greek and Latin should coexist in the same section."""
        work = self._make_work(1)
        work.chapters[0].sections[0].text["la"] = "In principio"
        greek = {1: "Ἐν ἀρχῇ"}
        merged = _merge_greek_into_work(work, greek)
        assert merged == 1
        assert work.chapters[0].sections[0].text["en"] == "English text for chapter 1."
        assert work.chapters[0].sections[0].text["la"] == "In principio"
        assert work.chapters[0].sections[0].text["el"] == "Ἐν ἀρχῇ"
