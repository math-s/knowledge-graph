"""Tests for the Latin patristic text fetcher."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.fetch_patristic_latin import (
    LATIN_FATHER_IDS,
    _LATIN_CATALOG,
    _extract_latin_text,
    _match_work,
    _merge_latin_into_work,
)
from pipeline.src.models import (
    PatristicChapter,
    PatristicSection,
    PatristicWork,
)


# ── Constants tests ──────────────────────────────────────────────────────────


class TestLatinFatherIds:
    """Tests for the LATIN_FATHER_IDS set."""

    def test_augustine_is_latin(self):
        assert "augustine" in LATIN_FATHER_IDS

    def test_thomas_aquinas_is_latin(self):
        assert "thomas-aquinas" in LATIN_FATHER_IDS

    def test_ambrose_is_latin(self):
        assert "ambrose" in LATIN_FATHER_IDS

    def test_jerome_is_latin(self):
        assert "jerome" in LATIN_FATHER_IDS

    def test_tertullian_is_latin(self):
        assert "tertullian" in LATIN_FATHER_IDS

    def test_chrysostom_is_not_latin(self):
        assert "john-chrysostom" not in LATIN_FATHER_IDS

    def test_basil_is_not_latin(self):
        assert "basil" not in LATIN_FATHER_IDS


class TestLatinCatalog:
    """Tests for the _LATIN_CATALOG mapping."""

    def test_augustine_has_confessions(self):
        entries = _LATIN_CATALOG.get("augustine", [])
        patterns = [e["work_pattern"] for e in entries]
        assert "confessions" in patterns

    def test_augustine_has_city_of_god(self):
        entries = _LATIN_CATALOG.get("augustine", [])
        patterns = [e["work_pattern"] for e in entries]
        assert "city-of-god" in patterns

    def test_ambrose_has_entries(self):
        assert "ambrose" in _LATIN_CATALOG
        assert len(_LATIN_CATALOG["ambrose"]) > 0

    def test_tertullian_has_entries(self):
        assert "tertullian" in _LATIN_CATALOG
        assert len(_LATIN_CATALOG["tertullian"]) > 0

    def test_all_entries_have_required_keys(self):
        for author_id, entries in _LATIN_CATALOG.items():
            for entry in entries:
                assert "work_pattern" in entry, f"Missing work_pattern in {author_id}"
                assert "url" in entry, f"Missing url in {author_id}"
                assert "chapter_count" in entry, f"Missing chapter_count in {author_id}"

    def test_urls_are_valid(self):
        for author_id, entries in _LATIN_CATALOG.items():
            for entry in entries:
                url = entry["url"]
                assert url.startswith("https://"), f"Invalid URL for {author_id}: {url}"

    def test_multi_chapter_works_have_templates(self):
        """Works with >1 chapter should have a URL template."""
        for author_id, entries in _LATIN_CATALOG.items():
            for entry in entries:
                if entry["chapter_count"] > 1:
                    assert entry.get("chapter_url_template"), (
                        f"Missing template for multi-chapter work in {author_id}: {entry['work_pattern']}"
                    )


# ── Text extraction tests ────────────────────────────────────────────────────


class TestExtractLatinText:
    """Tests for the _extract_latin_text function."""

    def test_simple_html(self):
        html = """
        <html><body>
        <p>In principio erat Verbum et Verbum erat apud Deum.</p>
        </body></html>
        """
        text = _extract_latin_text(html)
        assert "In principio" in text

    def test_skips_short_fragments(self):
        html = """
        <html><body>
        <p>Short</p>
        <p>Omnis homo naturaliter scire desiderat quae sit causa rerum.</p>
        </body></html>
        """
        text = _extract_latin_text(html)
        assert "Short" not in text
        assert "naturaliter" in text

    def test_skips_scripts(self):
        html = """
        <html><body>
        <script>var x = 1;</script>
        <p>Deus est summum bonum et fons omnis boni.</p>
        </body></html>
        """
        text = _extract_latin_text(html)
        assert "var x" not in text
        assert "summum bonum" in text

    def test_empty_html(self):
        text = _extract_latin_text("<html><body></body></html>")
        assert text == ""

    def test_no_body(self):
        text = _extract_latin_text("<html></html>")
        assert text == ""

    def test_collapses_whitespace(self):
        html = """
        <html><body>
        <p>Deus   est    summum    bonum    et    fons    omnis    boni.</p>
        </body></html>
        """
        text = _extract_latin_text(html)
        assert "  " not in text


# ── Work matching tests ──────────────────────────────────────────────────────


class TestMatchWork:
    """Tests for the _match_work function."""

    def test_matches_by_id(self):
        work = PatristicWork(
            id="augustine/confessions",
            author_id="augustine",
            title="The Confessions of St. Augustine",
        )
        entry = {"work_pattern": "confessions", "url": "http://example.com", "chapter_count": 1}
        assert _match_work(work, entry)

    def test_matches_by_title(self):
        work = PatristicWork(
            id="augustine/some-slug",
            author_id="augustine",
            title="Confessions",
        )
        entry = {"work_pattern": "confessions", "url": "http://example.com", "chapter_count": 1}
        assert _match_work(work, entry)

    def test_no_match(self):
        work = PatristicWork(
            id="augustine/on-the-trinity",
            author_id="augustine",
            title="On the Trinity",
        )
        entry = {"work_pattern": "confessions", "url": "http://example.com", "chapter_count": 1}
        assert not _match_work(work, entry)

    def test_case_insensitive(self):
        work = PatristicWork(
            id="augustine/CONFESSIONS",
            author_id="augustine",
            title="CONFESSIONS",
        )
        entry = {"work_pattern": "confessions", "url": "http://example.com", "chapter_count": 1}
        assert _match_work(work, entry)


# ── Merge tests ──────────────────────────────────────────────────────────────


class TestMergeLatinIntoWork:
    """Tests for the _merge_latin_into_work function."""

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
        latin = {1: "Latin chapter 1", 2: "Latin chapter 2"}
        merged = _merge_latin_into_work(work, latin)
        assert merged == 2
        assert work.chapters[0].sections[0].text["la"] == "Latin chapter 1"
        assert work.chapters[1].sections[0].text["la"] == "Latin chapter 2"
        assert "la" not in work.chapters[2].sections[0].text

    def test_preserves_english(self):
        work = self._make_work(1)
        latin = {1: "In principio"}
        _merge_latin_into_work(work, latin)
        assert work.chapters[0].sections[0].text["en"] == "English text for chapter 1."
        assert work.chapters[0].sections[0].text["la"] == "In principio"

    def test_empty_latin_dict(self):
        work = self._make_work(2)
        merged = _merge_latin_into_work(work, {})
        assert merged == 0

    def test_no_chapters(self):
        work = PatristicWork(
            id="test/work",
            author_id="test",
            title="Empty",
            chapters=[],
        )
        merged = _merge_latin_into_work(work, {1: "Latin text"})
        assert merged == 0

    def test_no_double_merge(self):
        """Merging twice should not overwrite existing Latin text."""
        work = self._make_work(1)
        work.chapters[0].sections[0].text["la"] = "Already present"
        latin = {1: "New Latin text"}
        merged = _merge_latin_into_work(work, latin)
        assert merged == 0  # Should not overwrite
        assert work.chapters[0].sections[0].text["la"] == "Already present"
