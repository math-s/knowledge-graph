"""Tests for Phase 2: Church Fathers full-text pipeline."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.models import (
    AuthorSource,
    MultiLangText,
    Paragraph,
    ParsedFootnote,
    PatristicChapter,
    PatristicReference,
    PatristicSection,
    PatristicWork,
)
from pipeline.src.footnote_parser import parse_footnote, _extract_work_info, _AUTHOR_WORK_ABBREVS
from pipeline.src.fetch_patristic_works import (
    _slugify,
    _extract_text_from_html,
    _is_chapter_link,
    _discover_chapter_links,
)


# ── Work abbreviation tests ──────────────────────────────────────────────────


class TestWorkAbbreviations:
    """Tests for the _AUTHOR_WORK_ABBREVS mapping."""

    def test_augustine_has_works(self):
        assert "augustine" in _AUTHOR_WORK_ABBREVS
        assert "Conf." in _AUTHOR_WORK_ABBREVS["augustine"]
        assert _AUTHOR_WORK_ABBREVS["augustine"]["Conf."] == "confessions"

    def test_thomas_aquinas_has_summa(self):
        assert "thomas-aquinas" in _AUTHOR_WORK_ABBREVS
        assert "STh" in _AUTHOR_WORK_ABBREVS["thomas-aquinas"]
        assert _AUTHOR_WORK_ABBREVS["thomas-aquinas"]["STh"] == "summa-theologica"

    def test_irenaeus_has_against_heresies(self):
        assert "irenaeus" in _AUTHOR_WORK_ABBREVS
        assert "Adv. haeres." in _AUTHOR_WORK_ABBREVS["irenaeus"]
        assert _AUTHOR_WORK_ABBREVS["irenaeus"]["Adv. haeres."] == "against-heresies"

    def test_chrysostom_has_homilies(self):
        assert "john-chrysostom" in _AUTHOR_WORK_ABBREVS
        assert "Hom. in Mt." in _AUTHOR_WORK_ABBREVS["john-chrysostom"]

    def test_all_authors_have_string_work_ids(self):
        for author_id, works in _AUTHOR_WORK_ABBREVS.items():
            for abbrev, work_id in works.items():
                assert isinstance(work_id, str), f"Work ID for {author_id}/{abbrev} is not str"
                assert len(work_id) > 0, f"Empty work ID for {author_id}/{abbrev}"

    def test_all_work_ids_are_slugified(self):
        """Work IDs should be lowercase, hyphenated slugs."""
        for author_id, works in _AUTHOR_WORK_ABBREVS.items():
            for abbrev, work_id in works.items():
                assert work_id == work_id.lower(), f"Work ID not lowercase: {work_id}"
                assert " " not in work_id, f"Work ID contains space: {work_id}"


# ── Footnote work extraction tests ───────────────────────────────────────────


class TestFootnoteWorkExtraction:
    """Tests for extracting work info from footnote text."""

    def test_augustine_confessions(self):
        work_id, location = _extract_work_info(
            "St. Augustine, Conf. 1, 1, 1: PL 32, 659-661.", "augustine"
        )
        assert work_id == "confessions"
        assert "1, 1, 1" in location

    def test_augustine_city_of_god(self):
        work_id, location = _extract_work_info(
            "St. Augustine, De civ. Dei 14, 28: PL 41, 436.", "augustine"
        )
        assert work_id == "city-of-god"
        assert "14, 28" in location

    def test_thomas_aquinas_summa(self):
        work_id, location = _extract_work_info(
            "St. Thomas Aquinas, STh I, 1, 1.", "thomas-aquinas"
        )
        assert work_id == "summa-theologica"
        assert "I, 1, 1" in location

    def test_irenaeus_against_heresies(self):
        work_id, location = _extract_work_info(
            "St. Irenaeus, Adv. haeres. 3, 24, 1: PG 7/1, 966.", "irenaeus"
        )
        assert work_id == "against-heresies"
        assert "3, 24, 1" in location

    def test_chrysostom_homilies_on_matthew(self):
        work_id, location = _extract_work_info(
            "St. John Chrysostom, Hom. in Mt. 19, 4: PG 57, 278.", "john-chrysostom"
        )
        assert work_id == "homilies-on-matthew"
        assert "19, 4" in location

    def test_no_work_found(self):
        work_id, location = _extract_work_info(
            "St. Augustine was a great theologian.", "augustine"
        )
        assert work_id == ""
        assert location == ""

    def test_unknown_author(self):
        work_id, location = _extract_work_info(
            "Some text about something.", "unknown-author"
        )
        assert work_id == ""
        assert location == ""

    def test_john_damascene_orthodox_faith(self):
        work_id, location = _extract_work_info(
            "St. John Damascene, De fide orth. 3, 27: PG 94, 1098.", "john-damascene"
        )
        assert work_id == "exposition-of-orthodox-faith"

    def test_basil_on_holy_spirit(self):
        work_id, location = _extract_work_info(
            "St. Basil, De Spir. S. 15, 36: PG 32, 132.", "basil"
        )
        assert work_id == "on-the-holy-spirit"


class TestParseFootnoteWithWorks:
    """Tests that parse_footnote correctly extracts work info."""

    def test_augustine_confessions_full(self):
        pf = parse_footnote("St. Augustine, Conf. 1, 1, 1: PL 32, 659-661.")
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "augustine"
        assert pf.author_refs[0].work == "confessions"
        assert "1, 1, 1" in pf.author_refs[0].location

    def test_thomas_aquinas_summa_full(self):
        pf = parse_footnote("St. Thomas Aquinas, STh I, 2, 3.")
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "thomas-aquinas"
        assert pf.author_refs[0].work == "summa-theologica"

    def test_footnote_with_bible_and_author(self):
        pf = parse_footnote(
            "⇒ Mt 5:3; St. Augustine, De civ. Dei 14, 28: PL 41, 436."
        )
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].book == "matthew"
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].work == "city-of-god"

    def test_footnote_without_work_abbrev(self):
        """Author mentioned without a work abbreviation."""
        pf = parse_footnote("St. Augustine taught about grace.")
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "augustine"
        assert pf.author_refs[0].work == ""
        assert pf.author_refs[0].location == ""


# ── Fetcher utility tests ────────────────────────────────────────────────────


class TestSlugify:
    """Tests for the _slugify utility."""

    def test_simple_title(self):
        assert _slugify("Confessions") == "confessions"

    def test_title_with_spaces(self):
        assert _slugify("The City of God") == "the-city-of-god"

    def test_title_with_special_chars(self):
        assert _slugify("On Prayer (De Oratione)") == "on-prayer-de-oratione"

    def test_empty_string(self):
        assert _slugify("") == ""

    def test_long_title_truncation(self):
        long_title = "A" * 100
        result = _slugify(long_title)
        assert len(result) <= 60


class TestExtractTextFromHtml:
    """Tests for the HTML text extraction."""

    def test_simple_html(self):
        html = "<html><body><p>This is a paragraph of sufficient length to pass the filter.</p></body></html>"
        text = _extract_text_from_html(html)
        assert "paragraph" in text

    def test_skips_scripts(self):
        html = "<html><body><script>alert('bad')</script><p>This is clean text that should remain in output.</p></body></html>"
        text = _extract_text_from_html(html)
        assert "alert" not in text
        assert "clean text" in text

    def test_skips_short_paragraphs(self):
        html = "<html><body><p>Short</p><p>This paragraph has enough content to be included.</p></body></html>"
        text = _extract_text_from_html(html)
        assert "Short" not in text
        assert "enough content" in text

    def test_empty_html(self):
        text = _extract_text_from_html("<html></html>")
        assert text == ""


class TestIsChapterLink:
    """Tests for the chapter link detection."""

    def test_valid_chapter_link(self):
        assert _is_chapter_link(
            "/fathers/170101.htm",
            "https://www.newadvent.org/fathers/1701.htm",
        )

    def test_same_page_rejected(self):
        assert not _is_chapter_link(
            "/fathers/1701.htm",
            "https://www.newadvent.org/fathers/1701.htm",
        )

    def test_external_link_rejected(self):
        assert not _is_chapter_link(
            "https://www.example.com/page.htm",
            "https://www.newadvent.org/fathers/1701.htm",
        )

    def test_non_fathers_link_rejected(self):
        assert not _is_chapter_link(
            "/cathen/01234.htm",
            "https://www.newadvent.org/fathers/1701.htm",
        )

    def test_non_htm_rejected(self):
        assert not _is_chapter_link(
            "/fathers/170101.html",
            "https://www.newadvent.org/fathers/1701.htm",
        )


class TestDiscoverChapterLinks:
    """Tests for chapter link discovery from index pages."""

    def test_discovers_father_links(self):
        html = """
        <html><body>
            <a href="/fathers/170101.htm">Book I</a>
            <a href="/fathers/170102.htm">Book II</a>
            <a href="https://www.example.com/other.htm">External</a>
        </body></html>
        """
        chapters = _discover_chapter_links(
            html, "https://www.newadvent.org/fathers/1701.htm"
        )
        assert len(chapters) == 2
        assert chapters[0][0] == "Book I"
        assert "170101" in chapters[0][1]

    def test_skips_navigation_links(self):
        html = """
        <html><body>
            <a href="/">Home</a>
            <a href="/fathers/">Fathers</a>
            <a href="/fathers/170101.htm">Book I</a>
        </body></html>
        """
        chapters = _discover_chapter_links(
            html, "https://www.newadvent.org/fathers/1701.htm"
        )
        assert len(chapters) == 1

    def test_empty_page(self):
        chapters = _discover_chapter_links(
            "<html><body></body></html>",
            "https://www.newadvent.org/fathers/1701.htm",
        )
        assert chapters == []


# ── Graph builder tests ───────────────────────────────────────────────────────


class TestGraphBuilderPatristic:
    """Tests for add_patristic_work_hierarchy."""

    def _make_works(self) -> dict[str, list[PatristicWork]]:
        """Create minimal patristic work data for testing."""
        section = PatristicSection(
            id="augustine/confessions/1/1",
            chapter_id="augustine/confessions/1",
            number=1,
            text={"en": "Late have I loved you, beauty so old and so new..."},
        )
        chapter = PatristicChapter(
            id="augustine/confessions/1",
            work_id="augustine/confessions",
            number=1,
            title="Book I",
            sections=[section],
        )
        work = PatristicWork(
            id="augustine/confessions",
            author_id="augustine",
            title="Confessions",
            source_url="https://www.newadvent.org/fathers/1701.htm",
            chapters=[chapter],
        )
        return {"augustine": [work]}

    def test_creates_work_nodes(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        G.add_node("author:augustine", node_type="author", label="St. Augustine")

        works = self._make_works()
        G = add_patristic_work_hierarchy(G, works, [])

        assert G.has_node("patristic-work:augustine/confessions")
        assert G.nodes["patristic-work:augustine/confessions"]["node_type"] == "patristic-work"
        assert G.nodes["patristic-work:augustine/confessions"]["label"] == "Confessions"

    def test_creates_child_of_edges(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        G.add_node("author:augustine", node_type="author", label="St. Augustine")

        works = self._make_works()
        G = add_patristic_work_hierarchy(G, works, [])

        assert G.has_edge("patristic-work:augustine/confessions", "author:augustine")
        edge_data = G.edges["patristic-work:augustine/confessions", "author:augustine"]
        assert edge_data["edge_type"] == "child_of"

    def test_rewires_cites_edges(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        G.add_node("author:augustine", node_type="author", label="St. Augustine")
        G.add_node("p:1", node_type="paragraph")

        para = Paragraph(
            id=1,
            text={"en": "Test"},
            parsed_footnotes=[
                ParsedFootnote(
                    raw="St. Augustine, Conf. 1, 1, 1",
                    author_refs=[
                        PatristicReference(
                            author="augustine",
                            work="confessions",
                            location="1, 1, 1",
                        )
                    ],
                )
            ],
        )

        works = self._make_works()
        G = add_patristic_work_hierarchy(G, works, [para])

        assert G.has_edge("p:1", "patristic-work:augustine/confessions")

    def test_skips_missing_author_node(self):
        """Works for an author without a graph node should be skipped."""
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        # No author:augustine node

        works = self._make_works()
        G = add_patristic_work_hierarchy(G, works, [])

        assert not G.has_node("patristic-work:augustine/confessions")

    def test_empty_works(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        G = add_patristic_work_hierarchy(G, {}, [])
        assert G.number_of_nodes() == 0

    def test_multiple_works_per_author(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        G.add_node("author:augustine", node_type="author", label="St. Augustine")

        work1 = PatristicWork(
            id="augustine/confessions",
            author_id="augustine",
            title="Confessions",
            chapters=[],
        )
        work2 = PatristicWork(
            id="augustine/city-of-god",
            author_id="augustine",
            title="The City of God",
            chapters=[],
        )

        works = {"augustine": [work1, work2]}
        G = add_patristic_work_hierarchy(G, works, [])

        assert G.has_node("patristic-work:augustine/confessions")
        assert G.has_node("patristic-work:augustine/city-of-god")
        assert G.has_edge("patristic-work:augustine/confessions", "author:augustine")
        assert G.has_edge("patristic-work:augustine/city-of-god", "author:augustine")

    def test_node_counts(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_patristic_work_hierarchy

        G = nx.Graph()
        G.add_node("author:augustine", node_type="author")
        G.add_node("author:irenaeus", node_type="author")

        works = {
            "augustine": [
                PatristicWork(id="augustine/confessions", author_id="augustine", title="Confessions"),
                PatristicWork(id="augustine/city-of-god", author_id="augustine", title="City of God"),
            ],
            "irenaeus": [
                PatristicWork(id="irenaeus/against-heresies", author_id="irenaeus", title="Against Heresies"),
            ],
        }
        G = add_patristic_work_hierarchy(G, works, [])

        # 2 authors + 3 works = 5 nodes
        assert G.number_of_nodes() == 5
        # 3 child_of edges (one per work)
        assert G.number_of_edges() == 3


# ── Export tests ──────────────────────────────────────────────────────────────


class TestExportAuthorsFull:
    """Tests for the export_authors_full function."""

    def test_creates_meta_json(self, tmp_path):
        from unittest.mock import patch
        from pipeline.src.export import export_authors_full

        author_sources = {
            "augustine": AuthorSource(
                id="augustine",
                name="St. Augustine",
                era="354-430 AD",
                citing_paragraphs=[1, 2, 3],
            ),
        }

        work = PatristicWork(
            id="augustine/confessions",
            author_id="augustine",
            title="Confessions",
            chapters=[
                PatristicChapter(
                    id="augustine/confessions/1",
                    work_id="augustine/confessions",
                    number=1,
                    title="Book I",
                    sections=[
                        PatristicSection(
                            id="augustine/confessions/1/1",
                            chapter_id="augustine/confessions/1",
                            number=1,
                            text={"en": "Late have I loved you..."},
                        ),
                    ],
                ),
            ],
        )

        patristic_works = {"augustine": [work]}

        with patch("pipeline.src.export.WEB_DATA_DIR", tmp_path):
            export_authors_full(author_sources, patristic_works)

        import json

        # Check meta file
        meta_path = tmp_path / "sources-authors-meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert "augustine" in meta
        assert meta["augustine"]["name"] == "St. Augustine"
        assert meta["augustine"]["work_count"] == 1
        assert meta["augustine"]["work_titles"] == ["Confessions"]

        # Check per-author work file
        works_path = tmp_path / "sources-authors-works" / "augustine.json"
        assert works_path.exists()
        works_data = json.loads(works_path.read_text())
        assert len(works_data) == 1
        assert works_data[0]["title"] == "Confessions"
        assert len(works_data[0]["chapters"]) == 1
        assert works_data[0]["chapters"][0]["sections"][0]["text"]["en"] == "Late have I loved you..."

    def test_empty_works(self, tmp_path):
        from unittest.mock import patch
        from pipeline.src.export import export_authors_full

        author_sources = {
            "augustine": AuthorSource(
                id="augustine",
                name="St. Augustine",
                era="354-430 AD",
            ),
        }

        with patch("pipeline.src.export.WEB_DATA_DIR", tmp_path):
            export_authors_full(author_sources, {})

        import json

        meta_path = tmp_path / "sources-authors-meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["augustine"]["work_count"] == 0
