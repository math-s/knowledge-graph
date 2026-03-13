"""Tests for the full Bible pipeline: DRB, Vulgate, Greek, Portuguese, cross-refs, merge."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.models import (
    BibleBookFull,
    BibleChapter,
    MultiLangText,
    resolve_lang,
    SUPPORTED_LANGS,
    FALLBACK_ORDER,
)
from pipeline.src.merge_languages import merge_bible_languages, merge_multilang_text
from pipeline.src.fetch_bible_drb import BOOK_ORDER, BOOK_CATEGORIES


# ── MultiLangText and resolve_lang tests ─────────────────────────────────────


class TestMultiLangText:
    """Tests for the MultiLangText type and resolve_lang helper."""

    def test_resolve_preferred_language(self):
        text: MultiLangText = {"en": "Hello", "la": "Salve", "pt": "Olá"}
        assert resolve_lang(text, "en") == "Hello"
        assert resolve_lang(text, "la") == "Salve"
        assert resolve_lang(text, "pt") == "Olá"

    def test_resolve_fallback_to_latin(self):
        text: MultiLangText = {"la": "In principio", "en": "In the beginning"}
        # Greek not available, should fall back to Latin (first in fallback order)
        assert resolve_lang(text, "el") == "In principio"

    def test_resolve_fallback_to_english(self):
        text: MultiLangText = {"en": "In the beginning"}
        # Only English available
        assert resolve_lang(text, "la") == "In the beginning"
        assert resolve_lang(text, "pt") == "In the beginning"
        assert resolve_lang(text, "el") == "In the beginning"

    def test_resolve_empty_text(self):
        text: MultiLangText = {}
        assert resolve_lang(text, "en") == ""

    def test_resolve_first_available(self):
        text: MultiLangText = {"el": "Ἐν ἀρχῇ"}
        assert resolve_lang(text, "pt") == "Ἐν ἀρχῇ"

    def test_supported_langs(self):
        assert "la" in SUPPORTED_LANGS
        assert "en" in SUPPORTED_LANGS
        assert "pt" in SUPPORTED_LANGS
        assert "el" in SUPPORTED_LANGS

    def test_fallback_order(self):
        assert FALLBACK_ORDER[0] == "la"
        assert FALLBACK_ORDER[1] == "en"


# ── Book metadata tests ──────────────────────────────────────────────────────


class TestBookMetadata:
    """Tests for Bible book ordering and categories."""

    def test_book_order_has_73_books(self):
        """Catholic Bible should have 73 books."""
        assert len(BOOK_ORDER) == 73

    def test_deuterocanonical_books_present(self):
        """Deuterocanonical books must be included."""
        book_ids = {b[0] for b in BOOK_ORDER}
        deuterocanonical = {
            "tobit", "judith", "wisdom", "sirach", "baruch",
            "1-maccabees", "2-maccabees",
        }
        for book in deuterocanonical:
            assert book in book_ids, f"Missing deuterocanonical book: {book}"

    def test_ot_and_nt_separation(self):
        """OT and NT books should be correctly labeled."""
        ot_books = [b for b in BOOK_ORDER if b[4] == "old"]
        nt_books = [b for b in BOOK_ORDER if b[4] == "new"]
        # Catholic OT: 46 books, NT: 27 books
        assert len(ot_books) == 46
        assert len(nt_books) == 27

    def test_all_books_have_categories(self):
        """Every book should have a category assignment."""
        for canon_id, _, _, _, _ in BOOK_ORDER:
            assert canon_id in BOOK_CATEGORIES, f"Missing category for: {canon_id}"

    def test_book_categories_valid(self):
        """All categories should be from the expected set."""
        valid_categories = {
            "pentateuch", "historical", "wisdom", "prophetic",
            "gospel", "historical-nt", "epistle", "apocalyptic",
        }
        for canon_id, cat in BOOK_CATEGORIES.items():
            assert cat in valid_categories, f"Invalid category '{cat}' for {canon_id}"


# ── Merge languages tests ────────────────────────────────────────────────────


class TestMergeLanguages:
    """Tests for the merge_languages module."""

    def _make_book(
        self, book_id: str, lang: str, verses: dict[int, dict[int, str]]
    ) -> BibleBookFull:
        """Create a single-language BibleBookFull for testing."""
        chapters: dict[int, BibleChapter] = {}
        total = 0
        for ch_num, ch_verses in verses.items():
            verse_texts: dict[int, MultiLangText] = {}
            for v_num, text in ch_verses.items():
                verse_texts[v_num] = {lang: text}
                total += 1
            chapters[ch_num] = BibleChapter(
                book_id=book_id,
                chapter=ch_num,
                verses=verse_texts,
            )
        return BibleBookFull(
            id=book_id,
            name="Test Book",
            abbreviation="TB",
            testament="new",
            chapters=chapters,
            total_verses=total,
        )

    def test_merge_two_languages(self):
        en_book = self._make_book("matthew", "en", {1: {1: "In the beginning"}})
        la_book = self._make_book("matthew", "la", {1: {1: "In principio"}})

        en_dict = {"matthew": en_book}
        la_dict = {"matthew": la_book}

        result = merge_bible_languages(en_dict, la_dict)

        assert "matthew" in result
        ch1 = result["matthew"].chapters[1]
        assert "en" in ch1.verses[1]
        assert "la" in ch1.verses[1]
        assert ch1.verses[1]["en"] == "In the beginning"
        assert ch1.verses[1]["la"] == "In principio"

    def test_merge_preserves_metadata(self):
        en_book = self._make_book("matthew", "en", {1: {1: "test"}})
        en_book.name = "Matthew"
        en_book.abbreviation = "Mt"
        en_book.testament = "new"

        result = merge_bible_languages({"matthew": en_book})

        assert result["matthew"].name == "Matthew"
        assert result["matthew"].abbreviation == "Mt"
        assert result["matthew"].testament == "new"

    def test_merge_empty_dicts(self):
        result = merge_bible_languages({}, {})
        assert result == {}

    def test_merge_no_args(self):
        result = merge_bible_languages()
        assert result == {}

    def test_merge_asymmetric_books(self):
        """One language has a book the other doesn't."""
        en_book = self._make_book("matthew", "en", {1: {1: "test"}})
        la_book = self._make_book("tobit", "la", {1: {1: "Tobias"}})

        result = merge_bible_languages({"matthew": en_book}, {"tobit": la_book})

        assert "matthew" in result
        assert "tobit" in result
        assert "en" in result["matthew"].chapters[1].verses[1]
        assert "la" in result["tobit"].chapters[1].verses[1]

    def test_merge_asymmetric_verses(self):
        """One language has extra verses the other doesn't."""
        en_book = self._make_book("matthew", "en", {1: {1: "v1", 2: "v2"}})
        la_book = self._make_book("matthew", "la", {1: {1: "v1_la"}})

        result = merge_bible_languages({"matthew": en_book}, {"matthew": la_book})

        ch1 = result["matthew"].chapters[1]
        # Verse 1 should have both languages
        assert "en" in ch1.verses[1]
        assert "la" in ch1.verses[1]
        # Verse 2 should only have English
        assert "en" in ch1.verses[2]
        assert "la" not in ch1.verses[2]

    def test_merge_citing_paragraphs(self):
        """Citing paragraphs should be merged from all sources."""
        en_book = self._make_book("matthew", "en", {1: {1: "test"}})
        en_book.citing_paragraphs = [1, 3, 5]
        la_book = self._make_book("matthew", "la", {1: {1: "test_la"}})
        la_book.citing_paragraphs = [2, 4]

        result = merge_bible_languages({"matthew": en_book}, {"matthew": la_book})

        assert result["matthew"].citing_paragraphs == [1, 2, 3, 4, 5]


class TestMergeMultilangText:
    """Tests for the merge_multilang_text helper."""

    def test_merge_two_texts(self):
        result = merge_multilang_text({"en": "Hello"}, {"la": "Salve"})
        assert result == {"en": "Hello", "la": "Salve"}

    def test_later_overrides_earlier(self):
        result = merge_multilang_text({"en": "old"}, {"en": "new"})
        assert result == {"en": "new"}

    def test_merge_empty(self):
        result = merge_multilang_text({}, {})
        assert result == {}


# ── Graph builder Bible hierarchy tests ──────────────────────────────────────


class TestGraphBuilderBible:
    """Tests for add_bible_hierarchy and add_bible_crossref_edges."""

    def _make_bible_books(self) -> dict[str, BibleBookFull]:
        """Create minimal Bible data for testing."""
        return {
            "matthew": BibleBookFull(
                id="matthew",
                name="Matthew",
                abbreviation="Mt",
                testament="new",
                category="gospel",
                chapters={
                    1: BibleChapter(
                        book_id="matthew",
                        chapter=1,
                        verses={
                            1: {"en": "The book of the generation..."},
                            2: {"en": "Abraham begat Isaac..."},
                        },
                    ),
                    5: BibleChapter(
                        book_id="matthew",
                        chapter=5,
                        verses={
                            3: {"en": "Blessed are the poor in spirit..."},
                        },
                    ),
                },
                total_verses=3,
            ),
            "genesis": BibleBookFull(
                id="genesis",
                name="Genesis",
                abbreviation="Gen",
                testament="old",
                category="pentateuch",
                chapters={
                    1: BibleChapter(
                        book_id="genesis",
                        chapter=1,
                        verses={
                            1: {"en": "In the beginning God created..."},
                        },
                    ),
                },
                total_verses=1,
            ),
        }

    def test_bible_hierarchy_creates_testament_nodes(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy
        from pipeline.src.models import Paragraph

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        assert G.has_node("bible-testament:ot")
        assert G.has_node("bible-testament:nt")
        assert G.nodes["bible-testament:ot"]["node_type"] == "bible-testament"

    def test_bible_hierarchy_creates_book_nodes(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        assert G.has_node("bible-book:matthew")
        assert G.has_node("bible-book:genesis")
        assert G.nodes["bible-book:matthew"]["node_type"] == "bible-book"

    def test_bible_hierarchy_creates_chapter_nodes(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        assert G.has_node("bible-chapter:matthew-1")
        assert G.has_node("bible-chapter:matthew-5")
        assert G.has_node("bible-chapter:genesis-1")

    def test_bible_hierarchy_creates_verse_nodes(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        assert G.has_node("bible-verse:matthew-1:1")
        assert G.has_node("bible-verse:matthew-1:2")
        assert G.has_node("bible-verse:matthew-5:3")
        assert G.has_node("bible-verse:genesis-1:1")

    def test_bible_hierarchy_child_of_edges(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        # Book -> testament
        assert G.has_edge("bible-book:matthew", "bible-testament:nt")
        assert G.has_edge("bible-book:genesis", "bible-testament:ot")
        # Chapter -> book
        assert G.has_edge("bible-chapter:matthew-1", "bible-book:matthew")
        # Verse -> chapter
        assert G.has_edge("bible-verse:matthew-1:1", "bible-chapter:matthew-1")

    def test_bible_hierarchy_rewires_cites(self):
        """CCC paragraph citing Mt 5:3 should get an edge to the verse node."""
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy
        from pipeline.src.models import (
            Paragraph,
            ParsedFootnote,
            BibleReference,
        )

        G = nx.Graph()
        # Add a paragraph node first
        G.add_node("p:1", node_type="paragraph")

        para = Paragraph(
            id=1,
            text={"en": "Test"},
            parsed_footnotes=[
                ParsedFootnote(
                    raw="test",
                    bible_refs=[BibleReference(book="matthew", abbreviation="Mt", reference="5:3")],
                )
            ],
        )

        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [para])

        # Should have a cites edge to the specific verse
        assert G.has_edge("p:1", "bible-verse:matthew-5:3")

    def test_bible_crossref_edges(self):
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy, add_bible_crossref_edges

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        crossrefs = {
            "genesis-1:1": ["matthew-1:1"],
        }
        G = add_bible_crossref_edges(G, crossrefs)

        assert G.has_edge("bible-verse:genesis-1:1", "bible-verse:matthew-1:1")
        edge_data = G.edges["bible-verse:genesis-1:1", "bible-verse:matthew-1:1"]
        assert edge_data["edge_type"] == "bible_cross_reference"

    def test_empty_bible_data(self):
        """Empty Bible data should not crash."""
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy

        G = nx.Graph()
        G = add_bible_hierarchy(G, {}, [])
        assert G.number_of_nodes() == 0

    def test_node_counts(self):
        """Verify expected node counts for test data."""
        import networkx as nx
        from pipeline.src.graph_builder import add_bible_hierarchy

        G = nx.Graph()
        books = self._make_bible_books()
        G = add_bible_hierarchy(G, books, [])

        # 2 testaments + 2 books + 3 chapters + 4 verses = 11
        assert G.number_of_nodes() == 11
