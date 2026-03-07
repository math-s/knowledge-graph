"""Tests for footnote_parser module."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.footnote_parser import parse_footnote, parse_all_footnotes
from pipeline.src.models import Paragraph


class TestParseBibleReference:
    """Test Bible reference extraction from footnotes."""

    def test_simple_bible_ref(self):
        pf = parse_footnote("⇒ Mt 28:19-20")
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].book == "matthew"
        assert pf.bible_refs[0].abbreviation == "Mt"

    def test_multiple_bible_refs(self):
        pf = parse_footnote("⇒ Jn 3:16; ⇒ Rom 8:28; ⇒ Gen 1:1")
        assert len(pf.bible_refs) == 3
        books = {r.book for r in pf.bible_refs}
        assert books == {"john", "romans", "genesis"}

    def test_roman_numeral_book(self):
        pf = parse_footnote("⇒ I Cor 13:4-7")
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].book == "1-corinthians"
        assert pf.bible_refs[0].abbreviation == "1 Cor"

    def test_arabic_numeral_book(self):
        pf = parse_footnote("⇒ 1 Cor 13:4-7")
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].book == "1-corinthians"

    def test_no_arrow_no_match(self):
        """Without the ⇒ arrow, Bible books should not be matched."""
        pf = parse_footnote("See Mt 28:19 for further reference")
        assert len(pf.bible_refs) == 0

    def test_different_references_same_book(self):
        """Different chapter:verse refs from the same book are kept."""
        pf = parse_footnote("⇒ Mt 5:1-12; ⇒ Mt 28:19-20")
        assert len(pf.bible_refs) == 2
        assert pf.bible_refs[0].book == "matthew"
        assert pf.bible_refs[0].reference == "5:1-12"
        assert pf.bible_refs[1].reference == "28:19-20"

    def test_deduplicates_same_book_and_reference(self):
        """Same book + same reference is deduplicated."""
        pf = parse_footnote("⇒ Mt 5:1-12; ⇒ Mt 5:1-12")
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].reference == "5:1-12"


class TestParsePatristicReference:
    """Test patristic author extraction from footnotes."""

    def test_simple_patristic(self):
        pf = parse_footnote("St. Augustine, Conf. I: PL 32, 659")
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "augustine"

    def test_thomas_aquinas(self):
        pf = parse_footnote("St. Thomas Aquinas, STh I, 2, 3")
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "thomas-aquinas"

    def test_standalone_author(self):
        pf = parse_footnote("Tertullian, Apol. 50, 13: PL 1, 603")
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "tertullian"

    def test_no_bare_john(self):
        """Bare 'John' without St./Saint should not match any author."""
        pf = parse_footnote("John wrote this passage")
        assert len(pf.author_refs) == 0


class TestParseDocumentReference:
    """Test ecclesiastical document extraction from footnotes."""

    def test_vatican_ii_doc(self):
        pf = parse_footnote("LG 12")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "lumen-gentium"
        assert pf.document_refs[0].abbreviation == "LG"

    def test_gaudium_et_spes(self):
        pf = parse_footnote("GS 19, § 1")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "gaudium-et-spes"

    def test_cic_with_arrow(self):
        pf = parse_footnote("⇒ CIC, can. 920")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "cic"
        assert pf.document_refs[0].abbreviation == "CIC"

    def test_cic_without_arrow(self):
        pf = parse_footnote("CIC, can. 748")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "cic"

    def test_cceo_without_arrow(self):
        pf = parse_footnote("CCEO, can. 675, § 1")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "cceo"

    def test_ds_reference(self):
        pf = parse_footnote("DS 3004")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "denzinger-schonmetzer"

    def test_pl_reference(self):
        pf = parse_footnote("PL 32, 659")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "patrologia-latina"

    def test_multiple_documents(self):
        pf = parse_footnote("LG 12; GS 19; DV 8")
        assert len(pf.document_refs) == 3
        docs = {r.document for r in pf.document_refs}
        assert docs == {"lumen-gentium", "gaudium-et-spes", "dei-verbum"}

    def test_deduplicates_same_document(self):
        pf = parse_footnote("LG 12; LG 48")
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "lumen-gentium"

    def test_no_false_positive_random_uppercase(self):
        """Random uppercase words should not match."""
        pf = parse_footnote("The LORD said to Moses")
        assert len(pf.document_refs) == 0

    def test_no_false_positive_abbreviation_without_digit(self):
        """Abbreviation without a following digit should not match."""
        pf = parse_footnote("See LG for reference")
        assert len(pf.document_refs) == 0


class TestMixedFootnote:
    """Test footnotes containing both Bible and patristic references."""

    def test_mixed_bible_and_author(self):
        pf = parse_footnote(
            "St. Augustine, De civ. Dei, XIV, 28: PL 41, 436; ⇒ Jn 17:3"
        )
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].book == "john"
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "augustine"
        # PL should also be detected as a document
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "patrologia-latina"

    def test_mixed_bible_author_document(self):
        pf = parse_footnote(
            "⇒ Mt 5:1; St. Augustine, Conf. I; LG 12"
        )
        assert len(pf.bible_refs) == 1
        assert pf.bible_refs[0].book == "matthew"
        assert len(pf.author_refs) == 1
        assert pf.author_refs[0].author == "augustine"
        assert len(pf.document_refs) == 1
        assert pf.document_refs[0].document == "lumen-gentium"

    def test_no_match_footnote(self):
        pf = parse_footnote("Council of Trent, Session VII")
        assert len(pf.bible_refs) == 0
        assert len(pf.author_refs) == 0
        assert len(pf.document_refs) == 0


class TestParseAllFootnotes:
    """Test bulk parsing across paragraphs."""

    def test_populates_parsed_footnotes(self):
        paras = [
            Paragraph(
                id=1,
                text="Test paragraph",
                footnotes=["⇒ Mt 28:19-20", "St. Augustine, Conf. I"],
            ),
            Paragraph(
                id=2,
                text="Another paragraph",
                footnotes=["No references here"],
            ),
        ]
        result = parse_all_footnotes(paras)
        assert len(result[0].parsed_footnotes) == 2
        assert result[0].parsed_footnotes[0].bible_refs[0].book == "matthew"
        assert result[0].parsed_footnotes[1].author_refs[0].author == "augustine"
        assert len(result[1].parsed_footnotes) == 1
        assert len(result[1].parsed_footnotes[0].bible_refs) == 0
