"""Tests for themes module."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.themes import assign_themes
from pipeline.src.models import Paragraph


class TestKeywordMatch:
    """Test keyword-based theme assignment."""

    def test_eucharist_keyword(self):
        paras = [
            Paragraph(
                id=9999,
                text="The Eucharist is the source and summit of the Christian life.",
            )
        ]
        assign_themes(paras)
        assert "eucharist" in paras[0].themes

    def test_trinity_keyword(self):
        paras = [
            Paragraph(id=9999, text="The mystery of the Holy Trinity is central.")
        ]
        assign_themes(paras)
        assert "trinity" in paras[0].themes

    def test_multiple_themes(self):
        paras = [
            Paragraph(
                id=9999,
                text="Baptism and the Eucharist are sacraments of initiation.",
            )
        ]
        assign_themes(paras)
        assert "baptism" in paras[0].themes
        assert "eucharist" in paras[0].themes
        assert "sacraments" in paras[0].themes


class TestRangeMatch:
    """Test range-based theme assignment."""

    def test_eucharist_range(self):
        paras = [
            Paragraph(
                id=1350,
                text="This paragraph discusses the celebration.",
            )
        ]
        assign_themes(paras)
        assert "eucharist" in paras[0].themes

    def test_baptism_range(self):
        paras = [
            Paragraph(id=1250, text="A paragraph about the rite.")
        ]
        assign_themes(paras)
        assert "baptism" in paras[0].themes

    def test_outside_range(self):
        paras = [
            Paragraph(id=50, text="A paragraph about general topics.")
        ]
        assign_themes(paras)
        assert "eucharist" not in paras[0].themes
        assert "baptism" not in paras[0].themes


class TestNoFalsePositives:
    """Test that unrelated content is not tagged."""

    def test_unrelated_text(self):
        paras = [
            Paragraph(
                id=9998,
                text="The dignity of the human person is rooted in creation.",
            )
        ]
        assign_themes(paras)
        assert "eucharist" not in paras[0].themes
        assert "baptism" not in paras[0].themes
        assert "trinity" not in paras[0].themes
        # Should match creation
        assert "creation" in paras[0].themes
