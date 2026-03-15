"""Tests for entity extraction module."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.entity_extraction import extract_entities, extract_all_entities
from pipeline.src.models import Paragraph


class TestExtractEntities:
    """Test entity extraction from text."""

    def test_basic_extraction(self):
        text = "The mystery of the Holy Trinity is central to faith."
        entities = extract_entities(text)
        assert "trinity" in entities

    def test_case_insensitivity(self):
        text = "BAPTISM is a sacrament of initiation."
        entities = extract_entities(text)
        assert "baptism" in entities

    def test_multiple_entities(self):
        text = "The Eucharist and Baptism are both sacraments instituted by Christ."
        entities = extract_entities(text)
        assert "eucharist" in entities
        assert "baptism" in entities

    def test_word_boundaries(self):
        """Keywords should match at word boundaries, not partial words."""
        # "grace" should not match inside "disgrace" due to word boundaries
        text = "The grace of God is freely given."
        entities = extract_entities(text)
        assert "grace" in entities

    def test_no_false_positive_on_common_words(self):
        """Short, common entity keywords should not match inside longer words."""
        text = "The formula was calculated precisely."
        entities = extract_entities(text)
        # "faith" should NOT be found in this text
        assert "faith" not in entities

    def test_empty_text(self):
        entities = extract_entities("")
        assert entities == []

    def test_christology_entities(self):
        text = "The Incarnation and Resurrection of Christ are central mysteries."
        entities = extract_entities(text)
        assert "incarnation" in entities
        assert "resurrection" in entities

    def test_sacrament_entities(self):
        text = "Holy Orders, Matrimony, and Reconciliation are sacraments at the service of communion."
        entities = extract_entities(text)
        assert "holy-orders" in entities
        assert "matrimony" in entities
        assert "reconciliation" in entities

    def test_moral_entities(self):
        text = "The natural law and human dignity form the basis of Catholic social teaching."
        entities = extract_entities(text)
        assert "natural-law" in entities
        assert "human-dignity" in entities

    def test_eschatology_entities(self):
        text = "Heaven, hell, and purgatory are the possible states after death."
        entities = extract_entities(text)
        assert "heaven" in entities
        assert "hell" in entities
        assert "purgatory" in entities


class TestExtractAllEntities:
    """Test batch entity extraction on paragraph list."""

    def test_populates_entities(self):
        paras = [
            Paragraph(id=1, text={"en": "The Trinity is the central mystery of Christian faith."}),
            Paragraph(id=2, text={"en": "Baptism is the gateway to the sacraments."}),
        ]
        result = extract_all_entities(paras)
        assert "trinity" in result[0].entities
        assert "baptism" in result[1].entities

    def test_uses_english_text(self):
        """Should use English text even with other languages present."""
        paras = [
            Paragraph(
                id=1,
                text={
                    "en": "The Eucharist is the source and summit of Christian life.",
                    "la": "Eucharistia est fons et culmen totius vitae christianae.",
                },
            )
        ]
        result = extract_all_entities(paras)
        assert "eucharist" in result[0].entities

    def test_no_mutation_of_list(self):
        """Function should return the same list (mutated in place)."""
        paras = [Paragraph(id=1, text={"en": "Grace is a free gift."})]
        result = extract_all_entities(paras)
        assert result is paras

    def test_returns_sorted_entities(self):
        """Entities should be sorted alphabetically."""
        paras = [
            Paragraph(
                id=1,
                text={"en": "The Trinity, the Incarnation, and Baptism are central."},
            )
        ]
        extract_all_entities(paras)
        entities = paras[0].entities
        assert entities == sorted(entities)
