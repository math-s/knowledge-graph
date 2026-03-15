"""Tests for topic modeling module."""

import sys
from pathlib import Path

# Ensure pipeline is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.src.topic_model import build_topic_model
from pipeline.src.models import Paragraph


def _make_corpus(n: int = 60) -> list[Paragraph]:
    """Create a minimal synthetic corpus for topic modeling tests.

    Generates paragraphs with distinct vocabulary clusters so LDA
    can discover at least a few meaningful topics.
    """
    texts = []
    # Cluster 1: sacraments
    for i in range(n // 3):
        texts.append(
            f"The sacrament of baptism initiation water spirit grace church "
            f"celebration liturgy rite faithful catechumen paragraph {i}"
        )
    # Cluster 2: moral law
    for i in range(n // 3):
        texts.append(
            f"The moral law conscience commandments natural law human dignity "
            f"justice prudence virtue common good society paragraph {i}"
        )
    # Cluster 3: scripture and revelation
    for i in range(n // 3):
        texts.append(
            f"The sacred scripture bible testament gospel revelation word "
            f"inspiration tradition apostolic teaching canon paragraph {i}"
        )

    return [
        Paragraph(id=i + 1, text={"en": t})
        for i, t in enumerate(texts)
    ]


class TestBuildTopicModel:
    """Test LDA topic model building."""

    def test_assigns_topics(self):
        """Paragraphs should get topic assignments."""
        paras = _make_corpus(60)
        result, topic_terms = build_topic_model(
            paras, n_topics=3, top_n=2, min_weight=0.01
        )
        # At least some paragraphs should have topics assigned
        with_topics = [p for p in result if p.topics]
        assert len(with_topics) > 0

    def test_topic_terms_shape(self):
        """topic_terms should have n_topics lists of strings."""
        paras = _make_corpus(60)
        _, topic_terms = build_topic_model(paras, n_topics=3)
        assert len(topic_terms) == 3
        for terms in topic_terms:
            assert len(terms) == 10
            assert all(isinstance(t, str) for t in terms)

    def test_topic_weights_are_tuples(self):
        """Each topic assignment should be a (topic_id, weight) tuple."""
        paras = _make_corpus(60)
        result, _ = build_topic_model(
            paras, n_topics=3, top_n=2, min_weight=0.01
        )
        for p in result:
            for topic in p.topics:
                assert isinstance(topic, tuple)
                assert len(topic) == 2
                topic_id, weight = topic
                assert isinstance(topic_id, int)
                assert isinstance(weight, float)
                assert 0 <= weight <= 1

    def test_too_few_paragraphs_skips(self):
        """With fewer paragraphs than topics, should skip gracefully."""
        paras = [
            Paragraph(id=1, text={"en": "Short text."}),
            Paragraph(id=2, text={"en": "Another short text."}),
        ]
        result, topic_terms = build_topic_model(paras, n_topics=40)
        assert topic_terms == []
        # Paragraphs should be unchanged
        assert result[0].topics == []

    def test_returns_same_list(self):
        """Should mutate and return the same paragraph list."""
        paras = _make_corpus(60)
        result, _ = build_topic_model(paras, n_topics=3)
        assert result is paras
