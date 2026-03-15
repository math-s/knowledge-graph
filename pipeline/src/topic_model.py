"""Discover latent topics in CCC paragraphs using LDA topic modeling."""

from __future__ import annotations

import logging

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer

from .models import Paragraph, resolve_lang

logger = logging.getLogger(__name__)


def build_topic_model(
    paragraphs: list[Paragraph],
    n_topics: int = 40,
    top_n: int = 3,
    min_weight: float = 0.05,
    max_features: int = 5000,
    min_df: int = 3,
    max_df: float = 0.6,
    random_state: int = 42,
) -> tuple[list[Paragraph], list[list[str]]]:
    """Run LDA topic modeling on CCC paragraphs.

    Assigns top-N topics (with weight > min_weight) to each paragraph.

    Args:
        paragraphs: List of CCC paragraphs to model.
        n_topics: Number of latent topics to discover.
        top_n: Maximum number of topics to assign per paragraph.
        min_weight: Minimum topic weight threshold for assignment.
        max_features: Maximum vocabulary size for CountVectorizer.
        min_df: Minimum document frequency for terms.
        max_df: Maximum document frequency ratio for terms.
        random_state: Random seed for reproducibility.

    Returns:
        Tuple of (paragraphs with topics assigned, topic_terms list).
        topic_terms[i] is a list of top-10 words for topic i.
    """
    # Extract English text for all paragraphs
    texts = [resolve_lang(p.text, "en") for p in paragraphs]

    # Filter out empty texts
    valid_indices = [i for i, t in enumerate(texts) if t.strip()]
    valid_texts = [texts[i] for i in valid_indices]

    if len(valid_texts) < n_topics:
        logger.warning(
            "Only %d paragraphs with text, fewer than %d topics — skipping topic modeling",
            len(valid_texts),
            n_topics,
        )
        return paragraphs, []

    logger.info("Building vocabulary from %d paragraphs...", len(valid_texts))

    # Build document-term matrix
    vectorizer = CountVectorizer(
        max_features=max_features,
        min_df=min_df,
        max_df=max_df,
        stop_words="english",
    )
    dtm = vectorizer.fit_transform(valid_texts)
    feature_names = vectorizer.get_feature_names_out()

    logger.info(
        "Vocabulary: %d terms. Fitting LDA with %d topics...",
        len(feature_names),
        n_topics,
    )

    # Fit LDA model
    lda = LatentDirichletAllocation(
        n_components=n_topics,
        learning_method="batch",
        n_jobs=-1,
        random_state=random_state,
    )
    doc_topic_matrix = lda.fit_transform(dtm)

    # Extract top-10 terms per topic for metadata/logging
    topic_terms: list[list[str]] = []
    for topic_idx, topic_dist in enumerate(lda.components_):
        top_word_indices = topic_dist.argsort()[-10:][::-1]
        top_words = [feature_names[i] for i in top_word_indices]
        topic_terms.append(top_words)
        logger.info("  Topic %d: %s", topic_idx, ", ".join(top_words))

    # Assign top-N topics to each paragraph
    topic_counts: dict[int, int] = {}
    for idx, valid_idx in enumerate(valid_indices):
        weights = doc_topic_matrix[idx]
        # Get top-N topics sorted by weight descending
        top_indices = weights.argsort()[-top_n:][::-1]
        assigned: list[tuple[int, float]] = []
        for topic_id in top_indices:
            w = float(weights[topic_id])
            if w >= min_weight:
                assigned.append((int(topic_id), w))
                topic_counts[int(topic_id)] = topic_counts.get(int(topic_id), 0) + 1
        paragraphs[valid_idx].topics = assigned

    total_with_topics = sum(1 for p in paragraphs if p.topics)
    logger.info(
        "Assigned topics to %d/%d paragraphs (%d topics discovered)",
        total_with_topics,
        len(paragraphs),
        n_topics,
    )

    return paragraphs, topic_terms
