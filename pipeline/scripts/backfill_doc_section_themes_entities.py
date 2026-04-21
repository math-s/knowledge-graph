"""Apply theme classification and entity extraction to every row in
``document_sections`` and wire ``has_theme`` / ``mentions`` edges into
``graph_edges``.

Covers:
  - the 355 pre-existing Vatican II / encyclical sections that had nodes but
    no theme/entity edges
  - the 9142 sections newly created by the library migration + council stubs

Uses the shared, deterministic classifiers from the main pipeline
(``themes.assign_themes``, ``entity_extraction.extract_entities``), so the
resulting taxonomy matches what CCC paragraphs already use — queries like
"every node in theme:eucharist" work uniformly across CCC + documents.

Topic modeling is intentionally skipped: the existing LDA topic space was
fit on CCC only, and retraining on the combined corpus would reassign every
existing topic ID.

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_doc_section_themes_entities
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.entity_extraction import extract_entities  # noqa: E402
from pipeline.src.models import Paragraph  # noqa: E402
from pipeline.src.themes import assign_themes  # noqa: E402

DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

# Offset to keep synthetic Paragraph ids far outside the CCC range (max ~2865),
# so the id-range checks in assign_themes never match.
ID_OFFSET = 10_000_000

log = logging.getLogger("backfill_doc_section_themes_entities")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        rows = cur.execute(
            "SELECT document_id, section_num, text_en, text_la, text_pt "
            "  FROM document_sections "
            " WHERE COALESCE(text_en, '') <> ''"
            "    OR COALESCE(text_la, '') <> ''"
            "    OR COALESCE(text_pt, '') <> ''"
        ).fetchall()
        log.info("document_sections with text (any lang): %d", len(rows))

        # Build synthetic Paragraph objects. Pass every available language:
        # the classifiers now scan EN+LA+PT together and the keyword lists
        # carry PT + Latin synonyms for the major themes/entities.
        paras: list[Paragraph] = []
        node_ids: list[str] = []
        combined_texts: list[str] = []
        for i, (doc_id, section_num, t_en, t_la, t_pt) in enumerate(rows):
            text_map: dict[str, str] = {}
            if t_en:
                text_map["en"] = t_en
            if t_la:
                text_map["la"] = t_la
            if t_pt:
                text_map["pt"] = t_pt
            paras.append(Paragraph(id=ID_OFFSET + i, text=text_map))
            node_ids.append(f"document-section:{doc_id}/{section_num}")
            combined_texts.append(" \n ".join(text_map.values()))

        log.info("-- themes --")
        assign_themes(paras)  # mutates paras in-place

        log.info("-- entities --")
        per_section_entities: list[list[str]] = [
            extract_entities(blob) for blob in combined_texts
        ]

        # Build edges
        theme_edges: set[tuple[str, str, str]] = set()
        mention_edges: set[tuple[str, str, str]] = set()
        for node_id, para, entities in zip(node_ids, paras, per_section_entities):
            for theme_id in para.themes:
                theme_edges.add((node_id, f"theme:{theme_id}", "has_theme"))
            for entity_id in entities:
                mention_edges.add((node_id, f"entity:{entity_id}", "mentions"))

        # Idempotent: clear prior edges sourced from document-section: nodes
        # of these two types
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type='has_theme' AND source LIKE 'document-section:%'"
        )
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type='mentions' AND source LIKE 'document-section:%'"
        )

        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            list(theme_edges),
        )
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            list(mention_edges),
        )

        conn.commit()

        log.info("== done ==")
        log.info("  %d sections processed", len(rows))
        log.info("  %d has_theme edges", len(theme_edges))
        log.info("  %d mentions edges", len(mention_edges))

        # Sanity: ensure all target nodes exist
        missing_themes = cur.execute(
            """SELECT COUNT(*) FROM (
                 SELECT DISTINCT target FROM graph_edges
                 WHERE edge_type='has_theme' AND source LIKE 'document-section:%'
               ) t LEFT JOIN graph_nodes n ON n.id = t.target
               WHERE n.id IS NULL"""
        ).fetchone()[0]
        missing_entities = cur.execute(
            """SELECT COUNT(*) FROM (
                 SELECT DISTINCT target FROM graph_edges
                 WHERE edge_type='mentions' AND source LIKE 'document-section:%'
               ) t LEFT JOIN graph_nodes n ON n.id = t.target
               WHERE n.id IS NULL"""
        ).fetchone()[0]
        if missing_themes or missing_entities:
            log.warning("  dangling targets: %d theme, %d entity",
                        missing_themes, missing_entities)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
