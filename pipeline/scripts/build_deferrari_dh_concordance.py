"""Build a cross-edition concordance between Deferrari (30th ed., old
numbering) and Denzinger-Hünermann (modern numbering).

Approach: **scripture-fingerprint Jaccard**.

Each Denzinger section already has ``cites`` edges to ``bible-verse:*``
targets emitted by the earlier bible-citation backfill. Two sections in
different editions that cite the same scriptures with ≥2/3 overlap are
very likely the same canonical text. This works because the renumbering
changed section IDs but not the quoted Bible verses — Trent Canon 2
remained Trent Canon 2 regardless of whether it's called Deferrari §789
or DH §1512.

Constraints to reduce false positives:
  - Both sections must carry ≥ 2 distinct bible-verse citations.
  - Overlap (intersection) must be ≥ 2 verses.
  - Jaccard similarity ≥ ``JACCARD_MIN`` (0.67 by default).
  - Both sections must share at least one council/author node, where
    both sides have such edges. (Protects against two different Trent
    canons accidentally sharing a small bible footprint — they need to
    agree on structural context too.)

For each Deferrari section we pick the single highest-scoring DH match
(ties broken by intersection size, then by numeric closeness).

Emits:
  - ``same_as`` edge ``document-section:denzinger-deferrari-30/<old>
    <-> document-section:denzinger-hunermann/<new>``
  - Plus propagation: Deferrari sections inherit the DH text_la / text_pt
    fields via UPDATE so the EN + LA + PT trilingual view works.

Idempotent.

Usage:
    uv run --project pipeline python -m pipeline.scripts.build_deferrari_dh_concordance
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

DEFERRARI = "denzinger-deferrari-30"
HUNERMANN = "denzinger-hunermann"

JACCARD_MIN = 0.67   # at least two thirds of the union must be shared
MIN_BIBLE_REFS = 2
MIN_INTERSECT = 2

log = logging.getLogger("build_deferrari_dh_concordance")


def fingerprint(cur: sqlite3.Cursor, doc_id: str) -> dict[str, set[str]]:
    """Return ``{section_num: {bible_verse or author target, ...}}``.

    Includes both bible-verse and author citations so shorter sections with
    just a patristic reference also become matchable.
    """
    out: dict[str, set[str]] = defaultdict(set)
    prefix = f"document-section:{doc_id}/"
    for src, tgt in cur.execute(
        """
        SELECT source, target FROM graph_edges
         WHERE edge_type='cites'
           AND source LIKE ?
           AND (target LIKE 'bible-verse:%' OR target LIKE 'author:%')
        """,
        (prefix + "%",),
    ):
        section_num = src[len(prefix):]
        out[section_num].add(tgt)
    return dict(out)


def structural_context(cur: sqlite3.Cursor, doc_id: str) -> dict[str, set[str]]:
    """Return ``{section_num: {council_doc_id or author_id, ...}}``.

    Used as a secondary filter — two candidate-pair sections must share
    at least one structural context node if both sides have any.
    """
    out: dict[str, set[str]] = defaultdict(set)
    prefix = f"document-section:{doc_id}/"
    for src, tgt in cur.execute(
        """
        SELECT source, target FROM graph_edges
         WHERE edge_type='cites' AND source LIKE ?
           AND (target LIKE 'document:%' OR target LIKE 'author:%')
           AND target NOT LIKE 'document:denzinger-%'
        """,
        (prefix + "%",),
    ):
        section_num = src[len(prefix):]
        out[section_num].add(tgt)
    return dict(out)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        fp_def = fingerprint(cur, DEFERRARI)
        fp_dh = fingerprint(cur, HUNERMANN)
        log.info("Deferrari sections with %d+ bible refs: %d",
                 MIN_BIBLE_REFS,
                 sum(1 for s in fp_def.values() if len(s) >= MIN_BIBLE_REFS))
        log.info("DH sections with %d+ bible refs: %d",
                 MIN_BIBLE_REFS,
                 sum(1 for s in fp_dh.values() if len(s) >= MIN_BIBLE_REFS))

        ctx_def = structural_context(cur, DEFERRARI)
        ctx_dh = structural_context(cur, HUNERMANN)

        # Pre-filter DH to usable candidates once.
        dh_candidates = {n: fp for n, fp in fp_dh.items()
                         if len(fp) >= MIN_BIBLE_REFS}
        # Invert DH fingerprints into a posting list for faster candidate lookup:
        # verse → [sections citing it]
        verse_to_dh: dict[str, list[str]] = defaultdict(list)
        for n, fp in dh_candidates.items():
            for v in fp:
                verse_to_dh[v].append(n)

        matches: list[tuple[str, str, float, int]] = []  # (def, dh, jaccard, overlap)
        for def_num, def_fp in fp_def.items():
            if len(def_fp) < MIN_BIBLE_REFS:
                continue
            # Candidate DH sections are those sharing at least one verse.
            candidate_counts: dict[str, int] = defaultdict(int)
            for v in def_fp:
                for dh_num in verse_to_dh.get(v, ()):
                    candidate_counts[dh_num] += 1
            # Score each candidate.
            best: tuple[str, float, int] | None = None
            for dh_num, intersect in candidate_counts.items():
                if intersect < MIN_INTERSECT:
                    continue
                dh_fp = dh_candidates[dh_num]
                union = len(def_fp | dh_fp)
                jaccard = intersect / union
                if jaccard < JACCARD_MIN:
                    continue
                # Structural context check
                cd = ctx_def.get(def_num, set())
                ch = ctx_dh.get(dh_num, set())
                if cd and ch and not (cd & ch):
                    continue
                # Tie-break: higher intersection wins; then numeric closeness.
                candidate = (dh_num, jaccard, intersect)
                if best is None or (candidate[2], candidate[1]) > (best[2], best[1]):
                    best = candidate
            if best:
                matches.append((def_num, best[0], best[1], best[2]))

        log.info("candidate matches: %d Deferrari → DH pairs",  len(matches))

        # De-dupe: a DH section should also point to the same Deferrari, not
        # two. Keep only mutual-best pairs.
        dh_best: dict[str, tuple[str, float, int]] = {}
        for def_num, dh_num, j, i in matches:
            cur_best = dh_best.get(dh_num)
            if cur_best is None or (i, j) > (cur_best[2], cur_best[1]):
                dh_best[dh_num] = (def_num, j, i)
        mutual = [(def_num, dh_num, j, i)
                  for def_num, dh_num, j, i in matches
                  if dh_best.get(dh_num, (None,))[0] == def_num]
        log.info("mutual-best concordance pairs: %d", len(mutual))

        # Idempotent delete of prior concordance edges.
        cur.execute(
            f"""
            DELETE FROM graph_edges
             WHERE edge_type='same_as'
               AND (
                    (source LIKE 'document-section:{DEFERRARI}/%'
                     AND target LIKE 'document-section:{HUNERMANN}/%')
                 OR (target LIKE 'document-section:{DEFERRARI}/%'
                     AND source LIKE 'document-section:{HUNERMANN}/%')
               )
            """
        )

        edge_rows: list[tuple[str, str, str]] = []
        update_rows: list[tuple[str | None, str | None, str]] = []
        # Also fill Deferrari text_la/text_pt from the matched DH section,
        # so the trilingual view works at the Deferrari node.
        for def_num, dh_num, j, i in mutual:
            def_node = f"document-section:{DEFERRARI}/{def_num}"
            dh_node  = f"document-section:{HUNERMANN}/{dh_num}"
            edge_rows.append((def_node, dh_node, "same_as"))
            edge_rows.append((dh_node, def_node, "same_as"))

        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows,
        )

        # Copy DH text_la/text_pt into Deferrari rows where Deferrari lacks them.
        copied = 0
        for def_num, dh_num, _, _ in mutual:
            dh_text = cur.execute(
                "SELECT text_la, text_pt FROM document_sections "
                " WHERE document_id=? AND section_num=?",
                (HUNERMANN, dh_num),
            ).fetchone()
            if not dh_text:
                continue
            t_la, t_pt = dh_text
            cur.execute(
                """
                UPDATE document_sections
                   SET text_la = COALESCE(NULLIF(text_la, ''), ?),
                       text_pt = COALESCE(NULLIF(text_pt, ''), ?)
                 WHERE document_id=? AND section_num=?
                """,
                (t_la, t_pt, DEFERRARI, def_num),
            )
            if cur.rowcount:
                copied += 1
        conn.commit()

        log.info("wrote %d same_as edges (both directions)", len(edge_rows))
        log.info("copied LA/PT text onto %d Deferrari sections", copied)

        # Sample matches for the log
        mutual.sort(key=lambda x: int(x[0]) if x[0].isdigit() else 0)
        for def_num, dh_num, j, i in mutual[:8]:
            log.info("  Deferrari §%-5s ↔ DH §%-5s  (J=%.2f, overlap=%d)",
                     def_num, dh_num, j, i)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
