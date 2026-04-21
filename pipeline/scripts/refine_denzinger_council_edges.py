"""Promote Denzinger→council edges to canon/section granularity where we can.

The initial backfill (``backfill_denzinger_source_edges.py``) emits
``cites`` edges at document-level (``-> document:chalcedon`` etc.). For
councils that already have canon-level sections in ``document_sections``
(the early ecumenical councils and local synods), we can parse canon
markers from the Denzinger section text and add a precision edge to the
specific canon.

Canon markers we match (PT + EN + Latin):
  - ``Cân. 14`` / ``Cân. 14.`` / ``Cân.  14 º``
  - ``Can. 2`` / ``Canon 2``
  - ``Cap. 5`` / ``Chapter 5``  (used for some medieval decrees)
  - ``cân. 14`` / ``can. 2`` (lowercase)

We only target councils that have ``>= 2`` sections in ``document_sections``
— Trent, Vatican I/II, Lateran I-V, Lyon, Constance, Florence, Vienne
are single-stub documents and can't be refined without first ingesting
their canon texts.

Document-level edges are KEPT. This pass ADDS section-level edges
alongside them, so a UI or query can show either granularity.

Idempotent: deletes any prior DH/Deferrari → document-section:<council>/*
edges before re-inserting.

Usage:
    uv run --project pipeline python -m pipeline.scripts.refine_denzinger_council_edges
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

DEFERRARI = "denzinger-deferrari-30"
HUNERMANN = "denzinger-hunermann"

log = logging.getLogger("refine_denzinger_council_edges")


# "Cân. 14", "Can. 2", "Canon 5", "Cap. 3", "Chapter 4" — case-insensitive.
# Forms matched:
#   Cân. 14      (PT, with accent)
#   Can. 2       (PT/Latin)
#   Canon 5 / Cannon
#   cân. 14º / cân. 14'  (ordinal markers)
#   Cap. 3       (decretal chapters)
# Rejects matches embedded in "cap. 3, art. 1" so we only take the first number.
CANON_RE = re.compile(
    r"""
    \b
    (?:Cân\.?|Can\.?|Canon|Cannon|Cap\.?|Chapter)
    \s+
    (?P<num>\d{1,3})
    (?:[ºª'"\s.,;:)\]]|$)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def extract_canon_candidates(blob: str) -> list[str]:
    """Return deduped canon number strings found in *blob* (in order)."""
    out: list[str] = []
    seen: set[str] = set()
    for m in CANON_RE.finditer(blob):
        n = m.group("num")
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Councils that have canon-level sections (skip single-stub ones).
        council_sections: dict[str, set[str]] = {}
        for cid, num in cur.execute(
            """
            SELECT d.id, ds.section_num
              FROM documents d
              JOIN document_sections ds ON ds.document_id = d.id
             WHERE d.category LIKE 'council%'
            """
        ):
            council_sections.setdefault(cid, set()).add(num)

        refinable = {cid: secs for cid, secs in council_sections.items()
                     if len(secs) >= 2}
        log.info("councils with >=2 canon sections (refinable): %d",
                 len(refinable))
        for cid, secs in sorted(refinable.items()):
            log.info("  %-20s  %d canons", cid, len(secs))

        # Gather every Denzinger → document:<refinable_council> edge.
        refinable_ids = tuple(refinable)
        if not refinable_ids:
            log.info("nothing to refine")
            return 0

        placeholders = ",".join("?" for _ in refinable_ids)
        edges = cur.execute(
            f"""
            SELECT source, target
              FROM graph_edges
             WHERE edge_type='cites'
               AND source LIKE 'document-section:denzinger-%'
               AND target IN ({','.join('?' for _ in refinable_ids)})
            """,
            tuple(f"document:{cid}" for cid in refinable_ids),
        ).fetchall()
        log.info("document-level edges to refinable councils: %d", len(edges))

        # Idempotent delete of previously-emitted section-level edges.
        cur.execute(
            f"""
            DELETE FROM graph_edges
             WHERE edge_type='cites'
               AND source LIKE 'document-section:denzinger-%'
               AND ({' OR '.join('target LIKE ?' for _ in refinable_ids)})
            """,
            tuple(f"document-section:{cid}/%" for cid in refinable_ids),
        )

        # For each source section, fetch its text + relevant meta.
        new_edges: set[tuple[str, str, str]] = set()
        hits_per_council: dict[str, int] = {}
        unresolved = 0

        for source, target in edges:
            cid = target[len("document:"):]
            valid = refinable[cid]

            # Split the source node id to get doc_id + section_num.
            # source = "document-section:<doc>/<num>"
            body = source[len("document-section:"):]
            doc_id, _, section_num = body.partition("/")

            row = cur.execute(
                "SELECT text_en, text_la, text_pt, meta_json "
                "  FROM document_sections "
                " WHERE document_id=? AND section_num=?",
                (doc_id, section_num),
            ).fetchone()
            if row is None:
                continue
            text_en, text_la, text_pt, meta_json = row
            meta = json.loads(meta_json or "{}")

            blob_parts: list[str] = []
            for t in (text_en, text_la, text_pt):
                if t:
                    blob_parts.append(t)
            for key in ("work", "intro_pt"):
                v = meta.get(key)
                if v:
                    blob_parts.append(str(v))
            path_meta = meta.get("path") or {}
            for v in path_meta.values():
                if v:
                    blob_parts.append(str(v))
            blob = " \n ".join(blob_parts)

            canons = extract_canon_candidates(blob)
            resolved_for_this = False
            for n in canons:
                if n in valid:
                    tgt_section = f"document-section:{cid}/{n}"
                    new_edges.add((source, tgt_section, "cites"))
                    hits_per_council[cid] = hits_per_council.get(cid, 0) + 1
                    resolved_for_this = True
            if not resolved_for_this:
                unresolved += 1

        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            list(new_edges),
        )
        conn.commit()

        log.info("wrote %d section-level precision edges", len(new_edges))
        for cid, n in sorted(hits_per_council.items(), key=lambda x: -x[1]):
            log.info("  -> document-section:%s/*  :  %d", cid, n)
        log.info("edges with no canon marker found: %d (document-level edge retained)", unresolved)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
