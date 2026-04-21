"""Wire internal cross-reference edges within each Denzinger edition.

EN (Deferrari 30): ``meta_json["cross_refs"]`` already holds a deduped list
  of target denz numbers (pre-extracted by the converter). Emit one edge
  per (source_denz → target_denz) where the target exists as a section.

PT (Hünermann): inline markers like ``*42``, ``*3-5``, ``*62s``, ``*64ss``
  in ``text_pt``, ``text_la``, ``text_bilingual``, ``intro_pt``.
    - ``*N``    → target N
    - ``*N-M``  → target range, expanded to every integer in [N, M]
    - ``*Ns``   → target N plus the immediately-following section
    - ``*Nss``  → target N plus two following sections (convention:
                  "sequens et sequentes" ≈ "and following")

Edges:
    ``document-section:<slug>/<src> -> document-section:<slug>/<tgt>``  (cites)

Idempotent: removes prior same-edition cross-ref edges before re-insert.
Only emits edges where the target section actually exists in the document.

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_denzinger_cross_refs
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

log = logging.getLogger("backfill_denzinger_cross_refs")


# Match ``*42``, ``*42a``, ``*3-5``, ``*62s``, ``*64ss``. The ``s``/``ss``
# suffix is optional and may follow either a single number or the end of a
# range. Range end, if present, is pure digits (no suffix).
PT_CROSS_RE = re.compile(
    r"\*(?P<start>\d{1,4}[a-zA-Z]?)"
    r"(?:\s*[-–]\s*(?P<end>\d{1,4}))?"
    r"(?P<more>s{1,2})?"
    r"(?!\d)"                             # don't eat into neighbouring numbers
)


def numeric_part(key: str) -> int | None:
    m = re.match(r"^(\d+)", key)
    return int(m.group(1)) if m else None


# Above this span, a "*N-M" marker is treated as a block reference
# ("see sections N through M") rather than N-M+1 individual citations.
# 10 keeps small legitimate multi-target refs (*3-5, *42-45, *2400-2410)
# expanded, but collapses "*1500-1835" into a single representative edge
# to avoid 300+ synthetic citations from one marker.
RANGE_EXPAND_LIMIT = 10


def expand_pt_marker(start: str, end: str | None, more: str | None,
                     valid: set[str]) -> list[str]:
    """Return every target section that this marker addresses.

    ``start`` may carry a suffix letter (e.g. ``42a``) — honoured as-is;
    ranges and ``s``/``ss`` extensions only apply to the numeric part.

    Large ranges (span > ``RANGE_EXPAND_LIMIT``) are treated as block
    references: we emit only the starting section as the representative
    target, instead of fanning out to every member.
    """
    targets: list[str] = []
    # Primary target — exact string match (preserves "42a" suffix).
    if start in valid:
        targets.append(start)

    n = numeric_part(start)
    if n is None:
        return targets

    if end is not None:
        m = int(end)
        if m < n:
            return targets
        span = m - n
        if span > RANGE_EXPAND_LIMIT:
            # Block ref: starting section is already appended above.
            return targets
        for i in range(n, m + 1):
            k = str(i)
            if k in valid and k not in targets:
                targets.append(k)
    elif more:
        # "s" = plus one following, "ss" = plus two.
        span = {"s": 1, "ss": 2}.get(more, 0)
        for i in range(n + 1, n + 1 + span):
            k = str(i)
            if k in valid and k not in targets:
                targets.append(k)

    return targets


def run_deferrari(cur: sqlite3.Cursor) -> tuple[int, int, int]:
    cur.execute(
        """
        DELETE FROM graph_edges
         WHERE edge_type = 'cites'
           AND source LIKE ? || '/%'
           AND target LIKE ? || '/%'
        """,
        (f"document-section:{DEFERRARI}", f"document-section:{DEFERRARI}"),
    )
    valid = {
        row[0]
        for row in cur.execute(
            "SELECT section_num FROM document_sections WHERE document_id=?",
            (DEFERRARI,),
        )
    }

    rows = cur.execute(
        "SELECT section_num, meta_json FROM document_sections WHERE document_id=?",
        (DEFERRARI,),
    ).fetchall()

    refs = 0
    resolved = 0
    edges: set[tuple[str, str, str]] = set()
    for src, meta_json in rows:
        meta = json.loads(meta_json or "{}")
        for tgt in meta.get("cross_refs", []):
            refs += 1
            if tgt == src:
                continue
            if tgt in valid:
                edges.add((
                    f"document-section:{DEFERRARI}/{src}",
                    f"document-section:{DEFERRARI}/{tgt}",
                    "cites",
                ))
                resolved += 1

    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        list(edges),
    )
    log.info("  %s: %d cross_refs found, %d resolved, %d unique edges",
             DEFERRARI, refs, resolved, len(edges))
    return refs, resolved, len(edges)


def run_hunermann(cur: sqlite3.Cursor) -> tuple[int, int, int]:
    cur.execute(
        """
        DELETE FROM graph_edges
         WHERE edge_type = 'cites'
           AND source LIKE ? || '/%'
           AND target LIKE ? || '/%'
        """,
        (f"document-section:{HUNERMANN}", f"document-section:{HUNERMANN}"),
    )
    valid = {
        row[0]
        for row in cur.execute(
            "SELECT section_num FROM document_sections WHERE document_id=?",
            (HUNERMANN,),
        )
    }

    rows = cur.execute(
        "SELECT section_num, text_en, text_la, text_pt, meta_json "
        "  FROM document_sections WHERE document_id=?",
        (HUNERMANN,),
    ).fetchall()

    markers_found = 0
    resolved = 0
    edges: set[tuple[str, str, str]] = set()
    for src, text_en, text_la, text_pt, meta_json in rows:
        meta = json.loads(meta_json or "{}")
        corpus_fields = [
            text_pt, text_la, text_en,
            meta.get("text_bilingual"),
            meta.get("intro_pt"),
            meta.get("editorial_ref"),
        ]
        for corpus in corpus_fields:
            if not corpus:
                continue
            for m in PT_CROSS_RE.finditer(corpus):
                markers_found += 1
                targets = expand_pt_marker(
                    m.group("start"), m.group("end"), m.group("more"), valid
                )
                for tgt in targets:
                    if tgt == src:
                        continue
                    edges.add((
                        f"document-section:{HUNERMANN}/{src}",
                        f"document-section:{HUNERMANN}/{tgt}",
                        "cites",
                    ))
                    resolved += 1

    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        list(edges),
    )
    log.info("  %s: %d *N markers found, %d expanded targets, %d unique edges",
             HUNERMANN, markers_found, resolved, len(edges))
    return markers_found, resolved, len(edges)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        run_deferrari(cur)
        run_hunermann(cur)
        conn.commit()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
