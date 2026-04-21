"""Materialize council canons into real ``document_sections`` rows by
copying them from their Denzinger-Hünermann source.

Motivation: Trent, Vatican I/II, Basel-Florence, and Constance are stored
as single-stub ``documents`` rows with no section-level content. But
Denzinger **is** the canonical compendium of their canons — the texts are
already in the DB, just under ``document_id='denzinger-hunermann'``. This
script promotes them to first-class council sections so they:
  - appear in ``search_fts`` under the council name
  - receive theme/entity classification under the council
  - can be cited at canon-level precision by CCC, Summa, cathen, etc.
  - can be navigated as a proper hierarchy from the council document

We do NOT mutate DH. For each DH section that belongs to one of the 5
target councils, we:
  1. Parse the DH ``work`` field to identify session / constitution / bull.
  2. Build a stable ``section_num`` like ``session-6-denz-1520`` or
     ``lumen-gentium-denz-4130``.
  3. INSERT (or UPDATE) a ``document_sections`` row under the council's
     ``document_id`` with text copied from DH's ``text_la`` / ``text_pt``.
  4. Emit a ``same_as`` graph edge between the two section nodes.

Idempotent: previous runs' rows are deleted before re-insert.

Usage:
    uv run --project pipeline python -m pipeline.scripts.materialize_council_sections_from_dh
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import unicodedata
from collections.abc import Callable
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

HUNERMANN = "denzinger-hunermann"

log = logging.getLogger("materialize_council_sections_from_dh")


def slugify(text: str) -> str:
    """Lowercase ASCII slug from any Unicode text. ``"Cantate Domino"`` → ``"cantate-domino"``."""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_only.lower()).strip("-")
    return slug


# ----------------------------------------------------------------------
# Per-council parsing rules
# ----------------------------------------------------------------------
# Each rule takes the DH work string and returns either:
#   - a "scope" string (used as the section_num prefix), or
#   - ``None`` if this DH section does not belong to this council.
# A returned scope of ``""`` is allowed and means "belongs to the council
# but no sub-scope identified — use the council id as scope".


def trent_scope(work: str | None) -> str | None:
    """``"Concílio de Trento: Sessão 6ª (justificação)"`` → ``"session-6"``."""
    if not work or "Concílio de Trento" not in work and "Concílio Ecumênico de Trento" not in work:
        return None
    m = re.search(r"Sessão\s+(\d{1,2})", work)
    if m:
        return f"session-{m.group(1)}"
    # Trent-era decrees without a session marker (e.g., post-council bulls).
    return "other"


def vatican_i_scope(work: str | None) -> str | None:
    if not work:
        return None
    work_l = work.lower()
    # Only accept works that explicitly name Vatican I or its two constitutions.
    if "vaticano i" not in work_l and "concílio vaticano i" not in work_l:
        # Accept works that name the specific constitutions even without
        # "Vaticano I" as Vatican I (Dei Filius and Pastor Aeternus are
        # unambiguously Vatican I).
        if "dei filius" not in work_l and "pastor aeternus" not in work_l:
            return None
    # Vatican II sneaks in with "Sacrosanctum Concilium" etc. — reject.
    if "vaticano ii" in work_l:
        return None
    if "dei filius" in work_l:
        return "dei-filius"
    if "pastor aeternus" in work_l:
        return "pastor-aeternus"
    return "other"


VATICAN_II_DOCS = {
    "lumen gentium":            "lumen-gentium",
    "sacrosanctum concilium":   "sacrosanctum-concilium",
    "gaudium et spes":          "gaudium-et-spes",
    "dei verbum":               "dei-verbum",
    "unitatis redintegratio":   "unitatis-redintegratio",
    "nostra aetate":            "nostra-aetate",
    "dignitatis humanae":       "dignitatis-humanae",
    "orientalium ecclesiarum":  "orientalium-ecclesiarum",
    "ad gentes":                "ad-gentes",
    "presbyterorum ordinis":    "presbyterorum-ordinis",
    "optatam totius":           "optatam-totius",
    "perfectae caritatis":      "perfectae-caritatis",
    "christus dominus":         "christus-dominus",
    "apostolicam actuositatem": "apostolicam-actuositatem",
    "inter mirifica":           "inter-mirifica",
    "gravissimum educationis":  "gravissimum-educationis",
    "nota explicativa prévia":  "lumen-gentium-nota-explicativa",
    "notificações":             "lumen-gentium-nota-explicativa",
}


def vatican_ii_scope(work: str | None) -> str | None:
    if not work:
        return None
    work_l = work.lower()
    if "vaticano ii" not in work_l and "vatican ii" not in work_l:
        # Some rows just name the constitution; accept if we can match it.
        for key in VATICAN_II_DOCS:
            if key in work_l:
                return VATICAN_II_DOCS[key]
        return None
    for key, slug in VATICAN_II_DOCS.items():
        if key in work_l:
            return slug
    return "other"


FLORENCE_BULLS = {
    "cantate domino":       "cantate-domino",
    "exsultate deo":        "exsultate-deo",
    "laetentur caeli":      "laetentur-caeli",
    "moyses vir dei":       "moyses-vir-dei",
    "regimini universalis": "regimini-universalis",
}


def florence_scope(work: str | None) -> str | None:
    if not work:
        return None
    work_l = work.lower()
    florence_marker = (
        "florença" in work_l or "florence" in work_l
        or "basiléia" in work_l or "basel" in work_l
        or "ferrara" in work_l
    )
    bull_match = next((slug for key, slug in FLORENCE_BULLS.items() if key in work_l), None)
    if not florence_marker and bull_match is None:
        return None
    if bull_match:
        return bull_match
    return "other"


def constance_scope(work: str | None) -> str | None:
    if not work:
        return None
    work_l = work.lower()
    if "constança" not in work_l and "constance" not in work_l and "konstanz" not in work_l:
        if "inter cunctas" not in work_l:
            return None
    if "inter cunctas" in work_l:
        return "inter-cunctas"
    m = re.search(r"sessão\s+(\d{1,2})", work_l)
    if m:
        return f"session-{m.group(1)}"
    return "other"


COUNCIL_RULES: dict[str, Callable[[str | None], str | None]] = {
    "trent":          trent_scope,
    "vatican-i":      vatican_i_scope,
    "vatican-ii":     vatican_ii_scope,
    "basel-florence": florence_scope,
    "constance":      constance_scope,
}


# ----------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        # Pull every DH section, with its work + text.
        dh_rows = cur.execute(
            "SELECT section_num, text_la, text_pt, meta_json "
            "  FROM document_sections WHERE document_id=?",
            (HUNERMANN,),
        ).fetchall()
        log.info("scanning %d DH sections", len(dh_rows))

        # Pre-delete previously-minted rows + edges for these councils. We
        # identify them by the ``same_as`` edge to a DH section — that's
        # unique to this materialization.
        for council in COUNCIL_RULES:
            cur.execute(
                """
                DELETE FROM document_sections
                 WHERE document_id = ?
                   AND 'document-section:' || document_id || '/' || section_num IN (
                       SELECT source FROM graph_edges
                        WHERE edge_type='same_as'
                          AND source LIKE 'document-section:' || ? || '/%'
                          AND target LIKE 'document-section:denzinger-hunermann/%'
                   )
                """,
                (council, council),
            )
            cur.execute(
                """
                DELETE FROM graph_edges
                 WHERE edge_type='same_as'
                   AND source LIKE 'document-section:' || ? || '/%'
                   AND target LIKE 'document-section:denzinger-hunermann/%'
                """,
                (council,),
            )

        per_council: dict[str, int] = {c: 0 for c in COUNCIL_RULES}
        new_sections: list[tuple] = []
        new_same_as: list[tuple[str, str, str]] = []
        seen_keys: set[tuple[str, str]] = set()

        for dh_num, text_la, text_pt, meta_json in dh_rows:
            meta = json.loads(meta_json or "{}")
            work = meta.get("work")

            for council, rule in COUNCIL_RULES.items():
                scope = rule(work)
                if scope is None:
                    continue

                # section_num = "<scope>-denz-<dh_num>"  (always-unique,
                # readable, preserves the DH back-link without duplicating it
                # inside the id).
                section_num = f"{scope}-denz-{dh_num}"
                key = (council, section_num)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                # Carry over DH text + a trimmed meta block pointing back.
                new_meta = {
                    "source_denz":   dh_num,
                    "source_work":   work,
                    "source_path":   meta.get("path"),
                    "scope":         scope,
                    "attribution":   meta.get("attribution"),
                    "page":          meta.get("page"),
                }
                new_meta = {k: v for k, v in new_meta.items()
                            if v not in (None, "", [], {})}

                new_sections.append((
                    council, section_num,
                    None,      # text_en — Deferrari uses different numbering;
                               # leave null, a later concordance pass can fill.
                    text_la, text_pt,
                    json.dumps(new_meta, ensure_ascii=False),
                ))
                new_same_as.append((
                    f"document-section:{council}/{section_num}",
                    f"document-section:{HUNERMANN}/{dh_num}",
                    "same_as",
                ))
                per_council[council] += 1

        cur.executemany(
            """
            INSERT OR REPLACE INTO document_sections
              (document_id, section_num, text_en, text_la, text_pt, meta_json)
            VALUES (?,?,?,?,?,?)
            """,
            new_sections,
        )
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            new_same_as,
        )

        # Update each council's section_count.
        for council, n in per_council.items():
            cur.execute(
                "UPDATE documents SET section_count = ? WHERE id=?",
                (n, council),
            )

        conn.commit()

        log.info("== results ==")
        for council, n in per_council.items():
            log.info("  %-16s  %d new sections", council, n)
        log.info("total new document_sections rows: %d", sum(per_council.values()))
        log.info("total same_as edges: %d", len(new_same_as))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
