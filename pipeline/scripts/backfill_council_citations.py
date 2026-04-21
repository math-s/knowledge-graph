"""Backfill citations into the 19 councils ingested by
``inject_councils_from_newadvent``.

Scans CCC paragraphs, patristic sections, encyclopedia articles, and
document_sections for explicit references to each council, then writes:

- ``graph_nodes`` rows for council documents (if missing)
- ``paragraph_document_citations`` rows for CCC → council matches
- ``graph_edges`` ``cites`` edges from every matching source to
  ``document:<council-id>``
- Updates ``documents.citing_paragraphs_json`` for councils with CCC citations

Re-runnable: prunes existing council-targeted cites-edges and citation rows
before re-inserting. English-only in v1 (signal lives almost entirely in
``text_en`` columns).

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_council_citations
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("backfill_council_citations")

# Per-council regex patterns. Each pattern, when matched, attributes the
# surrounding text to the council. All patterns use \b boundaries and are
# case-insensitive. "Creed" patterns are included since creeds are direct
# products of their councils and appear heavily in CCC footnotes.
#
# Note: bare single-word place names ("Ephesus", "Chalcedon") are
# intentionally NOT included — too many false positives from geography.
# We require an explicit "council"/"synod"/"creed"/ordinal qualifier.
COUNCIL_PATTERNS: dict[str, list[str]] = {
    "nicaea-i": [
        r"\bfirst\s+council\s+of\s+nic(?:aea|æa)\b",
        r"\b1st\s+council\s+of\s+nic(?:aea|æa)\b",
        r"\bcouncil\s+of\s+nic(?:aea|æa)\s*\(?\s*(?:a\.?d\.?)?\s*325\b",
        r"\bcouncil\s+of\s+nic(?:aea|æa)\b(?![^.]{0,40}(?:ii|787|second))",
        r"\bnic(?:aea|æa)\s+i\b(?![iv])",
        r"\bnicene\s+council\b",
        r"\bnicene\s+creed\b",
        r"\bsynod\s+of\s+nic(?:aea|æa)\b(?![^.]{0,40}(?:ii|787|second))",
    ],
    "nicaea-ii": [
        r"\bsecond\s+council\s+of\s+nic(?:aea|æa)\b",
        r"\b2nd\s+council\s+of\s+nic(?:aea|æa)\b",
        r"\bcouncil\s+of\s+nic(?:aea|æa)\s*\(?\s*(?:a\.?d\.?)?\s*787\b",
        r"\bnic(?:aea|æa)\s+ii\b",
        r"\bseventh\s+ecumenical\s+council\b",
    ],
    "constantinople-i": [
        r"\bfirst\s+council\s+of\s+constantinople\b",
        r"\b1st\s+council\s+of\s+constantinople\b",
        r"\bcouncil\s+of\s+constantinople\s*\(?\s*(?:a\.?d\.?)?\s*381\b",
        r"\bconstantinople\s+i\b(?![iv])",
        r"\bsecond\s+ecumenical\s+council\b",
        r"\bnic(?:eno|aeno|æno)[-\s]?constantinopolitan\b",
    ],
    "constantinople-ii": [
        r"\bsecond\s+council\s+of\s+constantinople\b",
        r"\b2nd\s+council\s+of\s+constantinople\b",
        r"\bcouncil\s+of\s+constantinople\s*(?:ii|2)\b",
        r"\bcouncil\s+of\s+constantinople\s*\(?\s*(?:a\.?d\.?)?\s*553\b",
        r"\bconstantinople\s+ii\b(?!i)",
        r"\bfifth\s+ecumenical\s+council\b",
    ],
    "constantinople-iii": [
        r"\bthird\s+council\s+of\s+constantinople\b",
        r"\b3rd\s+council\s+of\s+constantinople\b",
        r"\bcouncil\s+of\s+constantinople\s*(?:iii|3)\b",
        r"\bcouncil\s+of\s+constantinople\s*\(?\s*(?:a\.?d\.?)?\s*68[01]\b",
        r"\bconstantinople\s+iii\b",
        r"\bsixth\s+ecumenical\s+council\b",
    ],
    "ephesus": [
        r"\bcouncil\s+of\s+ephesus\b",
        r"\bephesian\s+council\b",
        r"\bthird\s+ecumenical\s+council\b",
        r"\b(?:a\.?d\.?)?\s*431\s+council\s+of\s+ephesus\b",
    ],
    "chalcedon": [
        r"\bcouncil\s+of\s+chalcedon\b",
        r"\bchalcedonian\s+council\b",
        r"\bchalcedonian\s+creed\b",
        r"\bchalcedonian\s+definition\b",
        r"\bdefinition\s+of\s+chalcedon\b",
        r"\bfourth\s+ecumenical\s+council\b",
    ],
    "trullo": [
        r"\bcouncil\s+in\s+trullo\b",
        r"\btrullan\s+council\b",
        r"\btrullan\s+synod\b",
        r"\bquini[-\s]?sext(?:ine)?\s+council\b",
    ],
    "ancyra": [
        r"\b(?:council|synod)\s+of\s+ancyra\b",
    ],
    "neocaesarea": [
        r"\b(?:council|synod)\s+of\s+neo[-\s]?caesarea\b",
    ],
    "gangra": [
        r"\b(?:council|synod)\s+of\s+gangra\b",
    ],
    "antioch": [
        r"\b(?:council|synod)\s+of\s+antioch\s+in\s+encaeniis\b",
        r"\bsynod\s+of\s+antioch\s*\(?\s*(?:a\.?d\.?)?\s*341\b",
    ],
    "laodicea": [
        r"\b(?:council|synod)\s+of\s+laodicea\b",
    ],
    "sardica": [
        r"\b(?:council|synod)\s+of\s+sardica\b",
    ],
    "carthage-257": [
        r"\bcouncil\s+of\s+carthage\s*\(?\s*(?:a\.?d\.?)?\s*257\b",
    ],
    "carthage-419": [
        r"\bcouncil\s+of\s+carthage\s*\(?\s*(?:a\.?d\.?)?\s*419\b",
        r"\bfourth\s+council\s+of\s+carthage\b",
        r"\biv\s+carthage\b",
    ],
    "constantinople-382": [
        r"\b(?:council|synod)\s+of\s+constantinople\s*\(?\s*(?:a\.?d\.?)?\s*382\b",
    ],
    "constantinople-394": [
        r"\b(?:council|synod)\s+of\s+constantinople\s*\(?\s*(?:a\.?d\.?)?\s*394\b",
    ],
    "apostolic-canons": [
        r"\bapostolic\s+canons?\b",
        r"\bcanons?\s+of\s+the\s+apostles\b",
    ],
    # Councils VIII–XXI (stub documents bootstrapped from almanac_14388a;
    # full text not yet ingested, but citations can still target them).
    "constantinople-iv": [
        r"\bfourth\s+council\s+of\s+constantinople\b",
        r"\b4th\s+council\s+of\s+constantinople\b",
        r"\bcouncil\s+of\s+constantinople\s*\(?\s*(?:a\.?d\.?)?\s*869\b",
        r"\bconstantinople\s+iv\b",
        r"\beighth\s+ecumenical\s+council\b",
    ],
    "lateran-i": [
        r"\bfirst\s+lateran\s+(?:council|synod)\b",
        r"\b1st\s+lateran\s+(?:council|synod)\b",
        r"\blateran\s+(?:council\s+)?i\b(?![iv])",
        r"\blateran\s+\(?\s*(?:a\.?d\.?)?\s*1123\b",
        r"\bninth\s+ecumenical\s+council\b",
    ],
    "lateran-ii": [
        r"\bsecond\s+lateran\s+(?:council|synod)\b",
        r"\b2nd\s+lateran\s+(?:council|synod)\b",
        r"\blateran\s+(?:council\s+)?ii\b(?!i)",
        r"\blateran\s+\(?\s*(?:a\.?d\.?)?\s*1139\b",
        r"\btenth\s+ecumenical\s+council\b",
    ],
    "lateran-iii": [
        r"\bthird\s+lateran\s+(?:council|synod)\b",
        r"\b3rd\s+lateran\s+(?:council|synod)\b",
        r"\blateran\s+(?:council\s+)?iii\b",
        r"\blateran\s+\(?\s*(?:a\.?d\.?)?\s*1179\b",
        r"\beleventh\s+ecumenical\s+council\b",
    ],
    "lateran-iv": [
        r"\bfourth\s+lateran\s+(?:council|synod)\b",
        r"\b4th\s+lateran\s+(?:council|synod)\b",
        r"\blateran\s+(?:council\s+)?iv\b",
        r"\blateran\s+\(?\s*(?:a\.?d\.?)?\s*1215\b",
        r"\btwelfth\s+ecumenical\s+council\b",
    ],
    "lateran-v": [
        r"\bfifth\s+lateran\s+(?:council|synod)\b",
        r"\b5th\s+lateran\s+(?:council|synod)\b",
        r"\blateran\s+(?:council\s+)?v\b(?![i])",
        r"\blateran\s+\(?\s*(?:a\.?d\.?)?\s*151[2-7]\b",
        r"\beighteenth\s+ecumenical\s+council\b",
    ],
    "lyon-i": [
        r"\bfirst\s+council\s+of\s+lyon(?:s)?\b",
        r"\b1st\s+council\s+of\s+lyon(?:s)?\b",
        r"\blyon(?:s)?\s+i\b(?![iv])",
        r"\bcouncil\s+of\s+lyon(?:s)?\s*\(?\s*(?:a\.?d\.?)?\s*1245\b",
        r"\bthirteenth\s+ecumenical\s+council\b",
    ],
    "lyon-ii": [
        r"\bsecond\s+council\s+of\s+lyon(?:s)?\b",
        r"\b2nd\s+council\s+of\s+lyon(?:s)?\b",
        r"\blyon(?:s)?\s+ii\b",
        r"\bcouncil\s+of\s+lyon(?:s)?\s*\(?\s*(?:a\.?d\.?)?\s*1274\b",
        r"\bfourteenth\s+ecumenical\s+council\b",
    ],
    "vienne": [
        r"\bcouncil\s+of\s+vienne\b",
        r"\bfifteenth\s+ecumenical\s+council\b",
    ],
    "constance": [
        r"\bcouncil\s+of\s+constance\b",
        r"\bsixteenth\s+ecumenical\s+council\b",
    ],
    "basel-florence": [
        r"\bcouncil\s+of\s+basel\b",
        r"\bcouncil\s+of\s+ferrara\b",
        r"\bcouncil\s+of\s+florence\b",
        r"\bbasel[-\s/]ferrara[-\s/]florence\b",
        r"\bbasel[-\s/]florence\b",
        r"\bferrara[-\s/]florence\b",
        r"\bseventeenth\s+ecumenical\s+council\b",
    ],
    "trent": [
        r"\bcouncil\s+of\s+trent\b",
        r"\btridentine\s+(?:council|reform|mass|decree)\b",
        r"\bnineteenth\s+ecumenical\s+council\b",
    ],
    "vatican-i": [
        r"\bfirst\s+vatican\s+council\b",
        r"\b1st\s+vatican\s+council\b",
        r"\bvatican\s+(?:council\s+)?i\b(?![iv])",
        r"\bvatican\s+council\s+\(?\s*(?:a\.?d\.?)?\s*186[89]\b",
        r"\btwentieth\s+ecumenical\s+council\b",
    ],
    "vatican-ii": [
        r"\bsecond\s+vatican\s+council\b",
        r"\b2nd\s+vatican\s+council\b",
        r"\bvatican\s+(?:council\s+)?ii\b",
        r"\btwenty-first\s+ecumenical\s+council\b",
    ],
}

# Compiled regex per council (alternation of all patterns).
COMPILED: dict[str, re.Pattern[str]] = {
    cid: re.compile("|".join(f"(?:{p})" for p in pats), re.IGNORECASE)
    for cid, pats in COUNCIL_PATTERNS.items()
}

# Canon attribution: look for a canon number within this many chars EITHER
# side of a council mention. The closest hit wins. "Canon 6 of the Council
# of Nicaea" and "Council of Nicaea, canon 6" should both attribute to 6.
CANON_WINDOW = 100

# Forms accepted: "canon 6", "canons 6", "can. 6", "c. 6", "cc. 6". We avoid
# bare "c." at word start without a number to reduce false positives on
# "c. 350 AD" style dates (word-boundary + digit requirement handles that).
CANON_NUM_RE = re.compile(
    r"\b(?:canons?|can\.|cc\.|c\.)\s*(\d{1,3})\b",
    re.IGNORECASE,
)
CANON_ROMAN_RE = re.compile(
    r"\b(?:canons?|can\.)\s+(X{0,3}(?:IX|IV|V?I{1,3})|L?X{1,3}(?:IX|IV|V?I{0,3}))\b",
    re.IGNORECASE,
)

_ROMAN = {
    "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7, "viii": 8,
    "ix": 9, "x": 10, "xi": 11, "xii": 12, "xiii": 13, "xiv": 14, "xv": 15,
    "xvi": 16, "xvii": 17, "xviii": 18, "xix": 19, "xx": 20, "xxi": 21,
    "xxii": 22, "xxiii": 23, "xxiv": 24, "xxv": 25, "xxvi": 26, "xxvii": 27,
    "xxviii": 28, "xxix": 29, "xxx": 30,
}


def _nearest_canon(text: str, match_start: int, match_end: int) -> str | None:
    """Scan ±CANON_WINDOW chars around [match_start, match_end) for the
    canon number closest to the council mention. Returns a decimal string
    or None."""
    win_lo = max(0, match_start - CANON_WINDOW)
    win_hi = min(len(text), match_end + CANON_WINDOW)
    window = text[win_lo:win_hi]
    # Position of the council mention within the window
    mention_lo = match_start - win_lo
    mention_hi = match_end - win_lo

    best: tuple[int, str] | None = None  # (distance, section_num)
    for m in CANON_NUM_RE.finditer(window):
        if mention_lo <= m.start() < mention_hi:
            continue  # inside the match itself
        dist = min(abs(m.start() - mention_hi), abs(mention_lo - m.end()))
        if dist > CANON_WINDOW:
            continue
        sec = m.group(1).lstrip("0") or "0"
        if best is None or dist < best[0]:
            best = (dist, sec)
    for m in CANON_ROMAN_RE.finditer(window):
        if mention_lo <= m.start() < mention_hi:
            continue
        dist = min(abs(m.start() - mention_hi), abs(mention_lo - m.end()))
        if dist > CANON_WINDOW:
            continue
        val = _ROMAN.get(m.group(1).lower())
        if not val:
            continue
        if best is None or dist < best[0]:
            best = (dist, str(val))
    return best[1] if best else None


def scan_text(text: str) -> dict[str, set[str | None]]:
    """Return {council_id: {section_num_or_None, ...}} for all matches.
    Section_num is the decimal canon number resolved via _nearest_canon, or
    None when no canon appears within ±CANON_WINDOW of the mention."""
    if not text:
        return {}
    hits: dict[str, set[str | None]] = defaultdict(set)
    for cid, pat in COMPILED.items():
        for m in pat.finditer(text):
            sec = _nearest_canon(text, m.start(), m.end())
            hits[cid].add(sec)
    return hits


def ensure_council_nodes(cur: sqlite3.Cursor, council_ids: list[str]) -> int:
    rows = []
    for cid in council_ids:
        name_row = cur.execute(
            "SELECT name FROM documents WHERE id = ?", (cid,)
        ).fetchone()
        if not name_row:
            log.warning("Council %s not present in documents table — skipping", cid)
            continue
        (name,) = name_row
        rows.append((
            f"document:{cid}",
            name,
            "document",
            0.0,
            0.0,
            6.0,
            "#E6A23C",
            "",
            0,
            0,
            "[]",
            "[]",
            "[]",
        ))
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    return len(rows)


def prune_old_edges(cur: sqlite3.Cursor, council_ids: list[str]) -> None:
    """Remove any prior cites-edges targeting councils (document-level and
    section-level), plus CCC citation rows."""
    ph = ",".join(["?"] * len(council_ids))
    targets_doc = [f"document:{cid}" for cid in council_ids]
    cur.execute(
        f"DELETE FROM graph_edges WHERE edge_type='cites' AND target IN ({','.join(['?']*len(targets_doc))})",
        targets_doc,
    )
    # section-level targets: document-section:<cid>/<anything>
    for cid in council_ids:
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type='cites' AND target LIKE ?",
            (f"document-section:{cid}/%",),
        )
    cur.execute(
        f"DELETE FROM paragraph_document_citations WHERE document IN ({ph})",
        council_ids,
    )


def load_valid_sections(cur: sqlite3.Cursor) -> set[tuple[str, str]]:
    """Return {(document_id, section_num), ...} — used to verify a captured
    canon number actually maps to an existing document_sections row before
    we emit a section-level edge."""
    return set(
        cur.execute("SELECT document_id, section_num FROM document_sections").fetchall()
    )


def backfill_ccc(
    cur: sqlite3.Cursor,
    valid_sections: set[tuple[str, str]],
) -> tuple[int, dict[str, set[int]]]:
    """Scan CCC paragraphs' text + footnotes. Writes to
    paragraph_document_citations and returns (edges_written, {cid: {p_ids}}).

    Emits section-level graph edges (``p:N -> document-section:<cid>/<sec>``)
    whenever a canon number was captured and resolves to a real section;
    falls back to document-level (``p:N -> document:<cid>``) otherwise.
    """
    rows = cur.execute(
        "SELECT id, text_en, footnotes_json FROM paragraphs"
    ).fetchall()

    citation_rows: set[tuple[int, str, str]] = set()  # (p_id, doc, section_str)
    citing_by_council: dict[str, set[int]] = defaultdict(set)
    edge_rows: set[tuple[str, str, str]] = set()

    for p_id, text_en, footnotes_json in rows:
        combined_parts: list[str] = []
        if text_en:
            combined_parts.append(text_en)
        if footnotes_json:
            try:
                fn_list = json.loads(footnotes_json)
                combined_parts.extend(str(f) for f in fn_list)
            except (json.JSONDecodeError, TypeError):
                pass
        if not combined_parts:
            continue
        combined = "\n".join(combined_parts)

        hits = scan_text(combined)
        for cid, sections in hits.items():
            citing_by_council[cid].add(p_id)
            for sec in sections:
                citation_rows.add((p_id, cid, sec or ""))
                if sec and (cid, sec) in valid_sections:
                    edge_rows.add((f"p:{p_id}", f"document-section:{cid}/{sec}", "cites"))
                else:
                    edge_rows.add((f"p:{p_id}", f"document:{cid}", "cites"))

    cur.executemany(
        "INSERT INTO paragraph_document_citations(paragraph_id, document, section) VALUES (?,?,?)",
        [(p, d, s if s else None) for (p, d, s) in citation_rows],
    )
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        list(edge_rows),
    )
    return len(edge_rows), citing_by_council


def backfill_generic(
    cur: sqlite3.Cursor,
    select_sql: str,
    row_to_node: callable,
    valid_sections: set[tuple[str, str]],
    params: tuple = (),
) -> tuple[int, dict[str, int]]:
    """Generic scan: yields (node_id, text) tuples; returns (edges_written,
    per-council-count). Prefers section-level edges when a canon number
    resolves to a real document_sections row; falls back to document level."""
    rows = cur.execute(select_sql, params).fetchall()
    edges: set[tuple[str, str, str]] = set()
    per_council: dict[str, int] = defaultdict(int)
    for row in rows:
        node_id, text = row_to_node(row)
        if not node_id or not text:
            continue
        hits = scan_text(text)
        for cid, sections in hits.items():
            per_council[cid] += 1
            for sec in sections:
                if sec and (cid, sec) in valid_sections:
                    edges.add((node_id, f"document-section:{cid}/{sec}", "cites"))
                else:
                    edges.add((node_id, f"document:{cid}", "cites"))
    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        list(edges),
    )
    return len(edges), dict(per_council)


def update_citing_paragraphs_json(
    cur: sqlite3.Cursor,
    citing_by_council: dict[str, set[int]],
    all_council_ids: list[str],
) -> None:
    for cid in all_council_ids:
        pids = sorted(citing_by_council.get(cid, set()))
        cur.execute(
            "UPDATE documents SET citing_paragraphs_json = ? WHERE id = ?",
            (json.dumps(pids), cid),
        )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        council_ids = sorted(COUNCIL_PATTERNS.keys())

        log.info("== pass 0: ensure graph_nodes for councils ==")
        added = ensure_council_nodes(cur, council_ids)
        log.info("  inserted/ignored %d council document nodes", added)

        log.info("== pass 0b: prune old citation artifacts ==")
        prune_old_edges(cur, council_ids)

        valid_sections = load_valid_sections(cur)
        log.info("loaded %d valid (doc,section) pairs for resolution", len(valid_sections))

        log.info("== pass 1: CCC paragraphs ==")
        n_edges, citing_by_council = backfill_ccc(cur, valid_sections)
        for cid in council_ids:
            n = len(citing_by_council.get(cid, set()))
            if n:
                log.info("  %-20s %4d CCC paragraphs", cid, n)
        log.info("  %d CCC→council cites edges", n_edges)
        update_citing_paragraphs_json(cur, citing_by_council, council_ids)

        log.info("== pass 2: patristic sections ==")
        n2, counts2 = backfill_generic(
            cur,
            "SELECT id, text_en FROM patristic_sections WHERE text_en IS NOT NULL",
            lambda r: (f"patristic-section:{r[0]}", r[1]),
            valid_sections,
        )
        for cid in sorted(counts2, key=lambda k: -counts2[k]):
            log.info("  %-20s %4d patristic sections", cid, counts2[cid])
        log.info("  %d patristic→council cites edges", n2)

        log.info("== pass 3: encyclopedia articles ==")
        n3, counts3 = backfill_generic(
            cur,
            "SELECT id, text_en FROM encyclopedia WHERE text_en IS NOT NULL",
            lambda r: (f"ency:{r[0]}", r[1]),
            valid_sections,
        )
        for cid in sorted(counts3, key=lambda k: -counts3[k]):
            log.info("  %-20s %4d encyclopedia articles", cid, counts3[cid])
        log.info("  %d encyclopedia→council cites edges", n3)

        log.info("== pass 4: document_sections (incl. councils citing councils) ==")
        n4, counts4 = backfill_generic(
            cur,
            "SELECT document_id, section_num, text_en FROM document_sections WHERE text_en IS NOT NULL",
            lambda r: (f"document-section:{r[0]}/{r[1]}", r[2]),
            valid_sections,
        )
        for cid in sorted(counts4, key=lambda k: -counts4[k]):
            log.info("  %-20s %4d document sections", cid, counts4[cid])
        log.info("  %d doc-section→council cites edges", n4)

        conn.commit()

        total = n_edges + n2 + n3 + n4
        log.info("== done: %d total cites edges into councils ==", total)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
