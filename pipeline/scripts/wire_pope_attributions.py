"""Wire pope attributions from Denzinger into the graph.

Denzinger is primarily a compilation of papal + conciliar utterances.
Every DH section's ``attribution`` field and every Deferrari section's
``path.author`` field identifies the originating speaker — 177 distinct
DH attributions, 129 distinct Deferrari path.authors, with heavy overlap
in popes (Pio IX: 254, João Paulo II: 339, Paulo VI: 375, etc.).

This script:
  1. Maps PT + EN pope-name variants to a canonical ``pope-<slug>`` id.
  2. Creates a ``pope:<slug>`` graph node per distinct pope.
  3. Emits ``authored_by`` edges:
       ``document-section:denzinger-*  -> pope:<slug>``
     One edge per DH/Deferrari section whose attribution names a pope.
  4. Where the pope authored a first-class document already in the DB
     (e.g., Rerum Novarum by Leo XIII), emits:
       ``pope:<slug> -> document:<encyclical-id>`` with edge_type ``authored``

This gives us ~12k new edges making the "who wrote what" dimension
navigable: "every text João Paulo II added to the magisterium", etc.

Idempotent: deletes prior ``pope:*`` nodes and ``authored_by``/``authored``
edges before re-insert.

Usage:
    uv run --project pipeline python -m pipeline.scripts.wire_pope_attributions
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

log = logging.getLogger("wire_pope_attributions")


# ----------------------------------------------------------------------
# Pope name → canonical slug
# ----------------------------------------------------------------------
# Keys are the "core name" extracted from the attribution (lowercased,
# with PT-to-EN substitution of first names applied). The numeral is
# preserved verbatim (i, ii, iii, iv, v, vi, vii, viii, ix, x, xi, xii,
# xiii, xiv, xv, xvi, xvii...). So "Paulo VI" → key "paul vi" → slug
# "pope-paul-vi".

PT_TO_EN_FIRST_NAME = {
    "paulo":        "paul",
    "joão":         "john",
    "pio":          "pius",
    "leão":         "leo",
    "inocêncio":    "innocent",
    "clemente":     "clement",
    "júlio":        "julius",
    "gregório":     "gregory",
    "alexandre":    "alexander",
    "bento":        "benedict",
    "bénedicto":    "benedict",
    "martinho":     "martin",
    "félix":        "felix",
    "felix":        "felix",
    "dâmaso":       "damasus",
    "damaso":       "damasus",
    "vigílio":      "vigilius",
    "celestino":    "celestine",
    "eugênio":      "eugene",
    "eugenio":      "eugene",
    "sisto":        "sixtus",
    "sixto":        "sixtus",
    "anastásio":    "anastasius",
    "anastasio":    "anastasius",
    "agatão":       "agatho",
    "símaco":       "symmachus",
    "simaco":       "symmachus",
    "gelásio":      "gelasius",
    "gelasio":      "gelasius",
    "hormisdas":    "hormisdas",
    "adriano":      "adrian",
    "nicolau":      "nicholas",
    "honório":      "honorius",
    "honorio":      "honorius",
    "siriço":       "siricius",
    "siricio":      "siricius",
    "sotério":      "soter",
    "soterio":      "soter",
    "estevão":      "stephen",
    "estevao":      "stephen",
    "liberio":      "liberius",
    "libério":      "liberius",
    "zacarias":     "zachary",
    "cornélio":     "cornelius",
    "cornelio":     "cornelius",
    "sérgio":       "sergius",
    "sergio":       "sergius",
    "hilário":      "hilary",
    "hilario":      "hilary",
    "urbano":       "urban",
    "bonifácio":    "boniface",
    "bonifacio":    "boniface",
    "calisto":      "callixtus",
    "calixto":      "callixtus",
    "vitor":        "victor",
    "vítor":        "victor",
    "telesforo":    "telesphorus",
    "telesfóro":    "telesphorus",
    "gaio":         "caius",
    "silvestre":    "sylvester",
    "zósimo":       "zosimus",
    "zosimo":       "zosimus",
    "símplicio":    "simplicius",
    "simplicio":    "simplicius",
    "pelágio":      "pelagius",
    "pelagio":      "pelagius",
    "teodoro":      "theodore",
    "joão paulo":   "john paul",
}

# Common sanitization — strip year ranges, parenthetical alt numerals, titles.
_YEAR_RE  = re.compile(r"\s*\d{3,4}\s*(?:-\s*\d{0,4})?")
_PAREN_RE = re.compile(r"\s*\([^)]*\)")
_TITLE_RE = re.compile(r"\b(st\.?|saint|são|santa|ss\.?|pope)\s+", re.IGNORECASE)


def normalize_pope_name(raw: str) -> str | None:
    """Return canonical key like ``"john paul ii"`` or ``None`` if not a pope."""
    if not raw:
        return None
    s = raw.strip()

    # Strip "COUNCIL OF …" trailing attribution on Deferrari paths.
    s = re.split(r"\bCOUNCIL\s+OF\b|\bLATERAN\s+COUNCIL\b|\bTHE\s+VATICAN\s+COUNCIL\b",
                 s, maxsplit=1, flags=re.IGNORECASE)[0].strip()

    s = _PAREN_RE.sub("", s)
    s = _YEAR_RE.sub("", s).strip(" -,")
    s = _TITLE_RE.sub("", s)

    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s).strip().lower()
    if not s:
        return None

    # Translate PT first name → EN.
    parts = s.split(" ", 1)
    first = parts[0]
    rest = parts[1] if len(parts) == 2 else ""

    # Handle "joão paulo" (two-word first name)
    if s.startswith("joão paulo"):
        first = "john paul"
        rest = s[len("joão paulo"):].strip()

    first_en = PT_TO_EN_FIRST_NAME.get(first, first)
    key = (first_en + " " + rest).strip()

    # Must have at least a name and a numeral (Roman or Arabic) to count as a pope.
    if not re.search(r"\b[ivxlcdm]+\b|\b\d+\b", key, re.IGNORECASE):
        return None

    # Must start with a known pope first name (guard against false positives
    # like "Símbolos da fé", "Padres apostólicos", "S. Ofício", councils, etc.)
    known_first_names = set(PT_TO_EN_FIRST_NAME.values())
    known_first_names.update(PT_TO_EN_FIRST_NAME.keys())
    # Also include common EN-native pope first-names that never show up as PT
    known_first_names.update({
        "paul", "john", "pius", "leo", "innocent", "clement", "julius",
        "gregory", "alexander", "benedict", "martin", "felix", "damasus",
        "vigilius", "celestine", "eugene", "sixtus", "anastasius", "agatho",
        "symmachus", "gelasius", "hormisdas", "adrian", "nicholas", "honorius",
        "siricius", "soter", "stephen", "liberius", "zachary", "cornelius",
        "sergius", "hilary", "urban", "boniface", "callixtus", "victor",
        "telesphorus", "caius", "sylvester", "zosimus", "simplicius",
        "pelagius", "theodore", "john paul",
    })
    if first_en not in known_first_names:
        return None

    return key


def slugify(key: str) -> str:
    return "pope-" + re.sub(r"[^a-z0-9]+", "-", key.lower()).strip("-")


# ----------------------------------------------------------------------
# Pope → encyclical authorship mapping via existing documents table
# ----------------------------------------------------------------------
# Hard to derive mechanically — encyclicals in the DB have no author field.
# We'd need external data (AAS / vatican.va). Leave for a follow-up pass.
# This script only wires section-level authorship for now.

# ----------------------------------------------------------------------

def load_attributions(cur: sqlite3.Cursor) -> list[tuple[str, str, str]]:
    """Return list of (doc_id, section_num, raw_attribution).

    Combines DH.meta_json.attribution with Deferrari.meta_json.path.author.
    """
    out: list[tuple[str, str, str]] = []
    for row in cur.execute(
        "SELECT document_id, section_num, meta_json "
        "  FROM document_sections "
        " WHERE document_id IN ('denzinger-hunermann','denzinger-deferrari-30')"
    ):
        doc_id, section_num, meta_json = row
        meta = json.loads(meta_json or "{}")
        if doc_id == "denzinger-hunermann":
            attr = meta.get("attribution")
        else:
            path = meta.get("path") or {}
            attr = path.get("author")
        if attr:
            out.append((doc_id, section_num, attr))
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        attrs = load_attributions(cur)
        log.info("scanning %d attributions from both editions", len(attrs))

        # (canonical_key, display_name_raw_sample) → list of (doc_id, section_num)
        pope_sections: dict[str, list[tuple[str, str]]] = {}
        # Track a human-readable sample per pope for node label
        pope_sample: dict[str, str] = {}
        unmapped = 0

        for doc_id, section_num, raw in attrs:
            key = normalize_pope_name(raw)
            if not key:
                unmapped += 1
                continue
            pope_sections.setdefault(key, []).append((doc_id, section_num))
            if key not in pope_sample:
                pope_sample[key] = raw.strip()

        log.info("distinct popes identified: %d", len(pope_sections))
        log.info("attributions not recognized as popes (councils, creeds, etc.): %d", unmapped)

        # Sorted by frequency for display
        ranked = sorted(pope_sections.items(), key=lambda x: -len(x[1]))[:10]
        log.info("top 10 popes by section count:")
        for key, secs in ranked:
            log.info("  %-24s %4d sections (e.g. '%s')",
                     slugify(key), len(secs), pope_sample[key])

        # --- idempotent clear of prior pope nodes + authored_by edges ---
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type IN ('authored_by','authored') "
            "  AND (source LIKE 'pope-%:%' OR target LIKE 'pope-%' OR source LIKE 'pope:%' OR target LIKE 'pope:%')"
        )
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type='authored_by' AND target LIKE 'pope-%'"
        )
        cur.execute(
            "DELETE FROM graph_edges WHERE edge_type='authored_by' AND target LIKE 'pope:%'"
        )
        cur.execute("DELETE FROM graph_nodes WHERE id LIKE 'pope-%'")
        cur.execute("DELETE FROM graph_nodes WHERE id LIKE 'pope:%'")

        # --- insert pope graph nodes ---
        node_rows: list[tuple] = []
        for key, sample in pope_sample.items():
            node_id = slugify(key)
            # Use the raw sample as the human label (preserves PT spelling).
            label = sample
            node_rows.append((
                node_id,
                label,
                "pope",
                0.0, 0.0, 3.0, "#C71585", "",
                0, 0, "[]", "[]", "[]",
            ))
        cur.executemany(
            "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            node_rows,
        )
        log.info("inserted %d pope graph nodes", len(node_rows))

        # --- authored_by edges ---
        edge_rows: list[tuple[str, str, str]] = []
        for key, sections in pope_sections.items():
            pope_node = slugify(key)
            for doc_id, section_num in sections:
                edge_rows.append((
                    f"document-section:{doc_id}/{section_num}",
                    pope_node,
                    "authored_by",
                ))
        cur.executemany(
            "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
            edge_rows,
        )
        conn.commit()
        log.info("wrote %d authored_by edges", len(edge_rows))

        # Sanity: which popes have the most reach?
        log.info("top popes by total authored_by edges (post-insert):")
        for row in cur.execute(
            """
            SELECT target, COUNT(*) n FROM graph_edges
             WHERE edge_type='authored_by' AND target LIKE 'pope-%'
             GROUP BY target ORDER BY n DESC LIMIT 10
            """
        ):
            log.info("  %-30s %4d", row[0], row[1])
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
