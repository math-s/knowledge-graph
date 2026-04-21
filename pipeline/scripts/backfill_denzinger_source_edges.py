"""Wire edges from Denzinger sections to their originating councils,
patristic authors, and — where applicable — the parallel DS/DH stub.

Denzinger is a *compilation* of magisterial texts. Every section
originates from a specific speaker: a Pope, an ecumenical or local
council, or (in the earliest entries) a Father of the Church. Both
source files carry this provenance in human-readable form:

  - EN Deferrari: ``path.author`` / ``path.document`` / ``path.document_source``
    — uppercase English, e.g. ``"COUNCIL OF CHALCEDON 451"``,
    ``"ST. CLEMENT I 90 O-99 (?)"``, ``"LATERAN COUNCIL IV 1215"``.
  - PT Hünermann: ``attribution`` / ``work`` in ``meta_json`` — Portuguese,
    e.g. ``"Paulo III"``, ``"Concílio ecumênico de Trento, sessão 6"``.

We emit three kinds of edge for each Denzinger section, at section
granularity on the source side and document-level on the target side:

  * ``document-section:<denz>/<n>  ->  document:<council-id>``  (cites)
    — when the section quotes a council text.
  * ``document-section:<denz>/<n>  ->  author:<patristic-id>``  (cites)
    — when the section quotes a Father.
  * ``document-section:denzinger-schonmetzer/<n>``  is created as a node
    and linked to ``document-section:denzinger-hunermann/<n>`` via a
    ``same_as`` edge. DS and DH share their numbering (post-1963 Rahner
    renumber), so the 185 existing CCC→DS paragraph citations become
    reachable via DH's actual text.

Idempotent. Uses pyproject.toml-declared deps only.

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_denzinger_source_edges
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
SCHONMETZER = "denzinger-schonmetzer"

log = logging.getLogger("backfill_denzinger_source_edges")


# ----------------------------------------------------------------------
# Council detector
# ----------------------------------------------------------------------
# Each entry: (compiled regex, target document id). Regex runs against the
# concatenated attribution+work+path fields, case-insensitive. Order
# matters — list more specific patterns first so "LATERAN IV" doesn't fire
# on a generic "LATERAN" rule.

    # Used between a council name and its Roman-numeral ordinal to permit
    # intervening words like "COUNCIL", "Concílio", "ecumênico" without
    # being fooled by long arbitrary text. Non-greedy, capped at ~40 chars.
_GAP = r"(?:\s+[A-Za-zÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÇÃÕáéíóúàèìòùâêîôûçãõ.,]+){0,5}\s+"

COUNCIL_RULES: list[tuple[re.Pattern[str], str]] = [
    # --- Vatican councils ---
    (re.compile(rf"(?:vatican|vaticano)(?:{_GAP}|\s+)(?:ii|2|segundo|second)|segundo\s+concílio\s+vaticano|second\s+vatican", re.I), "vatican-ii"),
    (re.compile(rf"(?:vatican|vaticano)(?:{_GAP}|\s+)(?:i\b|1\b|primeiro|first)|the\s+vatican\s+council", re.I), "vatican-i"),
    # --- Trent ---
    (re.compile(r"trent(?!in)|trento|tridentin", re.I), "trent"),
    # --- Nicaea ---
    (re.compile(rf"(?:nicaea|nicéia|nicea|niceno|nicene)(?:{_GAP}|\s+)(?:ii|2|second)|second\s+(?:council\s+of\s+)?nicaea", re.I), "nicaea-ii"),
    (re.compile(r"nicaea|nicéia|nicea\b|niceno\b|nicene\b", re.I), "nicaea-i"),
    # --- Chalcedon, Ephesus ---
    (re.compile(r"chalcedon|calcedônia|calchedonens|calcedon", re.I), "chalcedon"),
    (re.compile(r"ephesus|éfeso|efeso|ephesin", re.I), "ephesus"),
    # --- Constantinople I–IV ---
    (re.compile(rf"(?:constantinople|constantinopla)(?:{_GAP}|\s+)(?:iv|4|fourth|quarto)|fourth\s+constantinople", re.I), "constantinople-iv"),
    (re.compile(rf"(?:constantinople|constantinopla)(?:{_GAP}|\s+)(?:iii|3|third|terceiro)|third\s+constantinople", re.I), "constantinople-iii"),
    (re.compile(rf"(?:constantinople|constantinopla)(?:{_GAP}|\s+)(?:ii|2|second|segundo)|second\s+constantinople", re.I), "constantinople-ii"),
    (re.compile(r"constantinople|constantinopla", re.I), "constantinople-i"),
    # --- Lateran I–V ---
    (re.compile(rf"(?:lateran|laterano|lateranens)(?:{_GAP}|\s+)(?:v|5|fifth|quinto)", re.I), "lateran-v"),
    (re.compile(rf"(?:lateran|laterano|lateranens)(?:{_GAP}|\s+)(?:iv|4|fourth|quarto)", re.I), "lateran-iv"),
    (re.compile(rf"(?:lateran|laterano|lateranens)(?:{_GAP}|\s+)(?:iii|3|third|terceiro)", re.I), "lateran-iii"),
    (re.compile(rf"(?:lateran|laterano|lateranens)(?:{_GAP}|\s+)(?:ii|2|second|segundo)", re.I), "lateran-ii"),
    (re.compile(rf"(?:lateran|laterano|lateranens)(?:{_GAP}|\s+)(?:i\b|1\b|first|primeiro)", re.I), "lateran-i"),
    # --- Lyon I–II ---
    (re.compile(rf"(?:lyon|lions?|lugdun)(?:{_GAP}|\s+)(?:ii|2|second|segundo)|second\s+lyon", re.I), "lyon-ii"),
    (re.compile(r"(?:lyon|lions?|lugdun)\b|first\s+lyon", re.I), "lyon-i"),
    # --- Medieval ecumenical ---
    (re.compile(r"florence|florença|ferrara|basle|basel", re.I), "basel-florence"),
    (re.compile(r"constance|constança|konstanz", re.I), "constance"),
    (re.compile(r"vienne|viena(?!d)", re.I), "vienne"),
    # --- Early local synods (catalog entries exist) ---
    (re.compile(r"trullo", re.I), "trullo"),
    (re.compile(r"sardica", re.I), "sardica"),
    (re.compile(r"ancyra", re.I), "ancyra"),
    (re.compile(r"gangra", re.I), "gangra"),
    (re.compile(r"laodicea", re.I), "laodicea"),
    (re.compile(r"neocaesarea", re.I), "neocaesarea"),
    (re.compile(r"apostolic\s+canons|cânones?\s+apostólicos", re.I), "apostolic-canons"),
    (re.compile(r"synod\s+of\s+antioch|sínodo\s+de\s+antioquia|antioch[^a-z]*in\s+encaeniis", re.I), "antioch"),
    (re.compile(r"carthage\s*(?:\(|,)?\s*257", re.I), "carthage-257"),
    (re.compile(r"carthage\s*(?:\(|,)?\s*419", re.I), "carthage-419"),
    (re.compile(r"constantinople\s+\(?382\)?", re.I), "constantinople-382"),
    (re.compile(r"constantinople\s+\(?394\)?", re.I), "constantinople-394"),
]


# ----------------------------------------------------------------------
# Patristic author detector
# ----------------------------------------------------------------------
# Careful with papal vs patristic Clements. "ST. CLEMENT I" in a Denzinger
# attribution is Pope Clement I = Clement of Rome (a Father too). But
# "CLEMENT VII" etc. are later popes — not fathers, skip. We anchor the
# patristic rules on names that couldn't be confused with later popes.

AUTHOR_RULES: list[tuple[re.Pattern[str], str]] = [
    # Early Roman popes who are also Fathers
    (re.compile(r"\bst\.?\s*clement\s*i\b|\bclement\s+of\s+rome|\bclemente\s+de\s+roma", re.I), "clement-rome"),
    (re.compile(r"\bst\.?\s*ignatius\b|\binácio\s+de\s+antioquia|\bignatius\s+of\s+antioch", re.I), "ignatius-antioch"),
    (re.compile(r"\bst\.?\s*polycarp\b|\bpolicarpo\b", re.I), "polycarp"),
    (re.compile(r"\bjustin\s+martyr\b|\bjustino\s+mártir\b|\bst\.?\s*justin\b", re.I), "justin-martyr"),
    (re.compile(r"\bst\.?\s*irenaeus\b|\bireneu\s+de\s+lyon\b|\birenaeus\s+of\s+lyon", re.I), "irenaeus"),
    (re.compile(r"\btertullian\b|\btertuliano\b", re.I), "tertullian"),
    (re.compile(r"\borigen\b|\borígenes\b", re.I), "origen"),
    (re.compile(r"\bhippolytus\b|\bhipólito\s+de\s+roma\b", re.I), "hippolytus"),
    (re.compile(r"\bclement\s+of\s+alexandria|\bclemente\s+de\s+alexandria", re.I), "clement-alexandria"),
    (re.compile(r"\bcyprian\b|\bcipriano\b", re.I), "cyprian"),
    (re.compile(r"\blactantius\b|\blactâncio\b", re.I), "lactantius"),
    (re.compile(r"\barnobius\b|\barnóbio\b", re.I), "arnobius"),
    (re.compile(r"\bnovatian\b|\bnovaciano\b", re.I), "novatian"),
    (re.compile(r"\bcommodianus\b|\bcomodiano\b", re.I), "commodianus"),
    (re.compile(r"\bathanasius\b|\batanásio\b", re.I), "athanasius"),
    (re.compile(r"\bbasil\s+the\s+great\b|\bbasílio\s+(?:magno|de\s+cesareia)\b|\bst\.?\s*basil\b", re.I), "basil"),
    (re.compile(r"\bgregory\s+of\s+nazianzus|\bgregório\s+de\s+nazianzo", re.I), "gregory-nazianzen"),
    (re.compile(r"\bgregory\s+of\s+nyssa|\bgregório\s+de\s+nissa", re.I), "gregory-nyssa"),
    (re.compile(r"\bcyril\s+of\s+alexandria|\bcirilo\s+de\s+alexandria", re.I), "cyril-alexandria"),
    (re.compile(r"\bcyril\s+of\s+jerusalem|\bcirilo\s+de\s+jerusalém", re.I), "cyril-jerusalem"),
    (re.compile(r"\bjohn\s+chrysostom|\bjoão\s+crisóstomo", re.I), "john-chrysostom"),
    (re.compile(r"\bjohn\s+(?:of\s+)?damasc(?:en|us)|\bjoão\s+damasceno", re.I), "john-damascene"),
    (re.compile(r"\bhilary\s+of\s+poitiers|\bhilário\s+de\s+poitiers", re.I), "hilary"),
    (re.compile(r"\bambrose\b|\bambrósio\s+de\s+milão\b", re.I), "ambrose"),
    (re.compile(r"\bjerome\b|\bjerônimo\b", re.I), "jerome"),
    (re.compile(r"\baugustine\s+of\s+hippo|\bagostinho\s+(?:de\s+hipona)?", re.I), "augustine"),
    (re.compile(r"\bleo\s+the\s+great|\bleão\s+magno|\bleo\s+i\b", re.I), "leo-great"),
    (re.compile(r"\bvincent\s+of\s+lérins|\bvicente\s+de\s+lérins", re.I), "vincent-lerins"),
    (re.compile(r"\beucherius\b|\beuquério\b", re.I), "eucherius"),
    (re.compile(r"\bmacarius\b|\bmacário\b", re.I), "macarius-alexandria"),
    (re.compile(r"\bcassiodorus\b|\bcassiodoro\b", re.I), "cassiodorus"),
    (re.compile(r"\bgregory\s+the\s+great|\bgregório\s+magno", re.I), "gregory-great"),
    (re.compile(r"\bisidore\s+of\s+seville|\bisidoro\s+de\s+sevilha", re.I), "isidore-seville"),
    (re.compile(r"\bbede\b|\bbeda\b", re.I), "bede"),
    (re.compile(r"\bbenedict\s+of\s+nursia|\bbento\s+de\s+núrsia", re.I), "benedict"),
    (re.compile(r"\banselm\s+of\s+canterbury|\banselmo\s+de\s+cantuária", re.I), "anselm"),
    (re.compile(r"\bbernard\s+of\s+clairvaux|\bbernardo\s+de\s+claraval", re.I), "bernard-clairvaux"),
    (re.compile(r"\bhugh\s+of\s+st\.?\s*victor|\bhugo\s+de\s+são\s+vítor", re.I), "hugo-st-victor"),
    (re.compile(r"\bbonaventure\b|\bboaventura\b", re.I), "bonaventure"),
    (re.compile(r"\bthomas\s+aquinas|\btomás\s+de\s+aquino", re.I), "thomas-aquinas"),
]


def collect_text_fields(
    section_num: str,
    text_en: str | None, text_la: str | None, text_pt: str | None,
    meta: dict,
) -> str:
    """Concatenate every provenance-bearing field into one searchable blob."""
    parts: list[str] = []
    path = meta.get("path") or {}
    for v in path.values():
        if v:
            parts.append(str(v))
    for key in ("attribution", "work", "location_note", "intro_pt"):
        v = meta.get(key)
        if v:
            parts.append(str(v))
    # Also paragraph-level paths in EN meta (each source paragraph carries
    # its own path snapshot).
    for p in meta.get("paragraphs", []):
        for v in (p.get("path") or {}).values():
            if v:
                parts.append(str(v))
    return " │ ".join(parts)  # use a sentinel separator


def detect_councils(blob: str, valid_councils: set[str]) -> set[str]:
    out: set[str] = set()
    for pat, cid in COUNCIL_RULES:
        if cid not in valid_councils:
            continue
        if pat.search(blob):
            out.add(cid)
    return out


def detect_authors(blob: str, valid_authors: set[str]) -> set[str]:
    out: set[str] = set()
    for pat, aid in AUTHOR_RULES:
        if aid not in valid_authors:
            continue
        if pat.search(blob):
            out.add(aid)
    return out


def run_for_document(
    cur: sqlite3.Cursor, doc_id: str,
    valid_councils: set[str], valid_authors: set[str],
) -> tuple[int, int]:
    # Clear prior council + author edges from this document.
    cur.execute(
        """
        DELETE FROM graph_edges
         WHERE edge_type = 'cites'
           AND source LIKE ? || '/%'
           AND (target LIKE 'document:%' OR target LIKE 'author:%')
        """,
        (f"document-section:{doc_id}",),
    )

    rows = cur.execute(
        "SELECT section_num, text_en, text_la, text_pt, meta_json "
        "  FROM document_sections WHERE document_id=?",
        (doc_id,),
    ).fetchall()

    council_edges: set[tuple[str, str, str]] = set()
    author_edges: set[tuple[str, str, str]] = set()
    for section_num, text_en, text_la, text_pt, meta_json in rows:
        meta = json.loads(meta_json or "{}")
        blob = collect_text_fields(section_num, text_en, text_la, text_pt, meta)
        if not blob:
            continue
        src = f"document-section:{doc_id}/{section_num}"
        for cid in detect_councils(blob, valid_councils):
            # Skip self-edges to the exact same document (shouldn't happen
            # for councils vs denzinger, but defensive).
            tgt = f"document:{cid}"
            if tgt == f"document:{doc_id}":
                continue
            council_edges.add((src, tgt, "cites"))
        for aid in detect_authors(blob, valid_authors):
            author_edges.add((src, f"author:{aid}", "cites"))

    cur.executemany("INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
                    list(council_edges))
    cur.executemany("INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
                    list(author_edges))

    log.info("  %s: %d council edges, %d author edges",
             doc_id, len(council_edges), len(author_edges))
    return len(council_edges), len(author_edges)


def wire_schonmetzer_same_as(cur: sqlite3.Cursor) -> int:
    """DS and DH share the post-1963 numbering. For every DH section whose
    number also appears in the existing CCC→DS citations, create a
    ``document-section:denzinger-schonmetzer/<n>`` node and a ``same_as``
    edge to the DH section. This makes the 185 existing CCC→DS citations
    land on the actual Portuguese/Latin text via the bridge.
    """
    # Find every DS section_num that anyone has ever cited (via
    # paragraph_document_citations).
    ds_sections = {
        row[0]
        for row in cur.execute(
            "SELECT DISTINCT section FROM paragraph_document_citations "
            " WHERE document = 'denzinger-schonmetzer' AND section IS NOT NULL"
        )
    }
    dh_sections = {
        row[0]
        for row in cur.execute(
            "SELECT section_num FROM document_sections WHERE document_id=?",
            (HUNERMANN,),
        )
    }
    shared = ds_sections & dh_sections
    log.info("DS sections cited by CCC: %d; DH overlap: %d", len(ds_sections), len(shared))

    # Reset prior same-as edges from this bridge so we can re-emit.
    cur.execute(
        """
        DELETE FROM graph_edges
         WHERE edge_type = 'same_as'
           AND ((source LIKE ? AND target LIKE ?) OR
                (source LIKE ? AND target LIKE ?))
        """,
        (
            f"document-section:{SCHONMETZER}/%", f"document-section:{HUNERMANN}/%",
            f"document-section:{HUNERMANN}/%", f"document-section:{SCHONMETZER}/%",
        ),
    )

    node_rows = [
        (
            f"document-section:{SCHONMETZER}/{n}",
            f"{SCHONMETZER} §{n}",
            "document-section",
            0.0, 0.0, 2.5, "#F0C78C", "",
            0, 0, "[]", "[]", "[]",
        )
        for n in shared
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO graph_nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        node_rows,
    )

    edges: list[tuple[str, str, str]] = []
    for n in shared:
        edges.append((
            f"document-section:{SCHONMETZER}/{n}",
            f"document-section:{HUNERMANN}/{n}",
            "same_as",
        ))
        # child_of from DS section to DS document (so DS isn't an orphan).
        edges.append((
            f"document-section:{SCHONMETZER}/{n}",
            f"document:{SCHONMETZER}",
            "child_of",
        ))
    # Document-level same_as: DS and DH are the same work under different
    # editors, sharing the post-1963 Rahner numbering.
    edges.append((f"document:{SCHONMETZER}", f"document:{HUNERMANN}", "same_as"))
    cur.executemany("INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)", edges)
    log.info("  wrote %d DS→DH same_as section edges (+ DS child_of + doc-level)",
             len(shared))
    return len(shared)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()

        valid_councils = {row[0] for row in cur.execute(
            "SELECT id FROM documents WHERE category LIKE 'council%'"
        )}
        valid_authors = {row[0] for row in cur.execute("SELECT id FROM authors")}

        for doc_id in (DEFERRARI, HUNERMANN):
            run_for_document(cur, doc_id, valid_councils, valid_authors)

        wire_schonmetzer_same_as(cur)

        conn.commit()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
