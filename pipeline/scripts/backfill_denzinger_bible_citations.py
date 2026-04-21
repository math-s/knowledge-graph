"""Wire bible citations from both Denzinger editions at section granularity.

Two inputs:
  1. EN Deferrari — ``scripture_refs`` in ``document_sections.meta_json`` is
     already extracted by the converter as ``[{"ref": "Matt 17:5"}, ...]``.
     We just need to translate the book prefix to the canonical bible-verse
     node-id slug (e.g. ``bible-verse:matthew-17:5``).
  2. PT Hünermann — inline bracketed refs in ``text_pt``, ``text_la``, and
     the ``intro_pt``/``text_bilingual`` blocks inside ``meta_json``, e.g.
     ``[Ef 4,5]``, ``[Sl 33,6; Jo 14,10s]``, ``[1 Cor 15,23]``. Mixes
     Portuguese and Latin abbreviations.

Both pipelines emit ``cites`` edges:
    ``document-section:<slug>/<denz>  ->  bible-verse:<book-slug>-<ch>:<v>``

Also populates ``paragraph_document_citations``? No — that table is keyed on
CCC ``paragraph_id``. The Denzinger→Bible link is graph-only, matching the
existing convention for documents-to-bible edges already in the DB.

Idempotent: removes prior ``cites`` edges from these two documents before
re-inserting.

Usage:
    uv run --project pipeline python -m pipeline.scripts.backfill_denzinger_bible_citations
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

DEFERRARI = "denzinger-deferrari-30"
HUNERMANN = "denzinger-hunermann"

log = logging.getLogger("backfill_denzinger_bible_citations")


# Canonical book slugs matching existing ``bible-verse:<slug>-<ch>:<v>`` nodes
# in graph_nodes (e.g. bible-verse:matthew-10:1, bible-verse:1-corinthians-15:23).
# Every abbreviation we might see — Latin, English, Portuguese, with and
# without trailing dot, with/without space after the ordinal — maps to the
# slug below.
#
# NOTE: entries with leading digit ordinals are handled by the ordinal-
# prefix logic in the regex below; the book map here keys off the "main"
# token (e.g. "Cor" maps to "corinthians", and ordinal "1"/"2" prepends).
BOOK_ALIASES: dict[str, str] = {
    # --- Pentateuch ---
    "gen": "genesis", "gn": "genesis", "genesis": "genesis", "gene": "genesis",
    "exod": "exodus", "ex": "exodus", "êx": "exodus", "exo": "exodus", "exodus": "exodus",
    "lev": "leviticus", "lv": "leviticus", "leviticus": "leviticus",
    "num": "numbers", "nm": "numbers", "numeri": "numbers", "numbers": "numbers",
    "deut": "deuteronomy", "dt": "deuteronomy", "deuteronomy": "deuteronomy", "dtn": "deuteronomy",

    # --- Historical ---
    "jos": "joshua", "josh": "joshua", "js": "joshua", "joshua": "joshua",
    "judg": "judges", "jdg": "judges", "jud": "judges", "jz": "judges", "judges": "judges",
    "iud": "judges", "idc": "judges",
    "ruth": "ruth", "rut": "ruth", "rt": "ruth", "rth": "ruth",
    # Samuel
    "sam": "samuel", "sm": "samuel",
    # Kings (Vulgate: 1-4 Rg → 1-2 Sam + 1-2 Kings; here keep distinct)
    "kings": "kings", "kgs": "kings", "rg": "kings", "reg": "kings", "rs": "kings",
    # Chronicles (Paralipomenon)
    "chr": "chronicles", "chron": "chronicles", "cr": "chronicles",
    "par": "chronicles", "paral": "chronicles", "paralip": "chronicles",
    "ezra": "ezra", "esd": "ezra", "ezr": "ezra",
    "neh": "nehemiah", "ne": "nehemiah", "nehemiah": "nehemiah",
    "tob": "tobit", "tb": "tobit", "tobit": "tobit", "tbt": "tobit",
    "judt": "judith", "jdt": "judith", "jt": "judith", "iudt": "judith", "judith": "judith",
    "est": "esther", "esth": "esther", "esther": "esther",
    "mac": "maccabees", "macc": "maccabees", "mcc": "maccabees", "mach": "maccabees",
    # NB: bare "Mc" is Mark (PT Gospel abbreviation); ordinaled "1 Mc" / "2 Mc"
    # is Maccabees. Special-cased in ``resolve_book``.

    # --- Wisdom ---
    "job": "job", "jb": "job", "jó": "job", "iob": "job",
    "ps": "psalms", "psa": "psalms", "sl": "psalms", "salmos": "psalms", "psalm": "psalms", "psalmi": "psalms",
    "prov": "proverbs", "pr": "proverbs", "prv": "proverbs", "proverbs": "proverbs",
    "eccl": "ecclesiastes", "ecc": "ecclesiastes", "ecl": "ecclesiastes", "qoh": "ecclesiastes", "eccles": "ecclesiastes",
    "cant": "song-of-solomon", "ct": "song-of-solomon", "cantic": "song-of-solomon", "song": "song-of-solomon",
    "wisd": "wisdom", "sap": "wisdom", "sb": "wisdom", "wis": "wisdom",
    "sir": "sirach", "sr": "sirach", "eclo": "sirach", "ecclus": "sirach", "sirach": "sirach",

    # --- Prophets ---
    "isa": "isaiah", "is": "isaiah", "isaias": "isaiah", "isaiah": "isaiah",
    "jer": "jeremiah", "jr": "jeremiah", "ier": "jeremiah", "ieremiah": "jeremiah", "jeremiah": "jeremiah",
    "lam": "lamentations", "lm": "lamentations", "lament": "lamentations",
    "bar": "baruch", "br": "baruch", "baruch": "baruch",
    "ezek": "ezekiel", "ez": "ezekiel", "ezech": "ezekiel", "ezekiel": "ezekiel",
    "dan": "daniel", "dn": "daniel", "daniel": "daniel",
    "hos": "hosea", "os": "hosea", "osee": "hosea", "hosea": "hosea",
    "joel": "joel", "jl": "joel", "ioel": "joel",
    "amos": "amos", "am": "amos",
    "obad": "obadiah", "ab": "obadiah", "abd": "obadiah", "obadiah": "obadiah",
    "jonah": "jonah", "jon": "jonah", "jn": "jonah", "ion": "jonah",
    "mic": "micah", "mq": "micah", "mich": "micah", "micah": "micah",
    "nah": "nahum", "na": "nahum", "nahum": "nahum",
    "hab": "habakkuk", "habakkuk": "habakkuk",
    "zeph": "zephaniah", "soph": "zephaniah", "sf": "zephaniah", "zephaniah": "zephaniah",
    "hag": "haggai", "ag": "haggai", "agg": "haggai", "haggai": "haggai",
    "zech": "zechariah", "zc": "zechariah", "zach": "zechariah", "za": "zechariah", "zac": "zechariah", "zechariah": "zechariah",
    "mal": "malachi", "ml": "malachi", "mai": "malachi", "malachi": "malachi",

    # --- Gospels ---
    "matt": "matthew", "mt": "matthew", "matthew": "matthew",
    "mark": "mark", "mk": "mark", "marc": "mark", "mc": "mark",
    "luke": "luke", "lc": "luke", "luc": "luke",
    "john": "john", "jn": "john", "jo": "john", "ioh": "john", "io": "john", "ioan": "john",
    "joh": "john", "joan": "john",

    # --- Acts ---
    "acts": "acts", "act": "acts", "at": "acts", "actuum": "acts",

    # --- Pauline ---
    "rom": "romans", "rm": "romans", "roma": "romans", "romans": "romans",
    "cor": "corinthians", "co": "corinthians", "corinthians": "corinthians",
    "gal": "galatians", "gl": "galatians", "galatians": "galatians",
    "eph": "ephesians", "ef": "ephesians", "ephesians": "ephesians",
    "phil": "philippians", "fl": "philippians", "phi": "philippians", "philippians": "philippians",
    "col": "colossians", "cl": "colossians", "colossians": "colossians",
    "thess": "thessalonians", "ts": "thessalonians", "th": "thessalonians", "thessalonians": "thessalonians",
    "tim": "timothy", "tm": "timothy", "timothy": "timothy",
    "tit": "titus", "tt": "titus", "ti": "titus", "titus": "titus",
    "phm": "philemon", "fm": "philemon", "philem": "philemon", "philemon": "philemon",
    "heb": "hebrews", "hb": "hebrews", "hbr": "hebrews", "hebr": "hebrews", "hebrews": "hebrews",

    # --- General Epistles & Revelation ---
    "jas": "james", "jac": "james", "iac": "james", "tg": "james", "james": "james",
    "pet": "peter", "pt": "peter", "pd": "peter", "petr": "peter", "peter": "peter",
    # "1/2/3 John" → john (Gospel of John also "john"; distinguish by chapter/verse only? No —
    # the epistle vs gospel distinction is real. Override with ordinal prefix.)
    "jude": "jude", "iud": "jude", "jd": "jude",
    "apoc": "revelation", "ap": "revelation", "apc": "revelation", "apocal": "revelation",
    "rev": "revelation", "revelation": "revelation",
}


# Books that take ordinal prefixes (e.g. "1 Cor", "2 Tim", "3 John").
# ``REQUIRED`` means the ordinal must be present or the ref is ambiguous
# (e.g. "Corinthians 15:23" alone is meaningless). ``OPTIONAL`` books
# (just ``john``) exist both with and without an ordinal: bare "John" is
# the Gospel, "1/2/3 John" are the epistles.
ORDINAL_REQUIRED = {
    "corinthians", "thessalonians", "timothy", "peter",
    "samuel", "kings", "chronicles", "maccabees",
}
ORDINAL_OPTIONAL = {"john"}


# EN-side scripture_ref parser: "Matt 17:5", "I Cor 15:23", "II Tim 3:16",
# "Ps 32:6", "I John 5:7". Ordinal markers are English Roman numerals.
EN_REF_RE = re.compile(
    r"^(?P<ord>[IV]{1,3}\s+)?"
    r"(?P<book>[A-Za-z]+)\.?\s+"
    r"(?P<ch>\d{1,3})"
    r"[:\,]\s*"
    r"(?P<v>\d{1,3})"
)

EN_ORDINAL_MAP = {"I": "1", "II": "2", "III": "3"}


# PT-side bracketed-ref parser — enforces `[...]` wrapping to suppress
# false positives from editorial text. Inside the brackets, splits on
# ``;`` for multi-ref brackets like ``[Jo 1,14; 1 Cor 1,24]``.
PT_BRACKET_RE = re.compile(r"\[([^\[\]]{3,120})\]")
PT_CF_PREFIX = re.compile(r"^\s*(?:cf\.?\s+)?", re.IGNORECASE)
# One PT ref. Ordinal forms:
#   - "1 ", "2 ", "3 " (digit + required space, so "1Cor" stays book-only — see
#     note). Roman ordinals "I ", "II ", "III " must have a trailing space to
#     avoid greedy-eating the leading "I" of "Iac"/"Is"/"Io" (book tokens).
#   - "1Cor"/"2Tm" style (digit + no space + capital letter): captured by a
#     second alternation that requires an uppercase book start.
#
# Not anchored to start — callers use ``finditer`` to scan inside bracketed
# editorial notes that embed refs, e.g.
# ``[A esta afirmação se contrapõe Rm 16,17s.]``.
PT_ONE_REF_RE = re.compile(
    r"(?:^|(?<=[\s(,;.]))"
    r"(?P<ord>(?:[1-3]\s+|I{1,3}\s+|[1-3](?=[A-Z])))?"
    r"(?P<book>[A-Za-zÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÇÃÕáéíóúàèìòùâêîôûçãõ]{2,10})"
    r"\.?\s+"
    r"(?P<ch>\d{1,3})"
    r"\s*[,:]\s*"
    r"(?P<v>\d{1,3})"
)


def _norm_ordinal(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip().rstrip()
    # Already digit?
    if s in ("1", "2", "3"):
        return s
    return EN_ORDINAL_MAP.get(s)


def resolve_book(raw: str, ordinal: str | None) -> str | None:
    """Return canonical bible-verse slug book portion, or None if unknown."""
    token = raw.lower().rstrip(".")
    # Special case: "Mc" is Mark bare, Maccabees when ordinaled (PT Vulgate).
    if token == "mc" and ordinal is not None:
        return f"{ordinal}-maccabees"
    base = BOOK_ALIASES.get(token)
    if base is None:
        return None
    if ordinal is not None:
        if base in ORDINAL_REQUIRED or base in ORDINAL_OPTIONAL:
            return f"{ordinal}-{base}"
        # Ordinal given but this book doesn't take one — ambiguity; skip.
        return None
    # No ordinal given:
    if base in ORDINAL_REQUIRED:
        return None
    return base


def verse_node(book_slug: str, ch: int, v: int) -> str:
    return f"bible-verse:{book_slug}-{ch}:{v}"


def parse_en_ref(ref: str) -> tuple[str, int, int] | None:
    m = EN_REF_RE.match(ref)
    if not m:
        return None
    ord_norm = _norm_ordinal(m.group("ord"))
    book = resolve_book(m.group("book"), ord_norm)
    if not book:
        return None
    return book, int(m.group("ch")), int(m.group("v"))


# Bare "ch,verse" after a book-prefixed ref inside the same bracket:
# e.g. in "[Mt 20,28; 26,28; Hb 9,27]" the middle "26,28" is Matthew 26:28.
# Captured with start-of-clause anchoring so we don't swallow arbitrary
# number pairs in prose.
PT_ORPHAN_CHVERSE_RE = re.compile(
    r"(?:^|(?<=[;,]\s))"
    r"(?P<ch>\d{1,3})\s*[,:]\s*(?P<v>\d{1,3})"
    r"(?![0-9])"
)

# Common PDF-extraction artifacts we normalize away before parsing.
_NORMALIZATIONS: list[tuple[re.Pattern[str], str]] = [
    # "c f." -> "cf." (stray space between c and f)
    (re.compile(r"\bc\s+f\."), "cf."),
    # "1 C OR" / "1C OR" / "I C OR" -> compact form recognisable by the regex.
    (re.compile(r"\b([1-3]|I{1,3})\s*C\s+OR\b"), r"\1 Cor"),
    # Trailing ":" after chapter when verse intended: "ch:v" stays as-is,
    # but some PDFs yield "ch, v :" with a stray colon — fix is pointless,
    # PT_ONE_REF_RE already accepts ch:v via the [,:] class.
]


def _normalize_bracket(inner: str) -> str:
    out = inner
    for pat, repl in _NORMALIZATIONS:
        out = pat.sub(repl, out)
    return out


def parse_pt_bracket(inner: str) -> list[tuple[str, int, int]]:
    """Find every bible ref inside one bracketed group.

    Handles:
      - plain single ref (``Ef 4,5``)
      - multiple ``;``-separated refs (``Jo 1,14; 1 Cor 1,24``)
      - orphan chapter-verse refs that continue the prior book
        (``Mt 20,28; 26,28`` → second is Matthew 26:28)
      - refs embedded in editorial prose
      - PDF spacing artifacts (``1 C OR``, ``c f.``) via pre-normalization
    """
    inner = _normalize_bracket(inner)
    out: list[tuple[str, int, int]] = []

    # First pass: book-prefixed refs (stateful, tracks last book+ordinal).
    carries: list[tuple[int, str]] = []  # (match_end, book_slug)
    for m in PT_ONE_REF_RE.finditer(inner):
        raw_ord = m.group("ord")
        ordinal = _norm_ordinal(raw_ord.strip()) if raw_ord else None
        book = resolve_book(m.group("book"), ordinal)
        if not book:
            continue
        out.append((book, int(m.group("ch")), int(m.group("v"))))
        carries.append((m.end(), book))

    if not carries:
        return out

    # Second pass: orphan "ch,v" refs continue the most-recent prior book.
    emitted_positions = {m.end() for m in PT_ONE_REF_RE.finditer(inner)}
    emitted_starts = {m.start() for m in PT_ONE_REF_RE.finditer(inner)}
    for m in PT_ORPHAN_CHVERSE_RE.finditer(inner):
        # Skip if this position is already inside a book-prefixed ref.
        if any(start <= m.start() < end for start, end in (
                (sm.start(), sm.end()) for sm in PT_ONE_REF_RE.finditer(inner)
        )):
            continue
        # Find the most recent book-prefixed ref strictly before this one.
        prior_book: str | None = None
        for end, book in carries:
            if end <= m.start():
                prior_book = book
            else:
                break
        if prior_book is None:
            continue
        out.append((prior_book, int(m.group("ch")), int(m.group("v"))))

    return out


def extract_pt_refs(corpus: str) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    for m in PT_BRACKET_RE.finditer(corpus):
        out.extend(parse_pt_bracket(m.group(1)))
    return out


def load_known_verse_nodes(cur: sqlite3.Cursor) -> set[str]:
    """Set of every ``bible-verse:*`` node id in graph_nodes."""
    return {
        row[0]
        for row in cur.execute(
            "SELECT id FROM graph_nodes WHERE id LIKE 'bible-verse:%'"
        )
    }


def run_for_document(
    cur: sqlite3.Cursor,
    doc_id: str,
    known_verses: set[str],
) -> tuple[int, int, int]:
    """Extract and emit cites edges for one Denzinger edition.

    Returns (refs_extracted, refs_resolved, edges_written).
    """
    # Idempotent: clear prior edges from this document's sections to any
    # bible-verse node.
    cur.execute(
        """
        DELETE FROM graph_edges
         WHERE edge_type = 'cites'
           AND source LIKE ? || '/%'
           AND target LIKE 'bible-verse:%'
        """,
        (f"document-section:{doc_id}",),
    )

    sections = cur.execute(
        "SELECT section_num, text_en, text_la, text_pt, meta_json "
        "  FROM document_sections WHERE document_id = ?",
        (doc_id,),
    ).fetchall()

    refs_extracted = 0
    refs_resolved = 0
    edges: set[tuple[str, str, str]] = set()
    unresolved_samples: set[str] = set()

    for section_num, text_en, text_la, text_pt, meta_json in sections:
        meta = json.loads(meta_json or "{}")

        # Collect (book, ch, v) tuples from whichever side has them.
        tuples: list[tuple[str, int, int]] = []

        # EN: pre-extracted in meta_json["scripture_refs"]
        for r in meta.get("scripture_refs", []):
            ref = r.get("ref") if isinstance(r, dict) else None
            if not ref:
                continue
            refs_extracted += 1
            parsed = parse_en_ref(ref)
            if parsed:
                tuples.append(parsed)
            else:
                if len(unresolved_samples) < 20:
                    unresolved_samples.add(ref)

        # PT: inline bracketed refs in text_pt + text_la + bilingual + intro_pt
        for fld in (text_pt, text_la, meta.get("text_bilingual"), meta.get("intro_pt")):
            if not fld:
                continue
            for m in PT_BRACKET_RE.finditer(fld):
                inner = m.group(1).strip()
                # Only try to parse brackets that look bible-shaped.
                if not re.search(r"\d\s*[,:]\s*\d", inner):
                    continue
                refs_extracted += 1
                parsed_list = parse_pt_bracket(inner)
                if parsed_list:
                    tuples.extend(parsed_list)
                else:
                    if len(unresolved_samples) < 20:
                        unresolved_samples.add(inner[:50])

        # Resolve to known bible-verse nodes, emit edges.
        section_node = f"document-section:{doc_id}/{section_num}"
        for book, ch, v in tuples:
            node = verse_node(book, ch, v)
            if node in known_verses:
                edges.add((section_node, node, "cites"))
                refs_resolved += 1

    cur.executemany(
        "INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)",
        list(edges),
    )

    log.info("  %s: %d refs found, %d resolved to known bible nodes, %d unique edges written",
             doc_id, refs_extracted, refs_resolved, len(edges))
    if unresolved_samples:
        log.info("    unresolved sample: %s", sorted(unresolved_samples)[:10])
    return refs_extracted, refs_resolved, len(edges)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        known_verses = load_known_verse_nodes(cur)
        log.info("%d bible-verse nodes available to link against", len(known_verses))

        for doc_id in (DEFERRARI, HUNERMANN):
            run_for_document(cur, doc_id, known_verses)

        conn.commit()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
