"""Parse CCC footnotes into structured Bible and patristic references."""

from __future__ import annotations

import re
import logging

from .models import (
    BibleReference,
    DocumentReference,
    ParsedFootnote,
    Paragraph,
    PatristicReference,
)

logger = logging.getLogger(__name__)

# ── Bible book abbreviation lookup ──────────────────────────────────────────
# Maps abbreviation variants → (canonical_id, display_abbreviation)
# Covers standard Latin/English abbreviations used in CCC footnotes.

_BIBLE_BOOKS: list[tuple[str, str, list[str]]] = [
    # Canonical ID, Display Abbreviation, All recognized forms
    # Old Testament
    ("genesis", "Gen", ["Gen", "Gn"]),
    ("exodus", "Ex", ["Ex", "Exod"]),
    ("leviticus", "Lev", ["Lev", "Lv"]),
    ("numbers", "Num", ["Num", "Nm"]),
    ("deuteronomy", "Deut", ["Deut", "Dt"]),
    ("joshua", "Josh", ["Josh", "Jos"]),
    ("judges", "Judg", ["Judg", "Jgs"]),
    ("ruth", "Ruth", ["Ruth", "Ru"]),
    ("1-samuel", "1 Sam", ["1 Sam", "I Sam"]),
    ("2-samuel", "2 Sam", ["2 Sam", "II Sam"]),
    ("1-kings", "1 Kings", ["1 Kings", "I Kings", "1 Kg", "I Kg", "1 Kgs", "I Kgs"]),
    ("2-kings", "2 Kings", ["2 Kings", "II Kings", "2 Kg", "II Kg", "2 Kgs", "II Kgs"]),
    ("1-chronicles", "1 Chron", ["1 Chron", "I Chron", "1 Chr", "I Chr"]),
    ("2-chronicles", "2 Chron", ["2 Chron", "II Chron", "2 Chr", "II Chr"]),
    ("ezra", "Ezra", ["Ezra", "Ezr"]),
    ("nehemiah", "Neh", ["Neh"]),
    ("tobit", "Tob", ["Tob"]),
    ("judith", "Jdt", ["Jdt", "Jth"]),
    ("esther", "Esth", ["Esth", "Est"]),
    ("1-maccabees", "1 Macc", ["1 Macc", "I Macc"]),
    ("2-maccabees", "2 Macc", ["2 Macc", "II Macc"]),
    ("job", "Job", ["Job"]),
    ("psalms", "Ps", ["Ps", "Pss"]),
    ("proverbs", "Prov", ["Prov", "Prv"]),
    ("ecclesiastes", "Eccl", ["Eccl", "Qoh"]),
    ("song-of-solomon", "Song", ["Song", "Sg", "Cant"]),
    ("wisdom", "Wis", ["Wis"]),
    ("sirach", "Sir", ["Sir"]),
    ("isaiah", "Isa", ["Isa", "Is"]),
    ("jeremiah", "Jer", ["Jer"]),
    ("lamentations", "Lam", ["Lam"]),
    ("baruch", "Bar", ["Bar"]),
    ("ezekiel", "Ezek", ["Ezek", "Ez"]),
    ("daniel", "Dan", ["Dan", "Dn"]),
    ("hosea", "Hos", ["Hos"]),
    ("joel", "Joel", ["Joel", "Jl"]),
    ("amos", "Amos", ["Amos", "Am"]),
    ("obadiah", "Obad", ["Obad", "Ob"]),
    ("jonah", "Jonah", ["Jonah", "Jon"]),
    ("micah", "Mic", ["Mic"]),
    ("nahum", "Nah", ["Nah"]),
    ("habakkuk", "Hab", ["Hab"]),
    ("zephaniah", "Zeph", ["Zeph", "Zep"]),
    ("haggai", "Hag", ["Hag", "Hg"]),
    ("zechariah", "Zech", ["Zech", "Zec"]),
    ("malachi", "Mal", ["Mal"]),
    # New Testament
    ("matthew", "Mt", ["Mt", "Matt"]),
    ("mark", "Mk", ["Mk", "Mc"]),
    ("luke", "Lk", ["Lk"]),
    ("john", "Jn", ["Jn"]),
    ("acts", "Acts", ["Acts"]),
    ("romans", "Rom", ["Rom"]),
    ("1-corinthians", "1 Cor", ["1 Cor", "I Cor"]),
    ("2-corinthians", "2 Cor", ["2 Cor", "II Cor"]),
    ("galatians", "Gal", ["Gal"]),
    ("ephesians", "Eph", ["Eph"]),
    ("philippians", "Phil", ["Phil"]),
    ("colossians", "Col", ["Col"]),
    ("1-thessalonians", "1 Thess", ["1 Thess", "I Thess"]),
    ("2-thessalonians", "2 Thess", ["2 Thess", "II Thess"]),
    ("1-timothy", "1 Tim", ["1 Tim", "I Tim"]),
    ("2-timothy", "2 Tim", ["2 Tim", "II Tim"]),
    ("titus", "Titus", ["Titus", "Ti", "Tit"]),
    ("philemon", "Phlm", ["Phlm", "Philem"]),
    ("hebrews", "Heb", ["Heb"]),
    ("james", "Jas", ["Jas"]),
    ("1-peter", "1 Pet", ["1 Pet", "I Pet"]),
    ("2-peter", "2 Pet", ["2 Pet", "II Pet"]),
    ("1-john", "1 Jn", ["1 Jn", "I Jn"]),
    ("2-john", "2 Jn", ["2 Jn", "II Jn"]),
    ("3-john", "3 Jn", ["3 Jn", "III Jn"]),
    ("jude", "Jude", ["Jude"]),
    ("revelation", "Rev", ["Rev", "Rv", "Apoc"]),
]

# Build lookup: abbreviation → (canonical_id, display_abbreviation)
_ABBREV_TO_BOOK: dict[str, tuple[str, str]] = {}
for _canon_id, _display_abbr, _forms in _BIBLE_BOOKS:
    for _form in _forms:
        _ABBREV_TO_BOOK[_form] = (_canon_id, _display_abbr)

# Build regex pattern: match ⇒ followed by optional space then a Bible book abbreviation
# Sort by length descending so "1 Cor" matches before "Cor" would (if it existed)
_sorted_abbrevs = sorted(_ABBREV_TO_BOOK.keys(), key=len, reverse=True)
_abbrev_pattern = "|".join(re.escape(a) for a in _sorted_abbrevs)
# Match: ⇒ (optional whitespace) (book abbreviation) (chapter:verse reference)
_BIBLE_RE = re.compile(
    r"⇒\s*(" + _abbrev_pattern + r")\s+\d",
    re.UNICODE,
)

# ── Patristic author lookup ─────────────────────────────────────────────────
# Maps qualified name patterns → canonical author IDs.
# Only matches "St." / "Saint" prefixed full names to avoid ambiguity.

_PATRISTIC_AUTHORS: list[tuple[str, list[str]]] = [
    # (canonical_id, [name patterns to match after "St."/"Saint"])
    ("augustine", ["Augustine"]),
    ("thomas-aquinas", ["Thomas Aquinas"]),
    ("john-chrysostom", ["John Chrysostom"]),
    ("irenaeus", ["Irenaeus"]),
    ("ambrose", ["Ambrose"]),
    ("jerome", ["Jerome"]),
    ("athanasius", ["Athanasius"]),
    ("basil", ["Basil"]),
    ("gregory-nazianzen", ["Gregory of Nazianzus", "Gregory Nazianzen"]),
    ("gregory-nyssa", ["Gregory of Nyssa"]),
    ("gregory-great", ["Gregory the Great"]),
    ("cyril-jerusalem", ["Cyril of Jerusalem"]),
    ("cyril-alexandria", ["Cyril of Alexandria"]),
    ("john-damascene", ["John Damascene", "John of Damascus"]),
    ("leo-great", ["Leo the Great"]),
    ("hilary", ["Hilary of Poitiers", "Hilary"]),
    ("cyprian", ["Cyprian"]),
    ("clement-rome", ["Clement of Rome"]),
    ("clement-alexandria", ["Clement of Alexandria"]),
    ("justin-martyr", ["Justin", "Justin Martyr"]),
    ("ignatius-antioch", ["Ignatius of Antioch"]),
    ("polycarp", ["Polycarp"]),
    ("tertullian", ["Tertullian"]),
    ("origen", ["Origen"]),
    ("bonaventure", ["Bonaventure"]),
    ("anselm", ["Anselm"]),
]

# Build name → canonical_id mapping
_AUTHOR_NAME_TO_ID: dict[str, str] = {}
for _author_id, _name_patterns in _PATRISTIC_AUTHORS:
    for _name in _name_patterns:
        _AUTHOR_NAME_TO_ID[_name] = _author_id

# Build regex: matches "St." or "Saint" followed by known author name
_author_names_sorted = sorted(_AUTHOR_NAME_TO_ID.keys(), key=len, reverse=True)
_author_names_pattern = "|".join(re.escape(n) for n in _author_names_sorted)
_PATRISTIC_RE = re.compile(
    r"(?:St\.|Saint)\s+(" + _author_names_pattern + r")",
    re.UNICODE,
)

# Also match standalone names for well-known authors without St./Saint prefix
# (e.g. "Tertullian", "Origen") — only unambiguous names
_STANDALONE_AUTHORS: dict[str, str] = {
    "Tertullian": "tertullian",
    "Origen": "origen",
}
_standalone_pattern = "|".join(re.escape(n) for n in _STANDALONE_AUTHORS.keys())
_STANDALONE_RE = re.compile(
    r"\b(" + _standalone_pattern + r")\b",
    re.UNICODE,
)

# ── Ecclesiastical document lookup ────────────────────────────────────────
# Maps abbreviation → (canonical_id, display_name)
# Organized by category for clarity.

_DOCUMENTS: list[tuple[str, str, list[str]]] = [
    # (canonical_id, display_name, [abbreviation forms])
    # Vatican II
    ("lumen-gentium", "Lumen Gentium", ["LG"]),
    ("gaudium-et-spes", "Gaudium et Spes", ["GS"]),
    ("dei-verbum", "Dei Verbum", ["DV"]),
    ("sacrosanctum-concilium", "Sacrosanctum Concilium", ["SC"]),
    ("unitatis-redintegratio", "Unitatis Redintegratio", ["UR"]),
    ("ad-gentes", "Ad Gentes", ["AG"]),
    ("presbyterorum-ordinis", "Presbyterorum Ordinis", ["PO"]),
    ("dignitatis-humanae", "Dignitatis Humanae", ["DH"]),
    ("apostolicam-actuositatem", "Apostolicam Actuositatem", ["AA"]),
    ("nostra-aetate", "Nostra Aetate", ["NA"]),
    ("christus-dominus", "Christus Dominus", ["CD"]),
    ("perfectae-caritatis", "Perfectae Caritatis", ["PC"]),
    ("optatam-totius", "Optatam Totius", ["OT"]),
    ("gravissimum-educationis", "Gravissimum Educationis", ["GE"]),
    ("inter-mirifica", "Inter Mirifica", ["IM"]),
    ("orientalium-ecclesiarum", "Orientalium Ecclesiarum", ["OE"]),
    ("catechesi-tradendae", "Catechesi Tradendae", ["CT"]),
    ("evangelii-nuntiandi", "Evangelii Nuntiandi", ["EN"]),
    # Canon Law
    ("cic", "Code of Canon Law", ["CIC"]),
    ("cceo", "Code of Canons of the Eastern Churches", ["CCEO"]),
    # Papal Documents
    ("centesimus-annus", "Centesimus Annus", ["CA"]),
    ("familiaris-consortio", "Familiaris Consortio", ["FC"]),
    ("redemptoris-missio", "Redemptoris Missio", ["RMiss"]),
    ("sollicitudo-rei-socialis", "Sollicitudo Rei Socialis", ["SRS"]),
    ("reconciliatio-et-paenitentia", "Reconciliatio et Paenitentia", ["RP"]),
    ("humanae-vitae", "Humanae Vitae", ["HV"]),
    ("laborem-exercens", "Laborem Exercens", ["LE"]),
    ("mysterium-fidei", "Mysterium Fidei", ["MF"]),
    ("mulieris-dignitatem", "Mulieris Dignitatem", ["MD"]),
    ("dominum-et-vivificantem", "Dominum et Vivificantem", ["DeV"]),
    ("pacem-in-terris", "Pacem in Terris", ["PT"]),
    ("christifideles-laici", "Christifideles Laici", ["CL"]),
    ("marialis-cultus", "Marialis Cultus", ["MC"]),
    ("populorum-progressio", "Populorum Progressio", ["PP"]),
    # Reference Collections
    ("denzinger-schonmetzer", "Denzinger-Schönmetzer", ["DS"]),
    ("patrologia-latina", "Patrologia Latina", ["PL"]),
    ("patrologia-graeca", "Patrologia Graeca", ["PG"]),
    ("sources-chretiennes", "Sources Chrétiennes", ["SCh"]),
    ("acta-apostolicae-sedis", "Acta Apostolicae Sedis", ["AAS"]),
]

# Build lookup: abbreviation → (canonical_id, abbreviation)
_DOC_ABBREV_TO_ID: dict[str, tuple[str, str]] = {}
for _doc_id, _doc_name, _doc_forms in _DOCUMENTS:
    for _doc_form in _doc_forms:
        _DOC_ABBREV_TO_ID[_doc_form] = (_doc_id, _doc_form)

# Build general document regex: matches ABBR followed by a digit
# Excludes CIC and CCEO which have their own pattern
_general_doc_abbrevs = sorted(
    [a for a in _DOC_ABBREV_TO_ID if a not in ("CIC", "CCEO")],
    key=len,
    reverse=True,
)
_general_doc_pattern = "|".join(re.escape(a) for a in _general_doc_abbrevs)
_DOCUMENT_RE = re.compile(
    r"\b(" + _general_doc_pattern + r")\s+\d",
    re.UNICODE,
)

# Canon law regex: matches optional ⇒, then CIC or CCEO, then "can." or "cann."
_CANON_LAW_RE = re.compile(
    r"⇒?\s*\b(CIC|CCEO),?\s*cann?\.",
    re.UNICODE,
)


def parse_footnote(raw: str) -> ParsedFootnote:
    """Parse a single footnote string into structured references."""
    bible_refs: list[BibleReference] = []
    author_refs: list[PatristicReference] = []

    # Extract Bible references (anchored on ⇒ arrow)
    seen_books: set[str] = set()
    for match in _BIBLE_RE.finditer(raw):
        abbrev = match.group(1)
        canon_id, display_abbr = _ABBREV_TO_BOOK[abbrev]
        if canon_id not in seen_books:
            seen_books.add(canon_id)
            bible_refs.append(BibleReference(book=canon_id, abbreviation=display_abbr))

    # Extract patristic references (St./Saint prefix)
    seen_authors: set[str] = set()
    for match in _PATRISTIC_RE.finditer(raw):
        name = match.group(1)
        author_id = _AUTHOR_NAME_TO_ID[name]
        if author_id not in seen_authors:
            seen_authors.add(author_id)
            author_refs.append(PatristicReference(author=author_id))

    # Check standalone author names
    for match in _STANDALONE_RE.finditer(raw):
        name = match.group(1)
        author_id = _STANDALONE_AUTHORS[name]
        if author_id not in seen_authors:
            seen_authors.add(author_id)
            author_refs.append(PatristicReference(author=author_id))

    # Extract ecclesiastical document references
    document_refs: list[DocumentReference] = []
    seen_docs: set[str] = set()

    # General document pattern (ABBR + digit)
    for match in _DOCUMENT_RE.finditer(raw):
        abbrev = match.group(1)
        doc_id, display_abbr = _DOC_ABBREV_TO_ID[abbrev]
        if doc_id not in seen_docs:
            seen_docs.add(doc_id)
            document_refs.append(DocumentReference(document=doc_id, abbreviation=display_abbr))

    # Canon law pattern (CIC/CCEO + can.)
    for match in _CANON_LAW_RE.finditer(raw):
        abbrev = match.group(1)
        doc_id, display_abbr = _DOC_ABBREV_TO_ID[abbrev]
        if doc_id not in seen_docs:
            seen_docs.add(doc_id)
            document_refs.append(DocumentReference(document=doc_id, abbreviation=display_abbr))

    return ParsedFootnote(raw=raw, bible_refs=bible_refs, author_refs=author_refs, document_refs=document_refs)


def parse_all_footnotes(paragraphs: list[Paragraph]) -> list[Paragraph]:
    """Parse footnotes for all paragraphs, populating parsed_footnotes."""
    total_bible = 0
    total_author = 0
    total_document = 0

    for para in paragraphs:
        parsed = []
        for fn_text in para.footnotes:
            pf = parse_footnote(fn_text)
            parsed.append(pf)
            total_bible += len(pf.bible_refs)
            total_author += len(pf.author_refs)
            total_document += len(pf.document_refs)
        para.parsed_footnotes = parsed

    logger.info(
        "Parsed footnotes: %d Bible refs, %d patristic refs, %d document refs across %d paragraphs",
        total_bible,
        total_author,
        total_document,
        len(paragraphs),
    )
    return paragraphs
