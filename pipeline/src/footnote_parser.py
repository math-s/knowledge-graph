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
    r"⇒\s*(" + _abbrev_pattern + r")\s+(\d[\d:,\s\-]*\d|\d)",
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

# ── Patristic work abbreviation lookup ────────────────────────────────────
# Maps per-author work abbreviations found in CCC footnotes to canonical work IDs.
# CCC footnote patterns: "St. Augustine, Conf. 1, 1, 1: PL 32, 659-661."
# After matching an author, we look for these abbreviations in the same footnote.

_AUTHOR_WORK_ABBREVS: dict[str, dict[str, str]] = {
    "augustine": {
        "Conf.": "confessions",
        "De civ. Dei": "city-of-god",
        "De Trin.": "on-the-trinity",
        "De doctr. chr.": "on-christian-doctrine",
        "Serm.": "sermons",
        "En. in Ps.": "enarrations-on-psalms",
        "Ep.": "letters",
        "In Jo. ev.": "homilies-on-john",
        "De cat. rud.": "on-catechizing",
        "De Gen. ad litt.": "on-genesis",
        "De mor. eccl.": "on-the-morals-of-the-church",
        "De virg.": "on-virginity",
        "De nupt. et concup.": "on-marriage-and-concupiscence",
        "De lib. arb.": "on-free-will",
        "De grat. et lib. arb.": "on-grace-and-free-will",
        "De bapt.": "on-baptism",
        "De spir. et litt.": "on-the-spirit-and-the-letter",
        "De nat. et grat.": "on-nature-and-grace",
        "De cont.": "on-continence",
        "De bon. coniug.": "on-the-good-of-marriage",
    },
    "thomas-aquinas": {
        "STh": "summa-theologica",
        "S. Th.": "summa-theologica",
        "S.Th.": "summa-theologica",
    },
    "irenaeus": {
        "Adv. haeres.": "against-heresies",
    },
    "john-chrysostom": {
        "Hom. in Mt.": "homilies-on-matthew",
        "Hom. in Jo.": "homilies-on-john",
        "Hom. in Rom.": "homilies-on-romans",
        "Hom. in 1 Cor.": "homilies-on-1-corinthians",
        "Hom. in 2 Cor.": "homilies-on-2-corinthians",
        "Hom. in Eph.": "homilies-on-ephesians",
        "Hom. in Heb.": "homilies-on-hebrews",
        "De sac.": "on-the-priesthood",
        "De incomprehens.": "on-the-incomprehensible",
        "De prod. Judae": "on-the-betrayal-of-judas",
    },
    "athanasius": {
        "De inc.": "on-the-incarnation",
        "Ep. Serap.": "letters-to-serapion",
    },
    "basil": {
        "De Spir. S.": "on-the-holy-spirit",
        "Adv. Eunom.": "against-eunomius",
        "Reg. fus.": "longer-rules",
        "Reg. brev.": "shorter-rules",
    },
    "jerome": {
        "Comm. in Is.": "commentary-on-isaiah",
        "Comm. in Ezech.": "commentary-on-ezekiel",
    },
    "ambrose": {
        "De Sacr.": "on-the-sacraments",
        "De Myst.": "on-the-mysteries",
        "De off.": "on-the-duties",
        "Exam.": "hexameron",
        "In Luc.": "commentary-on-luke",
    },
    "gregory-nazianzen": {
        "Or. theol.": "theological-orations",
        "Or.": "orations",
    },
    "gregory-nyssa": {
        "De vita Mos.": "life-of-moses",
        "Or. cat.": "catechetical-oration",
        "Hom. in Cant.": "homilies-on-song-of-songs",
        "De hom. opif.": "on-the-making-of-man",
    },
    "gregory-great": {
        "Mor.": "moralia-on-job",
        "Past.": "pastoral-rule",
        "Hom. in Ev.": "homilies-on-gospels",
    },
    "cyril-jerusalem": {
        "Cat. myst.": "mystagogical-catecheses",
        "Catech. illum.": "catechetical-lectures",
    },
    "cyril-alexandria": {
        "In Jo. ev.": "commentary-on-john",
    },
    "john-damascene": {
        "De fide orth.": "exposition-of-orthodox-faith",
    },
    "leo-great": {
        "Serm.": "sermons",
    },
    "tertullian": {
        "De orat.": "on-prayer",
        "De bapt.": "on-baptism",
        "Apol.": "apologeticum",
        "De praescr.": "on-prescription",
        "De res.": "on-the-resurrection",
    },
    "origen": {
        "De princ.": "on-first-principles",
        "Hom. in Ex.": "homilies-on-exodus",
        "Hom. in Lev.": "homilies-on-leviticus",
        "Contra Cels.": "against-celsus",
    },
    "cyprian": {
        "De unit.": "on-the-unity-of-the-church",
    },
    "clement-rome": {
        "Ad Cor.": "first-epistle-to-corinthians",
    },
    "ignatius-antioch": {
        "Ad Eph.": "epistle-to-ephesians",
        "Ad Magn.": "epistle-to-magnesians",
        "Ad Rom.": "epistle-to-romans",
        "Ad Smyrn.": "epistle-to-smyrnaeans",
        "Ad Trall.": "epistle-to-trallians",
        "Ad Philad.": "epistle-to-philadelphians",
    },
    "polycarp": {
        "Ad Phil.": "epistle-to-philippians",
    },
    "bonaventure": {
        "In Sent.": "commentary-on-sentences",
        "Brev.": "breviloquium",
    },
    "anselm": {
        "Prosl.": "proslogion",
    },
    "hilary": {
        "In Mt.": "commentary-on-matthew",
        "De Trin.": "on-the-trinity",
    },
    "clement-alexandria": {
        "Strom.": "stromata",
        "Paed.": "paedagogus",
    },
    "justin-martyr": {
        "Dial.": "dialogue-with-trypho",
    },
}

# Build per-author work abbreviation regexes for efficient matching
_AUTHOR_WORK_REGEXES: dict[str, tuple[re.Pattern, dict[str, str]]] = {}
for _auth_id, _works_map in _AUTHOR_WORK_ABBREVS.items():
    if _works_map:
        _sorted_abbrevs_work = sorted(_works_map.keys(), key=len, reverse=True)
        _work_pattern = "|".join(re.escape(a) for a in _sorted_abbrevs_work)
        _AUTHOR_WORK_REGEXES[_auth_id] = (
            re.compile(
                r"(" + _work_pattern + r")\s*([\dIVXLCDM,\s.\-:]+)",
                re.UNICODE,
            ),
            _works_map,
        )


def _extract_work_info(raw: str, author_id: str) -> tuple[str, str]:
    """Try to extract work ID and location for a given author from footnote text.

    Returns (work_id, location) or ("", "") if not found.
    """
    entry = _AUTHOR_WORK_REGEXES.get(author_id)
    if not entry:
        return "", ""

    regex, abbrev_map = entry
    match = regex.search(raw)
    if not match:
        return "", ""

    abbrev = match.group(1)
    location = match.group(2).strip().rstrip(".,;:")
    work_id = abbrev_map[abbrev]
    return work_id, location

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
    r"\b(" + _general_doc_pattern + r")\s+(\d[\d,\s\-]*\d|\d)",
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
    seen_books: set[tuple[str, str]] = set()
    for match in _BIBLE_RE.finditer(raw):
        abbrev = match.group(1)
        reference = match.group(2).strip()
        canon_id, display_abbr = _ABBREV_TO_BOOK[abbrev]
        key = (canon_id, reference)
        if key not in seen_books:
            seen_books.add(key)
            bible_refs.append(BibleReference(book=canon_id, abbreviation=display_abbr, reference=reference))

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

    # Second pass: enhance author references with work abbreviation info
    for ar in author_refs:
        work_id, location = _extract_work_info(raw, ar.author)
        if work_id:
            ar.work = work_id
            ar.location = location

    # Extract ecclesiastical document references
    document_refs: list[DocumentReference] = []
    seen_docs: set[str] = set()

    # General document pattern (ABBR + section number)
    for match in _DOCUMENT_RE.finditer(raw):
        abbrev = match.group(1)
        section = match.group(2).strip()
        doc_id, display_abbr = _DOC_ABBREV_TO_ID[abbrev]
        if doc_id not in seen_docs:
            seen_docs.add(doc_id)
            document_refs.append(DocumentReference(document=doc_id, abbreviation=display_abbr, section=section))

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
