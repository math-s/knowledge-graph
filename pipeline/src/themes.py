"""Assign thematic tags to CCC paragraphs using keywords and structural ranges."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .models import Paragraph, resolve_lang

logger = logging.getLogger(__name__)


@dataclass
class ThemeDefinition:
    """A theme with keywords and CCC paragraph ranges."""

    id: str
    label: str
    keywords: list[str] = field(default_factory=list)
    ranges: list[tuple[int, int]] = field(default_factory=list)  # (start, end) inclusive


# ── Theme definitions ────────────────────────────────────────────────────────
# Keywords are matched case-insensitively against paragraph text.
# Ranges correspond to CCC structural sections definitionally about the theme.

THEME_DEFINITIONS: list[ThemeDefinition] = [
    ThemeDefinition(
        id="trinity",
        label="Trinity",
        keywords=[
            # English
            "trinity", "triune", "three persons", "consubstantial", "filioque",
            # Portuguese
            "trindade", "triuno", "três pessoas", "consubstancial",
            # Latin
            "trinitas", "trinitate", "trinitatis", "consubstantialis",
        ],
        ranges=[(232, 267)],
    ),
    ThemeDefinition(
        id="eucharist",
        label="Eucharist",
        keywords=[
            "eucharist", "transubstantiation", "real presence", "holy communion",
            "body and blood", "breaking of bread", "eucharistic",
            "eucaristia", "eucarística", "transubstanciação", "comunhão",
            "corpo e sangue", "pão e vinho",
            "eucharistia", "eucharisticus", "transsubstantiatio", "communio",
            "corpus et sanguis", "sacramentum altaris",
        ],
        ranges=[(1322, 1419)],
    ),
    ThemeDefinition(
        id="baptism",
        label="Baptism",
        keywords=[
            "baptism", "baptize", "baptized", "baptismal",
            "batismo", "batizado", "batizar", "batismal",
            "baptisma", "baptismus", "baptizare", "baptizatus",
        ],
        ranges=[(1213, 1284)],
    ),
    ThemeDefinition(
        id="prayer",
        label="Prayer",
        keywords=[
            "prayer", "pray", "praying", "our father", "lord's prayer",
            "contemplation", "meditation", "intercession",
            "oração", "orar", "pai nosso", "contemplação", "intercessão",
            "oratio", "orare", "pater noster", "contemplatio", "intercessio",
        ],
        ranges=[(2558, 2865)],
    ),
    ThemeDefinition(
        id="creation",
        label="Creation",
        keywords=[
            "creation", "creator", "created the world", "visible and invisible",
            "heaven and earth",
            "criação", "criador", "visível e invisível", "céu e terra",
            "creatio", "creator", "creavit", "caelum et terra",
            "visibilium et invisibilium",
        ],
        ranges=[(279, 354)],
    ),
    ThemeDefinition(
        id="salvation",
        label="Salvation",
        keywords=[
            "salvation", "redemption", "redeemer", "savior", "atonement",
            "paschal mystery", "saved",
            "salvação", "redenção", "redentor", "salvador", "mistério pascal",
            "salus", "salvus", "redemptio", "redemptor", "salvator",
            "mysterium paschale",
        ],
        ranges=[(599, 658)],
    ),
    ThemeDefinition(
        id="mary",
        label="Mary",
        keywords=[
            "mary", "blessed virgin", "mother of god", "theotokos",
            "immaculate conception", "assumption", "marian",
            "maria", "virgem santíssima", "mãe de deus", "bem-aventurada virgem",
            "imaculada conceição", "assunção",
            "maria", "beata virgo", "mater dei", "theotokos",
            "immaculata conceptio", "assumptio",
        ],
        ranges=[(484, 511), (963, 975)],
    ),
    ThemeDefinition(
        id="sin",
        label="Sin",
        keywords=[
            "sin", "original sin", "concupiscence", "mortal sin", "venial sin", "sinful",
            "pecado", "pecado original", "concupiscência", "pecado mortal", "pecado venial",
            "peccatum", "peccatum originale", "concupiscentia", "peccatum mortale",
        ],
        ranges=[(385, 421), (1846, 1876)],
    ),
    ThemeDefinition(
        id="grace",
        label="Grace",
        keywords=[
            "grace", "justification", "sanctification", "sanctifying grace", "merit",
            "graça", "justificação", "santificação", "graça santificante", "mérito",
            "gratia", "justificatio", "sanctificatio", "gratia sanctificans", "meritum",
        ],
        ranges=[(1987, 2029)],
    ),
    ThemeDefinition(
        id="commandments",
        label="Commandments",
        keywords=[
            "commandment", "decalogue", "ten commandments", "thou shalt",
            "mandamento", "decálogo", "dez mandamentos",
            "mandatum", "decalogus", "decem praecepta",
        ],
        ranges=[(2052, 2557)],
    ),
    ThemeDefinition(
        id="resurrection",
        label="Resurrection",
        keywords=[
            "resurrection", "risen", "rose from the dead", "empty tomb", "risen christ",
            "ressurreição", "ressuscitado", "ressuscitou",
            "resurrectio", "resurrexit", "resurrectionem",
        ],
        ranges=[(638, 658), (988, 1019)],
    ),
    ThemeDefinition(
        id="church",
        label="Church",
        keywords=[
            "church", "body of christ", "people of god", "ecclesial",
            "magisterium", "apostolic succession",
            "igreja", "corpo de cristo", "povo de deus", "eclesial",
            "magistério", "sucessão apostólica",
            "ecclesia", "corpus christi", "populus dei", "ecclesiasticus",
            "magisterium", "successio apostolica",
        ],
        ranges=[(748, 975)],
    ),
    ThemeDefinition(
        id="sacraments",
        label="Sacraments",
        keywords=[
            "sacrament", "sacramental", "liturgy", "liturgical",
            "sacramento", "sacramental", "liturgia", "litúrgico",
            "sacramentum", "sacramentalis", "liturgia",
        ],
        ranges=[(1076, 1209)],
    ),
    ThemeDefinition(
        id="moral-life",
        label="Moral Life",
        keywords=[
            "moral", "conscience", "virtue", "beatitudes", "natural law",
            "common good", "social justice",
            "moral", "consciência", "virtude", "bem-aventuranças", "lei natural",
            "bem comum", "justiça social",
            "moralis", "conscientia", "virtus", "lex naturalis", "bonum commune",
        ],
        ranges=[(1691, 1986)],
    ),
    ThemeDefinition(
        id="scripture",
        label="Scripture",
        keywords=[
            "scripture", "sacred scripture", "bible", "word of god",
            "inspiration", "canon of scripture", "old testament",
            "new testament", "gospel",
            "escritura", "sagrada escritura", "bíblia", "palavra de deus",
            "inspiração", "antigo testamento", "novo testamento", "evangelho",
            "scriptura", "sacra scriptura", "verbum dei", "inspiratio",
            "vetus testamentum", "novum testamentum", "evangelium",
        ],
        ranges=[(101, 141)],
    ),
]


def _in_range(para_id: int, ranges: list[tuple[int, int]]) -> bool:
    """Check if a paragraph ID falls within any of the given ranges."""
    return any(start <= para_id <= end for start, end in ranges)


def assign_themes(paragraphs: list[Paragraph]) -> list[Paragraph]:
    """Assign thematic tags to paragraphs via keyword scan + range check."""
    theme_counts: dict[str, int] = {}

    for para in paragraphs:
        themes: list[str] = []
        # Scan every available language of the paragraph — Denzinger-Hünermann
        # sections only carry text_la/text_pt, and those keyword lists include
        # Portuguese and Latin synonyms.
        text_lower = " \n ".join(
            (para.text.get(lang) or "")
            for lang in ("en", "la", "pt")
            if para.text.get(lang)
        ).lower()
        if not text_lower:
            text_lower = resolve_lang(para.text, "en").lower()

        for theme in THEME_DEFINITIONS:
            matched = False

            # Check range first (definitive)
            if _in_range(para.id, theme.ranges):
                matched = True

            # Check keywords
            if not matched:
                for kw in theme.keywords:
                    if kw in text_lower:
                        matched = True
                        break

            if matched:
                themes.append(theme.id)
                theme_counts[theme.id] = theme_counts.get(theme.id, 0) + 1

        para.themes = themes

    for theme_id, count in sorted(theme_counts.items(), key=lambda x: -x[1]):
        logger.info("Theme '%s': %d paragraphs", theme_id, count)

    logger.info("Assigned themes to %d paragraphs total",
                sum(1 for p in paragraphs if p.themes))
    return paragraphs
