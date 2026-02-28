"""Assign thematic tags to CCC paragraphs using keywords and structural ranges."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .models import Paragraph

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
        keywords=["trinity", "triune", "three persons", "consubstantial", "filioque"],
        ranges=[(232, 267)],
    ),
    ThemeDefinition(
        id="eucharist",
        label="Eucharist",
        keywords=["eucharist", "transubstantiation", "real presence", "holy communion",
                  "body and blood", "breaking of bread", "eucharistic"],
        ranges=[(1322, 1419)],
    ),
    ThemeDefinition(
        id="baptism",
        label="Baptism",
        keywords=["baptism", "baptize", "baptized", "baptismal"],
        ranges=[(1213, 1284)],
    ),
    ThemeDefinition(
        id="prayer",
        label="Prayer",
        keywords=["prayer", "pray", "praying", "our father", "lord's prayer",
                  "contemplation", "meditation", "intercession"],
        ranges=[(2558, 2865)],
    ),
    ThemeDefinition(
        id="creation",
        label="Creation",
        keywords=["creation", "creator", "created the world", "visible and invisible",
                  "heaven and earth"],
        ranges=[(279, 354)],
    ),
    ThemeDefinition(
        id="salvation",
        label="Salvation",
        keywords=["salvation", "redemption", "redeemer", "savior", "atonement",
                  "paschal mystery", "saved"],
        ranges=[(599, 658)],
    ),
    ThemeDefinition(
        id="mary",
        label="Mary",
        keywords=["mary", "blessed virgin", "mother of god", "theotokos",
                  "immaculate conception", "assumption", "marian"],
        ranges=[(484, 511), (963, 975)],
    ),
    ThemeDefinition(
        id="sin",
        label="Sin",
        keywords=["sin", "original sin", "concupiscence", "mortal sin",
                  "venial sin", "sinful"],
        ranges=[(385, 421), (1846, 1876)],
    ),
    ThemeDefinition(
        id="grace",
        label="Grace",
        keywords=["grace", "justification", "sanctification", "sanctifying grace",
                  "merit"],
        ranges=[(1987, 2029)],
    ),
    ThemeDefinition(
        id="commandments",
        label="Commandments",
        keywords=["commandment", "decalogue", "ten commandments", "thou shalt"],
        ranges=[(2052, 2557)],
    ),
    ThemeDefinition(
        id="resurrection",
        label="Resurrection",
        keywords=["resurrection", "risen", "rose from the dead", "empty tomb",
                  "risen christ"],
        ranges=[(638, 658), (988, 1019)],
    ),
    ThemeDefinition(
        id="church",
        label="Church",
        keywords=["church", "body of christ", "people of god", "ecclesial",
                  "magisterium", "apostolic succession"],
        ranges=[(748, 975)],
    ),
    ThemeDefinition(
        id="sacraments",
        label="Sacraments",
        keywords=["sacrament", "sacramental", "liturgy", "liturgical"],
        ranges=[(1076, 1209)],
    ),
    ThemeDefinition(
        id="moral-life",
        label="Moral Life",
        keywords=["moral", "conscience", "virtue", "beatitudes", "natural law",
                  "common good", "social justice"],
        ranges=[(1691, 1986)],
    ),
    ThemeDefinition(
        id="scripture",
        label="Scripture",
        keywords=["scripture", "sacred scripture", "bible", "word of god",
                  "inspiration", "canon of scripture", "old testament",
                  "new testament", "gospel"],
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
        text_lower = para.text.lower()

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
