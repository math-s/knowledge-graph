"""Fetch original Latin texts for Latin Church Fathers.

Downloads Latin source texts from The Latin Library (thelatinlibrary.com)
and other public domain sources, then merges them into existing
PatristicWork structures as the "la" language key.

Uses a curated URL catalog mapping canonical author/work IDs to source URLs.
Implements rate-limiting (2s between requests) and disk caching.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .models import PatristicChapter, PatristicSection, PatristicWork

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "patristic_latin"

# Rate-limit: delay between HTTP requests (seconds)
REQUEST_DELAY = 2.0

# ── Latin Authors ────────────────────────────────────────────────────────────
# Christian Latin authors whose original works are available on The Latin Library.
# NOTE: Cyprian and Hilary are CCC-cited Latin Fathers but their pages are
# currently 404 on The Latin Library, so they are excluded until an alternative
# source is found.

LATIN_FATHER_IDS = {
    # CCC-cited Church Fathers
    "augustine",
    "thomas-aquinas",
    "jerome",
    "ambrose",
    "gregory-great",
    "leo-great",
    "tertullian",
    "bonaventure",
    "anselm",
    # Pre-Nicene / Early Church
    "arnobius",
    "commodianus",
    "lactantius",
    "novatian",
    # Post-Nicene / Patristic
    "benedict",
    "cassiodorus",
    "eucherius",
    "macarius-alexandria",
    "vincent-lerins",
    # Medieval
    "bede",
    "bernard-clairvaux",
    "hugo-st-victor",
    "innocent-iii",
    "isidore-seville",
}

# ── URL Catalog ──────────────────────────────────────────────────────────────
# Maps author_id to a list of work entries from The Latin Library.
#
# Each entry has:
#   work_pattern  – substring matched against existing English work slugs/titles
#   title         – proper Latin title (used when creating standalone works)
#   url           – primary URL (single-page) or first page (multi-page)
#   chapter_url_template – Python format string with {n} for chapter number
#   chapter_count – total chapters (1 for single-page works)
#
# URL conventions on The Latin Library:
#   - Subdirectory pages: thelatinlibrary.com/{author}/{work}.shtml or .html
#   - Root-level pages:   thelatinlibrary.com/{work}.html or .shtml
#   - Extensions vary by author (.shtml vs .html) — must match exactly
#
# All URLs verified 2026-03-14 unless noted otherwise.

_BASE = "https://www.thelatinlibrary.com"

_LATIN_CATALOG: dict[str, list[dict]] = {
    # ── Augustine (354–430) ──────────────────────────────────────────────────
    "augustine": [
        {
            "work_pattern": "confessions",
            "title": "Confessiones",
            "url": f"{_BASE}/augustine/conf1.shtml",
            "chapter_url_template": f"{_BASE}/augustine/conf{{n}}.shtml",
            "chapter_count": 13,
        },
        {
            "work_pattern": "city-of-god",
            "title": "De Civitate Dei",
            "url": f"{_BASE}/augustine/civ1.shtml",
            "chapter_url_template": f"{_BASE}/augustine/civ{{n}}.shtml",
            "chapter_count": 22,
        },
        {
            "work_pattern": "on-the-trinity",
            "title": "De Trinitate",
            "url": f"{_BASE}/augustine/trin1.shtml",
            "chapter_url_template": f"{_BASE}/augustine/trin{{n}}.shtml",
            "chapter_count": 15,
        },
        {
            "work_pattern": "contra-iulianum",
            "title": "Contra Secundam Iuliani Responsionem",
            "url": f"{_BASE}/augustine/iulianus1.shtml",
            "chapter_url_template": f"{_BASE}/augustine/iulianus{{n}}.shtml",
            "chapter_count": 2,
        },
        {
            "work_pattern": "dialectica",
            "title": "De Dialectica",
            "url": f"{_BASE}/augustine/dia.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "fide-et-symbolo",
            "title": "De Fide et Symbolo",
            "url": f"{_BASE}/augustine/fide.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "catechizandis",
            "title": "De Catechizandis Rudibus",
            "url": f"{_BASE}/augustine/catechizandis.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "regula",
            "title": "Regula Sancti Augustini",
            "url": f"{_BASE}/augustine/reg.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Ambrose (c. 340–397) ─────────────────────────────────────────────────
    # NOTE: De Officiis and De Spiritu Sancto are NOT on The Latin Library (404).
    # Only De Mysteriis, Hymni, and Epistulae are available.
    "ambrose": [
        {
            "work_pattern": "mysteriis",
            "title": "De Mysteriis",
            "url": f"{_BASE}/ambrose/mysteriis.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "hymn",
            "title": "Hymni",
            "url": f"{_BASE}/ambrose/hymns.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "epistula-sororem",
            "title": "Epistula ad Sororem",
            "url": f"{_BASE}/ambrose/epist.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "epistulae-variae",
            "title": "Epistulae Variae",
            "url": f"{_BASE}/ambrose/epistvaria.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Anselm (c. 1033–1109) ────────────────────────────────────────────────
    # NOTE: Root-level URLs (no subdirectory). Cur Deus Homo is NOT available.
    "anselm": [
        {
            "work_pattern": "proslog",
            "title": "Proslogion",
            "url": f"{_BASE}/anselmproslogion.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "epistula-urbanum",
            "title": "Epistula ad Urbanum Papam",
            "url": f"{_BASE}/anselmepistula.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Arnobius (fl. c. 300) ────────────────────────────────────────────────
    "arnobius": [
        {
            "work_pattern": "adversus-nationes",
            "title": "Adversus Nationes",
            "url": f"{_BASE}/arnobius/arnobius1.shtml",
            "chapter_url_template": f"{_BASE}/arnobius/arnobius{{n}}.shtml",
            "chapter_count": 7,
        },
    ],
    # ── Bede (c. 672–735) ────────────────────────────────────────────────────
    "bede": [
        {
            "work_pattern": "historia-ecclesiastica",
            "title": "Historia Ecclesiastica Gentis Anglorum",
            "url": f"{_BASE}/bede/bede1.shtml",
            "chapter_url_template": f"{_BASE}/bede/bede{{n}}.shtml",
            "chapter_count": 5,
        },
        {
            "work_pattern": "proverbia",
            "title": "Proverbiorum Liber",
            "url": f"{_BASE}/bede/bedeproverbs.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Benedict (c. 480–547) ────────────────────────────────────────────────
    "benedict": [
        {
            "work_pattern": "regula",
            "title": "Regula Benedicti",
            "url": f"{_BASE}/benedict.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Bernard of Clairvaux (1090–1153) ─────────────────────────────────────
    "bernard-clairvaux": [
        {
            "work_pattern": "laude-novae-militiae",
            "title": "Liber ad Milites Templi de Laude Novae Militiae",
            "url": f"{_BASE}/bernardclairvaux.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Bonaventure (1221–1274) ──────────────────────────────────────────────
    # NOTE: Root-level URL (no subdirectory), .html extension.
    "bonaventure": [
        {
            "work_pattern": "itinerary",
            "title": "Itinerarium Mentis in Deum",
            "url": f"{_BASE}/bonaventura.itinerarium.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Cassiodorus (c. 485–585) ─────────────────────────────────────────────
    "cassiodorus": [
        {
            "work_pattern": "variae",
            "title": "Variae",
            "url": f"{_BASE}/cassiodorus/varia1.shtml",
            "chapter_url_template": f"{_BASE}/cassiodorus/varia{{n}}.shtml",
            "chapter_count": 12,
        },
        {
            "work_pattern": "epistulae-theodericianae",
            "title": "Epistulae Theodericianae Variae",
            "url": f"{_BASE}/cassiodorus/epist.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "orationes",
            "title": "Orationum Reliquiae",
            "url": f"{_BASE}/cassiodorus/orationes.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-anima",
            "title": "De Anima",
            "url": f"{_BASE}/cassiodorus/anima.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-musica",
            "title": "De Musica",
            "url": f"{_BASE}/cassiodorus/musica.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Commodianus (fl. c. 250) ─────────────────────────────────────────────
    "commodianus": [
        {
            "work_pattern": "carmen-duobus-populis",
            "title": "Carmen de Duobus Populis",
            "url": f"{_BASE}/commodianus/commodianus1.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "instructiones",
            "title": "Instructiones",
            "url": f"{_BASE}/commodianus/commodianus2.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-saeculi-fine",
            "title": "De Saeculi Istius Fine",
            "url": f"{_BASE}/commodianus/commodianus3.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Eucherius (d. c. 449) ────────────────────────────────────────────────
    "eucherius": [
        {
            "work_pattern": "laude-eremi",
            "title": "De Laude Eremi",
            "url": f"{_BASE}/eucherius.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Gregory the Great (c. 540–604) ───────────────────────────────────────
    # Very limited on The Latin Library — only a single letter.
    "gregory-great": [
        {
            "work_pattern": "epistula-constantina",
            "title": "Epistula IV.30 ad Constantinam Augustam",
            "url": f"{_BASE}/greg.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Hugo of St. Victor (c. 1096–1141) ────────────────────────────────────
    "hugo-st-victor": [
        {
            "work_pattern": "didascalicon",
            "title": "Didascalicon",
            "url": f"{_BASE}/hugo/hugo1.html",
            "chapter_url_template": f"{_BASE}/hugo/hugo{{n}}.html",
            "chapter_count": 6,
        },
        {
            "work_pattern": "soliloquium",
            "title": "Soliloquium de Arrha Animae",
            "url": f"{_BASE}/hugo/hugo.solo.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Innocent III (c. 1161–1216) ──────────────────────────────────────────
    # Index at innocent.html lists De Miseria + Dialogus, but the individual
    # work URLs are unknown (not verified). Skipped until URLs are confirmed.
    # "innocent-iii": [],
    # ── Isidore of Seville (c. 560–636) ──────────────────────────────────────
    "isidore-seville": [
        {
            "work_pattern": "etymologiae",
            "title": "Etymologiarum sive Originum Libri XX",
            "url": f"{_BASE}/isidore/1.shtml",
            "chapter_url_template": f"{_BASE}/isidore/{{n}}.shtml",
            "chapter_count": 20,
        },
        {
            "work_pattern": "sententiae",
            "title": "Sententiae",
            "url": f"{_BASE}/isidore/sententiae1.shtml",
            "chapter_url_template": f"{_BASE}/isidore/sententiae{{n}}.shtml",
            "chapter_count": 3,
        },
        {
            "work_pattern": "historia-regibus",
            "title": "Historia de Regibus Gothorum, Wandalorum et Suevorum",
            "url": f"{_BASE}/isidore/historia.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Jerome (c. 347–420) ──────────────────────────────────────────────────
    "jerome": [
        {
            "work_pattern": "epistulae",
            "title": "Epistulae",
            "url": f"{_BASE}/jerome/epistulae.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "vita-pauli",
            "title": "Vita Pauli",
            "url": f"{_BASE}/jerome/vitapauli.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "vita-malchi",
            "title": "Vita Malchi",
            "url": f"{_BASE}/jerome/vitamalchus.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "contra-ioannem",
            "title": "Contra Ioannem",
            "url": f"{_BASE}/jerome/contraioannem.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Lactantius (c. 250–325) ──────────────────────────────────────────────
    # Only Book I of Divinae Institutiones is linked; Books II-VII are 404.
    "lactantius": [
        {
            "work_pattern": "divinarum-institutionum",
            "title": "Divinarum Institutionum Liber I",
            "url": f"{_BASE}/lactantius/divinst1.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-mortibus",
            "title": "De Mortibus Persecutorum",
            "url": f"{_BASE}/lactantius/demort.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Leo the Great (c. 400–461) ───────────────────────────────────────────
    # Only 2 of 12 Lenten sermons have working links.
    "leo-great": [
        {
            "work_pattern": "sermones-quadragesima",
            "title": "Sermones de Quadragesima",
            "url": f"{_BASE}/leothegreat/quadragesima1.html",
            "chapter_url_template": f"{_BASE}/leothegreat/quadragesima{{n}}.html",
            "chapter_count": 2,
        },
    ],
    # ── Macarius of Alexandria (d. c. 394) ───────────────────────────────────
    "macarius-alexandria": [
        {
            "work_pattern": "regula-monachos",
            "title": "Regula ad Monachos",
            "url": f"{_BASE}/macarius.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Novatian (c. 200–258) ────────────────────────────────────────────────
    "novatian": [
        {
            "work_pattern": "de-trinitate",
            "title": "De Trinitate",
            "url": f"{_BASE}/novatian.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Tertullian (c. 155–220) ──────────────────────────────────────────────
    # The Latin Library has the most complete Tertullian corpus: 31 authentic
    # works + 6 spuria. All follow the pattern tertullian/tertullian.{slug}.shtml.
    "tertullian": [
        {
            "work_pattern": "apologeticum",
            "title": "Apologeticum",
            "url": f"{_BASE}/tertullian/tertullian.apol.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "adversus-praxean",
            "title": "Adversus Praxean",
            "url": f"{_BASE}/tertullian/tertullian.praxean.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "praescriptione",
            "title": "De Praescriptione Haereticorum",
            "url": f"{_BASE}/tertullian/tertullian.praescrip.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "ad-martyres",
            "title": "Ad Martyres",
            "url": f"{_BASE}/tertullian/tertullian.martyres.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "ad-nationes",
            "title": "Ad Nationes",
            "url": f"{_BASE}/tertullian/tertullian.nationes.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "ad-scapulam",
            "title": "Ad Scapulam",
            "url": f"{_BASE}/tertullian/tertullian.scapulam.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "ad-uxorem",
            "title": "Ad Uxorem",
            "url": f"{_BASE}/tertullian/tertullian.uxor.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "adversus-hermogenem",
            "title": "Adversus Hermogenem",
            "url": f"{_BASE}/tertullian/tertullian.herm.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "adversus-iudaeos",
            "title": "Adversus Iudaeos",
            "url": f"{_BASE}/tertullian/tertullian.iudaeos.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "adversus-valentinianos",
            "title": "Adversus Valentinianos",
            "url": f"{_BASE}/tertullian/tertullian.valentinianos.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "adversus-marcionem",
            "title": "Adversus Marcionem",
            "url": f"{_BASE}/tertullian/tertullian.marcionem.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-anima",
            "title": "De Anima",
            "url": f"{_BASE}/tertullian/tertullian.anima.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-baptismo",
            "title": "De Baptismo",
            "url": f"{_BASE}/tertullian/tertullian.baptismo.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-carne-christi",
            "title": "De Carne Christi",
            "url": f"{_BASE}/tertullian/tertullian.carne.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-corona",
            "title": "De Corona Militis",
            "url": f"{_BASE}/tertullian/tertullian.corona.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-cultu-feminarum",
            "title": "De Cultu Feminarum",
            "url": f"{_BASE}/tertullian/tertullian.cultu.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-exhortatione-castitatis",
            "title": "De Exhortatione Castitatis",
            "url": f"{_BASE}/tertullian/tertullian.castitatis.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-fuga",
            "title": "De Fuga in Persecutione",
            "url": f"{_BASE}/tertullian/tertullian.fuga.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-idololatria",
            "title": "De Idololatria",
            "url": f"{_BASE}/tertullian/tertullian.idololatria.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-ieiunio",
            "title": "De Ieiunio",
            "url": f"{_BASE}/tertullian/tertullian.ieiunio.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-monogamia",
            "title": "De Monogamia",
            "url": f"{_BASE}/tertullian/tertullian.monog.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-oratione",
            "title": "De Oratione",
            "url": f"{_BASE}/tertullian/tertullian.oratione.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-pallio",
            "title": "De Pallio",
            "url": f"{_BASE}/tertullian/tertullian.pallio.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-paenitentia",
            "title": "De Paenitentia",
            "url": f"{_BASE}/tertullian/tertullian.paen.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-patientia",
            "title": "De Patientia",
            "url": f"{_BASE}/tertullian/tertullian.patientia.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-pudicitia",
            "title": "De Pudicitia",
            "url": f"{_BASE}/tertullian/tertullian.pudicitia.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-resurrectione",
            "title": "De Resurrectione Carnis",
            "url": f"{_BASE}/tertullian/tertullian.resurrectione.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-spectaculis",
            "title": "De Spectaculis",
            "url": f"{_BASE}/tertullian/tertullian.spect.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-testimonio",
            "title": "De Testimonio Animae",
            "url": f"{_BASE}/tertullian/tertullian.testimonia.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-virginibus",
            "title": "De Virginibus Velandis",
            "url": f"{_BASE}/tertullian/tertullian.virginibus.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "scorpiace",
            "title": "Liber Scorpiace",
            "url": f"{_BASE}/tertullian/tertullian.scorpiace.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Thomas Aquinas (1225–1274) ────────────────────────────────────────────
    # Summa Prima Pars has ~80 questions linked (Q1-Q74, Q80-83, Q86-87).
    # Questions Q75-79 and Q84-85 will 404 and be silently skipped.
    "thomas-aquinas": [
        {
            "work_pattern": "summa-prima-pars",
            "title": "Summa Theologica — Prima Pars",
            "url": f"{_BASE}/aquinas/p1.shtml",
            "chapter_url_template": f"{_BASE}/aquinas/q1.{{n}}.shtml",
            "chapter_count": 87,
        },
        {
            "work_pattern": "de-ente-et-essentia",
            "title": "De Ente et Essentia",
            "url": f"{_BASE}/aquinas/ente.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "corpus-christi",
            "title": "Corpus Christi",
            "url": f"{_BASE}/aquinas/corpuschristi.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "de-principio-individuationis",
            "title": "De Principio Individuationis",
            "url": f"{_BASE}/aquinas/princ.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
        {
            "work_pattern": "expositio-orationem",
            "title": "Expositio in Orationem Dominicam",
            "url": f"{_BASE}/aquinas/expositio.shtml",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
    # ── Vincent of Lérins (d. c. 445) ────────────────────────────────────────
    "vincent-lerins": [
        {
            "work_pattern": "commonitorium",
            "title": "Commonitorium",
            "url": f"{_BASE}/vicentius.html",
            "chapter_url_template": None,
            "chapter_count": 1,
        },
    ],
}


def _download_page(url: str, cache_path: Path) -> str | None:
    """Download a page with caching and rate limiting."""
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Downloading Latin text: %s", url)
    time.sleep(REQUEST_DELAY)

    try:
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "CatechismKnowledgeGraph/1.0"},
        )
        resp.raise_for_status()
        html = resp.text
        cache_path.write_text(html, encoding="utf-8")
        return html
    except Exception as e:
        logger.warning("Failed to download %s: %s", url, e)
        return None


def _extract_latin_text(html: str) -> str:
    """Extract Latin text content from a Latin Library HTML page.

    The Latin Library uses simple HTML with text in <p> tags within the body.
    Strips navigation, headers, and metadata.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content elements
    for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    body = soup.find("body")
    if not body:
        return ""

    paragraphs: list[str] = []
    for p in body.find_all("p"):
        text = p.get_text(strip=True)
        # Latin Library uses <p> for content paragraphs
        # Skip very short fragments (likely navigation or headers)
        if text and len(text) > 15:
            # Clean common artifacts
            text = re.sub(r"\s+", " ", text)
            paragraphs.append(text)

    return "\n\n".join(paragraphs)


def _match_work(work: PatristicWork, catalog_entry: dict) -> bool:
    """Check if a PatristicWork matches a catalog entry's pattern."""
    pattern = catalog_entry["work_pattern"].lower()
    work_id_lower = work.id.lower()
    work_title_lower = work.title.lower()
    # Also check with hyphens replaced by spaces (title uses spaces, pattern uses hyphens)
    pattern_spaced = pattern.replace("-", " ")
    return (
        pattern in work_id_lower
        or pattern in work_title_lower
        or pattern_spaced in work_title_lower
    )


def _fetch_latin_for_work(
    author_id: str,
    work: PatristicWork,
    catalog_entry: dict,
) -> dict[int, str]:
    """Fetch Latin text for a single work. Returns {chapter_number: latin_text}."""
    work_dir = RAW_DIR / author_id / work.id.split("/")[-1]
    result: dict[int, str] = {}

    template = catalog_entry.get("chapter_url_template")
    chapter_count = catalog_entry.get("chapter_count", 1)

    if template and chapter_count > 1:
        # Multi-page work: download each chapter
        for ch_num in range(1, chapter_count + 1):
            url = template.format(n=ch_num)
            cache_path = work_dir / f"chapter_{ch_num:03d}.html"
            html = _download_page(url, cache_path)
            if html:
                text = _extract_latin_text(html)
                if text and len(text) > 30:
                    result[ch_num] = text
    else:
        # Single-page work
        url = catalog_entry["url"]
        cache_path = work_dir / "full.html"
        html = _download_page(url, cache_path)
        if html:
            text = _extract_latin_text(html)
            if text and len(text) > 30:
                result[1] = text

    return result


def _merge_latin_into_work(
    work: PatristicWork,
    latin_chapters: dict[int, str],
) -> int:
    """Merge Latin text into an existing PatristicWork's sections.

    Matches by chapter number. If the work has N English chapters and
    the Latin source has M chapters, merges min(N, M) chapters.

    Returns the number of sections that received Latin text.
    """
    merged = 0

    for chapter in work.chapters:
        ch_num = chapter.number
        if ch_num not in latin_chapters:
            continue

        latin_text = latin_chapters[ch_num]

        # If the chapter has sections, add Latin to the first section
        # (Latin Library typically gives us one text blob per chapter)
        if chapter.sections:
            for section in chapter.sections:
                if "la" not in section.text:
                    section.text["la"] = latin_text
                    merged += 1
                    break  # Only add to first section per chapter
        # If no sections exist (unlikely), skip this chapter

    return merged


def _create_latin_work(
    author_id: str,
    entry: dict,
    latin_chapters: dict[int, str],
) -> PatristicWork:
    """Create a new PatristicWork from Latin-only text."""
    work_id = entry["work_pattern"]
    title = entry.get("title", work_id.replace("-", " ").title())

    chapters: list[PatristicChapter] = []
    for ch_num in sorted(latin_chapters):
        section = PatristicSection(
            id=f"{author_id}/{work_id}/{ch_num}/1",
            chapter_id=f"{author_id}/{work_id}/{ch_num}",
            number=1,
            text={"la": latin_chapters[ch_num]},
        )
        chapter = PatristicChapter(
            id=f"{author_id}/{work_id}/{ch_num}",
            work_id=f"{author_id}/{work_id}",
            number=ch_num,
            title=f"Book {ch_num}" if len(latin_chapters) > 1 else title,
            sections=[section],
        )
        chapters.append(chapter)

    return PatristicWork(
        id=f"{author_id}/{work_id}",
        author_id=author_id,
        title=title,
        source_url=entry["url"],
        chapters=chapters,
    )


def fetch_patristic_latin(
    patristic_works: dict[str, list[PatristicWork]],
) -> dict[str, list[PatristicWork]]:
    """Fetch Latin source texts and merge into existing patristic works.

    For each Latin Father, downloads the original Latin text and adds it
    as the "la" key in the PatristicSection.text MultiLangText dict.
    If no existing English work matches a catalog entry, a new Latin-only
    work is created and appended to the author's work list.

    Args:
        patristic_works: Existing works dict (author_id -> list[PatristicWork])
            with English text already populated.

    Returns:
        The same dict with Latin text merged/added where available.
    """
    total_works_merged = 0
    total_works_created = 0
    total_sections_merged = 0

    for author_id in LATIN_FATHER_IDS:
        catalog = _LATIN_CATALOG.get(author_id, [])
        if not catalog:
            continue

        works = patristic_works.get(author_id, [])
        matched_entries: set[int] = set()

        # Pass 1: try to merge into existing English works
        for work in works:
            for i, entry in enumerate(catalog):
                if i in matched_entries:
                    continue
                if not _match_work(work, entry):
                    continue

                logger.info(
                    "Fetching Latin text for %s / %s",
                    author_id,
                    work.title,
                )
                latin_chapters = _fetch_latin_for_work(author_id, work, entry)

                if latin_chapters:
                    merged = _merge_latin_into_work(work, latin_chapters)
                    if merged > 0:
                        total_works_merged += 1
                        total_sections_merged += merged
                        logger.info(
                            "  Merged %d Latin sections into %s",
                            merged,
                            work.title,
                        )
                matched_entries.add(i)
                break  # Only match first catalog entry per work

        # Pass 2: create new Latin-only works for unmatched catalog entries
        for i, entry in enumerate(catalog):
            if i in matched_entries:
                continue

            # Use a dummy work for _fetch_latin_for_work's cache path
            dummy = PatristicWork(
                id=f"{author_id}/{entry['work_pattern']}",
                author_id=author_id,
                title=entry.get("title", entry["work_pattern"]),
                source_url=entry["url"],
                chapters=[],
            )
            logger.info(
                "Fetching Latin text (new work) for %s / %s",
                author_id,
                entry.get("title", entry["work_pattern"]),
            )
            latin_chapters = _fetch_latin_for_work(author_id, dummy, entry)

            if latin_chapters:
                new_work = _create_latin_work(author_id, entry, latin_chapters)
                if author_id not in patristic_works:
                    patristic_works[author_id] = []
                patristic_works[author_id].append(new_work)
                total_works_created += 1
                total_sections_merged += sum(1 for _ in latin_chapters)
                logger.info(
                    "  Created Latin work '%s' with %d chapters",
                    new_work.title,
                    len(new_work.chapters),
                )

    logger.info(
        "Latin patristic: %d works merged, %d works created, %d sections total",
        total_works_merged,
        total_works_created,
        total_sections_merged,
    )
    return patristic_works
