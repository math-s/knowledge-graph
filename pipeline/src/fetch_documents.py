"""Fetch ecclesiastical document texts from vatican.va.

Downloads and caches HTML, then parses numbered sections from Vatican II
documents, encyclicals, and other fetchable sources.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .models import DocumentSource, Paragraph

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "documents"

# Maps canonical document IDs to metadata and vatican.va URLs.
# Documents marked with url="" are reference collections (not fetchable).
_DOCUMENT_META: dict[str, dict] = {
    # Vatican II
    "lumen-gentium": {
        "name": "Lumen Gentium",
        "abbreviation": "LG",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19641121_lumen-gentium_en.html",
    },
    "gaudium-et-spes": {
        "name": "Gaudium et Spes",
        "abbreviation": "GS",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19651207_gaudium-et-spes_en.html",
    },
    "dei-verbum": {
        "name": "Dei Verbum",
        "abbreviation": "DV",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19651118_dei-verbum_en.html",
    },
    "sacrosanctum-concilium": {
        "name": "Sacrosanctum Concilium",
        "abbreviation": "SC",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_const_19631204_sacrosanctum-concilium_en.html",
    },
    "unitatis-redintegratio": {
        "name": "Unitatis Redintegratio",
        "abbreviation": "UR",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19641121_unitatis-redintegratio_en.html",
    },
    "ad-gentes": {
        "name": "Ad Gentes",
        "abbreviation": "AG",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19651207_ad-gentes_en.html",
    },
    "presbyterorum-ordinis": {
        "name": "Presbyterorum Ordinis",
        "abbreviation": "PO",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19651207_presbyterorum-ordinis_en.html",
    },
    "dignitatis-humanae": {
        "name": "Dignitatis Humanae",
        "abbreviation": "DH",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decl_19651207_dignitatis-humanae_en.html",
    },
    "apostolicam-actuositatem": {
        "name": "Apostolicam Actuositatem",
        "abbreviation": "AA",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19651118_apostolicam-actuositatem_en.html",
    },
    "nostra-aetate": {
        "name": "Nostra Aetate",
        "abbreviation": "NA",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decl_19651028_nostra-aetate_en.html",
    },
    "christus-dominus": {
        "name": "Christus Dominus",
        "abbreviation": "CD",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19651028_christus-dominus_en.html",
    },
    "perfectae-caritatis": {
        "name": "Perfectae Caritatis",
        "abbreviation": "PC",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19651028_perfectae-caritatis_en.html",
    },
    "optatam-totius": {
        "name": "Optatam Totius",
        "abbreviation": "OT",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19651028_optatam-totius_en.html",
    },
    "gravissimum-educationis": {
        "name": "Gravissimum Educationis",
        "abbreviation": "GE",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decl_19651028_gravissimum-educationis_en.html",
    },
    "inter-mirifica": {
        "name": "Inter Mirifica",
        "abbreviation": "IM",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19631204_inter-mirifica_en.html",
    },
    "orientalium-ecclesiarum": {
        "name": "Orientalium Ecclesiarum",
        "abbreviation": "OE",
        "category": "vatican-ii",
        "url": "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/vat-ii_decree_19641121_orientalium-ecclesiarum_en.html",
    },
    # Post-conciliar / papal
    "catechesi-tradendae": {
        "name": "Catechesi Tradendae",
        "abbreviation": "CT",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/apost_exhortations/documents/hf_jp-ii_exh_16101979_catechesi-tradendae.html",
    },
    "evangelii-nuntiandi": {
        "name": "Evangelii Nuntiandi",
        "abbreviation": "EN",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/paul-vi/en/apost_exhortations/documents/hf_p-vi_exh_19751208_evangelii-nuntiandi.html",
    },
    "centesimus-annus": {
        "name": "Centesimus Annus",
        "abbreviation": "CA",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_01051991_centesimus-annus.html",
    },
    "familiaris-consortio": {
        "name": "Familiaris Consortio",
        "abbreviation": "FC",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/apost_exhortations/documents/hf_jp-ii_exh_19811122_familiaris-consortio.html",
    },
    "redemptoris-missio": {
        "name": "Redemptoris Missio",
        "abbreviation": "RMiss",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_07121990_redemptoris-missio.html",
    },
    "sollicitudo-rei-socialis": {
        "name": "Sollicitudo Rei Socialis",
        "abbreviation": "SRS",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_30121987_sollicitudo-rei-socialis.html",
    },
    "reconciliatio-et-paenitentia": {
        "name": "Reconciliatio et Paenitentia",
        "abbreviation": "RP",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/apost_exhortations/documents/hf_jp-ii_exh_02121984_reconciliatio-et-paenitentia.html",
    },
    "humanae-vitae": {
        "name": "Humanae Vitae",
        "abbreviation": "HV",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/paul-vi/en/encyclicals/documents/hf_p-vi_enc_25071968_humanae-vitae.html",
    },
    "laborem-exercens": {
        "name": "Laborem Exercens",
        "abbreviation": "LE",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_14091981_laborem-exercens.html",
    },
    "mysterium-fidei": {
        "name": "Mysterium Fidei",
        "abbreviation": "MF",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/paul-vi/en/encyclicals/documents/hf_p-vi_enc_03091965_mysterium.html",
    },
    "mulieris-dignitatem": {
        "name": "Mulieris Dignitatem",
        "abbreviation": "MD",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/apost_letters/1988/documents/hf_jp-ii_apl_19880815_mulieris-dignitatem.html",
    },
    "dominum-et-vivificantem": {
        "name": "Dominum et Vivificantem",
        "abbreviation": "DeV",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/encyclicals/documents/hf_jp-ii_enc_18051986_dominum-et-vivificantem.html",
    },
    "pacem-in-terris": {
        "name": "Pacem in Terris",
        "abbreviation": "PT",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-xxiii/en/encyclicals/documents/hf_j-xxiii_enc_11041963_pacem.html",
    },
    "christifideles-laici": {
        "name": "Christifideles Laici",
        "abbreviation": "CL",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/john-paul-ii/en/apost_exhortations/documents/hf_jp-ii_exh_30121988_christifideles-laici.html",
    },
    "marialis-cultus": {
        "name": "Marialis Cultus",
        "abbreviation": "MC",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/paul-vi/en/apost_exhortations/documents/hf_p-vi_exh_19740202_marialis-cultus.html",
    },
    "populorum-progressio": {
        "name": "Populorum Progressio",
        "abbreviation": "PP",
        "category": "encyclical",
        "url": "https://www.vatican.va/content/paul-vi/en/encyclicals/documents/hf_p-vi_enc_26031967_populorum.html",
    },
    # Canon Law
    "cic": {
        "name": "Code of Canon Law",
        "abbreviation": "CIC",
        "category": "canon-law",
        "url": "https://www.vatican.va/archive/cod-iuris-canonici/cic_index_en.html",
    },
    "cceo": {
        "name": "Code of Canons of the Eastern Churches",
        "abbreviation": "CCEO",
        "category": "canon-law",
        "url": "",
    },
    # Reference Collections (not fetchable)
    "denzinger-schonmetzer": {
        "name": "Denzinger-Schönmetzer",
        "abbreviation": "DS",
        "category": "reference",
        "url": "",
    },
    "patrologia-latina": {
        "name": "Patrologia Latina",
        "abbreviation": "PL",
        "category": "reference",
        "url": "",
    },
    "patrologia-graeca": {
        "name": "Patrologia Graeca",
        "abbreviation": "PG",
        "category": "reference",
        "url": "",
    },
    "sources-chretiennes": {
        "name": "Sources Chrétiennes",
        "abbreviation": "SCh",
        "category": "reference",
        "url": "",
    },
    "acta-apostolicae-sedis": {
        "name": "Acta Apostolicae Sedis",
        "abbreviation": "AAS",
        "category": "reference",
        "url": "",
    },
}

# Reference collections that cannot be fetched online
_UNFETCHABLE_CATEGORIES = {"reference"}


def _download_document(doc_id: str, url: str) -> str | None:
    """Download and cache a document HTML page."""
    cache_path = RAW_DIR / f"{doc_id}.html"
    if cache_path.exists():
        logger.debug("Using cached document: %s", cache_path)
        with open(cache_path, encoding="utf-8", errors="replace") as f:
            return f.read()

    if not url:
        return None

    logger.info("Downloading document %s from %s", doc_id, url)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "CatechismKnowledgeGraph/1.0"})
        resp.raise_for_status()
        html = resp.text
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html
    except Exception as e:
        logger.warning("Failed to download %s: %s", doc_id, e)
        return None


def _parse_sections(html: str) -> dict[str, str]:
    """Parse numbered sections from a Vatican document HTML page.

    Vatican II documents typically have numbered paragraphs like "12." at the
    start of a paragraph. Encyclicals may use similar patterns.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style tags
    for tag in soup(["script", "style"]):
        tag.decompose()

    sections: dict[str, str] = {}

    # Strategy 1: Look for paragraphs starting with a number followed by period/dot
    for p_tag in soup.find_all("p"):
        text = p_tag.get_text(strip=True)
        if not text:
            continue
        # Match "12." or "12 " at the start of a paragraph
        m = re.match(r"^(\d{1,4})\.\s*(.+)", text)
        if m:
            section_num = m.group(1)
            section_text = m.group(2).strip()
            if section_text and len(section_text) > 20:
                sections[section_num] = section_text[:2000]  # Cap length

    return sections


def fetch_document_texts(paragraphs: list[Paragraph]) -> dict[str, DocumentSource]:
    """Fetch document texts for all documents cited in the given paragraphs.

    Returns a dict keyed by canonical document ID.
    """
    # Collect citing paragraphs per document
    doc_citing: dict[str, set[int]] = {}
    doc_sections_cited: dict[str, set[str]] = {}

    for p in paragraphs:
        for pf in p.parsed_footnotes:
            for dr in pf.document_refs:
                doc_citing.setdefault(dr.document, set()).add(p.id)
                if dr.section:
                    doc_sections_cited.setdefault(dr.document, set()).add(dr.section)

    result: dict[str, DocumentSource] = {}

    for doc_id in doc_citing:
        meta = _DOCUMENT_META.get(doc_id, {})
        name = meta.get("name", doc_id.replace("-", " ").title())
        abbreviation = meta.get("abbreviation", doc_id.upper())
        category = meta.get("category", "encyclical")
        url = meta.get("url", "")
        fetchable = category not in _UNFETCHABLE_CATEGORIES and bool(url)

        sections: dict[str, dict[str, str]] = {}
        if fetchable:
            html = _download_document(doc_id, url)
            if html:
                all_sections = _parse_sections(html)
                # Only keep cited sections — wrap as MultiLangText {"en": text}
                cited = doc_sections_cited.get(doc_id, set())
                if cited:
                    for sec_num in cited:
                        if sec_num in all_sections:
                            sections[sec_num] = {"en": all_sections[sec_num]}
                # If no specific sections were cited, keep first 10 as preview
                if not sections and all_sections:
                    for key in sorted(all_sections.keys(), key=lambda x: int(x) if x.isdigit() else 0)[:10]:
                        sections[key] = {"en": all_sections[key]}

        citing = sorted(doc_citing.get(doc_id, set()))
        result[doc_id] = DocumentSource(
            id=doc_id,
            name=name,
            abbreviation=abbreviation,
            category=category,
            source_url=url,
            fetchable=fetchable,
            citing_paragraphs=citing,
            sections=sections,
        )

    logger.info(
        "Fetched document texts: %d documents, %d with sections",
        len(result),
        sum(1 for d in result.values() if d.sections),
    )
    return result
