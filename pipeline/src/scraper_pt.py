"""Scrape the Portuguese CCC from Vatican.va and produce ccc-pt.json.

Output schema matches the nossbigg/catechism-ccc-json format so the
existing ingest pipeline works unchanged.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
CACHE_DIR = RAW_DATA_DIR / "pt"

# The CCC has exactly 2865 numbered paragraphs
MAX_CCC_PARAGRAPH = 2865

_HTTP_HEADERS = {
    "User-Agent": "knowledge-graph-scraper/1.0 (CCC bilingual pipeline)",
}

BASE_URL = "https://www.vatican.va/archive/cathechism_po/index_new/"
TOC_URL = BASE_URL + "prima-pagina-cic_po.html"

# Expected content page count — used to validate dynamic TOC parsing.
_EXPECTED_PAGE_COUNT = 27

# Regex to match content page filenames (e.g. "p1s1c1_26-49_po.html")
_CONTENT_PAGE_RE = re.compile(r"^(?:prologo|p\d).+_po\.html$")


def parse_toc_for_content_pages(toc_html: str) -> list[str]:
    """Extract content page filenames from the TOC HTML.

    Walks all <a href="..."> links in the TOC, keeps those that match
    the Vatican CCC Portuguese content page naming convention, and
    returns them in document order (deduped).
    """
    soup = BeautifulSoup(toc_html, "lxml")
    seen: set[str] = set()
    pages: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Strip any leading path component (e.g. "./") or full URL prefix
        filename = href.rsplit("/", 1)[-1] if "/" in href else href
        if _CONTENT_PAGE_RE.match(unquote(filename)) and filename not in seen:
            seen.add(filename)
            pages.append(filename)

    if len(pages) < _EXPECTED_PAGE_COUNT:
        logger.warning(
            "TOC parsing found %d pages (expected %d). "
            "Vatican.va TOC structure may have changed.",
            len(pages), _EXPECTED_PAGE_COUNT,
        )

    return pages


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def _fetch(url: str, cache_name: str) -> str:
    """Fetch a URL, caching the result locally."""
    cache_path = CACHE_DIR / cache_name
    if cache_path.exists():
        logger.debug("Cache hit: %s", cache_name)
        return cache_path.read_text(encoding="utf-8")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s", url)
    resp = requests.get(url, timeout=60, headers=_HTTP_HEADERS)
    resp.raise_for_status()
    # Vatican pages use iso-8859-1
    resp.encoding = "iso-8859-1"
    text = resp.text
    cache_path.write_text(text, encoding="utf-8")
    time.sleep(0.5)  # polite delay
    return text


def fetch_pages() -> tuple[dict[str, str], list[str]]:
    """Download TOC and all content pages.

    Returns:
        (pages, content_filenames) where pages is {filename: html}
        and content_filenames is the ordered list of content page names.
    """
    pages: dict[str, str] = {}
    # TOC
    toc_html = _fetch(TOC_URL, "toc.html")
    pages["toc"] = toc_html

    content_filenames = parse_toc_for_content_pages(toc_html)
    logger.info("Discovered %d content pages from TOC", len(content_filenames))

    # Content pages
    for filename in content_filenames:
        url = BASE_URL + filename
        cache_name = unquote(filename)
        pages[filename] = _fetch(url, cache_name)
    return pages, content_filenames


# ---------------------------------------------------------------------------
# TOC parsing
# ---------------------------------------------------------------------------

# Map filename -> structural info for the TOC tree.
# We build a simplified TOC that mirrors the English version's structure.

# The Portuguese CCC has this hierarchy:
#   indent 1: Prologue, Part I, Part II, Part III, Part IV
#   indent 2: Sections within parts
#   indent 3: Chapters within sections
#   indent 4+: Articles, sub-articles

# Structural metadata for known content pages: filename -> (indent, label).
# Used by build_toc() to assign hierarchy levels and labels.
# If a dynamically discovered page is not in this map, it gets a default.
_PAGE_METADATA: dict[str, tuple[int, str]] = {
    "prologo%201-25_po.html": (1, "PRÓLOGO"),
    # Part I
    "p1s1c1_26-49_po.html": (3, "CAPÍTULO PRIMEIRO: O homem é «capaz» de Deus"),
    "p1s1c2_50-141_po.html": (3, "CAPÍTULO SEGUNDO: Deus vem ao encontro do homem"),
    "p1s1c3_142-184_po.html": (3, "CAPÍTULO TERCEIRO: A resposta do homem a Deus"),
    "p1s2_185-197_po.html": (2, "SEGUNDA SECÇÃO: A profissão da fé cristã"),
    "p1s2c1_198-421_po.html": (3, "CAPÍTULO PRIMEIRO: Creio em Deus Pai"),
    "p1s2cap2_422-682_po.html": (3, "CAPÍTULO SEGUNDO: Creio em Jesus Cristo, Filho Único de Deus"),
    "p1s2cap3_683-1065_po.html": (3, "CAPÍTULO TERCEIRO: Creio no Espírito Santo"),
    # Part II
    "p2s1cap1_1066-1075_po.html": (2, "PRIMEIRA SECÇÃO: A Economia sacramental"),
    "p2s1cap1_1076-1134_po.html": (3, "CAPÍTULO PRIMEIRO: O Mistério Pascal no tempo da Igreja"),
    "p2s1cap2_1135-1209_po.html": (3, "CAPÍTULO SEGUNDO: A celebração sacramental do Mistério Pascal"),
    "p2s2cap1_1210-1419_po.html": (3, "CAPÍTULO PRIMEIRO: Os sacramentos da Iniciação cristã"),
    "p2s2cap1_1420-1532_po.html": (3, "CAPÍTULO SEGUNDO: Os sacramentos de Cura"),
    "p2s2cap3_1533-1666_po.html": (3, "CAPÍTULO TERCEIRO: Os sacramentos ao serviço da Comunhão"),
    "p2s2cap4_1667-1690_po.html": (3, "CAPÍTULO QUARTO: Outras celebrações litúrgicas"),
    # Part III
    "p3-intr_1691-1698_po.html": (2, "A vida em Cristo"),
    "p3s1cap1_1699-1876_po.html": (3, "CAPÍTULO PRIMEIRO: A dignidade da pessoa humana"),
    "p3s1cap2_1877-1948_po.html": (3, "CAPÍTULO SEGUNDO: A comunidade humana"),
    "p3s1cap3_1949-2051_po.html": (3, "CAPÍTULO TERCEIRO: A salvação de Deus: a Lei e a Graça"),
    "p3s2-intr_2052-2082_po.html": (2, "SEGUNDA SECÇÃO: Os Dez Mandamentos"),
    "p3s2cap1_2083-2195_po.html": (3, "CAPÍTULO PRIMEIRO: «Amarás o Senhor teu Deus...»"),
    "p3s2cap2_2196-2557_po.html": (3, "CAPÍTULO SEGUNDO: «Amarás o próximo como a ti mesmo»"),
    # Part IV
    "p4-intr_2558-2565_po.html": (2, "A oração cristã"),
    "p4s1cap1_2566-2649_po.html": (3, "CAPÍTULO PRIMEIRO: A revelação da Oração"),
    "p4s1cap2_2650-2696_po.html": (3, "CAPÍTULO SEGUNDO: A tradição da Oração"),
    "p4s1cap3_2697-2758_po.html": (3, "CAPÍTULO TERCEIRO: A vida de Oração"),
    "p4s2_2759-2865_po.html": (2, "SEGUNDA SECÇÃO: A Oração do Senhor: «Pai Nosso»"),
}

# Part-level labels for assigning to paragraphs
_PART_RANGES = [
    (1, 25, "PRÓLOGO"),
    (26, 1065, "PRIMEIRA PARTE: A PROFISSÃO DA FÉ"),
    (1066, 1690, "SEGUNDA PARTE: A CELEBRAÇÃO DO MISTÉRIO CRISTÃO"),
    (1691, 2557, "TERCEIRA PARTE: A VIDA EM CRISTO"),
    (2558, MAX_CCC_PARAGRAPH, "QUARTA PARTE: A ORAÇÃO CRISTÃ"),
]


def _part_for_para(para_num: int) -> str:
    for lo, hi, label in _PART_RANGES:
        if lo <= para_num <= hi:
            return label
    return ""


def _classify_part(filename: str) -> int:
    """Return the part index (0=prologue, 1-4=parts) for a content filename."""
    decoded = unquote(filename).lower()
    if "prologo" in decoded:
        return 0
    for prefix, idx in [("p1", 1), ("p2", 2), ("p3", 3), ("p4", 4)]:
        if decoded.startswith(prefix):
            return idx
    return -1


def build_toc(content_filenames: list[str]) -> tuple[list[dict], dict[str, dict]]:
    """Build toc_link_tree and toc_nodes matching nossbigg schema."""
    toc_link_tree: list[dict] = []
    toc_nodes: dict[str, dict] = {}

    part_labels = [
        "PRÓLOGO",
        "PRIMEIRA PARTE: A PROFISSÃO DA FÉ",
        "SEGUNDA PARTE: A CELEBRAÇÃO DO MISTÉRIO CRISTÃO",
        "TERCEIRA PARTE: A VIDA EM CRISTO",
        "QUARTA PARTE: A ORAÇÃO CRISTÃ",
    ]

    toc_counter = 0

    for part_idx, part_label in enumerate(part_labels):
        toc_counter += 1
        part_toc_id = f"toc-{toc_counter}"
        part_node: dict = {"id": part_toc_id, "children": []}
        toc_nodes[part_toc_id] = {
            "id": part_toc_id,
            "indent_level": 1,
            "text": part_label,
            "link": "",
        }

        pages_for_part = [f for f in content_filenames if _classify_part(f) == part_idx]

        for filename in pages_for_part:
            indent, label = _PAGE_METADATA.get(filename, (3, unquote(filename)))
            toc_counter += 1
            child_toc_id = f"toc-{toc_counter}"
            child_node: dict = {"id": child_toc_id, "children": []}
            toc_nodes[child_toc_id] = {
                "id": child_toc_id,
                "indent_level": indent,
                "text": label,
                "link": BASE_URL + filename,
            }
            part_node["children"].append(child_node)

        toc_link_tree.append(part_node)

    return toc_link_tree, toc_nodes


def _toc_id_for_file(filename: str, toc_nodes: dict[str, dict]) -> str:
    """Find the toc_id for a given content page filename."""
    url_suffix = BASE_URL + filename
    for toc_id, node in toc_nodes.items():
        if node.get("link") == url_suffix:
            return toc_id
    # Fallback: for prologue which has indent=1
    for toc_id, node in toc_nodes.items():
        if node.get("text") == "PRÓLOGO" and node.get("indent_level") == 1:
            return toc_id
    return "toc-1"


# ---------------------------------------------------------------------------
# HTML content parsing
# ---------------------------------------------------------------------------

_PARA_START_RE = re.compile(r"^\s*(\d{1,4})\s*\.\s*")
_PARA_START_NO_DOT_RE = re.compile(r"^\s*(\d{1,4})\s*$")  # Bold number without period
_FOOTNOTE_REF_RE = re.compile(r"\((\d{1,3})\)")
_FOOTNOTE_DEF_RE = re.compile(r"^\s*(\d{1,3})\s*[.\s]\s*")


def _get_content_td(soup: BeautifulSoup) -> Tag | None:
    """Find the main content <td> in the Vatican table layout."""
    # The content is in a <td width="609"> in the second table
    for td in soup.find_all("td", attrs={"width": "609"}):
        return td
    # Fallback: find the largest td
    tds = soup.find_all("td")
    best = None
    best_len = 0
    for td in tds:
        text_len = len(td.get_text())
        if text_len > best_len:
            best = td
            best_len = text_len
    return best


def _extract_text_with_attrs(el: Tag | NavigableString) -> list[dict]:
    """Recursively extract text elements preserving bold/italic attributes."""
    if isinstance(el, NavigableString):
        text = str(el)
        if text.strip():
            return [{"type": "text", "text": text}]
        return []

    results: list[dict] = []
    is_bold = el.name == "b" or el.name == "strong"
    is_italic = el.name == "i" or el.name == "em"

    for child in el.children:
        child_texts = _extract_text_with_attrs(child)
        for ct in child_texts:
            if ct["type"] == "text":
                attrs: dict = {}
                if is_bold:
                    attrs["b"] = True
                if is_italic:
                    attrs["i"] = True
                if attrs:
                    ct.setdefault("attrs", {}).update(attrs)
            results.append(ct)

    return results


def _parse_footnotes_after_hr(container: Tag) -> dict[str, dict]:
    """Parse footnote definitions from elements after the first <hr>."""
    # Build document-order index
    order: dict[int, int] = {}
    for seq, el in enumerate(container.descendants):
        order[id(el)] = seq

    hr = container.find("hr")
    if hr is None:
        return {}

    hr_seq = order[id(hr)]

    footnotes: dict[str, dict] = {}
    for p in container.find_all("p"):
        p_seq = order.get(id(p), 0)
        if p_seq <= hr_seq:
            continue
        text = p.get_text().strip()
        if not text:
            continue
        m = _FOOTNOTE_DEF_RE.match(text)
        if m:
            fn_num = int(m.group(1))
            fn_text = text[m.end():].strip()
            fn_text = re.sub(r"^[.\s]+|[.\s]+$", "", fn_text)
            if fn_text:
                refs = [{"text": ref.strip(), "link": ""} for ref in fn_text.split(";") if ref.strip()]
                footnotes[str(fn_num)] = {"number": fn_num, "refs": refs}

    return footnotes


def parse_page(html: str, page_filename: str) -> tuple[list[dict], dict[str, dict]]:
    """Parse a content page into paragraphs and footnotes.

    Uses a linear walk through the full text to detect paragraph boundaries
    (``N.`` bold or plain-text patterns) rather than relying on ``<p>`` tag
    structure, because the Vatican HTML often places paragraphs outside
    ``<p>`` tags (e.g. bare ``<b>17. </b>text``).

    Returns:
        paragraphs: list of {"elements": [...]} dicts
        footnotes: {str(number): {"number": N, "refs": [{"text": "...", "link": ""}]}}
    """
    soup = BeautifulSoup(html, "lxml")
    content_td = _get_content_td(soup)
    if content_td is None:
        logger.warning("No content td found in %s", page_filename)
        return [], {}

    footnotes = _parse_footnotes_after_hr(content_td)
    valid_fn_nums = set(int(k) for k in footnotes.keys())

    # Get the full text content before <hr>, splitting into paragraphs
    # by detecting the "N." pattern.
    # We use get_text() on the content area (before hr) to get a flat string,
    # then use regex to split on paragraph numbers.

    # First, remove the footnote section (after <hr>) from the tree
    hr = content_td.find("hr")
    if hr:
        # Remove everything after <hr>
        for sibling in list(hr.next_siblings):
            sibling.extract()
        hr.extract()

    # Now extract text, walking the tree to detect paragraph numbers
    # We iterate all <p> and bare text/bold in document order
    paragraphs: list[dict] = []
    current_elements: list[dict] = []
    seen_p_ids: set[int] = set()  # Avoid processing nested <p> twice

    def _flush() -> None:
        nonlocal current_elements
        if current_elements:
            paragraphs.append({"elements": list(current_elements)})
            current_elements = []

    def _walk(node: Tag) -> None:
        nonlocal current_elements
        for child in node.children:
            if isinstance(child, NavigableString):
                text = str(child)
                if text.strip():
                    _split_footnote_refs(text, current_elements, valid_fn_nums)
                continue

            if not isinstance(child, Tag):
                continue

            if child.name == "hr":
                break

            # Handle <p> tags
            if child.name == "p":
                if id(child) in seen_p_ids:
                    continue
                seen_p_ids.add(id(child))
                _process_content_p(child, current_elements, valid_fn_nums, paragraphs)

            # Handle <blockquote>
            elif child.name == "blockquote":
                inner_ps = child.find_all("p")
                if inner_ps:
                    for ip in inner_ps:
                        if id(ip) not in seen_p_ids:
                            seen_p_ids.add(id(ip))
                            _process_content_p(ip, current_elements, valid_fn_nums, paragraphs)
                else:
                    _process_content_p(child, current_elements, valid_fn_nums, paragraphs)

            # Handle bare <b> that may start a paragraph
            elif child.name == "b":
                bold_text = child.get_text().strip()
                m = _PARA_START_RE.match(bold_text)
                if not m:
                    m = _PARA_START_NO_DOT_RE.match(bold_text)
                if m:
                    para_num = int(m.group(1))
                    if 1 <= para_num <= MAX_CCC_PARAGRAPH:
                        _flush()
                        current_elements.append({"type": "ref-ccc", "ref_number": para_num})
                        remainder = bold_text[m.end():].strip()
                        if remainder:
                            current_elements.append({"type": "text", "text": remainder, "attrs": {"b": True}})
                else:
                    # Non-paragraph bold text
                    if bold_text:
                        current_elements.append({"type": "text", "text": bold_text, "attrs": {"b": True}})

            # Handle wrapper tags (font, div, span, etc.) — recurse
            elif child.name in ("font", "div", "span", "center", "table", "tbody", "tr", "td"):
                _walk(child)

            # Handle <i>, <em>, <a>, <u>, <sup> — inline formatting
            elif child.name in ("i", "em", "a", "u", "sup", "strong"):
                text = child.get_text()
                if text.strip():
                    _split_footnote_refs(text, current_elements, valid_fn_nums)

    _walk(content_td)
    _flush()

    return paragraphs, footnotes


def _process_content_p(
    p_tag: Tag,
    current_elements: list[dict],
    valid_fn_nums: set[int],
    paragraphs: list[dict],
) -> None:
    """Process a single <p> tag, detecting paragraph starts and footnote refs."""
    is_header = p_tag.get("align") == "center"
    full_text = p_tag.get_text().strip()

    if not full_text:
        return

    # Strategy 1: Bold paragraph number — <b>N.</b>, <b>N. text</b>, or <b>N</b>
    first_bold = p_tag.find("b")
    if first_bold and not is_header:
        bold_text = first_bold.get_text().strip()
        m = _PARA_START_RE.match(bold_text)
        if not m:
            m = _PARA_START_NO_DOT_RE.match(bold_text)
        if m:
            para_num = int(m.group(1))

            # Flush previous
            if current_elements:
                paragraphs.append({"elements": list(current_elements)})

            current_elements.clear()
            current_elements.append({"type": "ref-ccc", "ref_number": para_num})

            # Get rest of bold text after number
            remainder = bold_text[m.end():].strip()
            if remainder:
                current_elements.append({"type": "text", "text": remainder, "attrs": {"b": True}})

            # Process siblings after the first bold
            started = False
            for child in p_tag.children:
                if child is first_bold:
                    started = True
                    continue
                if not started:
                    continue
                _add_text_and_refs(child, current_elements, valid_fn_nums)
            return

    # Strategy 2: Plain text paragraph number — "N. text" without bold
    if not is_header:
        m = _PARA_START_RE.match(full_text)
        if m:
            para_num = int(m.group(1))
            # Sanity: CCC paragraphs are 1-2865
            if 1 <= para_num <= MAX_CCC_PARAGRAPH:
                if current_elements:
                    paragraphs.append({"elements": list(current_elements)})

                current_elements.clear()
                current_elements.append({"type": "ref-ccc", "ref_number": para_num})

                # Process children for text + footnote refs, skipping the leading number
                # We extract the full text and strip the leading "N. "
                _add_p_content_after_number(p_tag, m.end(), current_elements, valid_fn_nums)
                return

    # Not a paragraph start — header or continuation text
    if is_header:
        if full_text:
            current_elements.append({
                "type": "text",
                "text": full_text,
                "attrs": {"b": True, "heavy_header": True},
            })
        return

    # Regular continuation
    for child in p_tag.children:
        _add_text_and_refs(child, current_elements, valid_fn_nums)


def _add_p_content_after_number(
    p_tag: Tag,
    skip_chars: int,
    elements: list[dict],
    valid_fn_nums: set[int],
) -> None:
    """Add text content from a <p> tag, skipping the first skip_chars of text.

    Used when the paragraph number (e.g. "143. ") was detected in plain text
    (not wrapped in <b>), so we need to skip past it in the child nodes.
    """
    chars_skipped = 0
    for child in p_tag.children:
        if isinstance(child, NavigableString):
            text = str(child)
            remaining = skip_chars - chars_skipped
            if remaining > 0:
                if len(text) <= remaining:
                    chars_skipped += len(text)
                    continue
                else:
                    text = text[remaining:]
                    chars_skipped = skip_chars
            if text:
                _split_footnote_refs(text, elements, valid_fn_nums)
        elif isinstance(child, Tag):
            child_text_len = len(child.get_text())
            remaining = skip_chars - chars_skipped
            if remaining > 0 and child_text_len <= remaining:
                chars_skipped += child_text_len
                continue
            elif remaining > 0:
                # Partial skip inside this tag — just add whatever we get
                chars_skipped = skip_chars
            _add_text_and_refs(child, elements, valid_fn_nums)


def _add_text_and_refs(
    node: Tag | NavigableString,
    elements: list[dict],
    valid_fn_nums: set[int],
) -> None:
    """Add text and footnote references from a node to elements list."""
    if isinstance(node, NavigableString):
        text = str(node)
        if not text.strip():
            if text and elements:
                # Preserve whitespace between elements
                elements.append({"type": "text", "text": " "})
            return
        # Look for footnote references like (1), (23)
        _split_footnote_refs(text, elements, valid_fn_nums)
        return

    if isinstance(node, Tag):
        if node.name in ("b", "strong", "i", "em", "font", "span", "a", "u", "sup"):
            # Inline formatting — process children
            for child in node.children:
                _add_text_and_refs(child, elements, valid_fn_nums)
        elif node.name == "br":
            elements.append({"type": "text", "text": " "})
        elif node.name == "blockquote":
            for child in node.children:
                _add_text_and_refs(child, elements, valid_fn_nums)
        elif node.name == "p":
            for child in node.children:
                _add_text_and_refs(child, elements, valid_fn_nums)
        else:
            text = node.get_text()
            if text.strip():
                _split_footnote_refs(text, elements, valid_fn_nums)


def _split_footnote_refs(
    text: str,
    elements: list[dict],
    valid_fn_nums: set[int],
) -> None:
    """Split text on footnote references (N) and emit text + ref elements."""
    parts = _FOOTNOTE_REF_RE.split(text)
    # parts alternates: text, captured_number, text, captured_number, ...
    for i, part in enumerate(parts):
        if i % 2 == 0:
            # Text segment
            if part:
                elements.append({"type": "text", "text": part})
        else:
            # Potential footnote number
            num = int(part)
            if num in valid_fn_nums:
                elements.append({"type": "ref", "number": num})
            else:
                # Not a real footnote ref — put back as text
                elements.append({"type": "text", "text": f"({part})"})


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run() -> Path:
    """Scrape Portuguese CCC and write data/raw/ccc-pt.json."""
    logger.info("Fetching Portuguese CCC pages...")
    pages, content_filenames = fetch_pages()

    logger.info("Building TOC...")
    toc_link_tree, toc_nodes = build_toc(content_filenames)

    logger.info("Parsing %d content pages...", len(content_filenames))
    page_nodes: dict[str, dict] = {}

    for filename in content_filenames:
        html = pages[filename]
        toc_id = _toc_id_for_file(filename, toc_nodes)
        paragraphs, footnotes = parse_page(html, filename)
        page_nodes[toc_id] = {
            "id": toc_id,
            "paragraphs": paragraphs,
            "footnotes": footnotes,
        }
        para_count = sum(
            1 for p in paragraphs
            for el in p["elements"]
            if el.get("type") == "ref-ccc"
        )
        logger.debug("  %s -> toc_id=%s, paragraphs=%d, footnotes=%d",
                      filename, toc_id, para_count, len(footnotes))

    # Count total paragraphs
    total = sum(
        1 for pn in page_nodes.values()
        for p in pn["paragraphs"]
        for el in p["elements"]
        if el.get("type") == "ref-ccc"
    )
    logger.info("Total CCC paragraphs found: %d", total)

    # Build output
    output = {
        "toc_link_tree": toc_link_tree,
        "toc_nodes": toc_nodes,
        "page_nodes": page_nodes,
        "ccc_refs": {},
        "meta": {
            "language": "pt",
            "source": "vatican.va",
        },
    }

    out_path = RAW_DATA_DIR / "ccc-pt.json"
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=None)
    logger.info("Wrote %s (%.1f MB)", out_path, out_path.stat().st_size / 1e6)

    return out_path
