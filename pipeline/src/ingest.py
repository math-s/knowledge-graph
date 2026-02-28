"""Download and parse CCC JSON data from nossbigg/catechism-ccc-json v0.0.2."""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from pathlib import Path

import requests

from .models import Paragraph, StructuralNode

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

RELEASE_URL = "https://github.com/nossbigg/catechism-ccc-json/releases/download/v0.0.2/ccc.json"


def download_raw_data() -> Path:
    """Download CCC v0.0.2 release JSON to data/raw/."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DATA_DIR / "ccc.json"

    if path.exists():
        logger.info("Already downloaded: %s", path.name)
        return path

    logger.info("Downloading %s ...", RELEASE_URL)
    resp = requests.get(RELEASE_URL, timeout=60)
    resp.raise_for_status()
    path.write_bytes(resp.content)
    logger.info("Saved to %s (%d bytes)", path, len(resp.content))

    return path


def _extract_paragraph_text(elements: list[dict]) -> str:
    """Extract plain text from a paragraph's elements."""
    parts = []
    for el in elements:
        if el.get("type") == "text":
            parts.append(el.get("text", ""))
    return " ".join(parts).strip()


def _extract_footnote_numbers(elements: list[dict]) -> list[int]:
    """Extract footnote reference numbers from a paragraph's elements."""
    refs = []
    for el in elements:
        if el.get("type") == "ref":
            num = el.get("number")
            if num is not None:
                refs.append(int(num))
    return refs


def _build_hierarchy(toc_link_tree: list[dict], toc_nodes: dict) -> tuple[list[StructuralNode], dict[str, str]]:
    """Build structural hierarchy from TOC tree.

    Returns:
        - List of StructuralNode objects
        - Dict mapping toc_id -> structural_node_id (for paragraph assignment)
    """
    structures: list[StructuralNode] = []
    toc_to_struct: dict[str, str] = {}  # toc-X -> struct:part-1-section-2-...

    # The CCC has: Prologue, Part 1-4. Each part has sections, chapters, articles.
    # toc_link_tree has 5 top-level items: Prologue + 4 Parts
    # indent_level: 1=Part, 2=Section, 3=Chapter, 4+=Article/SubArticle

    level_names = {1: "part", 2: "section", 3: "chapter", 4: "article"}

    def walk(node: dict, parent_id: str | None, path: list[str]) -> None:
        toc_id = node["id"]
        toc_data = toc_nodes.get(toc_id, {})
        text = toc_data.get("text", "")
        indent = toc_data.get("indent_level", 1)
        level = level_names.get(indent, "article")

        # Build a clean structural ID
        slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:50]
        struct_id = f"{'-'.join(path)}-{slug}" if path else slug
        struct_id = struct_id.strip("-")

        structures.append(StructuralNode(
            id=struct_id,
            label=text,
            level=level,
            parent_id=parent_id,
        ))
        toc_to_struct[toc_id] = struct_id

        for child in node.get("children", []):
            walk(child, struct_id, path + [slug])

    for top_node in toc_link_tree:
        walk(top_node, None, [])

    return structures, toc_to_struct


def _extract_cross_references(
    page_nodes: dict,
    all_para_nums: set[int],
) -> dict[int, set[int]]:
    """Extract cross-references between CCC paragraphs.

    Two methods:
    1. Direct footnote number refs: footnotes containing bare CCC paragraph numbers
    2. Shared citations: paragraphs citing the same source
    """
    cross_refs: dict[int, set[int]] = defaultdict(set)

    # Track paragraph -> footnote citations for shared citation analysis
    para_citations: dict[int, set[str]] = defaultdict(set)

    for page_id, pn in page_nodes.items():
        footnotes = pn.get("footnotes", {})

        # Build footnote lookup: fn_number -> (citations, ccc_refs)
        fn_citations: dict[int, list[str]] = {}
        fn_ccc_refs: dict[int, list[int]] = {}
        for fn_key, fn in footnotes.items():
            fn_num = int(fn_key)
            citations = []
            ccc_targets = []
            for ref in fn.get("refs", []):
                text = ref.get("text", "").strip()
                if not text:
                    continue
                # Check if this is a bare CCC paragraph number
                if re.match(r"^\d{1,4}$", text):
                    target = int(text)
                    if target in all_para_nums:
                        ccc_targets.append(target)
                else:
                    # Normalize citation for shared-citation matching
                    normalized = re.sub(r"\s+", " ", text).strip()
                    if normalized and normalized != "etc.":
                        citations.append(normalized)
            fn_citations[fn_num] = citations
            fn_ccc_refs[fn_num] = ccc_targets

        # Walk paragraphs and assign footnotes to CCC numbers
        current_para: int | None = None
        for p in pn.get("paragraphs", []):
            for el in p.get("elements", []):
                if el.get("type") == "ref-ccc":
                    current_para = el["ref_number"]
                elif el.get("type") == "ref" and current_para is not None:
                    fn_num = el.get("number")
                    if fn_num is not None:
                        fn_num = int(fn_num)
                        # Method 1: Direct CCC paragraph refs in footnotes
                        for target in fn_ccc_refs.get(fn_num, []):
                            if target != current_para:
                                cross_refs[current_para].add(target)
                                cross_refs[target].add(current_para)
                        # Collect citations for method 2
                        for citation in fn_citations.get(fn_num, []):
                            para_citations[current_para].add(citation)

    # Method 2: Shared citation analysis
    citation_to_paras: dict[str, set[int]] = defaultdict(set)
    for para_id, citations in para_citations.items():
        for citation in citations:
            citation_to_paras[citation].add(para_id)

    for citation, paras in citation_to_paras.items():
        # Only connect paragraphs sharing specific citations (2-12 paragraphs)
        if 2 <= len(paras) <= 12:
            paras_list = sorted(paras)
            for i in range(len(paras_list)):
                for j in range(i + 1, len(paras_list)):
                    cross_refs[paras_list[i]].add(paras_list[j])
                    cross_refs[paras_list[j]].add(paras_list[i])

    return dict(cross_refs)


def parse_ccc(raw_path: Path) -> tuple[list[Paragraph], list[StructuralNode]]:
    """Parse the CCC JSON into Pydantic models."""
    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    toc_link_tree = data["toc_link_tree"]
    toc_nodes = data["toc_nodes"]
    page_nodes = data["page_nodes"]

    # 1. Build structural hierarchy
    structures, toc_to_struct = _build_hierarchy(toc_link_tree, toc_nodes)
    logger.info("Built %d structural nodes", len(structures))

    # 2. Extract all paragraphs with text
    all_para_nums: set[int] = set()
    raw_paragraphs: dict[int, dict] = {}  # para_id -> {text, footnote_nums, toc_id}

    for toc_id, pn in page_nodes.items():
        footnotes = pn.get("footnotes", {})

        # Build footnote text lookup for this page
        fn_text_map: dict[int, str] = {}
        for fn_key, fn in footnotes.items():
            texts = []
            for ref in fn.get("refs", []):
                t = ref.get("text", "").strip()
                if t:
                    texts.append(t)
            fn_text_map[int(fn_key)] = "; ".join(texts)

        current_para: int | None = None
        current_text_parts: list[str] = []
        current_fn_nums: list[int] = []

        def _flush():
            nonlocal current_para, current_text_parts, current_fn_nums
            if current_para is not None:
                text = " ".join(current_text_parts).strip()
                # Clean up whitespace
                text = re.sub(r"\s+", " ", text)
                raw_paragraphs[current_para] = {
                    "text": text,
                    "footnote_nums": current_fn_nums[:],
                    "toc_id": toc_id,
                }
                all_para_nums.add(current_para)
            current_para = None
            current_text_parts = []
            current_fn_nums = []

        for p in pn.get("paragraphs", []):
            for el in p.get("elements", []):
                el_type = el.get("type")
                if el_type == "ref-ccc":
                    _flush()
                    current_para = el["ref_number"]
                elif el_type == "text" and current_para is not None:
                    current_text_parts.append(el.get("text", ""))
                elif el_type == "ref" and current_para is not None:
                    fn_num = el.get("number")
                    if fn_num is not None:
                        current_fn_nums.append(int(fn_num))

        _flush()

    logger.info("Extracted %d paragraphs", len(raw_paragraphs))

    # 3. Extract cross-references
    cross_ref_map = _extract_cross_references(page_nodes, all_para_nums)
    total_refs = sum(len(refs) for refs in cross_ref_map.values()) // 2  # undirected
    logger.info("Extracted %d cross-reference edges", total_refs)

    # 4. Map paragraphs to structural position
    # Walk toc tree to determine Part/Section/Chapter/Article for each toc_id
    toc_hierarchy: dict[str, dict[str, str]] = {}  # toc_id -> {part, section, chapter, article}

    def _walk_hierarchy(node: dict, context: dict[str, str]) -> None:
        toc_id = node["id"]
        toc_data = toc_nodes.get(toc_id, {})
        text = toc_data.get("text", "")
        indent = toc_data.get("indent_level", 1)

        ctx = context.copy()
        if indent == 1:
            ctx["part"] = text
            ctx["section"] = ""
            ctx["chapter"] = ""
            ctx["article"] = ""
        elif indent == 2:
            ctx["section"] = text
            ctx["chapter"] = ""
            ctx["article"] = ""
        elif indent == 3:
            ctx["chapter"] = text
            ctx["article"] = ""
        elif indent >= 4:
            ctx["article"] = text

        toc_hierarchy[toc_id] = ctx

        for child in node.get("children", []):
            _walk_hierarchy(child, ctx)

    for top_node in toc_link_tree:
        _walk_hierarchy(top_node, {"part": "", "section": "", "chapter": "", "article": ""})

    # Also map toc_ids that are directly in page_nodes but not in the tree
    # (some pages map to leaf toc nodes)
    # Propagate from parent
    for toc_id in page_nodes:
        if toc_id not in toc_hierarchy:
            toc_hierarchy[toc_id] = {"part": "", "section": "", "chapter": "", "article": ""}

    # 5. Build final Paragraph objects
    paragraphs: list[Paragraph] = []
    for para_id in sorted(raw_paragraphs.keys()):
        raw = raw_paragraphs[para_id]
        toc_id = raw["toc_id"]
        hier = toc_hierarchy.get(toc_id, {})

        # Resolve footnote texts
        pn = page_nodes.get(toc_id, {})
        footnotes_dict = pn.get("footnotes", {})
        footnote_texts = []
        for fn_num in raw["footnote_nums"]:
            fn = footnotes_dict.get(str(fn_num), {})
            texts = []
            for ref in fn.get("refs", []):
                t = ref.get("text", "").strip()
                if t:
                    texts.append(t)
            if texts:
                footnote_texts.append("; ".join(texts))

        paragraphs.append(Paragraph(
            id=para_id,
            text=raw["text"],
            cross_references=sorted(cross_ref_map.get(para_id, set())),
            footnotes=footnote_texts,
            part=hier.get("part", ""),
            section=hier.get("section", ""),
            chapter=hier.get("chapter", ""),
            article=hier.get("article", ""),
        ))

    # 6. Assign paragraph_ids to structural nodes
    struct_map = {s.id: s for s in structures}
    for para in paragraphs:
        toc_id = raw_paragraphs[para.id]["toc_id"]
        struct_id = toc_to_struct.get(toc_id)
        if struct_id and struct_id in struct_map:
            struct_map[struct_id].paragraph_ids.append(para.id)

    return paragraphs, structures


def save_processed(paragraphs: list[Paragraph], structures: list[StructuralNode]) -> None:
    """Save processed data to JSON files."""
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    paragraphs_out = PROCESSED_DATA_DIR / "paragraphs.json"
    with open(paragraphs_out, "w", encoding="utf-8") as f:
        json.dump([p.model_dump() for p in paragraphs], f, ensure_ascii=False)
    logger.info("Saved %d paragraphs to %s", len(paragraphs), paragraphs_out)

    structures_out = PROCESSED_DATA_DIR / "structure.json"
    with open(structures_out, "w", encoding="utf-8") as f:
        json.dump([s.model_dump() for s in structures], f, ensure_ascii=False)
    logger.info("Saved %d structural nodes to %s", len(structures), structures_out)


def run(lang: str = "en") -> tuple[list[Paragraph], list[StructuralNode]]:
    """Run the full ingestion pipeline.

    Args:
        lang: "en" for English (downloads from nossbigg), "pt" for Portuguese
              (reads from data/raw/ccc-pt.json, must be scraped first).
    """
    if lang == "pt":
        raw_path = RAW_DATA_DIR / "ccc-pt.json"
        if not raw_path.exists():
            raise FileNotFoundError(
                f"{raw_path} not found. Run the Portuguese scraper first: "
                "python pipeline/scripts/run_scraper_pt.py"
            )
    else:
        raw_path = download_raw_data()

    paragraphs, structures = parse_ccc(raw_path)

    if lang == "en":
        save_processed(paragraphs, structures)

    return paragraphs, structures
