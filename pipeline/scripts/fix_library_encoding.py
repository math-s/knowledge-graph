"""Re-parse library HTML files that were ingested with wrong encoding.

New Advent library files declare charset=utf-8 but contain Windows-1252 bytes
(e.g. 0x96 = en-dash).  The original read used errors='replace', inserting
U+FFFD for every Windows-1252 char.

This script re-reads the affected HTML files, updates library_docs.text, and
re-derives the document_sections rows using the same paragraph-split logic as
migrate_library_docs_to_documents.py.

Usage:
    python pipeline/scripts/fix_library_encoding.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

from bs4 import BeautifulSoup

from pipeline.scripts.export_corpus_jsonl import sync as sync_corpus

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"
LIBRARY_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "newadvent" / "library"

REPL = "�"
PARAGRAPH_SPLIT_RE = re.compile(r"(?<=[\s.])(\d{1,3})\.\s+(?=[A-Z])")
MIN_SECTION_LEN = 40


def _read_html(path: Path) -> str:
    raw = path.read_bytes()
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("windows-1252")


def _parse_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", id="springfield2")
    if not content:
        return ""
    about = content.find(["h2", "h3"], string=re.compile(r"About this page", re.IGNORECASE))
    if about:
        for sib in list(about.next_siblings):
            if hasattr(sib, "decompose"):
                sib.decompose()
        about.decompose()
    for tag in content.find_all(["script", "style"]):
        tag.decompose()
    for div in content.find_all("div", class_=re.compile(r"catholicadnet")):
        div.decompose()
    return re.sub(r"\s+", " ", content.get_text(separator=" ", strip=True))


def _parse_sections(text: str) -> list[tuple[str, str]]:
    """Mirrors migrate_library_docs_to_documents._parse_sections exactly."""
    matches = list(PARAGRAPH_SPLIT_RE.finditer(text))
    if len(matches) < 3:
        return [("text", text.strip())]

    sections: list[tuple[str, str]] = []
    preamble = text[:matches[0].start()].strip()
    if preamble and len(preamble) >= MIN_SECTION_LEN:
        sections.append(("preamble", preamble))

    for i, m in enumerate(matches):
        num = m.group(1)
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[body_start:body_end].strip()
        if body and len(body) >= MIN_SECTION_LEN:
            idx = next((j for j, (n, _) in enumerate(sections) if n == num), None)
            if idx is not None:
                sections[idx] = (num, sections[idx][1] + "\n\n" + body)
            else:
                sections.append((num, body))
    return sections


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        affected_docs = [
            row["document_id"]
            for row in conn.execute(
                "SELECT DISTINCT document_id FROM document_sections "
                "WHERE text_en LIKE '%' || char(65533) || '%'"
            ).fetchall()
        ]
        print(f"Affected documents: {affected_docs}")

        for doc_id in affected_docs:
            doc_row = conn.execute(
                "SELECT source_url FROM documents WHERE id=?", (doc_id,)
            ).fetchone()
            if not doc_row:
                print(f"  {doc_id}: not in documents table — skipping")
                continue

            lib_key = doc_row["source_url"].rstrip("/").rsplit("/", 1)[-1]
            html_path = LIBRARY_DIR / f"docs_{lib_key}.htm"
            if not html_path.exists():
                print(f"  {doc_id}: HTML not found at {html_path} — skipping")
                continue

            html = _read_html(html_path)
            new_text = _parse_text(html)

            if REPL in new_text:
                print(f"  {doc_id}: STILL has replacement chars after re-decode — check file")
                continue

            new_sections = _parse_sections(new_text)
            new_map = {num: body for num, body in new_sections}

            # How many rows need updating?
            affected_rows = conn.execute(
                "SELECT section_num FROM document_sections "
                "WHERE document_id=? AND text_en LIKE '%' || char(65533) || '%'",
                (doc_id,),
            ).fetchall()
            print(f"  {doc_id}: {len(new_sections)} new sections, {len(affected_rows)} rows with replacement chars")

            if args.dry_run:
                for sec_row in affected_rows[:2]:
                    sec_num = sec_row["section_num"]
                    old = conn.execute(
                        "SELECT text_en FROM document_sections WHERE document_id=? AND section_num=?",
                        (doc_id, sec_num),
                    ).fetchone()["text_en"]
                    new_body = new_map.get(sec_num, "(section not found in re-parse)")
                    # Show just around the replacement chars
                    old_ctx = old[:200].replace(REPL, "[?]")
                    new_ctx = new_body[:200] if new_body else "(missing)"
                    print(f"    [{sec_num}] before: {old_ctx!r}")
                    print(f"    [{sec_num}] after:  {new_ctx!r}")
                continue

            # Update library_docs with clean text
            conn.execute(
                "UPDATE library_docs SET text=? WHERE id=?",
                (new_text, f"docs_{lib_key}"),
            )

            # Update all document_sections for this doc using re-parsed sections
            updates = 0
            for sec_num, new_body in new_sections:
                conn.execute(
                    "UPDATE document_sections SET text_en=? "
                    "WHERE document_id=? AND section_num=?",
                    (new_body, doc_id, sec_num),
                )
                updates += 1
            print(f"    Updated {updates} sections")

        if not args.dry_run:
            conn.commit()
            sync_corpus("library")
            for doc_id in affected_docs:
                sync_corpus("documents", doc_id=doc_id)
            print(f"synced corpus JSONL (library + {len(affected_docs)} document(s))")
            (remaining,) = conn.execute(
                "SELECT COUNT(*) FROM document_sections "
                "WHERE text_en LIKE '%' || char(65533) || '%'"
            ).fetchone()
            print(f"Remaining replacement-char rows: {remaining}")
        else:
            print("[dry-run] No changes written.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
