"""Parse Latin CCC HTML files and populate paragraphs.text_la.

Source: 115 HTML files from Vatican.va at pipeline/data/raw/ccc/la/
Encoding: iso-8859-1

Usage:
    python -m pipeline.src.load_ccc_la [--dry-run]
"""

from __future__ import annotations

import html
import re
import sqlite3
from pathlib import Path

import click
from bs4 import BeautifulSoup, NavigableString, Tag

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw" / "ccc" / "la"
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"


def is_content_file(filename: str) -> bool:
    """Check if HTML file contains CCC content (not index/metadata)."""
    # Content pages match pattern page_p*.html or page_prologo*.html
    return filename.startswith("page_p") or filename.startswith("page_prologo")


def extract_paragraph_number(tag: Tag) -> int | None:
    """Extract paragraph number from bold tag if it matches pattern."""
    if tag.name != "b":
        return None

    text = tag.get_text().strip()
    # Match paragraph numbers: 1-4 digits, optionally followed by whitespace
    match = re.match(r"^(\d{1,4})\s*$", text)
    if match:
        return int(match.group(1))
    return None


def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    # Decode HTML entities (like &laquo;, &raquo;, etc.)
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_paragraphs_from_html(file_path: Path) -> dict[int, str]:
    """Parse a single HTML file and extract {paragraph_id: latin_text} pairs.

    The HTML structure:
    - Content is in a <td> tag with width="99%"
    - Paragraph numbers appear as <b>456</b> or <b>456 </b>
    - Text continues until the next paragraph number or <hr> (footnotes)
    - Footnotes after <hr> should be excluded
    - <blockquote> sections belong to the current paragraph
    - Superscript footnote references (<sup>80</sup>) should be stripped
    """
    with open(file_path, "r", encoding="iso-8859-1") as f:
        soup = BeautifulSoup(f.read(), "lxml")

    # Find the main content td (usually has width="99%")
    content_td = soup.find("td", {"width": "99%"})
    if not content_td:
        return {}

    paragraphs: dict[int, str] = {}
    current_para_id: int | None = None
    current_text_parts: list[str] = []

    # Track if we've hit the footnotes section
    in_footnotes = False

    def flush_current_paragraph():
        """Save the current paragraph if we have one."""
        nonlocal current_para_id, current_text_parts
        if current_para_id is not None and current_text_parts:
            text = " ".join(current_text_parts)
            text = clean_text(text)
            if text:
                paragraphs[current_para_id] = text
        current_text_parts = []

    def extract_text_recursive(element) -> str:
        """Recursively extract text from an element, excluding <sup> tags."""
        if isinstance(element, NavigableString):
            return str(element)

        if isinstance(element, Tag):
            # Skip superscript footnote references
            if element.name == "sup":
                return ""

            # Recursively get text from children
            parts = []
            for child in element.children:
                text = extract_text_recursive(child)
                if text:
                    parts.append(text)
            return "".join(parts)

        return ""

    # Walk through all elements in the content td
    for element in content_td.descendants:
        # Stop at <hr> - footnotes section begins
        if isinstance(element, Tag) and element.name == "hr":
            in_footnotes = True
            flush_current_paragraph()
            break

        if in_footnotes:
            continue

        # Check if this is a paragraph number (bold tag)
        if isinstance(element, Tag) and element.name == "b":
            para_num = extract_paragraph_number(element)
            if para_num is not None:
                # Flush previous paragraph before starting new one
                flush_current_paragraph()
                current_para_id = para_num
                continue

        # Collect text content if we're in a paragraph
        if current_para_id is not None:
            # Only process direct text nodes and certain containers
            if isinstance(element, NavigableString):
                text = str(element).strip()
                if text and text not in ["", " "]:
                    # Make sure this text isn't inside a bold tag (para number)
                    parent = element.parent
                    if parent and parent.name == "b":
                        # Check if parent is a paragraph number
                        if extract_paragraph_number(parent) is not None:
                            continue
                    current_text_parts.append(text)
            elif isinstance(element, Tag):
                # For blockquotes and other containers, extract their text
                # but avoid double-processing (we'll get it via NavigableString)
                if element.name in ["blockquote", "p", "i"]:
                    # Skip - we'll get the text via NavigableString traversal
                    pass

    # Flush the last paragraph
    flush_current_paragraph()

    return paragraphs


def parse_all_html_files(data_dir: Path) -> dict[int, str]:
    """Parse all content HTML files and return combined paragraph dictionary."""
    all_paragraphs: dict[int, str] = {}

    html_files = sorted(data_dir.glob("*.html"))
    content_files = [f for f in html_files if is_content_file(f.name)]

    click.echo(f"Found {len(html_files)} HTML files, {len(content_files)} content files")

    for file_path in content_files:
        paras = extract_paragraphs_from_html(file_path)
        if paras:
            click.echo(f"  {file_path.name}: {len(paras)} paragraphs")
            all_paragraphs.update(paras)

    return all_paragraphs


def update_database(paragraphs: dict[int, str], db_path: Path, dry_run: bool) -> tuple[int, int, int]:
    """Update paragraphs.text_la in the database.

    Returns:
        (extracted, matched, updated) counts
    """
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get existing paragraph IDs
    cursor.execute("SELECT id FROM paragraphs ORDER BY id")
    existing_ids = {row[0] for row in cursor.fetchall()}

    extracted = len(paragraphs)
    matched = len(set(paragraphs.keys()) & existing_ids)
    updated = 0

    if not dry_run:
        for para_id, text in paragraphs.items():
            if para_id in existing_ids:
                cursor.execute(
                    "UPDATE paragraphs SET text_la = ? WHERE id = ?",
                    (text, para_id)
                )
                updated += 1

        conn.commit()
    else:
        updated = matched

    conn.close()

    return extracted, matched, updated


@click.command()
@click.option("--dry-run", is_flag=True, help="Show what would be done without writing")
@click.option("--db", type=click.Path(), default=None, help="Path to database")
def main(dry_run: bool, db: str | None) -> None:
    """Parse Latin CCC HTML files and populate paragraphs.text_la."""
    db_path = Path(db) if db else DB_PATH

    click.echo(f"\n=== Parsing Latin CCC HTML files from {DATA_DIR.name} ===\n")

    paragraphs = parse_all_html_files(DATA_DIR)

    if not paragraphs:
        click.echo("\nERROR: No paragraphs extracted!")
        return

    click.echo(f"\n=== Updating database: {db_path.name} ===\n")

    extracted, matched, updated = update_database(paragraphs, db_path, dry_run)

    click.echo(f"\nSummary:")
    click.echo(f"  Paragraphs extracted: {extracted}")
    click.echo(f"  Matched existing IDs: {matched}")
    click.echo(f"  Paragraphs updated: {updated}")

    if extracted > matched:
        click.echo(f"  WARNING: {extracted - matched} paragraphs had IDs not in database")

    if dry_run:
        click.echo("\n(dry-run — no changes written)")


if __name__ == "__main__":
    main()
