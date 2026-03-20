"""CLI for querying the CCC knowledge graph.

A data retrieval tool designed to be called by an LLM (Claude Code)
or scripts. Returns structured source data from the Catechism,
Bible, ecclesiastical documents, and Church Fathers.

Usage examples:

    # Search CCC paragraphs by topic
    knowledge-graph search "eucharist"

    # Get a paragraph with all its sources
    knowledge-graph paragraph 1324 --sources

    # Get paragraphs by theme or entity
    knowledge-graph theme sacraments
    knowledge-graph entity eucharist

    # Get cross-referenced paragraphs
    knowledge-graph cross-refs 1324

    # Get Bible and document citations
    knowledge-graph citations 1324

    # Get a document section
    knowledge-graph document lumen-gentium --section 11

    # List available themes / entities / documents
    knowledge-graph list-themes
    knowledge-graph list-entities
    knowledge-graph list-documents

    # Search Bible verses or patristic texts
    knowledge-graph search-bible "bread of life"
    knowledge-graph search-patristic "eucharist"

All commands support --json for structured output and --verbose for full text.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.chat.retriever import (
    DEFAULT_DB_PATH, Retriever, Paragraph,
)


def _para_dict(p: Paragraph) -> dict:
    return {
        "id": p.id, "text": p.text, "location": p.location,
        "themes": p.themes, "entities": p.entities, "source": p.source,
    }


def _print_para(p: Paragraph, verbose: bool) -> None:
    tag = f" [{p.source}]" if p.source != "direct" else ""
    click.echo(f"\n--- CCC {p.id}{tag} ---")
    click.echo(f"  {p.location}")
    if p.themes:
        click.echo(f"  themes: {', '.join(p.themes)}")
    if verbose:
        click.echo(f"\n{p.text}\n")
    else:
        text = p.text[:300] + ("..." if len(p.text) > 300 else "")
        click.echo(f"\n{text}\n")


def _json_out(data: object) -> None:
    click.echo(json.dumps(data, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

def _common_options(fn):
    """Decorator that adds --json and --verbose to any command."""
    fn = click.option("--verbose", "-v", is_flag=True, help="Show full text")(fn)
    fn = click.option("--json", "as_json", is_flag=True, help="Output as JSON")(fn)
    return fn


def _get_retriever(ctx: click.Context) -> Retriever:
    return ctx.obj["retriever"]


def _is_json(ctx: click.Context, kwargs: dict) -> bool:
    return kwargs.get("as_json", False) or ctx.obj.get("json", False)


def _is_verbose(ctx: click.Context, kwargs: dict) -> bool:
    return kwargs.get("verbose", False) or ctx.obj.get("verbose", False)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--db", type=click.Path(exists=True), default=None,
              help="Path to knowledge-graph.db")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.option("--verbose", "-v", is_flag=True, help="Show full text")
@click.pass_context
def cli(ctx: click.Context, db: str | None, as_json: bool, verbose: bool) -> None:
    """Query the CCC knowledge graph for Catholic Church teachings."""
    ctx.ensure_object(dict)
    db_path = Path(db) if db else DEFAULT_DB_PATH
    ctx.obj["retriever"] = Retriever(db_path=db_path)
    ctx.obj["json"] = as_json
    ctx.obj["verbose"] = verbose


@cli.result_callback()
@click.pass_context
def cleanup(ctx: click.Context, *_args, **_kwargs) -> None:
    ctx.obj["retriever"].close()


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("query")
@click.option("--limit", "-n", default=10, help="Max results")
@_common_options
@click.pass_context
def search(ctx: click.Context, query: str, limit: int, **kwargs) -> None:
    """Full-text search across CCC paragraphs."""
    r = _get_retriever(ctx)
    results = r.search(query, limit=limit)
    if _is_json(ctx, kwargs):
        _json_out([_para_dict(p) for p in results])
    else:
        click.echo(f"Found {len(results)} paragraphs for '{query}':")
        for p in results:
            _print_para(p, _is_verbose(ctx, kwargs))


# ---------------------------------------------------------------------------
# paragraph
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("id", type=int)
@click.option("--sources", "-s", is_flag=True, help="Include citations and cross-refs")
@_common_options
@click.pass_context
def paragraph(ctx: click.Context, id: int, sources: bool, **kwargs) -> None:
    """Get a CCC paragraph by number."""
    r = _get_retriever(ctx)
    p = r.get_paragraph(id)
    if not p:
        raise click.ClickException(f"Paragraph {id} not found.")

    if _is_json(ctx, kwargs):
        data = _para_dict(p)
        if sources:
            data["cross_refs"] = r.get_cross_refs(p.id)
            data["bible_citations"] = [
                {"book": c.book, "reference": c.reference, "text": c.text}
                for c in r.get_bible_citations(p.id)
            ]
            data["document_citations"] = [
                {"document": c.document_id, "section": c.section_num, "text": c.text}
                for c in r.get_document_citations(p.id)
            ]
        _json_out(data)
    else:
        _print_para(p, verbose=True)
        if sources:
            refs = r.get_cross_refs(p.id)
            if refs:
                click.echo(f"Cross-references: {refs}")

            bible = r.get_bible_citations(p.id)
            if bible:
                click.echo(f"\nBible citations ({len(bible)}):")
                for c in bible:
                    line = f"  {c.book} {c.reference}"
                    if c.text:
                        line += f": {c.text[:120]}..."
                    click.echo(line)

            docs = r.get_document_citations(p.id)
            if docs:
                click.echo(f"\nDocument citations ({len(docs)}):")
                for c in docs:
                    line = f"  {c.document_id} §{c.section_num}"
                    if c.text:
                        line += f": {c.text[:120]}..."
                    click.echo(line)


# ---------------------------------------------------------------------------
# cross-refs
# ---------------------------------------------------------------------------

@cli.command("cross-refs")
@click.argument("id", type=int)
@_common_options
@click.pass_context
def cross_refs(ctx: click.Context, id: int, **kwargs) -> None:
    """Get paragraphs cross-referenced by a given paragraph."""
    r = _get_retriever(ctx)
    paragraphs = r.get_cross_ref_paragraphs(id)
    if _is_json(ctx, kwargs):
        _json_out([_para_dict(p) for p in paragraphs])
    else:
        click.echo(f"Cross-references from CCC {id} ({len(paragraphs)}):")
        for p in paragraphs:
            _print_para(p, _is_verbose(ctx, kwargs))


# ---------------------------------------------------------------------------
# citations
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("id", type=int)
@_common_options
@click.pass_context
def citations(ctx: click.Context, id: int, **kwargs) -> None:
    """Get Bible and document citations for a paragraph."""
    r = _get_retriever(ctx)
    bible = r.get_bible_citations(id)
    docs = r.get_document_citations(id)

    if _is_json(ctx, kwargs):
        _json_out({
            "paragraph_id": id,
            "bible": [{"book": c.book, "reference": c.reference, "text": c.text} for c in bible],
            "documents": [{"document": c.document_id, "section": c.section_num, "text": c.text} for c in docs],
        })
    else:
        if bible:
            click.echo(f"Bible citations for CCC {id} ({len(bible)}):")
            for c in bible:
                line = f"  {c.book} {c.reference}"
                if c.text:
                    line += f"\n    {c.text[:200]}"
                click.echo(line)
        if docs:
            click.echo(f"\nDocument citations for CCC {id} ({len(docs)}):")
            for c in docs:
                line = f"  {c.document_id} §{c.section_num}"
                if c.text:
                    line += f"\n    {c.text[:200]}"
                click.echo(line)
        if not bible and not docs:
            click.echo(f"No citations found for CCC {id}.")


# ---------------------------------------------------------------------------
# theme
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("theme_id")
@click.option("--limit", "-n", default=20, help="Max results")
@_common_options
@click.pass_context
def theme(ctx: click.Context, theme_id: str, limit: int, **kwargs) -> None:
    """Get paragraphs belonging to a theme."""
    r = _get_retriever(ctx)
    paragraphs = r.get_paragraphs_by_theme(theme_id, limit=limit)
    if _is_json(ctx, kwargs):
        _json_out([_para_dict(p) for p in paragraphs])
    else:
        click.echo(f"Theme '{theme_id}' — {len(paragraphs)} paragraphs:")
        for p in paragraphs:
            _print_para(p, _is_verbose(ctx, kwargs))


# ---------------------------------------------------------------------------
# entity
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("entity_id")
@click.option("--limit", "-n", default=20, help="Max results")
@_common_options
@click.pass_context
def entity(ctx: click.Context, entity_id: str, limit: int, **kwargs) -> None:
    """Get paragraphs mentioning an entity."""
    r = _get_retriever(ctx)
    paragraphs = r.get_paragraphs_by_entity(entity_id, limit=limit)
    if _is_json(ctx, kwargs):
        _json_out([_para_dict(p) for p in paragraphs])
    else:
        click.echo(f"Entity '{entity_id}' — {len(paragraphs)} paragraphs:")
        for p in paragraphs:
            _print_para(p, _is_verbose(ctx, kwargs))


# ---------------------------------------------------------------------------
# document
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("doc_id")
@click.option("--section", "-s", default=None, help="Specific section number")
@_common_options
@click.pass_context
def document(ctx: click.Context, doc_id: str, section: str | None, **kwargs) -> None:
    """Get document info or a specific section."""
    r = _get_retriever(ctx)
    if section:
        ds = r.get_document_section(doc_id, section)
        if not ds:
            raise click.ClickException(f"Section {section} not found in {doc_id}.")
        if _is_json(ctx, kwargs):
            _json_out({"document": ds.document_id, "section": ds.section_num, "text": ds.text})
        else:
            click.echo(f"\n{ds.document_id} §{ds.section_num}:\n")
            click.echo(ds.text)
    else:
        rows = r.conn.execute("""
            SELECT section_num, substr(text_en, 1, 120) as preview
            FROM document_sections WHERE document_id = ?
            ORDER BY CAST(section_num AS INTEGER)
        """, (doc_id,)).fetchall()
        if _is_json(ctx, kwargs):
            _json_out([{"section": row["section_num"], "preview": row["preview"]} for row in rows])
        else:
            click.echo(f"Document '{doc_id}' — {len(rows)} sections:")
            for row in rows:
                click.echo(f"  §{row['section_num']}: {row['preview']}...")


# ---------------------------------------------------------------------------
# search-bible
# ---------------------------------------------------------------------------

@cli.command("search-bible")
@click.argument("query")
@click.option("--limit", "-n", default=10, help="Max results")
@_common_options
@click.pass_context
def search_bible(ctx: click.Context, query: str, limit: int, **kwargs) -> None:
    """Search Bible verses by text (note: verse text may be unavailable)."""
    r = _get_retriever(ctx)
    results = r.search_bible(query, limit=limit)
    if _is_json(ctx, kwargs):
        _json_out([{"book": c.book, "reference": c.reference, "text": c.text} for c in results])
    else:
        if not results:
            click.echo(f"No Bible verses found for '{query}'.")
            click.echo("Note: Bible verse text is not yet populated in the database.")
        else:
            click.echo(f"Found {len(results)} Bible verses for '{query}':")
            for c in results:
                click.echo(f"  {c.book} {c.reference}: {c.text[:200]}")


# ---------------------------------------------------------------------------
# search-patristic
# ---------------------------------------------------------------------------

@cli.command("search-patristic")
@click.argument("query")
@click.option("--limit", "-n", default=10, help="Max results")
@_common_options
@click.pass_context
def search_patristic(ctx: click.Context, query: str, limit: int, **kwargs) -> None:
    """Search Church Fathers texts."""
    r = _get_retriever(ctx)
    results = r.search_patristic(query, limit=limit)
    if _is_json(ctx, kwargs):
        _json_out([{"id": t.id, "chapter": t.chapter_id, "text": t.text[:300]} for t in results])
    else:
        click.echo(f"Found {len(results)} patristic texts for '{query}':")
        for t in results:
            click.echo(f"  {t.chapter_id}: {t.text[:200]}...")


# ---------------------------------------------------------------------------
# list-*
# ---------------------------------------------------------------------------

@cli.command("list-themes")
@_common_options
@click.pass_context
def list_themes(ctx: click.Context, **kwargs) -> None:
    """List all themes with paragraph counts."""
    r = _get_retriever(ctx)
    themes = r.get_themes()
    if _is_json(ctx, kwargs):
        _json_out(themes)
    else:
        for t in themes:
            click.echo(f"  {t['id']:30s} {t['label']:40s} ({t['count']} paragraphs)")


@cli.command("list-entities")
@_common_options
@click.pass_context
def list_entities(ctx: click.Context, **kwargs) -> None:
    """List all entities with paragraph counts."""
    r = _get_retriever(ctx)
    entities = r.get_entities()
    if _is_json(ctx, kwargs):
        _json_out(entities)
    else:
        for e in entities:
            click.echo(f"  {e['id']:30s} {e['label']:40s} [{e['category']}] ({e['count']} paragraphs)")


@cli.command("list-documents")
@_common_options
@click.pass_context
def list_documents(ctx: click.Context, **kwargs) -> None:
    """List all ecclesiastical documents."""
    r = _get_retriever(ctx)
    docs = r.list_documents()
    if _is_json(ctx, kwargs):
        _json_out(docs)
    else:
        for d in docs:
            click.echo(f"  {d['id']:40s} {d['name'][:50]:50s} ({d['sections']} sections)")


# ---------------------------------------------------------------------------
# lexicon
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("query")
@_common_options
@click.pass_context
def lexicon(ctx: click.Context, query: str, **kwargs) -> None:
    """Look up a theological term in the lexicon."""
    r = _get_retriever(ctx)
    rows = r.conn.execute("""
        SELECT l.id, l.term_en, l.term_la, l.term_el, l.etymology, l.definition, l.category,
               (SELECT COUNT(*) FROM lexicon_paragraphs lp WHERE lp.term_id = l.id) as para_count
        FROM lexicon_fts f
        JOIN lexicon l ON l.id = f.id
        WHERE lexicon_fts MATCH ?
        ORDER BY f.rank
        LIMIT 5
    """, (query,)).fetchall()

    if _is_json(ctx, kwargs):
        _json_out([{
            "id": r["id"], "en": r["term_en"], "la": r["term_la"], "el": r["term_el"],
            "etymology": r["etymology"], "definition": r["definition"],
            "category": r["category"], "paragraphs": r["para_count"],
        } for r in rows])
    else:
        if not rows:
            click.echo(f"No lexicon entries found for '{query}'.")
            return
        for r in rows:
            click.echo(f"\n  {r['term_en']}  ({r['term_la']} / {r['term_el']})")
            click.echo(f"  Category: {r['category']}")
            click.echo(f"  Etymology: {r['etymology']}")
            click.echo(f"  Definition: {r['definition']}")
            click.echo(f"  CCC references: {r['para_count']} paragraphs")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
