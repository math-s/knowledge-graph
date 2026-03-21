"""Claude tool definitions and dispatcher for the knowledge graph retriever."""

from __future__ import annotations

import json
from typing import Any

from api.retriever import get_retriever

# ---------------------------------------------------------------------------
# Tool schemas (passed to Claude API)
# ---------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "search_ccc",
        "description": (
            "Full-text search across Catechism of the Catholic Church (CCC) paragraphs. "
            "Use this to find what the Catechism says about a topic."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search terms"},
                "limit": {"type": "integer", "default": 5, "description": "Max results (1-20)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_paragraph",
        "description": "Get a specific CCC paragraph by its number, with optional citations.",
        "input_schema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer", "description": "CCC paragraph number"},
                "include_citations": {
                    "type": "boolean",
                    "default": False,
                    "description": "Include Bible and document citations",
                },
            },
            "required": ["id"],
        },
    },
    {
        "name": "search_encyclopedia",
        "description": (
            "Full-text search across the Catholic Encyclopedia (1907-1913, ~11,600 articles). "
            "Use for historical, theological, or biographical background."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search terms"},
                "limit": {"type": "integer", "default": 5, "description": "Max results (1-20)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_encyclopedia_article",
        "description": "Get the full text of a Catholic Encyclopedia article by its ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "article_id": {
                    "type": "string",
                    "description": "Article ID (e.g. '06689a' for Grace)",
                },
            },
            "required": ["article_id"],
        },
    },
    {
        "name": "search_patristic",
        "description": (
            "Search Church Fathers texts (Augustine, Aquinas, Chrysostom, etc.). "
            "Use for patristic quotations and early Church commentary."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search terms"},
                "limit": {"type": "integer", "default": 5, "description": "Max results"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_bible",
        "description": "Search Bible verses by text content.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search terms"},
                "limit": {"type": "integer", "default": 5, "description": "Max results"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_citations",
        "description": "Get Bible and document citations for a CCC paragraph.",
        "input_schema": {
            "type": "object",
            "properties": {
                "paragraph_id": {"type": "integer", "description": "CCC paragraph number"},
            },
            "required": ["paragraph_id"],
        },
    },
]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def dispatch(tool_name: str, tool_input: dict) -> Any:
    """Execute a tool call and return a JSON-serialisable result."""
    r = get_retriever()

    if tool_name == "search_ccc":
        results = r.search(tool_input["query"], limit=tool_input.get("limit", 5))
        return [
            {
                "id": p.id,
                "location": p.location,
                "themes": p.themes,
                "text": p.text[:600],
            }
            for p in results
        ]

    if tool_name == "get_paragraph":
        p = r.get_paragraph(tool_input["id"])
        if not p:
            return {"error": f"Paragraph {tool_input['id']} not found"}
        result: dict = {
            "id": p.id,
            "location": p.location,
            "themes": p.themes,
            "text": p.text,
        }
        if tool_input.get("include_citations"):
            bible = r.get_bible_citations(p.id)
            docs = r.get_document_citations(p.id)
            result["bible_citations"] = [
                {"book": c.book, "reference": c.reference, "text": c.text[:300]}
                for c in bible
            ]
            result["document_citations"] = [
                {"document": c.document_id, "section": c.section_num, "text": c.text[:300]}
                for c in docs
            ]
        return result

    if tool_name == "search_encyclopedia":
        try:
            rows = r.conn.execute("""
                SELECT e.id, e.title, e.summary, substr(e.text_en, 1, 500) as preview
                FROM encyclopedia_fts f
                JOIN encyclopedia e ON e.id = f.id
                WHERE encyclopedia_fts MATCH ?
                ORDER BY f.rank
                LIMIT ?
            """, (tool_input["query"], tool_input.get("limit", 5))).fetchall()
        except Exception as e:
            return {"error": str(e)}
        return [
            {"id": row["id"], "title": row["title"], "summary": row["summary"], "preview": row["preview"]}
            for row in rows
        ]

    if tool_name == "get_encyclopedia_article":
        try:
            row = r.conn.execute(
                "SELECT id, title, summary, text_en, url FROM encyclopedia WHERE id = ?",
                (tool_input["article_id"],),
            ).fetchone()
        except Exception as e:
            return {"error": str(e)}
        if not row:
            return {"error": f"Article '{tool_input['article_id']}' not found"}
        return {
            "id": row["id"],
            "title": row["title"],
            "summary": row["summary"],
            "text": row["text_en"],
            "url": row["url"],
        }

    if tool_name == "search_patristic":
        results = r.search_patristic(tool_input["query"], limit=tool_input.get("limit", 5))
        return [
            {"id": t.id, "chapter": t.chapter_id, "text": t.text[:600]}
            for t in results
        ]

    if tool_name == "search_bible":
        results = r.search_bible(tool_input["query"], limit=tool_input.get("limit", 5))
        return [
            {"book": c.book, "reference": c.reference, "text": c.text[:400]}
            for c in results
        ]

    if tool_name == "get_citations":
        pid = tool_input["paragraph_id"]
        bible = r.get_bible_citations(pid)
        docs = r.get_document_citations(pid)
        return {
            "paragraph_id": pid,
            "bible": [{"book": c.book, "reference": c.reference, "text": c.text} for c in bible],
            "documents": [{"document": c.document_id, "section": c.section_num, "text": c.text[:300]} for c in docs],
        }

    return {"error": f"Unknown tool: {tool_name}"}


def dispatch_to_content(tool_name: str, tool_input: dict) -> str:
    """Run a tool and return result as a JSON string for the Claude tool_result block."""
    result = dispatch(tool_name, tool_input)
    return json.dumps(result, ensure_ascii=False, indent=2)
