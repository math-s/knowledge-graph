"""MCP server exposing the knowledge graph retriever as tools.

Run with:
    uv run --project api python -m api.mcp_server

Or register in Claude Code settings:
    {
      "mcpServers": {
        "knowledge-graph": {
          "command": "uv",
          "args": ["run", "--project", "/path/to/api", "python", "-m", "api.mcp_server"]
        }
      }
    }
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))
sys.path.insert(0, str(PROJECT_ROOT))

from mcp.server.fastmcp import FastMCP

from api.tools import dispatch

mcp = FastMCP("knowledge-graph")


@mcp.tool()
def search_ccc(query: str, limit: int = 5) -> str:
    """Search the Catechism of the Catholic Church (CCC) paragraphs by topic."""
    import json
    result = dispatch("search_ccc", {"query": query, "limit": limit})
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def get_paragraph(id: int, include_citations: bool = False) -> str:
    """Get a specific CCC paragraph by number."""
    import json
    result = dispatch("get_paragraph", {"id": id, "include_citations": include_citations})
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def search_encyclopedia(query: str, limit: int = 5) -> str:
    """Search the Catholic Encyclopedia (1907-1913, ~11,600 articles)."""
    import json
    result = dispatch("search_encyclopedia", {"query": query, "limit": limit})
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def get_encyclopedia_article(article_id: str) -> str:
    """Get the full text of a Catholic Encyclopedia article by ID (e.g. '06689a')."""
    import json
    result = dispatch("get_encyclopedia_article", {"article_id": article_id})
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def search_patristic(query: str, limit: int = 5) -> str:
    """Search Church Fathers texts (Augustine, Aquinas, Chrysostom, etc.)."""
    import json
    result = dispatch("search_patristic", {"query": query, "limit": limit})
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def search_bible(query: str, limit: int = 5) -> str:
    """Search Bible verses by text content."""
    import json
    result = dispatch("search_bible", {"query": query, "limit": limit})
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def get_citations(paragraph_id: int) -> str:
    """Get Bible and document citations for a CCC paragraph."""
    import json
    result = dispatch("get_citations", {"paragraph_id": paragraph_id})
    return json.dumps(result, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    mcp.run()
