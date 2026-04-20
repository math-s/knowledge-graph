"""Shared utilities for API routers."""

from __future__ import annotations

import sqlite3


def multilang_text(row: sqlite3.Row, langs: tuple[str, ...]) -> dict[str, str]:
    """Extract non-empty multilingual text columns into a dict."""
    return {lang: row[f"text_{lang}"] for lang in langs if row[f"text_{lang}"]}


def truncate(text: str | None, n: int) -> str | None:
    """Truncate text to n chars, appending ellipsis if cut."""
    if not text or len(text) <= n:
        return text
    return text[:n].rstrip() + "…"
