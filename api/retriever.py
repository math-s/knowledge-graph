"""Singleton retriever instance for the API."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))

from pipeline.src.chat.retriever import Retriever
from api.db import DB_PATH

_retriever: Retriever | None = None


def get_retriever(db_path: Path | None = None) -> Retriever:
    global _retriever
    if _retriever is None:
        _retriever = Retriever(db_path=db_path or DB_PATH)
    return _retriever


def close_retriever() -> None:
    global _retriever
    if _retriever:
        _retriever.close()
        _retriever = None
