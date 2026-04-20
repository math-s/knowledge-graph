"""Summa Theologiae endpoints."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db

router = APIRouter(prefix="/summa", tags=["summa"])


@router.get("/parts")
def list_parts(db: sqlite3.Connection = Depends(get_db)):
    """List all Summa Theologiae parts with question counts."""
    rows = db.execute(
        """
        SELECT p.num, p.name,
               COUNT(q.id) AS question_count
        FROM summa_parts p
        LEFT JOIN summa_questions q ON q.part_num = p.num
        GROUP BY p.num
        ORDER BY p.num
        """
    ).fetchall()
    return [{"num": r["num"], "name": r["name"], "question_count": r["question_count"]} for r in rows]


@router.get("/parts/{part_num}")
def get_part(part_num: int, db: sqlite3.Connection = Depends(get_db)):
    """Get a Summa part with its list of questions."""
    part = db.execute("SELECT * FROM summa_parts WHERE num = ?", (part_num,)).fetchone()
    if not part:
        raise HTTPException(404, f"Part {part_num} not found")
    questions = db.execute(
        "SELECT id, question_num, title, summary FROM summa_questions WHERE part_num = ? ORDER BY question_num",
        (part_num,),
    ).fetchall()
    return {
        "num": part["num"],
        "name": part["name"],
        "questions": [
            {"id": q["id"], "num": q["question_num"], "title": q["title"], "summary": q["summary"]}
            for q in questions
        ],
    }


@router.get("/questions/{question_id}")
def get_question(question_id: int, db: sqlite3.Connection = Depends(get_db)):
    """Get a Summa question with all its articles."""
    q = db.execute("SELECT * FROM summa_questions WHERE id = ?", (question_id,)).fetchone()
    if not q:
        raise HTTPException(404, f"Question {question_id} not found")
    articles = db.execute(
        "SELECT id, article_num, title, text FROM summa_articles WHERE question_id = ? ORDER BY article_num",
        (question_id,),
    ).fetchall()
    return {
        "id": q["id"],
        "part_num": q["part_num"],
        "num": q["question_num"],
        "title": q["title"],
        "summary": q["summary"],
        "articles": [
            {"id": a["id"], "num": a["article_num"], "title": a["title"], "text": a["text"]}
            for a in articles
        ],
    }


@router.get("/articles/{article_id:path}")
def get_article(article_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Get a single Summa article by ID (e.g. '1001:1')."""
    a = db.execute("SELECT * FROM summa_articles WHERE id = ?", (article_id,)).fetchone()
    if not a:
        raise HTTPException(404, f"Article '{article_id}' not found")
    return {
        "id": a["id"],
        "question_id": a["question_id"],
        "num": a["article_num"],
        "title": a["title"],
        "text": a["text"],
    }
