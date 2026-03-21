"""FastAPI application entry point."""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))

from api.chat import router as chat_router
from api.retriever import get_retriever, close_retriever


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: open DB connection
    get_retriever()
    yield
    # Shutdown: close DB connection
    close_retriever()


app = FastAPI(
    title="Catholic Knowledge Graph API",
    description="LLM-powered chat over the Catholic knowledge graph",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)


@app.get("/health")
async def health():
    r = get_retriever()
    count = r.conn.execute("SELECT COUNT(*) FROM paragraphs").fetchone()[0]
    return {"status": "ok", "paragraphs": count}


def start():
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    start()
