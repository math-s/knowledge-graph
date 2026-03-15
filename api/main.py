"""Knowledge Graph API — read-only FastAPI server backed by SQLite."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import DB_PATH, get_connection
from .routers import authors, bible, documents, graph, paragraphs, search


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify DB exists on startup
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM paragraphs").fetchone()[0]
    conn.close()
    print(f"Database loaded: {DB_PATH} ({count} paragraphs)")
    yield


app = FastAPI(
    title="Knowledge Graph API",
    description="Read-only API for the CCC Knowledge Graph",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow the frontend origin(s)
allowed_origins = os.environ.get("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(search.router)
app.include_router(graph.router)
app.include_router(paragraphs.router)
app.include_router(bible.router)
app.include_router(documents.router)
app.include_router(authors.router)


@app.get("/health")
def health():
    return {"status": "ok"}
