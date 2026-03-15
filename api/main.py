"""Knowledge Graph API — read-only FastAPI server backed by SQLite."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .db import DB_PATH, get_connection
from .routers import authors, bible, documents, graph, paragraphs, search

# Cache durations (seconds)
CACHE_IMMUTABLE = 60 * 60 * 24  # 24h — data only changes on redeploy
CACHE_SHORT = 60 * 5  # 5min — for search results


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

# Gzip — compress JSON responses (big win for graph subgraphs)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS — allow the frontend origin(s)
allowed_origins = os.environ.get("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    response: Response = await call_next(request)
    path = request.url.path
    if path.startswith("/search"):
        response.headers["Cache-Control"] = f"public, max-age={CACHE_SHORT}"
    elif path == "/health":
        response.headers["Cache-Control"] = "no-cache"
    else:
        response.headers["Cache-Control"] = f"public, max-age={CACHE_IMMUTABLE}"
    return response


app.include_router(search.router)
app.include_router(graph.router)
app.include_router(paragraphs.router)
app.include_router(bible.router)
app.include_router(documents.router)
app.include_router(authors.router)


@app.get("/health")
def health():
    return {"status": "ok"}
