"""Knowledge Graph API — data + LLM chat, backed by SQLite."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .db import DB_PATH, get_connection
from .routers import authors, bible, documents, graph, lexicon, paragraphs, search
from .chat import router as chat_router

# Cache durations (seconds)
CACHE_IMMUTABLE = 60 * 60 * 24  # 24h — data only changes on redeploy
CACHE_SHORT = 60 * 5  # 5min — for search results


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM paragraphs").fetchone()[0]
    conn.close()
    print(f"Database loaded: {DB_PATH} ({count} paragraphs)")
    yield


app = FastAPI(
    title="Knowledge Graph API",
    description="CCC knowledge graph — data API + LLM chat",
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

allowed_origins = os.environ.get("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    response: Response = await call_next(request)
    path = request.url.path
    if path.startswith("/chat"):
        response.headers["Cache-Control"] = "no-cache"
    elif path.startswith("/search"):
        response.headers["Cache-Control"] = f"public, max-age={CACHE_SHORT}"
    elif path == "/health":
        response.headers["Cache-Control"] = "no-cache"
    else:
        response.headers["Cache-Control"] = f"public, max-age={CACHE_IMMUTABLE}"
    return response


# Data routers
app.include_router(search.router)
app.include_router(graph.router)
app.include_router(paragraphs.router)
app.include_router(bible.router)
app.include_router(documents.router)
app.include_router(authors.router)
app.include_router(lexicon.router)

# LLM chat router
app.include_router(chat_router)


@app.get("/health")
def health():
    return {"status": "ok"}
