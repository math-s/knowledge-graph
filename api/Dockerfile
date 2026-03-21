FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install AWS CLI for DB sync
RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli curl \
    && rm -rf /var/lib/apt/lists/*

# Copy API package
COPY api/pyproject.toml api/uv.lock* ./api/
COPY api/__init__.py api/main.py api/chat.py api/tools.py api/retriever.py api/mcp_server.py ./api/

# Copy pipeline (retriever depends on it)
COPY pipeline/src ./pipeline/src
COPY pipeline/pyproject.toml ./pipeline/

# Install dependencies
RUN uv sync --project api --no-dev

# Startup script: pull DB from S3, then launch uvicorn
COPY api/entrypoint.sh ./entrypoint.sh
RUN chmod +x entrypoint.sh

ENV PYTHONPATH=/app
ENV PORT=8000

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
