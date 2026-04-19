#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="/app/data"
DB_PATH="$DATA_DIR/knowledge-graph.db"
S3_PATH="s3://knowledge-graph-matheus-personal/db/knowledge-graph.db"

mkdir -p "$DATA_DIR"

# Pull DB from S3 if not present or if REFRESH_DB is set
if [[ ! -f "$DB_PATH" || "${REFRESH_DB:-}" == "1" ]]; then
  echo "Pulling database from S3..."
  aws s3 cp "$S3_PATH" "$DB_PATH"
  echo "Database ready ($(du -sh "$DB_PATH" | cut -f1))"
fi

exec uv run --project /app/api uvicorn api.main:app \
  --host 0.0.0.0 \
  --port "${PORT:-8000}" \
  --workers "${WORKERS:-1}"
