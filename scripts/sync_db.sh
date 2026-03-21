#!/usr/bin/env bash
# Sync knowledge-graph databases to/from S3.
#
# Usage:
#   ./scripts/sync_db.sh push          # upload all DBs to S3
#   ./scripts/sync_db.sh pull          # download all DBs from S3
#   ./scripts/sync_db.sh push encyclopedia   # upload one DB
#   ./scripts/sync_db.sh pull encyclopedia   # download one DB

set -euo pipefail

BUCKET="s3://knowledge-graph-matheus/db"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"

DATABASES=(
  "knowledge-graph.db"
  "encyclopedia.db"
)

usage() {
  echo "Usage: $0 [push|pull] [db-name (optional)]"
  exit 1
}

[[ $# -lt 1 ]] && usage
ACTION="$1"
FILTER="${2:-}"

for DB in "${DATABASES[@]}"; do
  # Skip if a specific DB was requested and this isn't it
  if [[ -n "$FILTER" && "$DB" != "${FILTER%.db}.db" ]]; then
    continue
  fi

  LOCAL="$DATA_DIR/$DB"
  REMOTE="$BUCKET/$DB"

  case "$ACTION" in
    push)
      echo "Uploading $DB..."
      aws s3 cp "$LOCAL" "$REMOTE" --storage-class STANDARD_IA
      echo "  done: $REMOTE"
      ;;
    pull)
      echo "Downloading $DB..."
      mkdir -p "$DATA_DIR"
      aws s3 cp "$REMOTE" "$LOCAL"
      echo "  done: $LOCAL"
      ;;
    *)
      usage
      ;;
  esac
done

echo "Sync complete."
