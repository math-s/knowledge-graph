#!/usr/bin/env bash
# Sync knowledge-graph databases to/from S3.
#
# Usage:
#   ./scripts/sync_db.sh push          # upload all DBs to S3
#   ./scripts/sync_db.sh pull          # download all DBs from S3
#   ./scripts/sync_db.sh push encyclopedia   # upload one DB
#   ./scripts/sync_db.sh pull encyclopedia   # download one DB
#
# Options:
#   --profile <name>   AWS profile to use (or set AWS_PROFILE env var)

set -euo pipefail

BUCKET="s3://knowledge-graph-matheus-personal/db"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"

DATABASES=(
  "knowledge-graph.db"
  "encyclopedia.db"
)

usage() {
  echo "Usage: $0 [push|pull] [db-name] [--profile <aws-profile>]"
  exit 1
}

[[ $# -lt 1 ]] && usage

ACTION=""
FILTER=""
PROFILE="${AWS_PROFILE:-personal}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      [[ -z "$PROFILE" ]] && usage
      shift 2
      ;;
    --profile=*)
      PROFILE="${1#--profile=}"
      shift
      ;;
    push|pull)
      ACTION="$1"
      shift
      ;;
    *)
      if [[ -z "$FILTER" ]]; then
        FILTER="$1"
        shift
      else
        usage
      fi
      ;;
  esac
done

[[ -z "$ACTION" ]] && usage

AWS_ARGS=()
[[ -n "$PROFILE" ]] && AWS_ARGS=(--profile "$PROFILE")

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
      aws "${AWS_ARGS[@]}" s3 cp "$LOCAL" "$REMOTE" --storage-class STANDARD_IA
      echo "  done: $REMOTE"
      ;;
    pull)
      echo "Downloading $DB..."
      mkdir -p "$DATA_DIR"
      aws "${AWS_ARGS[@]}" s3 cp "$REMOTE" "$LOCAL"
      echo "  done: $LOCAL"
      ;;
    *)
      usage
      ;;
  esac
done

echo "Sync complete."
