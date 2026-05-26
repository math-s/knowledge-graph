#!/usr/bin/env bash
# Sync knowledge-graph databases to/from S3.
#
# Usage:
#   ./scripts/sync_db.sh push          # upload the DB to S3
#   ./scripts/sync_db.sh pull          # download the DB from S3
#
# Options:
#   --profile <name>   AWS profile to use (or set AWS_PROFILE env var)

set -euo pipefail

BUCKET="s3://knowledge-graph-matheus-personal/db"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"

DATABASES=(
  "knowledge-graph.db"
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
  META_NAME="${DB%.db}.meta.json"
  LOCAL_META="$DATA_DIR/$META_NAME"
  REMOTE_META="$BUCKET/$META_NAME"

  case "$ACTION" in
    push)
      echo "Generating $META_NAME (sha256 over $(du -h "$LOCAL" | cut -f1))..."
      python "$(dirname "$0")/../pipeline/scripts/write_meta_json.py" \
        --db "$LOCAL" --out "$LOCAL_META"
      echo "Uploading $DB..."
      aws "${AWS_ARGS[@]}" s3 cp "$LOCAL" "$REMOTE" --storage-class STANDARD_IA
      echo "  done: $REMOTE"
      echo "Uploading $META_NAME..."
      aws "${AWS_ARGS[@]}" s3 cp "$LOCAL_META" "$REMOTE_META"
      echo "  done: $REMOTE_META"
      ;;
    pull)
      echo "Downloading $DB..."
      mkdir -p "$DATA_DIR"
      aws "${AWS_ARGS[@]}" s3 cp "$REMOTE" "$LOCAL"
      echo "  done: $LOCAL"
      aws "${AWS_ARGS[@]}" s3 cp "$REMOTE_META" "$LOCAL_META" 2>/dev/null \
        && echo "  done: $LOCAL_META" \
        || echo "  (no $META_NAME in S3 yet — skipping)"
      ;;
    *)
      usage
      ;;
  esac
done

echo "Sync complete."
