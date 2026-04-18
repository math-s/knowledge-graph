#!/usr/bin/env bash
# Sync pipeline/data/raw/ to/from S3.
#
# The raw data (scraped corpora, New Advent HTML bundle, TSK cross-refs, etc.)
# is large and partly commercially licensed — it belongs in S3, not git.
#
# Usage:
#   ./scripts/sync_raw.sh push            # upload entire raw/ tree
#   ./scripts/sync_raw.sh pull            # download entire raw/ tree
#   ./scripts/sync_raw.sh push newadvent  # upload only one subdir
#   ./scripts/sync_raw.sh pull newadvent  # download only one subdir
#
# Options:
#   --profile <name>      AWS profile to use (default: personal)
#   --delete              pass --delete to `aws s3 sync` to mirror exactly

set -euo pipefail

BUCKET="s3://knowledge-graph-matheus-personal/raw"
RAW_DIR="$(cd "$(dirname "$0")/.." && pwd)/pipeline/data/raw"

usage() {
  echo "Usage: $0 [push|pull] [subdir] [--profile <aws-profile>] [--delete]"
  exit 1
}

[[ $# -lt 1 ]] && usage

ACTION=""
SUBDIR=""
PROFILE="${AWS_PROFILE:-personal}"
DELETE_FLAG=""

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
    --delete)
      DELETE_FLAG="--delete"
      shift
      ;;
    push|pull)
      ACTION="$1"
      shift
      ;;
    *)
      if [[ -z "$SUBDIR" ]]; then
        SUBDIR="$1"
        shift
      else
        usage
      fi
      ;;
  esac
done

[[ -z "$ACTION" ]] && usage

AWS_ARGS=(--profile "$PROFILE")

LOCAL="$RAW_DIR"
REMOTE="$BUCKET"
if [[ -n "$SUBDIR" ]]; then
  LOCAL="$RAW_DIR/$SUBDIR"
  REMOTE="$BUCKET/$SUBDIR"
fi

case "$ACTION" in
  push)
    echo "Pushing $LOCAL → $REMOTE"
    mkdir -p "$LOCAL"
    aws "${AWS_ARGS[@]}" s3 sync "$LOCAL" "$REMOTE" \
      --storage-class STANDARD_IA \
      --exclude ".DS_Store" \
      $DELETE_FLAG
    ;;
  pull)
    echo "Pulling $REMOTE → $LOCAL"
    mkdir -p "$LOCAL"
    aws "${AWS_ARGS[@]}" s3 sync "$REMOTE" "$LOCAL" $DELETE_FLAG
    ;;
  *)
    usage
    ;;
esac

echo "Sync complete."
