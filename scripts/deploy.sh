#!/usr/bin/env bash
# Deploy to fly.io, skipping if the S3 DB version matches what's live.
#
# Compares:
#   - S3 db/knowledge-graph.meta.json (source of truth — what we'd ship)
#   - https://knowledge-graph-api.fly.dev/version (what's currently live)
# If their `version` strings match, exits 0 without calling `fly deploy`.
#
# Usage:
#   ./scripts/deploy.sh           # skip if versions match
#   ./scripts/deploy.sh --force   # always deploy
#
# Options:
#   --profile <name>   AWS profile (default: AWS_PROFILE or "personal")
#   --app <name>       Fly app name (default: read from fly.toml)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="s3://knowledge-graph-matheus-personal/db"
META_KEY="knowledge-graph.meta.json"
DB_KEY="knowledge-graph.db"

PROFILE="${AWS_PROFILE:-personal}"
APP=""
FORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)   FORCE=1; shift ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --app)     APP="$2"; shift 2 ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$APP" ]]; then
  APP="$(awk -F' *= *' '/^app *=/ {gsub(/"/, "", $2); print $2; exit}' "$ROOT/fly.toml")"
fi
[[ -z "$APP" ]] && { echo "could not determine fly app name" >&2; exit 1; }

LIVE_URL="https://${APP}.fly.dev/version"

# Pre-flight: local DB exists and is stamped, S3 DB matches local sha.
LOCAL_DB="$ROOT/data/$DB_KEY"
LOCAL_META="$ROOT/data/$META_KEY"

[[ -f "$LOCAL_DB" ]]   || { echo "missing $LOCAL_DB — build the pipeline first" >&2; exit 1; }
[[ -f "$LOCAL_META" ]] || { echo "missing $LOCAL_META — run scripts/sync_db.sh push first" >&2; exit 1; }

LOCAL_VERSION="$(jq -r .version "$LOCAL_META")"
LOCAL_SHA="$(jq -r .sha256 "$LOCAL_META")"
[[ "$LOCAL_VERSION" == "null" || -z "$LOCAL_VERSION" ]] && {
  echo "local meta.json missing 'version' — re-run scripts/sync_db.sh push" >&2; exit 1; }

# Read S3's meta.json (the canonical "what would ship") into a temp file.
S3_META="$(mktemp)"
trap 'rm -f "$S3_META"' EXIT
if ! aws --profile "$PROFILE" s3 cp "$BUCKET/$META_KEY" "$S3_META" --quiet 2>/dev/null; then
  echo "S3 has no $META_KEY yet — run ./scripts/sync_db.sh push first" >&2
  exit 1
fi

S3_VERSION="$(jq -r .version "$S3_META")"
S3_SHA="$(jq -r .sha256 "$S3_META")"

if [[ "$LOCAL_VERSION" != "$S3_VERSION" || "$LOCAL_SHA" != "$S3_SHA" ]]; then
  echo "Local DB does not match S3 — push first (or pull if S3 is newer)." >&2
  echo "  local:  version=$LOCAL_VERSION sha=$LOCAL_SHA" >&2
  echo "  s3:     version=$S3_VERSION sha=$S3_SHA" >&2
  exit 1
fi

# Compare against what's currently live on fly.
LIVE_VERSION=""
if LIVE_BODY="$(curl -fsS -m 15 "$LIVE_URL" 2>/dev/null)"; then
  LIVE_VERSION="$(printf '%s' "$LIVE_BODY" | jq -r '.version // empty')"
fi

echo "S3:   version=$S3_VERSION"
echo "Live: version=${LIVE_VERSION:-<none>}"

if [[ $FORCE -eq 0 && -n "$LIVE_VERSION" && "$LIVE_VERSION" == "$S3_VERSION" ]]; then
  echo "Already deployed — skipping fly deploy. (use --force to override)"
  exit 0
fi

echo "Deploying $APP..."
exec fly deploy --app "$APP"
