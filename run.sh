#!/usr/bin/env sh
# Build & run the hybrid search API, index, and query.
# Connects to AWS Managed OpenSearch + S3 using the values in .env (no local
# OpenSearch container) — run from inside the VPC, e.g. on EC2.
#
# Usage:
#   ./run.sh up                 # build + start the API
#   ./run.sh smoke              # safe dry-run: index a few records into a throwaway index, then clean up
#   ./run.sh index [--recreate] # index the manifest into OpenSearch
#   ./run.sh search "<query>"   # run a hybrid search via the API
#   ./run.sh logs               # tail the app logs
#   ./run.sh down               # stop & remove containers
#   ./run.sh all "<query>"      # up -> index --recreate -> search
set -eu

API_URL="${API_URL:-http://localhost:8000}"
# smoke knobs: throwaway index name + how many manifest records to test with.
SMOKE_INDEX="${SMOKE_INDEX:-images_smoke}"
SMOKE_N="${SMOKE_N:-20}"

usage() {
  sed -n '2,13p' "$0"
  exit "${1:-0}"
}

up() {
  docker compose up -d --build
  echo "API up: ${API_URL}  (docs: ${API_URL}/docs)"
}

index() {
  # Pass extra args through, e.g. --recreate. One-off container, removed after.
  docker compose run --rm app \
    uv run --no-dev python -m hybridsearch.index.run_from_manifest "$@"
}

smoke() {
  # End-to-end dry-run BEFORE the real `index`. Exercises the full path —
  # manifest -> S3 sidecars -> embedding -> OpenSearch — on a small manifest
  # slice and a throwaway index, then deletes that index. Never touches the real
  # index (HS_INDEX_NAME, default 'images'), and is idempotent: re-run anytime.
  [ -f data/manifest.jsonl ] || {
    echo "error: data/manifest.jsonl not found — run prepare_dataset or copy it to this host" >&2
    exit 2
  }
  # Clean up the temp slice no matter how we exit (failed smoke leaves nothing behind).
  trap 'rm -f data/manifest.smoke.jsonl' EXIT
  head -n "$SMOKE_N" data/manifest.jsonl > data/manifest.smoke.jsonl

  echo ">> smoke: indexing first ${SMOKE_N} record(s) into '${SMOKE_INDEX}' (real index untouched)"
  docker compose run --rm \
    -e HS_INDEX_NAME="$SMOKE_INDEX" \
    -e HS_MANIFEST_PATH=/app/data/manifest.smoke.jsonl \
    app uv run --no-dev python -m hybridsearch.index.run_from_manifest --recreate

  echo ">> smoke: verifying doc count, then deleting '${SMOKE_INDEX}'"
  docker compose run --rm -e HS_INDEX_NAME="$SMOKE_INDEX" app \
    uv run --no-dev python -c "from hybridsearch import config; from hybridsearch.search.client import get_client; c=get_client(); print('smoke doc count =', c.count(index=config.INDEX_NAME)['count']); c.indices.delete(index=config.INDEX_NAME); print('deleted index', config.INDEX_NAME)"

  echo ">> smoke OK — manifest, S3 sidecars, embedding, and OpenSearch all reachable."
}

search() {
  [ "$#" -ge 1 ] || { echo "error: search needs a query" >&2; exit 2; }
  query="$1"
  shift
  size="${1:-10}"
  # --get + --data-urlencode handles spaces/special chars in the query.
  curl -sf --get "${API_URL}/search" \
    --data-urlencode "q=${query}" \
    --data-urlencode "size=${size}"
  echo
}

logs() { docker compose logs -f app; }
down() { docker compose down; }

cmd="${1:-}"
[ "$#" -gt 0 ] && shift || true

case "$cmd" in
  up)     up ;;
  smoke)  smoke ;;
  index)  index "$@" ;;
  search) search "$@" ;;
  logs)   logs ;;
  down)   down ;;
  all)
    up
    index --recreate
    [ "$#" -ge 1 ] && search "$@"
    ;;
  ""|-h|--help) usage 0 ;;
  *) echo "unknown command: ${cmd}" >&2; usage 1 ;;
esac
