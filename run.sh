#!/usr/bin/env sh
# Build & run the hybrid search stack (OpenSearch + API), index, and query.
#
# Usage:
#   ./run.sh up                 # build + start opensearch & app
#   ./run.sh index [--recreate] # index the manifest into OpenSearch
#   ./run.sh search "<query>"   # run a hybrid search via the API
#   ./run.sh logs               # tail the app logs
#   ./run.sh down               # stop & remove containers
#   ./run.sh all "<query>"      # up -> index --recreate -> search
set -eu

API_URL="${API_URL:-http://localhost:8000}"

usage() {
  sed -n '2,12p' "$0"
  exit "${1:-0}"
}

up() {
  docker compose up -d --build
  echo "stack up. API: ${API_URL}  (docs: ${API_URL}/docs)"
}

index() {
  # Pass extra args through, e.g. --recreate. One-off container, removed after.
  docker compose run --rm app \
    uv run --no-dev python -m hybridsearch.index.run_from_manifest "$@"
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
