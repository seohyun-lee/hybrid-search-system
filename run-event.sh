#!/usr/bin/env sh
# Run the event-driven indexing worker: Kafka image-events -> OpenSearch.
# Same single consumer, two ways to run it:
#   docker -> `docker compose up -d worker` (scalable: add --scale worker=N)
#   bare   -> a host process via uv, no docker
#
# Prereq: the topic must exist (run once) -> `./run.sh kafka-init`, and the
# manifest must be replayed onto Kafka -> `./run.sh publish` for the worker to
# have anything to consume. This script only RUNS the consumer.
# Bare mode writes a pid to ./.run and logs to ./logs (both gitignored).
#
# Usage:
#   ./run-event.sh docker              # start the worker via docker compose
#   ./run-event.sh bare                # start the worker as a host process
#   ./run-event.sh down  [docker|bare] # stop (default: try both)
#   ./run-event.sh logs  [docker|bare] # tail logs (default: docker)
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/logs"

usage() {
  cat >&2 <<'EOF'
Usage:
  ./run-event.sh docker              # start the worker via docker compose
  ./run-event.sh bare                # start the worker as a host process
  ./run-event.sh down  [docker|bare] # stop (default: try both)
  ./run-event.sh logs  [docker|bare] # tail logs (default: docker)
EOF
  exit "${1:-0}"
}

# ── docker mode ──────────────────────────────────────────────────────────────
up_docker() { docker compose up -d --build worker; echo "worker up (docker)"; }
down_docker() { docker compose rm -sf worker; }
logs_docker() { docker compose logs -f worker; }

# ── bare mode ────────────────────────────────────────────────────────────────
up_bare() {
  mkdir -p "$RUN_DIR" "$LOG_DIR"
  pidf="$RUN_DIR/event-worker.pid"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    echo "worker already running (pid $(cat "$pidf"))"; exit 0
  fi
  nohup uv run --no-dev python -m hybridsearch.events.consumer \
    >"$LOG_DIR/event-worker.log" 2>&1 &
  echo $! >"$pidf"
  echo "worker started (pid $!) -> $LOG_DIR/event-worker.log"
}
down_bare() {
  pidf="$RUN_DIR/event-worker.pid"
  [ -f "$pidf" ] || { echo "no bare worker pid"; return 0; }
  pid=$(cat "$pidf")
  if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null && echo "worker stopped (pid $pid)"; fi
  rm -f "$pidf"
}
logs_bare() { tail -n 50 -f "$LOG_DIR/event-worker.log"; }

cmd="${1:-}"
[ "$#" -gt 0 ] && shift || true
case "$cmd" in
  docker) up_docker ;;
  bare)   up_bare ;;
  down)   case "${1:-}" in
            docker) down_docker ;;
            bare)   down_bare ;;
            *)      down_docker 2>/dev/null || true; down_bare ;;
          esac ;;
  logs)   case "${1:-}" in bare) logs_bare ;; *) logs_docker ;; esac ;;
  ""|-h|--help) usage 0 ;;
  *) echo "unknown command: $cmd" >&2; usage 1 ;;
esac
