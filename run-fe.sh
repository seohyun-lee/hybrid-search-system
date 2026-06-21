#!/usr/bin/env sh
# Run the search front-of-house: API (8000) + Streamlit FE (8501).
# FE can't work without the coordinator, so the two ship together here.
# Same two apps, two ways to run them:
#   docker -> `docker compose up -d app fe` (FE reaches the API over the compose net)
#   bare   -> host processes via uv, no docker (FE reaches the API on localhost)
#
# Data upload/indexing lives in run.sh; this script only RUNS the query apps.
# Bare mode writes pids to ./.run and logs to ./logs (both gitignored).
#
# Usage:
#   ./run-fe.sh docker              # start API+FE via docker compose
#   ./run-fe.sh bare                # start API+FE as background host processes
#   ./run-fe.sh down  [docker|bare] # stop (default: try both)
#   ./run-fe.sh logs  [docker|bare] # tail logs (default: docker)
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/logs"
API_PORT="${API_PORT:-8000}"
FE_PORT="${FE_PORT:-8501}"
# Same host in bare mode, so the FE talks to the API on localhost.
HS_API_URL="${HS_API_URL:-http://localhost:$API_PORT}"

usage() {
  cat >&2 <<'EOF'
Usage:
  ./run-fe.sh docker              # start API+FE via docker compose
  ./run-fe.sh bare                # start API+FE as background host processes
  ./run-fe.sh down  [docker|bare] # stop (default: try both)
  ./run-fe.sh logs  [docker|bare] # tail logs (default: docker)
EOF
  exit "${1:-0}"
}

# ── bare-mode process helpers ────────────────────────────────────────────────
start_proc() { # name logfile cmd...
  name=$1; log=$2; shift 2
  pidf="$RUN_DIR/$name.pid"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    echo "$name already running (pid $(cat "$pidf"))"; return 0
  fi
  nohup "$@" >"$log" 2>&1 &
  echo $! >"$pidf"
  echo "$name started (pid $!) -> $log"
}

stop_proc() { # name
  name=$1; pidf="$RUN_DIR/$name.pid"
  [ -f "$pidf" ] || return 0
  pid=$(cat "$pidf")
  if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null && echo "$name stopped (pid $pid)"; fi
  rm -f "$pidf"
}

# ── docker mode ──────────────────────────────────────────────────────────────
up_docker() {
  docker compose up -d --build app fe
  echo "API: http://localhost:$API_PORT/docs   FE: http://localhost:$FE_PORT"
}
down_docker() { docker compose rm -sf app fe; }
logs_docker() { docker compose logs -f app fe; }

# ── bare mode ────────────────────────────────────────────────────────────────
up_bare() {
  mkdir -p "$RUN_DIR" "$LOG_DIR"
  export HS_API_URL
  start_proc fe-api "$LOG_DIR/fe-api.log" \
    uv run --no-dev uvicorn main:app --host 0.0.0.0 --port "$API_PORT"
  start_proc fe-streamlit "$LOG_DIR/fe-streamlit.log" \
    uv run --no-dev streamlit run streamlit_app.py \
      --server.port "$FE_PORT" --server.address 0.0.0.0 --server.headless true
  echo "API: http://localhost:$API_PORT/docs   FE: http://localhost:$FE_PORT"
}
down_bare() { stop_proc fe-streamlit; stop_proc fe-api; }
logs_bare() { tail -n 50 -f "$LOG_DIR/fe-api.log" "$LOG_DIR/fe-streamlit.log"; }

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
