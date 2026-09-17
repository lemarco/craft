#!/usr/bin/env bash
# founder-cluster.sh — B-39 local 3-node cluster (shared session secret + LB curl).
#
#   ./scripts/founder-cluster.sh setup          # certs + release build (realtime default)
#   ./scripts/founder-cluster.sh up             # 3 nodes in background
#   ./scripts/founder-cluster.sh session-smoke  # login :8290 → /me on :8291
#   ./scripts/founder-cluster.sh lb-up          # nginx round-robin on :18290 (Docker)
#   ./scripts/founder-cluster.sh lb-smoke       # /ready spread via LB or direct ports
#   ./scripts/founder-cluster.sh stop
#
# Prefer debug CLI when available:
#   cargo build -p trembita-cli
#   ./target/debug/trembita dev cluster-up --setup [--lb]
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${TREMBITA_CLI:-$ROOT/target/debug/trembita}"
SHOWCASE="${TREMBITA_FOUNDER_SHOWCASE:-realtime}"
LB_PORT="${FOUNDER_LB_PORT:-18290}"
TOKEN="${GATEWAY_TOKEN:-dev-secret}"
SECRET="${TREMBITA_GATEWAY_SESSION_SECRET:-trembita-founder-3node-dev-secret}"

case "$SHOWCASE" in
  realtime) BASE_PORT=8290 ;;
  background-jobs) BASE_PORT=8090 ;;
  stateful-workers) BASE_PORT=8190 ;;
  workflows) BASE_PORT=8490 ;;
  *)
    echo "error: unknown TREMBITA_FOUNDER_SHOWCASE=$SHOWCASE" >&2
    exit 1
    ;;
esac

port_for_node() {
  echo $((BASE_PORT + $1 - 1))
}

ensure_cli() {
  if [ ! -x "$CLI" ]; then
    echo ">> building trembita-cli (debug)"
    cargo build -p trembita-cli --manifest-path "$ROOT/Cargo.toml"
  fi
}

setup() {
  ensure_cli
  "$CLI" dev setup --showcase "$SHOWCASE"
}

up() {
  ensure_cli
  export TREMBITA_GATEWAY_SESSION_SECRET="$SECRET"
  "$CLI" dev cluster-up --showcase "$SHOWCASE" --nodes 3 "$@"
}

stop() {
  ensure_cli
  "$CLI" dev stop --showcase "$SHOWCASE"
  "$CLI" dev cluster-lb-down 2>/dev/null || true
}

lb_up() {
  ensure_cli
  "$CLI" dev cluster-lb-up --showcase "$SHOWCASE"
}

lb_down() {
  ensure_cli
  "$CLI" dev cluster-lb-down
}

session_smoke() {
  local use_lb=${1:-}
  local login_host peer_host
  if [ "$use_lb" = "--lb" ]; then
    login_host="127.0.0.1:${LB_PORT}"
    peer_host="$login_host"
  else
    login_host="127.0.0.1:$(port_for_node 1)"
    peer_host="127.0.0.1:$(port_for_node 2)"
  fi
  COOKIE_JAR="$(mktemp)"
  trap 'rm -f "$COOKIE_JAR"' RETURN
  echo ">> POST /login on $login_host"
  curl -sf -m 5 -X POST "http://${login_host}/login" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "X-Trembita-User: founder-lb" \
    -c "$COOKIE_JAR" >/dev/null
  echo ">> GET /me on $peer_host (cluster session cookie)"
  me=$(curl -sf -m 5 "http://${peer_host}/me" -b "$COOKIE_JAR")
  if [ "$me" != '{"user":"founder-lb"}' ] && [ "$me" != "founder-lb" ]; then
    echo "FAIL: unexpected /me body: $me" >&2
    exit 1
  fi
  echo "PASS: cluster session cookie accepted on peer gateway"
}

lb_smoke() {
  local host distinct=0 seen="" nid tries
  if curl -sf -m 1 "http://127.0.0.1:${LB_PORT}/ready" >/dev/null 2>&1; then
    host="127.0.0.1:${LB_PORT}"
    echo ">> LB /ready on :${LB_PORT}"
  else
    host=""
    echo ">> direct /ready on nodes (no LB — run lb-up for round-robin)"
  fi
  for tries in $(seq 1 36); do
    if [ -n "$host" ]; then
      nid=$(curl -sf -m 2 "http://${host}/ready" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2 || true)
    else
      local n=$(( (tries % 3) + 1 ))
      nid=$(curl -sf -m 2 "http://127.0.0.1:$(port_for_node "$n")/ready" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2 || true)
    fi
    [ -z "$nid" ] && continue
    case " $seen " in
      *" $nid "*) ;;
      *)
        seen="$seen $nid"
        distinct=$((distinct + 1))
        ;;
    esac
  done
  if [ "$distinct" -lt 2 ]; then
    echo "FAIL: only saw $distinct distinct node_id(s):$seen" >&2
    exit 1
  fi
  echo "PASS: saw $distinct backends:$seen"
}

usage() {
  sed -n '3,14p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
  setup) setup ;;
  up) shift; up "$@" ;;
  stop) stop ;;
  lb-up) lb_up ;;
  lb-down) lb_down ;;
  session-smoke) shift; session_smoke "${1:-}" ;;
  lb-smoke) lb_smoke ;;
  -h|--help) usage ;;
  *)
    echo "error: usage: $0 setup|up|stop|session-smoke|lb-up|lb-down|lb-smoke" >&2
    exit 1
    ;;
esac
