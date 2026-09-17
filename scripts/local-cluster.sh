#!/usr/bin/env bash
# local-cluster.sh — B-39 local cluster + B-42 optional 4th elastic joiner.
#
#   ./scripts/local-cluster.sh setup          # certs + release build (realtime default)
#   ./scripts/local-cluster.sh up             # 3 nodes in background
#   ./scripts/local-cluster.sh elastic-up     # 4 nodes (staged join, same as --nodes 4)
#   ./scripts/local-cluster.sh session-smoke  # login :8290 → /me on :8291
#   ./scripts/local-cluster.sh lb-up          # nginx round-robin on :18290 (Docker)
#   ./scripts/local-cluster.sh lb-smoke       # /ready spread via LB or direct ports
#   ./scripts/local-cluster.sh cap-smoke      # PerNode GET /e2e/whoami (realtime; needs lb-up)
#   ./scripts/local-cluster.sh elastic-smoke  # lb-smoke + session-smoke + cap-smoke
#   ./scripts/local-cluster.sh stop
#
# Prefer debug CLI when available:
#   cargo build -p trembita-cli
#   ./target/debug/trembita dev cluster-up --setup [--lb] [--nodes 4]
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${TREMBITA_CLI:-$ROOT/target/debug/trembita}"
SHOWCASE="${TREMBITA_LOCAL_CLUSTER_SHOWCASE:-realtime}"
LB_PORT="${LOCAL_CLUSTER_LB_PORT:-18290}"
TOKEN="${GATEWAY_TOKEN:-dev-secret}"
SECRET="${TREMBITA_GATEWAY_SESSION_SECRET:-trembita-local-3node-dev-secret}"
NODES="${LOCAL_CLUSTER_NODES:-3}"

case "$SHOWCASE" in
  realtime) BASE_PORT=8290 ;;
  background-jobs) BASE_PORT=8090 ;;
  stateful-workers) BASE_PORT=8190 ;;
  workflows) BASE_PORT=8490 ;;
  *)
    echo "error: unknown TREMBITA_LOCAL_CLUSTER_SHOWCASE=$SHOWCASE" >&2
    exit 1
    ;;
esac

port_for_node() {
  echo $((BASE_PORT + $1 - 1))
}

node_ready() {
  curl -sf -m 2 "http://127.0.0.1:$(port_for_node "$1")/ready" >/dev/null 2>&1
}

lb_min_distinct() {
  if node_ready 4; then
    echo 3
  else
    echo 2
  fi
}

max_nodes_for_smoke() {
  if node_ready 4; then
    echo 4
  else
    echo 3
  fi
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
  "$CLI" dev cluster-up --showcase "$SHOWCASE" --nodes "$NODES" "$@"
}

elastic_up() {
  NODES=4 up "$@"
}

stop() {
  ensure_cli
  "$CLI" dev stop --showcase "$SHOWCASE"
  "$CLI" dev cluster-lb-down 2>/dev/null || true
}

lb_up() {
  ensure_cli
  local lb_nodes=3
  if node_ready 4; then
    lb_nodes=4
  fi
  "$CLI" dev cluster-lb-up --showcase "$SHOWCASE" --nodes "$lb_nodes"
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
    -H "X-Trembita-User: local-lb-smoke" \
    -c "$COOKIE_JAR" >/dev/null
  echo ">> GET /me on $peer_host (cluster session cookie)"
  me=$(curl -sf -m 5 "http://${peer_host}/me" -b "$COOKIE_JAR")
  if [ "$me" != '{"user":"local-lb-smoke"}' ] && [ "$me" != "local-lb-smoke" ]; then
    echo "FAIL: unexpected /me body: $me" >&2
    exit 1
  fi
  echo "PASS: cluster session cookie accepted on peer gateway"
}

lb_smoke() {
  local host distinct=0 seen="" nid tries min_distinct max_n
  min_distinct=$(lb_min_distinct)
  max_n=$(max_nodes_for_smoke)
  if curl -sf -m 1 "http://127.0.0.1:${LB_PORT}/ready" >/dev/null 2>&1; then
    host="127.0.0.1:${LB_PORT}"
    echo ">> LB /ready on :${LB_PORT} (expect ≥${min_distinct} distinct node_id)"
  else
    host=""
    echo ">> direct /ready on nodes (no LB — run lb-up for round-robin)"
  fi
  for tries in $(seq 1 48); do
    if [ -n "$host" ]; then
      nid=$(curl -sf -m 2 "http://${host}/ready" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2 || true)
    else
      local n=$(( (tries % max_n) + 1 ))
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
  if [ "$distinct" -lt "$min_distinct" ]; then
    echo "FAIL: only saw $distinct distinct node_id(s) (need ≥${min_distinct}):$seen" >&2
    exit 1
  fi
  echo "PASS: saw $distinct backends:$seen"
}

cap_smoke() {
  if [ "$SHOWCASE" != "realtime" ]; then
    echo "skip: cap-smoke needs realtime showcase (/e2e/whoami)" >&2
    exit 0
  fi
  local host cap_seen="" cap_distinct=0 nid
  if curl -sf -m 1 "http://127.0.0.1:${LB_PORT}/ready" >/dev/null 2>&1; then
    host="127.0.0.1:${LB_PORT}"
    echo ">> PerNode cap GET /e2e/whoami via LB :${LB_PORT}"
  else
    echo "FAIL: LB not up — run lb-up first" >&2
    exit 1
  fi
  for _ in $(seq 1 36); do
    nid=$(curl -sf -m 2 "http://${host}/e2e/whoami" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2 || true)
    [ -z "$nid" ] && continue
    case " $cap_seen " in
      *" $nid "*) ;;
      *)
        cap_seen="$cap_seen $nid"
        cap_distinct=$((cap_distinct + 1))
        ;;
    esac
  done
  if [ "$cap_distinct" -lt 2 ]; then
    echo "FAIL: cap route only hit $cap_distinct node(s):$cap_seen" >&2
    exit 1
  fi
  echo "PASS: inline cap served from $cap_distinct nodes via LB:$cap_seen"
}

elastic_smoke() {
  lb_smoke
  if [ "$SHOWCASE" = "realtime" ]; then
    session_smoke
    session_smoke --lb
    cap_smoke
  else
    echo ">> skip session/cap smoke (showcase=$SHOWCASE)"
  fi
}

usage() {
  sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
  setup) setup ;;
  up)
    shift
    while [ $# -gt 0 ]; do
      case "$1" in
        --nodes)
          NODES="$2"
          shift 2
          ;;
        *)
          break
          ;;
      esac
    done
    up "$@"
    ;;
  elastic-up) shift; elastic_up "$@" ;;
  stop) stop ;;
  lb-up) lb_up ;;
  lb-down) lb_down ;;
  session-smoke) shift; session_smoke "${1:-}" ;;
  lb-smoke) lb_smoke ;;
  cap-smoke) cap_smoke ;;
  elastic-smoke) elastic_smoke ;;
  -h|--help) usage ;;
  *)
    echo "error: usage: $0 setup|up|elastic-up|stop|session-smoke|lb-up|lb-down|lb-smoke|cap-smoke|elastic-smoke" >&2
    exit 1
    ;;
esac
