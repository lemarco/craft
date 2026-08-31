#!/usr/bin/env bash
# Workflows — 3-node QUIC cluster (tier A: saga + Meta-Raft journal)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${CRAFTY_WF_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-workflows-cluster}"
CERTS="$DEV/certs"
PEERS="1@127.0.0.1:7843,2@127.0.0.1:7853,3@127.0.0.1:7863"
BIN="crafty-showcase-workflows"
CLUSTER_PORTS=(7843 7853 7863 8490 8491 8492 9480 9481 9482)

cluster_common_init "$ROOT" "$BIN" "$DEV" "$CERTS" "$PEERS"

health() {
    local adm=000 tr=000 i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9480/health 2>/dev/null || echo 000)
        tr=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8490/health 2>/dev/null || echo 000)
        [ "$adm" = 200 ] && [ "$tr" = 200 ] && break
        sleep 1
    done
    echo "admin   GET /health → $adm"
    echo "trigger GET /health → $tr"
    [ "$adm" = 200 ] && [ "$tr" = 200 ] && echo "OK: cluster ready" || echo "not ready"
}

node_env() {
    local id=$1 listen=$2 admin=$3 trigger=$4
    cluster_node_env_base "$id" "$listen" "$admin" "-"
    export CRAFTY_TRIGGER="$trigger"
}

reset() {
    cluster_stop
    rm -rf "$DEV/data" "$DEV/logs"
    mkdir -p "$DEV/data"/{node-1,node-2,node-3}
    echo "OK: ./cluster.sh up  or  ./cluster.sh 1|2|3"
}

setup() {
    cluster_setup_all 1 2 3
    echo "OK. Quick start: ./cluster.sh up"
    echo "Forward 8490 (trigger) and 9480 (dashboard)."
}

run_node() {
    local id=$1 listen=$2 admin=$3 trigger=$4
    node_env "$id" "$listen" "$admin" "$trigger"
    cluster_run_node "$id" "$listen" "$admin" "-"
}

run_node_bg() {
    local id=$1 listen=$2 admin=$3 trigger=$4
    node_env "$id" "$listen" "$admin" "$trigger"
    cluster_run_node_bg "$id"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:7843 "${CLUSTER_ADMIN_BIND}:9480" 127.0.0.1:8490
    run_node_bg 2 127.0.0.1:7853 "${CLUSTER_ADMIN_BIND}:9481" 127.0.0.1:8491
    run_node_bg 3 127.0.0.1:7863 "${CLUSTER_ADMIN_BIND}:9482" 127.0.0.1:8492
    echo ">> waiting for health"
    sleep 3
    health
}

case "${1:-}" in
  setup) setup ;;
  reset) reset ;;
  stop) cluster_stop ;;
  up) up ;;
  logs) cluster_logs_tail "${2:-1}" ;;
  health) health ;;
  1) run_node 1 127.0.0.1:7843 "${CLUSTER_ADMIN_BIND}:9480" 127.0.0.1:8490 ;;
  2) run_node 2 127.0.0.1:7853 "${CLUSTER_ADMIN_BIND}:9481" 127.0.0.1:8491 ;;
  3) run_node 3 127.0.0.1:7863 "${CLUSTER_ADMIN_BIND}:9482" 127.0.0.1:8492 ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | health | 1 | 2 | 3" >&2; exit 1 ;;
esac
