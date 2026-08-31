#!/usr/bin/env bash
# Background jobs — 3-node QUIC cluster (tier C showcase)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${CRAFTY_BG_JOBS_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-bg-jobs-cluster}"
CERTS="$DEV/certs"
PEERS="1@127.0.0.1:7543"
SEED="$PEERS"
BIN="crafty-showcase-background-jobs"
CLUSTER_PORTS=(7543 7553 7563 7573 8090 8091 8092 9180 9181 9182 9183)

cluster_common_init "$ROOT" "$BIN" "$DEV" "$CERTS" "$SEED"

stop() {
    cluster_stop
    local busy=0 port
    for port in "${CLUSTER_PORTS[@]}"; do
        if cluster_port_in_use "$port"; then
            busy=1
            echo "  still listening on :$port"
            cluster_show_port_holders "$port" | sed 's/^/    /'
        fi
    done
    [ "$busy" = 0 ] && echo "OK: cluster ports free"
}

status() {
    echo "background-jobs cluster ports (forward 8090 + 9180):"
    for port in "${CLUSTER_PORTS[@]}"; do
        if cluster_port_in_use "$port"; then
            echo "  :$port IN USE"
            cluster_show_port_holders "$port" | sed 's/^/    /'
        else
            echo "  :$port free"
        fi
    done
    pgrep -af "$BIN" 2>/dev/null | sed 's/^/  /' || echo "  (no processes)"
}

health() {
    local gw=000 adm=000 i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        gw=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8090/jobs/emails \
            -H 'content-type: application/json' -d '{"payload":"health"}' 2>/dev/null || echo 000)
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9180/health 2>/dev/null || echo 000)
        [ "$gw" = 202 ] && [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "gateway POST /jobs/emails → $gw"
    echo "admin   GET  /health       → $adm"
    [ "$gw" = 202 ] && [ "$adm" = 200 ] && echo "OK: cluster ready" || echo "not ready"
}

node_env() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_prepare_node "$id" "$listen" "$admin" "$gateway"
    export CRAFTY_JOB_QUEUE=emails
    export CRAFTY_JOB_QUEUE_LEASE_SECS=300
}

reset() {
    stop
    rm -rf "$DEV/data" "$DEV/logs"
    mkdir -p "$DEV/data"/{node-1,node-2,node-3,node-4}
    echo "OK: ./cluster.sh up  or  ./cluster.sh 1|2|3"
}

setup() {
    cluster_setup_all 1 2 3 4
    echo "OK. Quick start: ./cluster.sh up"
    echo "Or terminals: ./cluster.sh 1 | 2 | 3"
}

run_node() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    node_env "$id" "$listen" "$admin" "$gateway"
    cluster_run_node "$id" "$listen" "$admin" "$gateway"
}

run_node_bg() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    node_env "$id" "$listen" "$admin" "$gateway"
    cluster_run_node_bg "$id" "$listen" "$admin" "$gateway"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:7543 "${CLUSTER_ADMIN_BIND}:9180" 127.0.0.1:8090
    run_node_bg 2 127.0.0.1:7553 "${CLUSTER_ADMIN_BIND}:9181" 127.0.0.1:8091
    run_node_bg 3 127.0.0.1:7563 "${CLUSTER_ADMIN_BIND}:9182" 127.0.0.1:8092
    echo ">> waiting for health"
    sleep 3
    health
}

case "${1:-}" in
  setup) setup ;;
  reset) reset ;;
  stop) stop ;;
  up) up ;;
  logs) cluster_logs_tail "${2:-1}" ;;
  status) status ;;
  health) health ;;
  1) run_node 1 127.0.0.1:7543 "${CLUSTER_ADMIN_BIND}:9180" 127.0.0.1:8090 ;;
  2) run_node 2 127.0.0.1:7553 "${CLUSTER_ADMIN_BIND}:9181" 127.0.0.1:8091 ;;
  3) run_node 3 127.0.0.1:7563 "${CLUSTER_ADMIN_BIND}:9182" 127.0.0.1:8092 ;;
  4) run_node 4 127.0.0.1:7573 "${CLUSTER_ADMIN_BIND}:9183" - ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | 1 | 2 | 3 | 4" >&2; exit 1 ;;
esac
