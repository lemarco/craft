#!/usr/bin/env bash
# Workflows — 3-node QUIC cluster (saga coordination showcase)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${TREMBITA_WORKFLOWS_CLUSTER_DIR:-$CRAFT_ROOT/target/trembita-workflows-cluster}"
CERTS="$DEV/certs"
SEED="1@127.0.0.1:7643"
BIN="trembita-showcase-workflows"
CLUSTER_PORTS=(7643 7653 7663 7673 8490 8491 8492 9480 9481 9482 9483)

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
    echo "workflows cluster ports:"
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
    local gw=000 adm=000
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        gw=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8490/workflows/run \
            -H 'content-type: application/json' -d '{"saga_id":"health"}' 2>/dev/null || echo 000)
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9480/health 2>/dev/null || echo 000)
        [ "$gw" = 200 ] && [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "gateway POST /workflows/run → $gw"
    echo "admin   GET  /health       → $adm"
    [ "$gw" = 200 ] && [ "$adm" = 200 ] && echo "OK: cluster ready" || echo "not ready"
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
    cluster_run_node "$id" "$listen" "$admin" "$gateway"
}

node_env() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_prepare_node "$id" "$listen" "$admin" "$gateway"
    env | rg '^TREMBITA_' | sort
}

run_node_bg() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_run_node_bg "$id" "$listen" "$admin" "$gateway"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9480" 127.0.0.1:8490
    run_node_bg 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9481" 127.0.0.1:8491
    run_node_bg 3 127.0.0.1:7663 "${CLUSTER_ADMIN_BIND}:9482" 127.0.0.1:8492
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
  1) run_node 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9480" 127.0.0.1:8490 ;;
  2) run_node 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9481" 127.0.0.1:8491 ;;
  3) run_node 3 127.0.0.1:7663 "${CLUSTER_ADMIN_BIND}:9482" 127.0.0.1:8492 ;;
  4) run_node 4 127.0.0.1:7673 "${CLUSTER_ADMIN_BIND}:9483" - ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | 1 | 2 | 3 | 4" >&2; exit 1 ;;
esac
