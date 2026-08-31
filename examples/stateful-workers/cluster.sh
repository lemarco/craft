#!/usr/bin/env bash
# Stateful workers — 3-node QUIC cluster (tier B)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${CRAFTY_SW_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-stateful-workers-cluster}"
CERTS="$DEV/certs"
PEERS="1@127.0.0.1:7643,2@127.0.0.1:7653,3@127.0.0.1:7663"
BIN="crafty-showcase-stateful-workers"
CLUSTER_PORTS=(7643 7653 7663 8190 8191 8192 9280 9281 9282)

cluster_common_init "$ROOT" "$BIN" "$DEV" "$CERTS" "$PEERS"

health() {
    local gw=000 adm=000 i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        gw=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8190/actors/orders/cast \
            -H 'content-type: application/json' -d '{"payload":"1001"}' 2>/dev/null || echo 000)
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9280/health 2>/dev/null || echo 000)
        [ "$gw" = 202 ] && [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "gateway POST /actors/orders/cast → $gw"
    echo "admin   GET  /health              → $adm"
    [ "$gw" = 202 ] && [ "$adm" = 200 ] && echo "OK: cluster ready" || echo "not ready"
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
}

run_node() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_node_env_base "$id" "$listen" "$admin" "$gateway"
    cluster_run_node "$id" "$listen" "$admin" "$gateway"
}

run_node_bg() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_node_env_base "$id" "$listen" "$admin" "$gateway"
    cluster_run_node_bg "$id"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9280" 127.0.0.1:8190
    run_node_bg 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9281" 127.0.0.1:8191
    run_node_bg 3 127.0.0.1:7663 "${CLUSTER_ADMIN_BIND}:9282" 127.0.0.1:8192
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
  status)
    for port in "${CLUSTER_PORTS[@]}"; do
        cluster_port_in_use "$port" && echo "  :$port IN USE" || echo "  :$port free"
    done
    ;;
  health) health ;;
  1) run_node 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9280" 127.0.0.1:8190 ;;
  2) run_node 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9281" 127.0.0.1:8191 ;;
  3) run_node 3 127.0.0.1:7663 "${CLUSTER_ADMIN_BIND}:9282" 127.0.0.1:8192 ;;
  1-migrate)
    export CRAFTY_MIGRATE_DEMO=1
    run_node 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9280" 127.0.0.1:8190
    ;;
  2-migrate)
    export CRAFTY_MIGRATE_DEMO=1
    run_node 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9281" 127.0.0.1:8191
    ;;
  migrate-run)
    echo "POST http://127.0.0.1:8190/demo/migrate/run"
    curl -sf -X POST http://127.0.0.1:8190/demo/migrate/run -w '\n→ HTTP %{http_code}\n'
    ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | 1 | 2 | 3 | 1-migrate | 2-migrate | migrate-run" >&2; exit 1 ;;
esac
