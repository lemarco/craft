#!/usr/bin/env bash
# Stateful workers — 3-node QUIC cluster (stateful actor showcase)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${TREMBITA_SW_CLUSTER_DIR:-$CRAFT_ROOT/target/trembita-stateful-workers-cluster}"
CERTS="$DEV/certs"
SEED="1@127.0.0.1:8190"
BIN="trembita-showcase-stateful-workers"
CLUSTER_PORTS=(8190 8191 8192 7873)

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
    echo "stateful-workers cluster (wire+HTTP same port; forward :8190 on node 1):"
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
        gw=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            'http://127.0.0.1:8190/orders/submit?user=health&token=showcase' \
            -H 'content-type: application/json' -d '{"order_id":0}' 2>/dev/null || echo 000)
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8190/health 2>/dev/null || echo 000)
        [ "$gw" = 202 ] && [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "HTTP POST /orders/submit → $gw"
    echo "HTTP GET  /health             → $adm"
    [ "$gw" = 202 ] && [ "$adm" = 200 ] && echo "OK: cluster ready" || echo "not ready"
}

migrate_env() {
    export TREMBITA_MIGRATE_DEMO=1
}

run_node_migrate() {
    local id=$1 listen=$2
    migrate_env
    cluster_run_node "$id" "$listen"
}

migrate_run() {
    echo ">> POST /demo/migrate/run on node 1"
    curl -sf -X POST "http://127.0.0.1:8190/demo/migrate/run" -H 'content-type: application/json' -d '{}' \
        && echo && echo "OK: migration triggered" \
        || echo "failed — start ./cluster.sh 1-migrate and ./cluster.sh 2-migrate first"
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
    echo "Migration demo: ./cluster.sh 1-migrate | 2-migrate, then ./cluster.sh migrate-run"
}

run_node() {
    local id=$1 listen=$2
    cluster_run_node "$id" "$listen" "${3:-http}"
}

run_node_bg() {
    local id=$1 listen=$2
    cluster_run_node_bg "$id" "$listen" "${3:-http}"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:8190
    run_node_bg 2 127.0.0.1:8191
    run_node_bg 3 127.0.0.1:8192
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
  migrate-run) migrate_run ;;
  1-migrate) run_node_migrate 1 127.0.0.1:8190 ;;
  2-migrate) run_node_migrate 2 127.0.0.1:8191 ;;
  1) run_node 1 127.0.0.1:8190 ;;
  2) run_node 2 127.0.0.1:8191 ;;
  3) run_node 3 127.0.0.1:8192 ;;
  4) run_node 4 127.0.0.1:7873 no-http ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | migrate-run | 1-migrate | 2-migrate | 1 | 2 | 3 | 4" >&2; exit 1 ;;
esac
