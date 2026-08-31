#!/usr/bin/env bash
# Real-time sessions — 3-node QUIC cluster (tier B WebSocket showcase)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${CRAFTY_RT_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-realtime-cluster}"
CERTS="$DEV/certs"
SEED="1@127.0.0.1:7743"
BIN="crafty-showcase-realtime"
CLUSTER_PORTS=(7743 7753 7763 7773 8290 8291 8292 9380 9381 9382 9383)

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
    echo "realtime cluster ports (forward WS 8290 + admin 9380):"
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
    local adm=000 i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9380/health 2>/dev/null || echo 000)
        [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "admin GET /health → $adm"
    [ "$adm" = 200 ] && echo "OK: cluster ready (connect ws://127.0.0.1:8290/ws?user=alice)" || echo "not ready"
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

run_node_bg() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_run_node_bg "$id" "$listen" "$admin" "$gateway"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:7743 "${CLUSTER_ADMIN_BIND}:9380" 127.0.0.1:8290
    run_node_bg 2 127.0.0.1:7753 "${CLUSTER_ADMIN_BIND}:9381" 127.0.0.1:8291
    run_node_bg 3 127.0.0.1:7763 "${CLUSTER_ADMIN_BIND}:9382" 127.0.0.1:8292
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
  1) run_node 1 127.0.0.1:7743 "${CLUSTER_ADMIN_BIND}:9380" 127.0.0.1:8290 ;;
  2) run_node 2 127.0.0.1:7753 "${CLUSTER_ADMIN_BIND}:9381" 127.0.0.1:8291 ;;
  3) run_node 3 127.0.0.1:7763 "${CLUSTER_ADMIN_BIND}:9382" 127.0.0.1:8292 ;;
  4) run_node 4 127.0.0.1:7773 "${CLUSTER_ADMIN_BIND}:9383" - ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | 1 | 2 | 3 | 4" >&2; exit 1 ;;
esac
