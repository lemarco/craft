#!/usr/bin/env bash
# Realtime — 3-node QUIC cluster (tier B: WebSocket + ActorSession)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${CRAFTY_RT_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-realtime-cluster}"
CERTS="$DEV/certs"
PEERS="1@127.0.0.1:7743,2@127.0.0.1:7753,3@127.0.0.1:7763"
BIN="crafty-showcase-realtime"
CLUSTER_PORTS=(7743 7753 7763 8294 8295 8296 9380 9381 9382)

cluster_common_init "$ROOT" "$BIN" "$DEV" "$CERTS" "$PEERS"

health() {
    local adm=000 i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9380/health 2>/dev/null || echo 000)
        [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "admin GET /health → $adm"
    CLIENT="$CRAFT_ROOT/target/debug/crafty-showcase-client"
    if [ -x "$CLIENT" ]; then
        if reply=$("$CLIENT" ws 127.0.0.1:8294 health ping 2>/dev/null) && echo "$reply" | rg -q 'ok:|session open'; then
            echo "websocket /ws → OK ($reply)"
            echo "OK: cluster ready"
        else
            echo "websocket /ws → failed ($reply)"
        fi
    elif command -v websocat >/dev/null 2>&1; then
        if printf 'ping\n' | timeout 3 websocat -1 'ws://127.0.0.1:8294/ws?user=health' 2>/dev/null | rg -q 'ok:|session open'; then
            echo "websocket /ws → OK"
            echo "OK: cluster ready"
        else
            echo "websocket /ws → failed (ensure nodes 2+3 running)"
        fi
    else
        [ "$adm" = 200 ] && echo "OK: admin up (build crafty-showcase-client or install websocat)" || echo "not ready"
    fi
}

node_env() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_node_env_base "$id" "$listen" "$admin" "$gateway"
}

reset() { cluster_stop; rm -rf "$DEV/data" "$DEV/logs"; mkdir -p "$DEV/data"/{node-1,node-2,node-3}; echo "OK"; }

setup() {
    cluster_setup_all 1 2 3
    echo "OK. Quick start: ./cluster.sh up"
}

run_node() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    node_env "$id" "$listen" "$admin" "$gateway"
    cluster_run_node "$id" "$listen" "$admin" "$gateway"
}

run_node_bg() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    node_env "$id" "$listen" "$admin" "$gateway"
    cluster_run_node_bg "$id"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:7743 "${CLUSTER_ADMIN_BIND}:9380" 127.0.0.1:8294
    run_node_bg 2 127.0.0.1:7753 "${CLUSTER_ADMIN_BIND}:9381" 127.0.0.1:8295
    run_node_bg 3 127.0.0.1:7763 "${CLUSTER_ADMIN_BIND}:9382" 127.0.0.1:8296
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
  1) run_node 1 127.0.0.1:7743 "${CLUSTER_ADMIN_BIND}:9380" 127.0.0.1:8294 ;;
  2) run_node 2 127.0.0.1:7753 "${CLUSTER_ADMIN_BIND}:9381" 127.0.0.1:8295 ;;
  3) run_node 3 127.0.0.1:7763 "${CLUSTER_ADMIN_BIND}:9382" 127.0.0.1:8296 ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | 1 | 2 | 3" >&2; exit 1 ;;
esac
