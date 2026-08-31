#!/usr/bin/env bash
# Self-update — 3-node QUIC cluster (upgrade-coordinator showcase)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${CRAFTY_SELF_UPDATE_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-self-update-cluster}"
CERTS="$DEV/certs"
PEERS="1@127.0.0.1:7643,2@127.0.0.1:7653,3@127.0.0.1:7663"
SEED="1@127.0.0.1:7643"
BIN="crafty-showcase-self-update"
CLUSTER_PORTS=(7643 7653 7663 8190 8191 8192 9280 9281 9282)

cluster_common_init "$ROOT" "$BIN" "$DEV" "$CERTS" "$SEED"

stop() { cluster_stop; }
status() { pgrep -af "$BIN" 2>/dev/null || echo "(no processes)"; }

node_env() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    cluster_prepare_node "$id" "$listen" "$admin" "$gateway"
    export CRAFTY_NODE_ID="$id"
    export CRAFTY_UPGRADE_DRY_RUN="${CRAFTY_UPGRADE_DRY_RUN:-1}"
}

reset() {
    stop
    rm -rf "$DEV/data" "$DEV/logs" "$DEV/artifacts"
    mkdir -p "$DEV/data"/{p7643,p7653,p7663} "$DEV/artifacts"
    echo "OK: ./cluster.sh up"
}

setup() {
    cluster_setup_all 1 2 3
    mkdir -p "$DEV/artifacts"
    echo "demo artifact" >"$DEV/artifacts/app-9.9.9.bin"
    echo "OK. ./cluster.sh up && ./trigger-upgrade.sh"
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
    run_node_bg 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9280" 127.0.0.1:8190
    run_node_bg 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9281" 127.0.0.1:8191
    run_node_bg 3 127.0.0.1:7663 "${CLUSTER_ADMIN_BIND}:9282" 127.0.0.1:8192
    sleep 3
    curl -sf http://127.0.0.1:8190/cluster/upgrade | head -c 200 || true
    echo
}

case "${1:-}" in
  setup) setup ;;
  reset) reset ;;
  stop) stop ;;
  up) up ;;
  logs) cluster_logs_tail "${2:-1}" ;;
  status) status ;;
  1) run_node 1 127.0.0.1:7643 "${CLUSTER_ADMIN_BIND}:9280" 127.0.0.1:8190 ;;
  2) run_node 2 127.0.0.1:7653 "${CLUSTER_ADMIN_BIND}:9281" 127.0.0.1:8191 ;;
  3) run_node 3 127.0.0.1:7663 "${CLUSTER_ADMIN_BIND}:9282" 127.0.0.1:8192 ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | 1 | 2 | 3" >&2; exit 1 ;;
esac
