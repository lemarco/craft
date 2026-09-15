#!/usr/bin/env bash
# Background jobs — 3-node QUIC cluster (queue showcase)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
source "$CRAFT_ROOT/dev/cluster-common.sh"

DEV="${TREMBITA_BG_JOBS_CLUSTER_DIR:-$CRAFT_ROOT/target/trembita-bg-jobs-cluster}"
CERTS="$DEV/certs"
SEED="1@127.0.0.1:8090"
BIN="trembita-showcase-background-jobs"
CLUSTER_PORTS=(8090 8091 8092 7573)

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
    echo "background-jobs cluster (wire+HTTP :8090 on node 1):"
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
        gw=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8090/jobs/emails \
            -H 'content-type: application/json' -d '{"payload":"health"}' 2>/dev/null || echo 000)
        adm=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8090/health 2>/dev/null || echo 000)
        [ "$gw" = 202 ] && [ "$adm" = 200 ] && break
        sleep 1
    done
    echo "HTTP POST /jobs/emails → $gw"
    echo "HTTP GET  /health      → $adm"
    [ "$gw" = 202 ] && [ "$adm" = 200 ] && echo "OK: cluster ready" || echo "not ready"
}

node_env() {
    local id=$1 listen=$2 http_mode=${3:-http}
    cluster_prepare_node "$id" "$listen" "$http_mode"
    export TREMBITA_JOB_QUEUE=emails
    export TREMBITA_JOB_QUEUE_LEASE_SECS=300
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
    local id=$1 listen=$2 http_mode=${3:-http}
    node_env "$id" "$listen" "$http_mode"
    cluster_run_node "$id" "$listen" "$http_mode"
}

run_node_bg() {
    local id=$1 listen=$2 http_mode=${3:-http}
    node_env "$id" "$listen" "$http_mode"
    cluster_run_node_bg "$id" "$listen" "$http_mode"
}

up() {
    cluster_stop
    rm -rf "$DEV/logs"
    run_node_bg 1 127.0.0.1:8090
    run_node_bg 2 127.0.0.1:8091
    run_node_bg 3 127.0.0.1:8092
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
  1) run_node 1 127.0.0.1:8090 ;;
  2) run_node 2 127.0.0.1:8091 ;;
  3) run_node 3 127.0.0.1:8092 ;;
  4) run_node 4 127.0.0.1:7573 no-http ;;
  *) echo "usage: $0 setup | reset | stop | up | logs [N] | status | health | 1 | 2 | 3 | 4" >&2; exit 1 ;;
esac
