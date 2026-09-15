#!/usr/bin/env bash
# Shared helpers for product showcase cluster.sh scripts.
# Source from examples/*/cluster.sh — do not execute directly.
#
#   source "$CRAFT_ROOT/dev/cluster-common.sh"
#   cluster_common_init "$ROOT" "$BIN_NAME" "$DEV_DIR" "$CERTS_DIR" "$SEED"
#
# Multi-node layout uses dynamic join — only node 1 needs `TREMBITA_ALLOW_JOIN=1`;
# nodes 2+ set `TREMBITA_JOIN_SEEDS` to the seed address (no static `TREMBITA_PEERS` mesh).
#
# One port number per node: TREMBITA_LISTEN → QUIC (UDP) + HTTP (TCP) on the same host:port.
# Set TREMBITA_HTTP=- only for QUIC-only nodes (e.g. optional node 4).
set -euo pipefail

cluster_common_init() {
    CLUSTER_ROOT="${1:?root}"
    CLUSTER_BIN="${2:?binary name}"
    CLUSTER_DEV="${3:?dev dir}"
    CLUSTER_CERTS="${4:?certs dir}"
    CLUSTER_SEED="${5:?seed id@host:port for node 1}"
    CRAFT_ROOT="$(cd "$CLUSTER_ROOT/../.." && pwd)"
}

cluster_port_in_use() {
    ss -ltn 2>/dev/null | rg -q ":${1}\b"
}

cluster_show_port_holders() {
    ss -ltnp 2>/dev/null | rg ":${1}\b" || true
}

cluster_require_port_free() {
    local label=$1 port=$2
    if cluster_port_in_use "$port"; then
        echo "error: $label port $port already in use:" >&2
        cluster_show_port_holders "$port" >&2
        echo "hint: ./cluster.sh stop" >&2
        exit 1
    fi
}

cluster_stop() {
    echo ">> stopping $CLUSTER_BIN processes"
    pkill -f "$CLUSTER_BIN" 2>/dev/null || true
    sleep 0.5
}

cluster_display_addr() {
    local addr=$1
    if [[ "$addr" == 0.0.0.0:* ]]; then
        echo "127.0.0.1:${addr#0.0.0.0:}"
    else
        echo "$addr"
    fi
}

# $1 = QUIC+HTTP listen (host:port), $2 = optional `no-http` to set TREMBITA_HTTP=-
cluster_node_env_base() {
    local listen=$1
    local http_mode=${2:-http}
    export TREMBITA_LISTEN="$listen"
    unset TREMBITA_HTTP TREMBITA_GATEWAY
    if [ "$http_mode" = "no-http" ]; then
        export TREMBITA_HTTP=-
    fi
    export TREMBITA_DATA_DIR="$CLUSTER_DEV/data/p${listen##*:}"
    export TREMBITA_CERT_DIR="$CLUSTER_CERTS"
    unset TREMBITA_NODE_ID TREMBITA_NODE_CERT TREMBITA_NODE_KEY
    export TREMBITA_CA_CERT="$CLUSTER_CERTS/ca.pem"
    export TREMBITA_ALLOW_LEAVE=1
    export TREMBITA_GRACEFUL_LEAVE=1
    unset TREMBITA_PEERS
    if [ -z "${TREMBITA_JOIN_SEEDS:-}" ]; then
        export TREMBITA_ALLOW_JOIN=1
        unset TREMBITA_JOIN_SEEDS
    else
        unset TREMBITA_ALLOW_JOIN
    fi
    export RUST_LOG="${RUST_LOG:-info,showcase=debug,trembita=info,trembita_net=warn}"
}

cluster_setup_certs() {
    local ids=("$@")
    mkdir -p "$CLUSTER_DEV/data" "$CLUSTER_DEV/logs"
    if [ ! -f "$CLUSTER_CERTS/ca.pem" ]; then
        echo ">> minting cluster CA + node certs in $CLUSTER_CERTS"
        bash "$CRAFT_ROOT/dev/certs/generate.sh" --ca-only --out "$CLUSTER_CERTS"
        bash "$CRAFT_ROOT/dev/certs/generate.sh" --node-id 0 --out "$CLUSTER_CERTS" \
            --ca "$CLUSTER_CERTS/ca.pem" --ca-key "$CLUSTER_CERTS/ca.key"
        for id in "${ids[@]}"; do
            bash "$CRAFT_ROOT/dev/certs/generate.sh" --node-id "$id" --out "$CLUSTER_CERTS" \
                --ca "$CLUSTER_CERTS/ca.pem" --ca-key "$CLUSTER_CERTS/ca.key"
        done
    else
        echo ">> reusing certs in $CLUSTER_CERTS"
    fi
}

cluster_prepare_node() {
    local id=$1 listen=$2 http_mode=${3:-http}
    if [ "$id" = "1" ]; then
        unset TREMBITA_JOIN_SEEDS
    else
        export TREMBITA_JOIN_SEEDS="$CLUSTER_SEED"
    fi
    cluster_node_env_base "$listen" "$http_mode"
}

cluster_run_node() {
    local id=$1 listen=$2 http_mode=${3:-http}
    [ -f "$CLUSTER_CERTS/ca.pem" ] || { echo "error: run ./cluster.sh setup first" >&2; exit 1; }
    cluster_require_port_free QUIC "${listen##*:}"
    if [ "$http_mode" != "no-http" ]; then
        cluster_require_port_free HTTP "${listen##*:}"
    fi
    cluster_prepare_node "$id" "$listen" "$http_mode"
    mkdir -p "$TREMBITA_DATA_DIR"
    echo ">> node $id  wire+HTTP=$listen  data=$TREMBITA_DATA_DIR"
    if [ "$id" = "1" ]; then
        echo ">> seed node (TREMBITA_ALLOW_JOIN=1)"
    else
        echo ">> join via TREMBITA_JOIN_SEEDS=$CLUSTER_SEED"
    fi
    exec "$CLUSTER_ROOT/target/release/$CLUSTER_BIN"
}

cluster_build_showcase() {
    echo ">> building showcase (release)"
    cargo build --manifest-path "$CLUSTER_ROOT/Cargo.toml" --release
}

cluster_build_client() {
    echo ">> building trembita-showcase-client"
    cargo build -p trembita-tools --bin trembita-showcase-client --manifest-path "$CRAFT_ROOT/Cargo.toml"
}

cluster_setup_all() {
    cluster_setup_certs "$@"
    cluster_build_showcase
    cluster_build_client
}

cluster_run_node_bg() {
    local id=$1 listen=$2 http_mode=${3:-http}
    cluster_prepare_node "$id" "$listen" "$http_mode"
    local log="$CLUSTER_DEV/logs/node-$id.log"
    mkdir -p "$CLUSTER_DEV/logs" "$TREMBITA_DATA_DIR"
    nohup "$CLUSTER_ROOT/target/release/$CLUSTER_BIN" >>"$log" 2>&1 &
    echo ">> node $id pid=$! log=$log"
}

cluster_logs_tail() {
    local id="${1:-1}"
    tail -f "$CLUSTER_DEV/logs/node-$id.log"
}
