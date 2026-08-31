#!/usr/bin/env bash
# Shared helpers for product showcase cluster.sh scripts.
# Source from examples/*/cluster.sh — do not execute directly.
#
#   source "$CRAFT_ROOT/dev/cluster-common.sh"
#   cluster_common_init "$ROOT" "$BIN_NAME" "$DEV_DIR" "$CERTS_DIR" "$PEERS"
#
set -euo pipefail

cluster_common_init() {
    CLUSTER_ROOT="${1:?root}"
    CLUSTER_BIN="${2:?binary name}"
    CLUSTER_DEV="${3:?dev dir}"
    CLUSTER_CERTS="${4:?certs dir}"
    CLUSTER_PEERS="${5:?peers}"
    CLUSTER_ADMIN_BIND="${CRAFTY_DEV_ADMIN_BIND:-0.0.0.0}"
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

cluster_node_env_base() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    export CRAFTY_NODE_ID="$id"
    export CRAFTY_LISTEN="$listen"
    export CRAFTY_ADMIN="$admin"
    export CRAFTY_GATEWAY="$gateway"
    export CRAFTY_DATA_DIR="$CLUSTER_DEV/data/node-$id"
    export CRAFTY_PEERS="$CLUSTER_PEERS"
    export CRAFTY_CA_CERT="$CLUSTER_CERTS/ca.pem"
    export CRAFTY_NODE_CERT="$CLUSTER_CERTS/node-$id.pem"
    export CRAFTY_NODE_KEY="$CLUSTER_CERTS/node-$id.key"
    export RUST_LOG="${RUST_LOG:-info,showcase=debug,crafty=info,crafty_net=warn}"
}

cluster_setup_certs() {
    local ids=("$@")
    mkdir -p "$CLUSTER_DEV/data"/{node-1,node-2,node-3,node-4}
    if [ ! -f "$CLUSTER_CERTS/ca.pem" ]; then
        echo ">> minting cluster CA + node certs in $CLUSTER_CERTS"
        bash "$CRAFT_ROOT/dev/certs/generate.sh" --ca-only --out "$CLUSTER_CERTS"
        for id in "${ids[@]}"; do
            bash "$CRAFT_ROOT/dev/certs/generate.sh" --node-id "$id" --out "$CLUSTER_CERTS" \
                --ca "$CLUSTER_CERTS/ca.pem" --ca-key "$CLUSTER_CERTS/ca.key"
        done
    else
        echo ">> reusing certs in $CLUSTER_CERTS"
    fi
}

cluster_build_showcase() {
    echo ">> building showcase (release)"
    cargo build --manifest-path "$CLUSTER_ROOT/Cargo.toml" --release
}

cluster_build_client() {
    echo ">> building crafty-showcase-client"
    cargo build -p crafty-showcase-client --manifest-path "$CRAFT_ROOT/Cargo.toml"
}

cluster_setup_all() {
    cluster_setup_certs "$@"
    cluster_build_showcase
    cluster_build_client
}

cluster_run_node() {
    local id=$1 listen=$2 admin=$3 gateway=$4
    [ -f "$CLUSTER_CERTS/node-$id.pem" ] || { echo "error: run ./cluster.sh setup first" >&2; exit 1; }
    cluster_require_port_free QUIC "${listen##*:}"
    [ "$gateway" != "-" ] && cluster_require_port_free gateway "${gateway##*:}"
    [ "$admin" != "-" ] && cluster_require_port_free admin "${admin##*:}"
    mkdir -p "$CRAFTY_DATA_DIR"
    echo ">> node $id  QUIC=$listen  admin=$admin  gateway=$gateway"
    exec "$CLUSTER_ROOT/target/release/$CLUSTER_BIN"
}

cluster_run_node_bg() {
    local id=$1
    local log="$CLUSTER_DEV/logs/node-$id.log"
    mkdir -p "$CLUSTER_DEV/logs"
    nohup "$CLUSTER_ROOT/target/release/$CLUSTER_BIN" >>"$log" 2>&1 &
    echo ">> node $id pid=$! log=$log"
}

cluster_logs_tail() {
    local id="${1:-1}"
    tail -f "$CLUSTER_DEV/logs/node-$id.log"
}
