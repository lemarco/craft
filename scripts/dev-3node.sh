#!/usr/bin/env bash
# dev-3node.sh — run a real 3-node crafty cluster on localhost (three terminals).
#
# One-time setup (any terminal):
#   ./scripts/dev-3node.sh setup
#
# Then one process per terminal:
#   ./scripts/dev-3node.sh 1
#   ./scripts/dev-3node.sh 2
#   ./scripts/dev-3node.sh 3
#
# Fourth terminal — hands-on demo (Raft + job queue over QUIC/mTLS):
#   ./scripts/dev-3node.sh demo          # fast smoke (~3s)
#   ./scripts/dev-3node.sh watch         # staged ~2+ min for dashboard
#
# Failover smoke (e2e harness):
#   ./scripts/dev-3node.sh queue-smoke
#
# Admin dashboards (908x avoids common 8080/8081 conflicts — k3d, qbittorrent, nginx):
#   node1 http://<host>:9080/dashboard
#   node2 http://<host>:9081/dashboard
#   node3 http://<host>:9082/dashboard
#
# Admin binds 0.0.0.0 by default so a browser on another machine (SSH session)
# can reach the dashboard via the server's IP. For localhost-only:
#   CRAFTY_DEV_ADMIN_BIND=127.0.0.1 ./scripts/dev-3node.sh 1
# Or SSH port-forward from your laptop:
#   ssh -L 9080:127.0.0.1:9080 -L 9081:127.0.0.1:9081 -L 9082:127.0.0.1:9082 lecomp
#
# See docs/scenarios/background-jobs.md for product-layer HTTP (CraftyApp gateway).

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEV="${CRAFTY_DEV_3NODE_DIR:-$ROOT/target/crafty-3node-dev}"
CERTS="$DEV/certs"
PEERS="1@127.0.0.1:7443,2@127.0.0.1:7453,3@127.0.0.1:7463"
ADMIN_BIND="${CRAFTY_DEV_ADMIN_BIND:-0.0.0.0}"

die() { echo "error: $*" >&2; exit 1; }

node_env() {
    local id=$1 listen=$2 admin=$3
    export CRAFTY_NODE_ID="$id"
    export CRAFTY_LISTEN="$listen"
    export CRAFTY_ADMIN="$admin"
    export CRAFTY_DATA_DIR="$DEV/data/node-$id"
    export CRAFTY_JOB_QUEUE=jobs
    export CRAFTY_JOB_QUEUE_LEASE_SECS=60
    export CRAFTY_PEERS="$PEERS"
    export CRAFTY_CA_CERT="$CERTS/ca.pem"
    export CRAFTY_NODE_CERT="$CERTS/node-$id.pem"
    export CRAFTY_NODE_KEY="$CERTS/node-$id.key"
    export RUST_LOG="${RUST_LOG:-info,crafty=info,crafty_net=warn}"
}

client_env() {
    export CRAFTY_NODE_ID=4
    export CRAFTY_PEERS="$PEERS"
    export CRAFTY_CA_CERT="$CERTS/ca.pem"
    export CRAFTY_NODE_CERT="$CERTS/node-4.pem"
    export CRAFTY_NODE_KEY="$CERTS/node-4.key"
    export CRAFTY_JOB_QUEUE=jobs
    [ -f "$CRAFTY_NODE_CERT" ] || die "missing $CRAFTY_NODE_CERT — run ./scripts/dev-3node.sh setup"
}

setup() {
    mkdir -p "$DEV/data"/{node-1,node-2,node-3}
    if [ ! -f "$CERTS/ca.pem" ]; then
        echo ">> minting cluster CA + node certs in $CERTS"
        bash "$ROOT/dev/certs/generate.sh" --ca-only --out "$CERTS"
        for id in 1 2 3 4; do
            bash "$ROOT/dev/certs/generate.sh" --node-id "$id" --out "$CERTS" \
                --ca "$CERTS/ca.pem" --ca-key "$CERTS/ca.key"
        done
    else
        echo ">> reusing certs in $CERTS"
    fi
    echo ">> building crafty-node (release)"
    cargo build -p crafty-node --release
    echo "OK: ready. Open three terminals and run:"
    echo "  ./scripts/dev-3node.sh 1"
    echo "  ./scripts/dev-3node.sh 2"
    echo "  ./scripts/dev-3node.sh 3"
}

run_node() {
    local id=$1 listen=$2 admin=$3
    [ -f "$CERTS/node-$id.pem" ] || die "run ./scripts/dev-3node.sh setup first"
    node_env "$id" "$listen" "$admin"
    mkdir -p "$CRAFTY_DATA_DIR"
    echo ">> node $id  QUIC=$listen  admin=$admin  data=$CRAFTY_DATA_DIR"
    exec "$ROOT/target/release/crafty-node"
}

queue_smoke() {
    [ -f "$CERTS/ca.pem" ] || die "run ./scripts/dev-3node.sh setup first"
    cargo build -p crafty-e2e-queue-client --release
    client_env
    export CRAFTY_QUEUE_SUBMIT_NODE=1
    export CRAFTY_QUEUE_WORKER_PEER=2
    export CRAFTY_QUEUE_WORKER_NODE=2
    export CRAFTY_QUEUE_WORKER_INSTANCE=1
    export CRAFTY_E2E_QUEUE_PHASE=before_failover
    export CRAFTY_QUEUE_CONTACT_NODE=1
    echo ">> enqueue + lease/ack smoke (QUIC, follower worker on node 2)"
    "$ROOT/target/release/crafty-e2e-queue-client"
}

demo() {
    [ -f "$CERTS/ca.pem" ] || die "run ./scripts/dev-3node.sh setup first"
    cargo build -p crafty-dev-client --release
    client_env
    export CRAFTY_DEV_MODE=fast
    echo ">> demo: Raft propose (node 1) + read (node 3), then job queue enqueue/lease/ack"
    "$ROOT/target/release/crafty-dev-client" fast
    _demo_admin_tail
}

watch() {
    [ -f "$CERTS/ca.pem" ] || die "run ./scripts/dev-3node.sh setup first"
    cargo build -p crafty-dev-client --release
    client_env
    echo ">> watch demo (~2+ min) — open http://127.0.0.1:9080/dashboard first"
    echo ">> pause between steps: \${CRAFTY_DEV_WATCH_PAUSE_SECS:-10}s"
    "$ROOT/target/release/crafty-dev-client" watch
    _demo_admin_tail
}

_demo_admin_tail() {
    echo ""
    echo ">> admin (any node — read-only HTTP):"
    echo "  curl -s http://127.0.0.1:9080/introspect/cluster | jq ."
    echo "  curl -s http://127.0.0.1:9080/introspect/queues | jq ."
    echo "  open http://127.0.0.1:9080/dashboard"
    if curl -sf --max-time 2 "http://127.0.0.1:9080/health" >/dev/null; then
        echo ""
        echo "cluster:"
        curl -s "http://127.0.0.1:9080/introspect/cluster"
        echo ""
        echo "queues:"
        curl -s "http://127.0.0.1:9080/introspect/queues"
        echo ""
    fi
}

usage() {
    sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
    setup) setup ;;
    1) run_node 1 "127.0.0.1:7443" "${ADMIN_BIND}:9080" ;;
    2) run_node 2 "127.0.0.1:7453" "${ADMIN_BIND}:9081" ;;
    3) run_node 3 "127.0.0.1:7463" "${ADMIN_BIND}:9082" ;;
    queue-smoke) queue_smoke ;;
    demo) demo ;;
    watch) watch ;;
    -h|--help) usage ;;
    *) die "usage: $0 setup | 1 | 2 | 3 | demo | watch | queue-smoke" ;;
esac
