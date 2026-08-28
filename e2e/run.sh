#!/usr/bin/env bash
#
# run.sh — bring up the docker-compose crafty cluster (real QUIC + mTLS) and
# assert it (1) elects a single agreed leader and (2) re-elects after the
# leader is killed. Tears everything down on exit. (Backlog T8.)
#
# Requires Docker + `docker compose`. Run from anywhere:
#   ./e2e/run.sh

set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=lib.sh
. ./lib.sh

trap cleanup EXIT

echo "building + starting 3-node cluster (QUIC + mTLS)…"
$COMPOSE up -d --build

echo "waiting for an agreed leader…"
if ! LEADER=$(wait_leader "" 1 2 3); then
    echo "FAIL: nodes did not converge on a leader"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: leader elected = node $LEADER (all three agree)"

echo "checking admin /health and /introspect/cluster on all nodes…"
for id in 1 2 3; do
    admin_curl "$id" "/health" >/dev/null
    body=$(admin_curl "$id" "/introspect/cluster")
    echo "$body" | grep -q '"leader":' || {
        echo "FAIL: node$id introspect missing leader field: $body"; exit 1
    }
done
echo "PASS: admin endpoints reachable"

echo "stopping node$LEADER to force re-election…"
$COMPOSE stop "node$LEADER" >/dev/null
SURV=(); for id in 1 2 3; do [ "$id" != "$LEADER" ] && SURV+=("$id"); done

if ! NEW=$(wait_leader "$LEADER" "${SURV[@]}"); then
    echo "FAIL: survivors did not re-elect after leader loss"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: re-elected leader = node $NEW (was $LEADER)"

echo "E2E OK ✓"
