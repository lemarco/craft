#!/usr/bin/env bash
#
# queue.sh — 3-node QUIC/mTLS cluster with durable job queue: enqueue, follower
# lease/ack, leader kill, drain surviving backlog.
#
# Requires Docker + `docker compose`. Run from anywhere:
#   ./e2e/queue.sh

set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=lib.sh
. ./lib.sh

trap cleanup EXIT

echo "building + starting 3-node cluster with job queue (QUIC + mTLS)…"
$COMPOSE up -d --build

echo "waiting for an agreed leader…"
if ! LEADER=$(wait_leader "" 1 2 3); then
    echo "FAIL: nodes did not converge on a leader"; $COMPOSE logs --tail 40; exit 1
fi
echo "leader elected = node $LEADER"

echo "phase 1: enqueue + follower lease/ack…"
if ! run_queue_client before_failover; then
    echo "FAIL: queue before_failover"; $COMPOSE logs --tail 40; exit 1
fi

echo "stopping node$LEADER to force queue leader failover…"
$COMPOSE stop "node$LEADER" >/dev/null
SURV=(); for id in 1 2 3; do [ "$id" != "$LEADER" ] && SURV+=("$id"); done

if ! NEW=$(wait_leader "$LEADER" "${SURV[@]}"); then
    echo "FAIL: survivors did not re-elect"; $COMPOSE logs --tail 40; exit 1
fi
echo "re-elected leader = node $NEW (was $LEADER)"

echo "phase 2: drain backlog after failover…"
if ! TREMBITA_QUEUE_CONTACT_NODE="$NEW" run_queue_client after_failover; then
    echo "FAIL: queue after_failover"; $COMPOSE logs --tail 40; exit 1
fi

echo "QUEUE E2E OK ✓"
