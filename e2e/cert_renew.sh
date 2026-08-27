#!/usr/bin/env bash
#
# cert_renew.sh — reissue on-disk PEMs and hot-reload TLS without restarting
# craft-node (cert-automation). Two paths exercised on a live docker-compose cluster:
#
#   1. SIGHUP after rewrite (simulates `step ca renew` + hook)
#   2. File poll (CRAFT_CERT_WATCH_SECS) after rewrite, no signal
#
# Requires Docker + `docker compose`. Run from anywhere:
#   ./e2e/cert_renew.sh

set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=lib.sh
. ./lib.sh

# Short poll window so the poll scenario finishes quickly in CI.
export CRAFT_CERT_WATCH_SECS="${CRAFT_CERT_WATCH_SECS:-5}"

trap cleanup EXIT

echo "building + starting 3-node cluster (QUIC + mTLS, cert watch ${CRAFT_CERT_WATCH_SECS}s)…"
$COMPOSE up -d --build

echo "waiting for a live majority leader…"
if ! LEADER=$(wait_majority_leader); then
    echo "FAIL: cluster did not elect a majority leader"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: majority leader = node $LEADER (≥2 healthy nodes agree)"

# Pick two followers (stable order: lower id first).
FOLLOWERS=()
for id in 1 2 3; do
    [ "$id" != "$LEADER" ] && FOLLOWERS+=("$id")
done
HUP_TARGET="${FOLLOWERS[0]}"
POLL_TARGET="${FOLLOWERS[1]}"

# --- Scenario 1: reissue PEM + SIGHUP (follower first, cert-automation) ------------
echo "reissuing node${HUP_TARGET} cert + SIGHUP (no restart)…"
reissue_node_cert "$HUP_TARGET" 1
sighup_node "$HUP_TARGET"
sleep 2

if ! AFTER_HUP=$(wait_majority_leader); then
    echo "FAIL: cluster lost majority after SIGHUP reload on node${HUP_TARGET}"
    $COMPOSE logs --tail 40 "node${HUP_TARGET}"; exit 1
fi
echo "PASS: majority leader = node $AFTER_HUP after SIGHUP reload"

# --- Scenario 2: reissue PEM + poll (no signal) ----------------------------
echo "reissuing node${POLL_TARGET} cert and waiting for poll reload…"
reissue_node_cert "$POLL_TARGET" 1
# Two watch intervals + slack for mtime detection and reload.
sleep $((CRAFT_CERT_WATCH_SECS * 2 + 3))

if ! AFTER_POLL=$(wait_majority_leader); then
    echo "FAIL: cluster lost majority after poll reload on node${POLL_TARGET}"
    $COMPOSE logs --tail 40 "node${POLL_TARGET}"; exit 1
fi
echo "PASS: majority leader = node $AFTER_POLL after poll reload"

echo "CERT RENEW E2E OK ✓"
