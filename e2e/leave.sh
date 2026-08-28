#!/usr/bin/env bash
#
# leave.sh — verify crafty-node graceful leave (CRAFTY_GRACEFUL_LEAVE=1) removes
# the departing node from membership before exit.

set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=lib.sh
. ./lib.sh

trap cleanup EXIT

echo "building + starting 3-node cluster for graceful leave…"
$COMPOSE up -d --build

echo "waiting for an agreed leader…"
if ! wait_leader "" 1 2 3 >/dev/null; then
    echo "FAIL: nodes did not converge on a leader"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: cluster ready"

echo "SIGINT node3 (CRAFTY_GRACEFUL_LEAVE=1)…"
$COMPOSE kill -s INT node3

tries=0
while [ "$tries" -lt 90 ]; do
    members=$(admin_curl 1 "/introspect/cluster" 2>/dev/null \
        | grep -o '"member":true' | wc -l | tr -d ' ')
    if [ "${members:-0}" -eq 2 ]; then
        echo "PASS: surviving peers report two voting members after graceful leave"
        echo "LEAVE E2E OK ✓"
        exit 0
    fi
    tries=$((tries + 1)); sleep 1
done

echo "FAIL: peers did not drop node3 from membership after graceful leave"
$COMPOSE logs --tail 40 node3
exit 1
