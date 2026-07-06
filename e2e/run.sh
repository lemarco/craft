#!/usr/bin/env bash
#
# run.sh — bring up the docker-compose craft cluster (real QUIC + mTLS) and
# assert it (1) elects a single agreed leader and (2) re-elects after the
# leader is killed. Tears everything down on exit. (Backlog T8.)
#
# Requires Docker + `docker compose`. Run from anywhere:
#   ./e2e/run.sh

set -euo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f docker-compose.yml"

cleanup() { $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

# NodeId -> host admin port (see docker-compose.yml).
declare -A PORT=([1]=18081 [2]=18082 [3]=18083)

# Host the published admin ports are reachable on. Localhost normally; under
# GitLab dind the ports live on the `docker` service host, so set
# CRAFT_E2E_HOST=docker there.
HOST="${CRAFT_E2E_HOST:-127.0.0.1}"

# Print the leader id a node currently reports, or empty if unreachable/none.
leader_at() {
    local body
    body=$(curl -s -m 2 "http://$HOST:$1/introspect/cluster" 2>/dev/null) || return 0
    echo "$body" | grep -o '"leader":[0-9]*' | head -1 | cut -d: -f2
}

# Wait until the given nodes all agree on one leader id that is not $exclude.
# Echoes the leader id on success.
wait_leader() {
    local exclude="$1"; shift
    local ids=("$@") tries=0
    while [ "$tries" -lt 120 ]; do
        local first="" ok=1 l
        for id in "${ids[@]}"; do
            l=$(leader_at "${PORT[$id]}")
            { [ -z "$l" ]; } && { ok=0; break; }
            { [ -n "$exclude" ] && [ "$l" = "$exclude" ]; } && { ok=0; break; }
            if [ -z "$first" ]; then first="$l"; elif [ "$l" != "$first" ]; then ok=0; break; fi
        done
        if [ "$ok" = 1 ] && [ -n "$first" ]; then echo "$first"; return 0; fi
        tries=$((tries + 1)); sleep 1
    done
    return 1
}

echo "building + starting 3-node cluster (QUIC + mTLS)…"
$COMPOSE up -d --build

echo "waiting for an agreed leader…"
if ! LEADER=$(wait_leader "" 1 2 3); then
    echo "FAIL: nodes did not converge on a leader"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: leader elected = node $LEADER (all three agree)"

echo "stopping node$LEADER to force re-election…"
$COMPOSE stop "node$LEADER" >/dev/null
SURV=(); for id in 1 2 3; do [ "$id" != "$LEADER" ] && SURV+=("$id"); done

if ! NEW=$(wait_leader "$LEADER" "${SURV[@]}"); then
    echo "FAIL: survivors did not re-elect after leader loss"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: re-elected leader = node $NEW (was $LEADER)"

echo "E2E OK ✓"
