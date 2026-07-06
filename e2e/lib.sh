#!/usr/bin/env bash
#
# lib.sh — shared helpers for the E2E scripts (run.sh, chaos.sh). Source it
# after `cd`-ing into the e2e directory.

COMPOSE="docker compose -f docker-compose.yml"

# NodeId -> host admin port (see docker-compose.yml).
declare -A PORT=([1]=18081 [2]=18082 [3]=18083)

# Host the published admin ports are reachable on. Localhost normally; under
# GitLab dind the ports live on the `docker` service host, so set
# CRAFT_E2E_HOST=docker there.
HOST="${CRAFT_E2E_HOST:-127.0.0.1}"

# Tear the cluster + volumes down. Register with `trap cleanup EXIT`.
cleanup() { $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true; }

# Print the leader id a node currently reports, or empty if unreachable/none.
leader_at() {
    local body
    body=$(curl -s -m 2 "http://$HOST:$1/introspect/cluster" 2>/dev/null) || return 0
    echo "$body" | grep -o '"leader":[0-9]*' | head -1 | cut -d: -f2
}

# Wait until the given node ids all agree on one leader id that is not $exclude
# (pass "" to accept any). Echoes the agreed leader id on success.
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

# The running container id for node $1 (e.g. 1 -> e2e-node1-1's id).
container_of() { $COMPOSE ps -q "node$1"; }

# The docker network a container is attached to (first one).
network_of() {
    docker inspect -f '{{range $k,$_ := .NetworkSettings.Networks}}{{$k}} {{end}}' "$1" \
        | awk '{print $1}'
}
