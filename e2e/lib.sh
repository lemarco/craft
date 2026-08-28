#!/usr/bin/env bash
#
# lib.sh — shared helpers for the E2E scripts (run.sh, chaos.sh, cert_renew.sh).
# Source after `cd`-ing into the e2e directory.

COMPOSE="docker compose -f docker-compose.yml"

# NodeId -> host admin port (see docker-compose.yml).
declare -A PORT=([1]=18443 [2]=18082 [3]=18083)
# node1 serves admin HTTPS; nodes 2/3 plain HTTP.
declare -A ADMIN_TLS=([1]=1 [2]=0 [3]=0)

# Host the published admin ports are reachable on. Localhost normally; under
# GitLab dind the ports live on the `docker` service host, so set
# CRAFT_E2E_HOST=docker there.
HOST="${CRAFT_E2E_HOST:-127.0.0.1}"

# Cluster CA PEM for HTTPS probes (populated by ensure_ca_file after compose up).
CA_FILE="${CRAFT_E2E_CA_FILE:-}"

# Tear the cluster + volumes down. Register with `trap cleanup EXIT`.
cleanup() { $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true; }

# Copy the cluster CA out of node1 for curl --cacert (admin TLS on node1).
ensure_ca_file() {
    if [ -n "$CA_FILE" ] && [ -f "$CA_FILE" ]; then
        return 0
    fi
    CA_FILE="$(mktemp)"
    $COMPOSE exec -T node1 cat /certs/ca.pem >"$CA_FILE"
}

admin_curl() {
    local id="$1" path="$2"
    if [ "${ADMIN_TLS[$id]:-0}" = "1" ]; then
        ensure_ca_file
        curl -sf -m 2 --cacert "$CA_FILE" "https://$HOST:${PORT[$id]}$path"
    else
        curl -sf -m 2 "http://$HOST:${PORT[$id]}$path"
    fi
}

# Print the leader id a node currently reports, or empty if unreachable/none.
leader_at() {
    admin_curl "$1" "/introspect/cluster" 2>/dev/null \
        | grep -o '"leader":[0-9]*' | head -1 | cut -d: -f2
}

# True when /health returns 200 on a node's admin port.
health_ok() {
    admin_curl "$1" "/health" >/dev/null 2>&1
}

# Wait until the given node ids all agree on one leader id that is not $exclude
# (pass "" to accept any). Echoes the agreed leader id on success.
wait_leader() {
    local exclude="$1"; shift
    local ids=("$@") tries=0
    while [ "$tries" -lt 120 ]; do
        local first="" ok=1 l
        for id in "${ids[@]}"; do
            l=$(leader_at "$id")
            { [ -z "$l" ]; } && { ok=0; break; }
            { [ -n "$exclude" ] && [ "$l" = "$exclude" ]; } && { ok=0; break; }
            if [ -z "$first" ]; then first="$l"; elif [ "$l" != "$first" ]; then ok=0; break; fi
        done
        if [ "$ok" = "1" ] && [ -n "$first" ]; then echo "$first"; return 0; fi
        tries=$((tries + 1)); sleep 1
    done
    return 1
}

# Wait until a majority of nodes (≥2 of 3) report the same leader and pass
# /health. Tolerates a single lagging peer while the cluster is still live.
wait_majority_leader() {
    local tries=0
    while [ "$tries" -lt 120 ]; do
        local -A tally=()
        local healthy=0 id l
        for id in 1 2 3; do
            health_ok "$id" || continue
            healthy=$((healthy + 1))
            l=$(leader_at "$id")
            [ -z "$l" ] && continue
            tally[$l]=$((${tally[$l]:-0} + 1))
        done
        if [ "$healthy" -ge 2 ]; then
            for l in "${!tally[@]}"; do
                if [ "${tally[$l]}" -ge 2 ]; then
                    echo "$l"
                    return 0
                fi
            done
        fi
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

# Run concurrent QUIC inc/read rounds and check with craft_sim::History (phase 2).
run_linclient() {
    $COMPOSE --profile linclient run --rm linclient
}

# Run the QUIC job-queue client (`CRAFT_E2E_QUEUE_PHASE` = before/after_failover).
run_queue_client() {
    local phase="$1"
    CRAFT_E2E_QUEUE_PHASE="$phase" $COMPOSE --profile queueclient run --rm queueclient
}

# Reissue node $1's PEM in the shared /certs volume (via node $2's container).
reissue_node_cert() {
    local id="$1" via="${2:-1}"
    $COMPOSE exec -T "node${via}" /bin/sh -c \
        "/app/generate.sh --node-id ${id} --out /certs --ca /certs/ca.pem --ca-key /certs/ca.key"
}

# Trigger on-disk cert reload (cert-automation) without restarting the process.
sighup_node() { $COMPOSE kill -s HUP "node$1"; }
