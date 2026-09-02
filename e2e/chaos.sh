#!/usr/bin/env bash
#
# chaos.sh — inject network faults into the docker-compose trembita cluster and
# assert consensus survives them (backlog T9, testing-strategy). Two scenarios:
#
#   1. Partition + heal (always): isolate the leader from the cluster network
#      (a clean split), assert the majority side re-elects, then reconnect it
#      and assert the whole cluster re-converges on one leader.
#   2. Latency (opt-in, TREMBITA_E2E_PUMBA=1): use `pumba` to add delay + jitter to
#      every node for a window and assert a leader stays agreed throughout.
#
# Partition uses only `docker network disconnect/connect` — no extra tooling.
# Requires Docker + `docker compose`. Run from anywhere:
#   ./e2e/chaos.sh

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

# --- Scenario 1: partition the leader, then heal ---------------------------
CID=$(container_of "$LEADER")
NET=$(network_of "$CID")
[ -n "$NET" ] || { echo "FAIL: could not resolve cluster network"; exit 1; }

echo "partitioning node$LEADER off the cluster network ($NET)…"
docker network disconnect "$NET" "$CID"
SURV=(); for id in 1 2 3; do [ "$id" != "$LEADER" ] && SURV+=("$id"); done

if ! NEW=$(wait_leader "$LEADER" "${SURV[@]}"); then
    echo "FAIL: majority did not re-elect during partition"; $COMPOSE logs --tail 40; exit 1
fi
echo "PASS: majority re-elected node $NEW while node $LEADER was isolated"

echo "healing partition (reconnecting node$LEADER)…"
docker network connect "$NET" "$CID"

if ! HEALED=$(wait_leader "" 1 2 3); then
    echo "FAIL: cluster did not re-converge after heal"; $COMPOSE logs --tail 40; exit 1
fi
if [ "$HEALED" = "$LEADER" ]; then
    echo "FAIL: stale leader $LEADER reasserted after heal (split brain?)"; exit 1
fi
echo "PASS: cluster re-converged on node $HEALED after heal (old leader $LEADER rejoined as follower)"

# --- Scenario 2: latency injection via pumba (opt-in) ----------------------
# Off by default: needs to pull the pumba + iproute2 images and a reachable
# docker socket, which isn't guaranteed under dind. Enable locally with
# TREMBITA_E2E_PUMBA=1.
if [ "${TREMBITA_E2E_PUMBA:-0}" = "1" ]; then
    echo "injecting 250ms±50ms latency on all nodes for 30s (pumba)…"
    docker run -d --rm --name trembita-pumba \
        -v /var/run/docker.sock:/var/run/docker.sock \
        gaiaadm/pumba:latest \
        netem --duration 30s --tc-image gaiadocker/iproute2 \
        delay --time 250 --jitter 50 "re2:e2e-node[0-9]+-1" >/dev/null

    echo "asserting the cluster keeps an agreed leader under latency…"
    if ! LAT=$(wait_leader "" 1 2 3); then
        echo "FAIL: lost consensus under injected latency"; $COMPOSE logs --tail 40; exit 1
    fi
    echo "PASS: leader = node $LAT stayed agreed under latency"
    docker rm -f trembita-pumba >/dev/null 2>&1 || true
else
    echo "SKIP: latency scenario (set TREMBITA_E2E_PUMBA=1 to enable pumba netem)"
fi

echo "CHAOS OK ✓"
