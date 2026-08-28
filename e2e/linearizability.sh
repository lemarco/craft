#!/usr/bin/env bash
#
# linearizability.sh — Jepsen-lite nightly gate (read-consistency ADR):
#   1) seeded crafty-sim linearizability sweep (checker in-process)
#   2) docker E2E: concurrent QUIC clients + external checker, then partition
#      chaos under admin poll, then QUIC checker again after heal
#
# Requires Docker for phase 2. Run from anywhere:
#   ./e2e/linearizability.sh
#   CRAFTY_E2E_LINEARIZABILITY=1 ./e2e/linearizability.sh  # CI nightly

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ "${CRAFTY_LINEARIZABILITY_SKIP_SIM:-0}" != "1" ]; then
  echo "=== phase 1: crafty-sim linearizability (seed sweep) ==="
  ./scripts/test-with-log.sh -p crafty-sim --test linearizability 2>&1 | tail -5

  SEEDS="${CRAFTY_LINEARIZABILITY_SEEDS:-42 99 1234}"
  for seed in $SEEDS; do
    echo "  read_index adversarial seed=$seed"
    CRAFTY_SIM_SEED="$seed" ./scripts/test-with-log.sh -p crafty-sim --test read_index 2>&1 | tail -3
  done
else
  echo "SKIP phase 1 (CRAFTY_LINEARIZABILITY_SKIP_SIM=1)"
fi

if [ "${CRAFTY_E2E_LINEARIZABILITY:-1}" != "1" ]; then
  echo "SKIP phase 2 (set CRAFTY_E2E_LINEARIZABILITY=1 to enable docker E2E gate)"
  echo "LINEARIZABILITY OK ✓ (sim only)"
  exit 0
fi

echo "=== phase 2: docker QUIC linearizability + partition chaos ==="
cd e2e
# shellcheck source=lib.sh
. ./lib.sh
trap cleanup EXIT

$COMPOSE up -d --build
LEADER=$(wait_leader "" 1 2 3) || { echo "FAIL: no leader"; exit 1; }
echo "cluster leader = node $LEADER"

echo "--- 2a: concurrent QUIC inc/read + crafty_sim checker (healthy cluster) ---"
run_linclient || { echo "FAIL: QUIC linearizability before partition"; exit 1; }

CID=$(container_of "$LEADER")
NET=$(network_of "$CID")

poll_concurrent() {
  local ok=1
  for _ in $(seq 1 20); do
    curl -sf -m 1 "http://$HOST:${PORT[1]}/health" >/dev/null &
    curl -sf -m 1 "http://$HOST:${PORT[2]}/health" >/dev/null &
    curl -sf -m 1 "http://$HOST:${PORT[3]}/health" >/dev/null &
    wait
  done
  wait_majority_leader >/dev/null || ok=0
  [ "$ok" = 1 ]
}

echo "--- 2b: partition leader + concurrent admin poll ---"
poll_concurrent || { echo "FAIL: cluster unhealthy before partition"; exit 1; }

docker network disconnect "$NET" "$CID"
poll_concurrent || { echo "FAIL: majority lost during partition"; exit 1; }
docker network connect "$NET" "$CID"
wait_leader "" 1 2 3 >/dev/null || { echo "FAIL: no heal"; exit 1; }
poll_concurrent || { echo "FAIL: cluster unhealthy after heal"; exit 1; }

echo "--- 2c: concurrent QUIC inc/read + checker after heal ---"
run_linclient || { echo "FAIL: QUIC linearizability after partition"; exit 1; }

echo "LINEARIZABILITY OK ✓"
