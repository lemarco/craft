#!/usr/bin/env bash
#
# linearizability.sh — Jepsen-lite nightly gate (read-consistency ADR):
#   1) seeded craft-sim linearizability sweep (checker in-process)
#   2) docker chaos partition while the cluster stays live (wire + majority)
#
# Sim checker is the primary linearizability gate; docker proves the real
# stack survives partition under concurrent admin polling (concurrent clients
# without a QUIC load generator in E2E is tracked separately).
#
# Requires Docker for phase 2. Run from anywhere:
#   ./e2e/linearizability.sh
#   CRAFT_E2E_LINEARIZABILITY=1 ./e2e/linearizability.sh  # CI nightly

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ "${CRAFT_LINEARIZABILITY_SKIP_SIM:-0}" != "1" ]; then
  echo "=== phase 1: craft-sim linearizability (seed sweep) ==="
  ./scripts/test-with-log.sh -p craft-sim --test linearizability 2>&1 | tail -5

  SEEDS="${CRAFT_LINEARIZABILITY_SEEDS:-42 99 1234}"
  for seed in $SEEDS; do
    echo "  read_index adversarial seed=$seed"
    CRAFT_SIM_SEED="$seed" ./scripts/test-with-log.sh -p craft-sim --test read_index 2>&1 | tail -3
  done
else
  echo "SKIP phase 1 (CRAFT_LINEARIZABILITY_SKIP_SIM=1)"
fi

if [ "${CRAFT_E2E_LINEARIZABILITY:-1}" != "1" ]; then
  echo "SKIP phase 2 (set CRAFT_E2E_LINEARIZABILITY=1 to enable docker partition gate)"
  echo "LINEARIZABILITY OK ✓ (sim only)"
  exit 0
fi

echo "=== phase 2: docker partition + concurrent admin poll ==="
cd e2e
# shellcheck source=lib.sh
. ./lib.sh
trap cleanup EXIT

$COMPOSE up -d --build
LEADER=$(wait_leader "" 1 2 3) || { echo "FAIL: no leader"; exit 1; }
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

poll_concurrent || { echo "FAIL: cluster unhealthy before partition"; exit 1; }

docker network disconnect "$NET" "$CID"
poll_concurrent || { echo "FAIL: majority lost during partition"; exit 1; }
docker network connect "$NET" "$CID"
wait_leader "" 1 2 3 >/dev/null || { echo "FAIL: no heal"; exit 1; }
poll_concurrent || { echo "FAIL: cluster unhealthy after heal"; exit 1; }

echo "LINEARIZABILITY OK ✓"
