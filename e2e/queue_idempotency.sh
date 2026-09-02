#!/usr/bin/env bash
#
# queue_idempotency.sh — idempotency under redelivery + dedup across leader failover (B-14h).
#
#   ./e2e/queue_idempotency.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "running queue idempotency integration tests…"
./scripts/test-fast.sh -p trembita --test consumer_idempotency -- --nocapture
./scripts/test-fast.sh -p trembita --test queue dedup_key_survives_leader_failover
echo "QUEUE IDEMPOTENCY OK ✓"
