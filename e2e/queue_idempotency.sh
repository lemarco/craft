#!/usr/bin/env bash
#
# queue_idempotency.sh — idempotency under redelivery (B-14h).
# Exercises IdempotencyOpts + dedup_key; docker failover covered by ./e2e/queue.sh.
#
#   ./e2e/queue_idempotency.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "running queue idempotency integration tests…"
./scripts/test-fast.sh -p crafty --test consumer_idempotency -- --nocapture
echo "QUEUE IDEMPOTENCY OK ✓"
