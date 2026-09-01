#!/usr/bin/env bash
# Enqueue the SAME job twice with one dedup key, then watch one delivery become two.
#
# Demonstrates the two idempotency layers that do different jobs:
#   1. `?dedup=` collapses duplicate *enqueues*   → one job id, one job
#   2. the handler marker survives *redelivery*   → one side effect
set -euo pipefail
GATEWAY="${CRAFTY_GATEWAY:-127.0.0.1:8090}"
GATEWAY="${GATEWAY#http://}"
GATEWAY="${GATEWAY#https://}"
PAYLOAD="${1:-welcome-user-42}"
STREAM="${CRAFTY_JOB_QUEUE:-emails}"
DEDUP="${CRAFTY_DEDUP_KEY:-$PAYLOAD}"

post() {
    curl -sS -X POST "http://$GATEWAY/jobs/$STREAM?dedup=$DEDUP" \
        -H 'content-type: application/json' \
        -d "{\"payload\":\"$PAYLOAD\"}"
}

echo "POST http://$GATEWAY/jobs/$STREAM?dedup=$DEDUP  (submission 1)"
first="$(post)"
echo "  → $first"

echo "POST http://$GATEWAY/jobs/$STREAM?dedup=$DEDUP  (submission 2 — the retry)"
second="$(post)"
echo "  → $second"

if [ "$first" = "$second" ]; then
    echo "same job id returned — the duplicate enqueue was collapsed"
else
    echo "WARNING: different job ids — dedup key did not apply" >&2
fi

cat <<'TXT'

Now watch the server terminal. Expect, for one job:

  [worker] delivery #1 — <key>: sending email (side effects so far: 1)
  [worker] delivery #1 — <key>: crashing before ack (expect redelivery)
  [worker] delivery #2 — <key>: duplicate, side effect already applied (skipping)

Two deliveries, one side effect. Set CRAFTY_SIMULATE_REDELIVERY=0 on the server
to turn the simulated crash off.
TXT
