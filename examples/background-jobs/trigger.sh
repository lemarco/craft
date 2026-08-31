#!/usr/bin/env bash
# Enqueue one email job via the product HTTP API (202 Accepted).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
GATEWAY="${CRAFTY_GATEWAY:-127.0.0.1:8090}"
GATEWAY="${GATEWAY#http://}"
GATEWAY="${GATEWAY#https://}"
PAYLOAD="${1:-hello-from-trigger.sh}"
STREAM="${CRAFTY_JOB_QUEUE:-emails}"
CLIENT="$CRAFT_ROOT/target/debug/crafty-showcase-client"

if [ -x "$CLIENT" ]; then
    exec "$CLIENT" job "$GATEWAY" "$STREAM" "$PAYLOAD"
fi

echo "POST http://$GATEWAY/jobs/$STREAM"
curl -sf -X POST "http://$GATEWAY/jobs/$STREAM" \
  -H 'content-type: application/json' \
  -d "{\"payload\":\"$PAYLOAD\"}" \
  -w '\n→ HTTP %{http_code}\n'
echo "watch the server terminal for [worker] lines"
