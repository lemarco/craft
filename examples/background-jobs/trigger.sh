#!/usr/bin/env bash
# Enqueue one email job via the product HTTP API (202 Accepted).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
GATEWAY="${TREMBITA_HTTP:-${TREMBITA_GATEWAY:-127.0.0.1:8090}}"
GATEWAY="${GATEWAY#http://}"
GATEWAY="${GATEWAY#https://}"
PAYLOAD="${1:-hello-from-trigger.sh}"
CAP_PATH="${TREMBITA_CAP_ENQUEUE_PATH:-/jobs/emails}"
CLIENT="$CRAFT_ROOT/target/debug/trembita-showcase-client"

if [ -x "$CLIENT" ]; then
    exec "$CLIENT" cap-enqueue "$GATEWAY" "$CAP_PATH" "$PAYLOAD"
fi

echo "POST http://$GATEWAY$CAP_PATH"
curl -sf -X POST "http://$GATEWAY$CAP_PATH" \
  -H 'content-type: application/json' \
  -d "{\"text\":\"$PAYLOAD\"}" \
  -w '\n→ HTTP %{http_code}\n'
echo "watch the server terminal for [worker] lines"
