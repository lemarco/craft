#!/usr/bin/env bash
# Cast an order id to the orders actor (HTTP 202).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
ORDER="${1:-1001}"
GATEWAY="${CRAFTY_GATEWAY:-127.0.0.1:8190}"
GATEWAY="${GATEWAY#http://}"
GATEWAY="${GATEWAY#https://}"
CLIENT="$CRAFT_ROOT/target/debug/crafty-showcase-client"

if [ -x "$CLIENT" ]; then
    exec "$CLIENT" cast "$GATEWAY" orders "$ORDER"
fi

echo "POST http://$GATEWAY/actors/orders/cast (order $ORDER, JSON payload)"
curl -sf -X POST "http://$GATEWAY/actors/orders/cast" \
    -H 'content-type: application/json' \
    -d "{\"payload\":\"$ORDER\"}" \
    -w '\n→ HTTP %{http_code}\n'

echo "send twice to see idempotent skip in server logs"
