#!/usr/bin/env bash
# Submit an order via POST /orders/submit (capability fire, HTTP 202).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
ORDER="${1:-1001}"
GATEWAY="${TREMBITA_HTTP:-${TREMBITA_GATEWAY:-127.0.0.1:8190}}"
GATEWAY="${GATEWAY#http://}"
GATEWAY="${GATEWAY#https://}"
CLIENT="$CRAFT_ROOT/target/debug/trembita-showcase-client"

if [ -x "$CLIENT" ]; then
    exec "$CLIENT" submit "$GATEWAY" dev-trigger "$ORDER" showcase
fi

echo "POST http://$GATEWAY/orders/submit (order $ORDER)"
curl -sf -X POST "http://$GATEWAY/orders/submit?user=dev-trigger&token=showcase" \
    -H 'content-type: application/json' \
    -d "{\"order_id\":$ORDER}" \
    -w '\n→ HTTP %{http_code}\n'

echo "send twice to see idempotent skip in server logs"
