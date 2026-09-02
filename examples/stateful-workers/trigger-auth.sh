#!/usr/bin/env bash
# POST an order via authenticated gateway route (stateful-workers showcase).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
TENANT="${1:-tenant-1}"
ORDER="${2:-1001}"
HOST="${TREMBITA_GATEWAY:-127.0.0.1:8190}"
HOST="${HOST#http://}"
HOST="${HOST#https://}"
TOKEN="${GATEWAY_TOKEN:-}"
CLIENT="$CRAFT_ROOT/target/debug/trembita-showcase-client"

if [ -x "$CLIENT" ]; then
    if [ -n "$TOKEN" ]; then
        exec "$CLIENT" submit "$HOST" "$TENANT" "$ORDER" "$TOKEN"
    else
        exec "$CLIENT" submit "$HOST" "$TENANT" "$ORDER"
    fi
fi

QUERY="user=${TENANT}"
if [ -n "$TOKEN" ]; then
    QUERY="${QUERY}&token=${TOKEN}"
fi
curl -fsS -X POST "http://${HOST}/orders/submit?${QUERY}" \
    -H 'Content-Type: application/json' \
    -d "{\"order_id\":${ORDER}}"
echo
