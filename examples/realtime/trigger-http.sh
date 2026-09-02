#!/usr/bin/env bash
# POST one chat line over authenticated HTTP (realtime showcase).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
USER="${1:-alice}"
MSG="${2:-hello}"
HOST="${TREMBITA_GATEWAY:-127.0.0.1:8294}"
HOST="${HOST#http://}"
HOST="${HOST#https://}"
TOKEN="${GATEWAY_TOKEN:-}"
CLIENT="$CRAFT_ROOT/target/debug/trembita-showcase-client"

if [ -x "$CLIENT" ]; then
    if [ -n "$TOKEN" ]; then
        exec "$CLIENT" chat "$HOST" "$USER" "$MSG" "$TOKEN"
    else
        exec "$CLIENT" chat "$HOST" "$USER" "$MSG"
    fi
fi

if [ -n "$TOKEN" ]; then
    curl -fsS -X POST "http://${HOST}/chat" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "X-Trembita-User: ${USER}" \
        -H 'Content-Type: application/json' \
        -d "{\"message\":\"${MSG}\"}"
else
    curl -fsS -X POST "http://${HOST}/chat?user=${USER}" \
        -H 'Content-Type: application/json' \
        -d "{\"message\":\"${MSG}\"}"
fi
echo
