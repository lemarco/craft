#!/usr/bin/env bash
# Open a WebSocket session and send one chat line.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
USER="${1:-alice}"
MSG="${2:-hello}"
HOST="${TREMBITA_GATEWAY:-127.0.0.1:8294}"
HOST="${HOST#http://}"
HOST="${HOST#https://}"
CLIENT="$CRAFT_ROOT/target/debug/trembita-showcase-client"

if [ -x "$CLIENT" ]; then
    exec "$CLIENT" ws "$HOST" "$USER" "$MSG"
fi

if command -v websocat >/dev/null 2>&1; then
    WS="ws://${HOST}/ws?user=${USER}"
    printf '%s\n' "$MSG" | websocat -1 "$WS"
    exit 0
fi

echo "build client: cargo build -p trembita-showcase-client" >&2
echo "or install websocat: https://github.com/vi/websocat" >&2
echo "manual: websocat 'ws://${HOST}/ws?user=${USER}'" >&2
exit 1
