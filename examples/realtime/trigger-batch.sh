#!/usr/bin/env bash
# Send chat lines for several users (round-robin gateways 8294/8295/8296 in cluster).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
CLIENT="$CRAFT_ROOT/target/debug/crafty-showcase-client"
GATEWAYS=(127.0.0.1:8294 127.0.0.1:8295 127.0.0.1:8296)

USERS=(alice bob carol)
COUNT="${1:-6}"
OK=0
echo "chat × $COUNT messages (gateways ${GATEWAYS[*]})"
for i in $(seq 1 "$COUNT"); do
    user="${USERS[$(( (i - 1) % ${#USERS[@]} ))]}"
    msg="batch-$i-from-$user"
    gw="${GATEWAYS[$(( (i - 1) % ${#GATEWAYS[@]} ))]}"
    if [ -x "$CLIENT" ]; then
        out=$("$CLIENT" ws "$gw" "$user" "$msg" 2>/dev/null || true)
    elif command -v websocat >/dev/null 2>&1; then
        out=$(printf '%s\n' "$msg" | websocat -1 "ws://${gw}/ws?user=${user}" 2>/dev/null || true)
    else
        echo "install crafty-showcase-client or websocat" >&2
        exit 1
    fi
    if echo "$out" | rg -q "ok: $msg"; then
        OK=$((OK + 1))
        echo "  $user @ $gw → ok: $msg"
    else
        echo "  $user @ $gw → failed ($out)"
    fi
done
echo "done: $OK/$COUNT — dashboard http://127.0.0.1:9380/dashboard"
