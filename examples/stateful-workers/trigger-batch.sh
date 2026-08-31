#!/usr/bin/env bash
# Cast order ids — round-robin gateways; re-sends first id to demo idempotency.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
GATEWAYS="${CRAFTY_GATEWAYS:-http://127.0.0.1:8190 http://127.0.0.1:8191 http://127.0.0.1:8192}"
read -r -a GW <<< "$GATEWAYS"
COUNT="${1:-10}"
BASE="${2:-2000}"
OK=0 FAIL=0
echo "cast orders $BASE..$((BASE + COUNT - 1)) (round-robin: ${GW[*]})"
for i in $(seq 0 $((COUNT - 1))); do
    order=$((BASE + i))
    gw="${GW[$(( i % ${#GW[@]} ))]}"
    host="${gw#http://}"
    host="${host#https://}"
    if CRAFTY_GATEWAY="$host" "$ROOT/trigger.sh" "$order" >/dev/null 2>&1; then
        OK=$((OK + 1))
        echo "  order $order @ ${host##*/} → 202"
    else
        FAIL=$((FAIL + 1))
        echo "  order $order @ ${host##*/} → failed"
    fi
done
echo "idempotency: re-cast order $BASE"
CRAFTY_GATEWAY="${GW[0]#http://}" CRAFTY_GATEWAY="${CRAFTY_GATEWAY#https://}" "$ROOT/trigger.sh" "$BASE" || true
echo "done: $OK ok, $FAIL failed — dashboard http://127.0.0.1:9280/dashboard"
