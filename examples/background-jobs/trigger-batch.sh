#!/usr/bin/env bash
# Enqueue N jobs — round-robin across cluster gateways (8090/8091/8092).
set -euo pipefail
# Space-separated list; each node runs its own gateway (forwards enqueue to queue leader).
GATEWAYS="${TREMBITA_GATEWAYS:-http://127.0.0.1:8090 http://127.0.0.1:8091 http://127.0.0.1:8092}"
read -r -a GW <<< "$GATEWAYS"
COUNT="${1:-20}"
OK=0 FAIL=0
echo "POST /jobs/emails × $COUNT (round-robin: ${GW[*]})"
for i in $(seq 1 "$COUNT"); do
  gw="${GW[$(( (i - 1) % ${#GW[@]} ))]}"
  code=$(curl -s -o /tmp/trembita-job-$$.json -w '%{http_code}' -X POST "$gw/jobs/emails" \
    -H 'content-type: application/json' \
    -d "{\"payload\":\"batch-$i\"}") || code=000
  if [ "$code" = 202 ]; then
    OK=$((OK + 1))
    echo "  batch-$i @ ${gw##*/} → 202 $(tr -d '\n' < /tmp/trembita-job-$$.json)"
  else
    FAIL=$((FAIL + 1))
    echo "  batch-$i @ ${gw##*/} → $code"
  fi
done
rm -f /tmp/trembita-job-$$.json
echo "done: $OK ok, $FAIL failed — dashboard http://127.0.0.1:9180/dashboard"
