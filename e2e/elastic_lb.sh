#!/usr/bin/env bash
#
# elastic_lb.sh — B-34: 4th joiner, nginx LB round-robin, cluster session cookie
# on node B after login on node A, PerNode cap smoke.
#
#   ./e2e/elastic_lb.sh
#
# Heavy lane (CI: MR label run-heavy). Requires Docker + docker compose.

set -euo pipefail
cd "$(dirname "$0")"

COMPOSE="docker compose -f docker-compose-elastic.yml"
HOST="${TREMBITA_E2E_HOST:-127.0.0.1}"
LB_PORT=18180
declare -A DIRECT=([1]=18181 [2]=18182 [3]=18183 [4]=18184)
COOKIE_JAR="$(mktemp)"
trap 'rm -f "$COOKIE_JAR"; $COMPOSE --profile joiner --profile lb down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

curl_node() {
    local id="$1" path="$2"
    curl -sf -m 5 "http://$HOST:${DIRECT[$id]}$path"
}

curl_lb() {
    curl -sf -m 5 "http://$HOST:$LB_PORT$1"
}

ready_node_id() {
    curl_node "$1" "/ready" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2
}

wait_ready() {
    local id="$1" tries=0
    while [ "$tries" -lt 120 ]; do
        if curl -sf -m 2 "http://$HOST:${DIRECT[$id]}/ready" >/dev/null 2>&1; then
            return 0
        fi
        tries=$((tries + 1))
        sleep 1
    done
    echo "FAIL: node$id /ready timeout"
    $COMPOSE logs --tail 30 "node$id"
    exit 1
}

echo "building seed + 3 joiners…"
$COMPOSE up -d --build node1 node2 node3

for id in 1 2 3; do
    echo "waiting for node$id /ready…"
    wait_ready "$id"
done
echo "PASS: nodes 1–3 ready"

echo "starting 4th joiner (elastic)…"
$COMPOSE --profile joiner up -d node4
wait_ready 4
echo "PASS: node4 joined and ready (node_id=$(ready_node_id 4))"

echo "starting nginx LB (round-robin)…"
$COMPOSE --profile joiner --profile lb up -d lb
sleep 1

echo "LB round-robin via GET /ready (expect ≥3 distinct node_id values)…"
seen=""
distinct=0
for _ in $(seq 1 48); do
    nid=$(curl_lb "/ready" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2 || true)
    if [ -z "$nid" ]; then
        continue
    fi
    case " $seen " in
        *" $nid "*) ;;
        *)
            seen="$seen $nid"
            distinct=$((distinct + 1))
            ;;
    esac
done
if [ "$distinct" -lt 3 ]; then
    echo "FAIL: LB only saw $distinct distinct node_id(s):$seen"
    exit 1
fi
echo "PASS: LB spread across $distinct backends:$seen"

echo "cluster session: login on node1, /me on node2…"
curl -sf -m 5 -c "$COOKIE_JAR" "http://$HOST:${DIRECT[1]}/login?user=lbproof" >/dev/null
me=$(curl -sf -m 5 -b "$COOKIE_JAR" "http://$HOST:${DIRECT[2]}/me")
if [ "$me" != "lbproof" ]; then
    echo "FAIL: expected body lbproof, got: $me"
    exit 1
fi
echo "PASS: cluster session cookie verified on peer gateway"

echo "PerNode cap smoke via LB (/e2e/whoami)…"
cap_seen=""
cap_distinct=0
for _ in $(seq 1 36); do
    nid=$(curl_lb "/e2e/whoami" | grep -o '"node_id":[0-9]*' | head -1 | cut -d: -f2 || true)
    [ -z "$nid" ] && continue
    case " $cap_seen " in
        *" $nid "*) ;;
        *)
            cap_seen="$cap_seen $nid"
            cap_distinct=$((cap_distinct + 1))
            ;;
    esac
done
if [ "$cap_distinct" -lt 2 ]; then
    echo "FAIL: cap route only hit $cap_distinct node(s):$cap_seen"
    exit 1
fi
echo "PASS: inline cap served from $cap_distinct nodes via LB:$cap_seen"

echo "ELASTIC LB E2E OK ✓"
