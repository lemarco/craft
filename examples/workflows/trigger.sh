#!/usr/bin/env bash
# Run or resume the onboarding saga (round-robin gateways 8490/8491/8492 in cluster).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
GATEWAYS=(127.0.0.1:8490 127.0.0.1:8491 127.0.0.1:8492)
GATEWAY="${TREMBITA_GATEWAY:-127.0.0.1:8490}"
GATEWAY="${GATEWAY#http://}"
GATEWAY="${GATEWAY#https://}"
CLIENT="$CRAFT_ROOT/target/debug/trembita-showcase-client"

pick_gateway() {
    for g in "${GATEWAYS[@]}"; do
        if curl -sf "http://$g/health" >/dev/null 2>&1; then
            echo "$g"
            return 0
        fi
    done
    echo "$GATEWAY"
}

if [ "${1:-}" = "resume" ]; then
    SAGA="${2:-onboard-42}"
    HOST=$(pick_gateway)
    if [ -x "$CLIENT" ]; then
        exec "$CLIENT" workflow resume "$HOST" "$SAGA"
    fi
    echo "POST http://$HOST/workflows/resume"
    curl -sf -X POST "http://$HOST/workflows/resume" \
        -H 'content-type: application/json' \
        -d "{\"saga_id\":\"$SAGA\"}" \
        -w '\n→ HTTP %{http_code}\n'
else
    SAGA="${1:-onboard-42}"
    HOST=$(pick_gateway)
    if curl -sf "http://$HOST/health" >/dev/null 2>&1; then
        if [ -x "$CLIENT" ]; then
            exec "$CLIENT" workflow run "$HOST" "$SAGA"
        fi
        echo "POST http://$HOST/workflows/run"
        curl -sf -X POST "http://$HOST/workflows/run" \
            -H 'content-type: application/json' \
            -d "{\"saga_id\":\"$SAGA\"}" \
            -w '\n→ HTTP %{http_code}\n'
    else
        echo "error: no gateway on $GATEWAY — start the server: cargo run --release" >&2
        exit 1
    fi
fi

echo "dashboard: http://127.0.0.1:9480/dashboard (Sagas panel)"
