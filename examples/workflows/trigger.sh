#!/usr/bin/env bash
# Run or resume the onboarding saga (round-robin triggers 8490/8491/8492 in cluster).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
TRIGGERS=(127.0.0.1:8490 127.0.0.1:8491 127.0.0.1:8492)
TRIGGER="${CRAFTY_TRIGGER:-127.0.0.1:8490}"
TRIGGER="${TRIGGER#http://}"
TRIGGER="${TRIGGER#https://}"
CLIENT="$CRAFT_ROOT/target/debug/crafty-showcase-client"

pick_trigger() {
    for t in "${TRIGGERS[@]}"; do
        if curl -sf "http://$t/health" >/dev/null 2>&1; then
            echo "$t"
            return 0
        fi
    done
    echo "$TRIGGER"
}

if [ "${1:-}" = "resume" ]; then
    SAGA="${2:-onboard-42}"
    HOST=$(pick_trigger)
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
    HOST=$(pick_trigger)
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
        exec cargo run --manifest-path "$ROOT/Cargo.toml" --release --quiet -- run "$SAGA"
    fi
fi

echo "dashboard: http://127.0.0.1:9480/dashboard (Sagas panel)"
