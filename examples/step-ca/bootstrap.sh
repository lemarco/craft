#!/usr/bin/env bash
# bootstrap.sh — mint cluster certs for the step-ca compose demo (ADR 034).
#
# Uses examples/certs/generate.sh (same contract as production). The step-ca
# service in docker-compose.yml is an optional reference CA; operators can
# switch issuance to `step ca certificate` once step-ca is initialized — see
# docs/certs.md § Automation.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUT="$ROOT/certs"
GEN="$ROOT/../certs/generate.sh"

command -v openssl >/dev/null || { echo "openssl required" >&2; exit 1; }
[ -x "$GEN" ] || chmod +x "$GEN"

mkdir -p "$OUT"
"$GEN" --node-id 1 --out "$OUT"
for id in 2 3; do
  "$GEN" --node-id "$id" --out "$OUT" --ca "$OUT/ca.pem" --ca-key "$OUT/ca.key"
done

echo "certs ready under $OUT"
echo "next: docker compose -f $ROOT/docker-compose.yml up --build"
