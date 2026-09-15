#!/usr/bin/env bash
# Session cookie flow: login (Bearer) → Set-Cookie → POST /chat with cookie.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
USER="${1:-alice}"
MSG="${2:-hello}"
HOST="${TREMBITA_HTTP:-${TREMBITA_GATEWAY:-127.0.0.1:8290}}"
HOST="${HOST#http://}"
HOST="${HOST#https://}"
TOKEN="${GATEWAY_TOKEN:-dev-secret}"
COOKIE_JAR="$(mktemp)"
trap 'rm -f "$COOKIE_JAR"' EXIT

echo "POST /login (Bearer + X-Trembita-User: ${USER})"
curl -fsS -X POST "http://${HOST}/login" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "X-Trembita-User: ${USER}" \
    -c "$COOKIE_JAR"

echo
echo "POST /chat (session cookie)"
curl -fsS -X POST "http://${HOST}/chat" \
    -b "$COOKIE_JAR" \
    -H 'Content-Type: application/json' \
    -d "{\"message\":\"${MSG}\"}"
echo

echo "GET /me"
curl -fsS "http://${HOST}/me" -b "$COOKIE_JAR"
echo
