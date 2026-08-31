#!/usr/bin/env bash
# POST a rolling upgrade manifest to the seed node's upgrade API.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
CRAFT_ROOT="$(cd "$ROOT/../.." && pwd)"
DEV="${CRAFTY_SELF_UPDATE_CLUSTER_DIR:-$CRAFT_ROOT/target/crafty-self-update-cluster}"
ARTIFACT="${DEV}/artifacts/app-9.9.9.bin"
GATEWAY="${1:-http://127.0.0.1:8190}"
SHA=$(sha256sum "$ARTIFACT" | awk '{print $1}')

BODY=$(cat <<EOF
{
  "app_version": "9.9.9",
  "url": "file://${ARTIFACT}",
  "sha256_hex": "${SHA}"
}
EOF
)

echo ">> POST ${GATEWAY}/cluster/upgrade/desired"
curl -sf -X POST "${GATEWAY}/cluster/upgrade/desired" \
  -H 'content-type: application/json' \
  -d "$BODY"
echo
echo ">> status:"
curl -sf "${GATEWAY}/cluster/upgrade"
echo
