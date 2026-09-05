#!/usr/bin/env bash
# Build the VitePress site (sync docs/ → website/, production bundle).
#
# Usage: ./scripts/gate-website.sh

set -euo pipefail
cd "$(dirname "$0")/../website"

if ! command -v npm >/dev/null 2>&1; then
  echo "error: npm required for website build" >&2
  exit 1
fi

npm ci
npm run build

echo "OK: website build"
