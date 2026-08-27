#!/usr/bin/env bash
# Shellcheck gate for repo scripts (optional if shellcheck missing).

set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v shellcheck >/dev/null 2>&1; then
  echo "warn: shellcheck not installed — skipping (pacman -S shellcheck / apt install shellcheck)" >&2
  exit 0
fi

mapfile -t files < <(find scripts e2e -name '*.sh' -type f 2>/dev/null | sort)
if [[ ${#files[@]} -eq 0 ]]; then
  exit 0
fi

shellcheck -e SC1091 "${files[@]}"
