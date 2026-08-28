#!/usr/bin/env bash
# Shellcheck gate for repo scripts (optional if shellcheck missing).
#
# Usage:
#   ./scripts/check-shell-scripts.sh              # all scripts/ and e2e/*.sh
#   ./scripts/check-shell-scripts.sh path/to/a.sh # staged files only (lefthook)

set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v shellcheck >/dev/null 2>&1; then
  echo "warn: shellcheck not installed — skipping (pacman -S shellcheck / apt install shellcheck)" >&2
  exit 0
fi

declare -a files=()
if [[ $# -gt 0 ]]; then
  for path in "$@"; do
    [[ "$path" == *.sh ]] || continue
    [[ -f "$path" ]] || continue
    files+=("$path")
  done
else
  mapfile -t files < <(find scripts e2e -name '*.sh' -type f 2>/dev/null | sort)
fi

if [[ ${#files[@]} -eq 0 ]]; then
  exit 0
fi

shellcheck -e SC1091 "${files[@]}"
