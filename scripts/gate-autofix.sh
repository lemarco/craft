#!/usr/bin/env bash
# Auto-fix fmt and fixable clippy lints; optionally stage tracked changes.
#
# Usage:
#   ./scripts/gate-autofix.sh                    # fix whole workspace
#   ./scripts/gate-autofix.sh --stage            # fix + git add -u
#   ./scripts/gate-autofix.sh --staged-only      # clippy --fix only for staged crates
#   ./scripts/gate-autofix.sh --stage --staged-only

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh
source scripts/clippy-args.sh

STAGE=0
STAGED_ONLY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) STAGE=1; shift ;;
    --staged-only) STAGED_ONLY=1; shift ;;
    *) echo "error: unknown argument: $1" >&2; exit 1 ;;
  esac
done

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }

packages_from_staged_rs() {
  local file pkg
  declare -A seen=()
  while IFS= read -r file; do
    [[ -n "$file" ]] || continue
    case "$file" in
      crates/*)
        pkg="${file#crates/}"
        pkg="${pkg%%/*}"
        seen["$pkg"]=1
        ;;
      *)
        # Examples / other roots — fall back to full workspace.
        echo "__workspace__"
        return 0
        ;;
    esac
  done < <(git diff --cached --name-only --diff-filter=ACM -- '*.rs' 2>/dev/null || true)

  if [[ ${#seen[@]} -eq 0 ]]; then
    return 0
  fi
  printf '%s\n' "${!seen[@]}"
}

log ">> autofix: fmt"
cargo fmt --all

log ">> autofix: clippy (--fix)"
if [[ "$STAGED_ONLY" == "1" ]]; then
  mapfile -t packages < <(packages_from_staged_rs)
  if [[ ${#packages[@]} -eq 0 ]]; then
    log ">> autofix: no staged .rs — skipping clippy --fix"
  elif [[ "${packages[0]:-}" == "__workspace__" ]]; then
    cargo clippy --fix --workspace --all-targets --all-features \
      --allow-dirty --allow-staged \
      -- "${CLIPPY_ARGS[@]}" || true
  else
    for pkg in "${packages[@]}"; do
      log ">> autofix: clippy --fix -p ${pkg}"
      cargo clippy --fix -p "$pkg" --all-targets --all-features \
        --allow-dirty --allow-staged \
        -- "${CLIPPY_ARGS[@]}" || true
    done
  fi
else
  cargo clippy --fix --workspace --all-targets --all-features \
    --allow-dirty --allow-staged \
    -- "${CLIPPY_ARGS[@]}" || true
fi

if [[ "$STAGE" == "1" ]]; then
  if ! git diff --quiet || ! git diff --cached --quiet; then
    log ">> autofix: staging tracked changes"
    git add -u
  fi
fi
