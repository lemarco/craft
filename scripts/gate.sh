#!/usr/bin/env bash
# Unified quality gate entry point.
#
# Usage:
#   ./scripts/gate.sh --tier commit [--staged-only] [--stage]
#   ./scripts/gate.sh --tier push [--no-autofix-commit]
#   ./scripts/gate.sh --tier release [--release-build]
#
# Lefthook pre-push runs ./scripts/gate-step.sh per step for live progress.

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

log() { printf '[%s] >> %s\n' "$(date -Is)" "$*" >&2; }

usage() {
  cat <<'EOF'
usage: gate.sh --tier commit|push|release [options]

  --tier commit          autofix, doc-links, clippy
  --tier push            autofix + ci-fast-lane + examples + msrv + release build
  --tier release         push tier with MSRV strict + full-workspace autofix

options:
  --staged-only          autofix only staged crates (commit tier)
  --stage                git add -u after autofix (manual pre-commit)
  --no-autofix-commit    do not create chore autofix commit on push
  --release-build        run cargo build --release after other checks
EOF
  exit 1
}

TIER=""
STAGED_ONLY=0
NO_AUTOFIX_COMMIT=0
RELEASE_BUILD=0
GATE_STAGE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier)
      TIER="${2:-}"
      shift 2
      ;;
    --staged-only) STAGED_ONLY=1; shift ;;
    --stage) GATE_STAGE=1; shift ;;
    --no-autofix-commit) NO_AUTOFIX_COMMIT=1; shift ;;
    --release-build) RELEASE_BUILD=1; shift ;;
    -h | --help) usage ;;
    *) echo "error: unknown argument: $1" >&2; usage ;;
  esac
done

[[ -n "$TIER" ]] || usage

run_autofix_commit() {
  local args=()
  if [[ "$STAGED_ONLY" == "1" ]]; then
    args+=(--staged-only)
  fi
  if [[ "$TIER" == "commit" && "$GATE_STAGE" == "1" ]] || [[ "$TIER" != "commit" ]]; then
    args+=(--stage)
  fi
  log "autofix (fmt + clippy --fix)"
  bash scripts/gate-autofix.sh "${args[@]}"
  if [[ "$TIER" == "push" || "$TIER" == "release" ]]; then
    if [[ "${CRAFTY_NO_AUTOFIX_COMMIT:-0}" == "1" || "$NO_AUTOFIX_COMMIT" == "1" ]]; then
      return 0
    fi
    if [[ "${CRAFTY_AUTOFIX_COMMIT:-0}" == "1" ]] && ! git diff --cached --quiet; then
      log "autofix commit"
      git commit -m "chore: apply fmt/clippy autofix"
    fi
  fi
}

run_push_steps() {
  if [[ "${CRAFTY_GATE_AUTOFIX:-0}" == "1" ]]; then
    run_autofix_commit
  fi
  bash scripts/ci-fast-lane.sh
  bash scripts/gate-step.sh examples
  bash scripts/gate-step.sh showcase
  bash scripts/gate-step.sh msrv
  if [[ "$RELEASE_BUILD" == "1" || "${CRAFTY_SKIP_RELEASE:-1}" != "1" ]]; then
    bash scripts/gate-step.sh release-build
  else
    log "release build skipped (use --release-build or CRAFTY_SKIP_RELEASE=0)"
  fi
}

case "$TIER" in
  commit)
    log "gate: commit tier"
    run_autofix_commit
    bash scripts/gate-step.sh doc-links
    bash scripts/gate-step.sh clippy
    log "commit gate ok"
    ;;
  push)
    log "gate: push tier"
    run_push_steps
    log "push gate ok"
    ;;
  release)
    export CRAFTY_MSRV_STRICT=1
    export CRAFTY_GATE_AUTOFIX=1
    STAGED_ONLY=0
    log "gate: release tier"
    run_push_steps
    log "release gate ok"
    ;;
  *)
    echo "error: unknown tier: $TIER (expected commit|push|release)" >&2
    exit 1
    ;;
esac
