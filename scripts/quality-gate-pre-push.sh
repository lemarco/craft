#!/usr/bin/env bash
# Pre-push quality gate — run manually or via lefthook pre-push.
# Aligned with .gitlab-ci.yml fast lane (+ optional release build).

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }

maybe_tee() {
  if [[ -n "${CRAFTY_HOOK_LOG:-}" ]]; then
    tee -a "${CRAFTY_TEST_LOG:-target/test-run.log}"
  else
    cat
  fi
}

export NEXTEST_PROFILE=ci

log ">> fmt"
bash scripts/gate-fmt.sh --check 2>&1 | maybe_tee

log ">> clippy (pedantic)"
bash scripts/gate-clippy.sh 2>&1 | maybe_tee

log ">> tests"
if command -v cargo-nextest >/dev/null 2>&1; then
  # Examples are clippy-checked above; skip linking them again (CI fast lane).
  cargo nextest run --profile ci --workspace --all-features --lib --tests --bins 2>&1 | maybe_tee
else
  cargo test --workspace --all-features --lib --tests --bins 2>&1 | maybe_tee
fi

log ">> doctests"
cargo test --workspace --doc --all-features 2>&1 | maybe_tee

log ">> doc"
cargo doc --workspace --no-deps --all-features 2>&1 | maybe_tee

log ">> publish dry-run"
# Workspace dry-run resolves path deps locally (per-crate dry-run fails when the
# new workspace version is not yet on crates.io — e.g. crafty-proto 0.2.0 during
# a 0.1 → 0.2 release).
cargo publish --workspace --dry-run --allow-dirty 2>&1 | maybe_tee

log ">> msrv"
bash scripts/check-msrv.sh 2>&1 | maybe_tee

log ">> release build"
if [[ "${CRAFTY_SKIP_RELEASE:-1}" != "1" ]]; then
  cargo build --workspace --all-features --release 2>&1 | maybe_tee
else
  log ">> release build skipped (CRAFTY_SKIP_RELEASE=1; lefthook: lefthook run pre-push --tags release)"
fi

log ">> pre-push gate ok"
