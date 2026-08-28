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
# Per-crate dry-run in dependency order; --no-verify avoids resolving against
# crates.io when the workspace has API ahead of the last published release.
PUBLISH_DRY_RUN_ORDER=(
    crafty-macros crafty-proto crafty-core crafty-storage crafty-net
    crafty-actor crafty-client crafty-dashboard crafty-http crafty-sim crafty-store-redis
    crafty crafty-node
)
WS_VERSION="$(grep -m1 '^version = ' Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
crate_on_index() {
    curl -fsS -H "User-Agent: crafty-gate" \
        "https://crates.io/api/v1/crates/${1}/${WS_VERSION}" >/dev/null 2>&1
}
for pkg in "${PUBLISH_DRY_RUN_ORDER[@]}"; do
    if [[ "$pkg" == "crafty" || "$pkg" == "crafty-node" ]] \
        && ! crate_on_index crafty-http; then
        log ">> publish dry-run skip $pkg (crafty-http ${WS_VERSION} not on crates.io yet)"
        continue
    fi
    cargo publish -p "$pkg" --dry-run --no-verify --allow-dirty 2>&1 | maybe_tee
done

log ">> msrv"
bash scripts/check-msrv.sh 2>&1 | maybe_tee

log ">> release build"
if [[ "${CRAFTY_SKIP_RELEASE:-1}" != "1" ]]; then
  cargo build --workspace --all-features --release 2>&1 | maybe_tee
else
  log ">> release build skipped (CRAFTY_SKIP_RELEASE=1; lefthook: lefthook run pre-push --tags release)"
fi

log ">> pre-push gate ok"
