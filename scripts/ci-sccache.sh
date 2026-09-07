#!/usr/bin/env bash
# Install a pinned sccache binary for GitLab CI (matches .cargo/config.toml
# rustc-wrapper). Prefer a release tarball over `cargo install` so before_script
# stays fast and does not thrash the registry cache.
set -euo pipefail

SCCACHE_VERSION="${SCCACHE_VERSION:-0.17.0}"
ARCH="${SCCACHE_ARCH:-x86_64-unknown-linux-musl}"
BIN_DIR="${CARGO_HOME:-${HOME}/.cargo}/bin"
mkdir -p "$BIN_DIR"
export PATH="${BIN_DIR}:${PATH}"

if command -v sccache >/dev/null 2>&1; then
  echo "ci-sccache: already on PATH ($(sccache --version 2>&1 | head -1))"
else
  url="https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${ARCH}.tar.gz"
  echo "ci-sccache: downloading ${url}"
  tmp="$(mktemp -d)"
  # curl follows redirects; fail the job if the release asset is missing.
  curl -fsSL "$url" | tar -xz -C "$tmp"
  install -m755 "$tmp"/sccache-v"${SCCACHE_VERSION}"-"${ARCH}"/sccache "$BIN_DIR/sccache"
  rm -rf "$tmp"
  sccache --version
fi

# Keep the cache small on saas-linux-small (~26G root).
export SCCACHE_DIR="${SCCACHE_DIR:-${CI_PROJECT_DIR:-.}/.sccache}"
export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-2G}"
mkdir -p "$SCCACHE_DIR"

# Ensure the wrapper from .cargo/config.toml resolves even if RUSTC_WRAPPER is unset.
export RUSTC_WRAPPER="${RUSTC_WRAPPER:-sccache}"

sccache --start-server >/dev/null 2>&1 || true
echo "ci-sccache: SCCACHE_DIR=$SCCACHE_DIR SCCACHE_CACHE_SIZE=$SCCACHE_CACHE_SIZE RUSTC_WRAPPER=$RUSTC_WRAPPER"
sccache --show-stats 2>&1 | sed -n '1,20p' || true
