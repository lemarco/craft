#!/usr/bin/env bash
# Install local tools that materially cut compile/test time for crafty.
#
# Usage:
#   ./scripts/install-dev-tools.sh          # nextest + mold (if missing)
#   ./scripts/install-dev-tools.sh --sccache  # also enable sccache wrapper in shell rc

set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="${HOME}/.cargo/bin:/usr/local/bin:/opt/homebrew/bin:${PATH}"

install_nextest() {
  if command -v cargo-nextest >/dev/null 2>&1; then
    echo "cargo-nextest already installed: $(cargo nextest --version)"
    return 0
  fi
  echo ">> installing cargo-nextest (parallel test runner)"
  mkdir -p "${HOME}/.cargo/bin"
  curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C "${HOME}/.cargo/bin"
  cargo nextest --version
}

install_mold() {
  if command -v mold >/dev/null 2>&1; then
    echo "mold installed: $(mold --version 2>&1 | head -1)"
    echo "   .cargo/config.toml uses -fuse-ld=mold on Linux"
    return 0
  fi
  echo ">> mold not found — .cargo/config.toml falls back to lld or system ld"
  if command -v pacman >/dev/null 2>&1; then
    echo "   install: sudo pacman -S mold"
  elif command -v brew >/dev/null 2>&1; then
    echo "   install: brew install mold"
  fi
}

enable_sccache() {
  if ! command -v sccache >/dev/null 2>&1; then
    echo "sccache not found — skip (pacman/brew install sccache)"
    return 1
  fi
  local rc="${HOME}/.bashrc"
  if [[ -f "${HOME}/.zshrc" ]]; then
    rc="${HOME}/.zshrc"
  fi
  local line='export RUSTC_WRAPPER=sccache'
  if grep -qF "$line" "$rc" 2>/dev/null; then
    echo "sccache already enabled in $rc"
  else
    printf '\n# crafty: cache rustc artifacts across clean builds\n%s\n' "$line" >>"$rc"
    echo "appended RUSTC_WRAPPER=sccache to $rc (restart shell or: source $rc)"
  fi
  sccache --show-stats 2>/dev/null || true
}

install_nextest
install_mold

if [[ "${1:-}" == "--sccache" ]]; then
  enable_sccache
fi

echo ">> dev tools ready — try: ./scripts/test-fast.sh -p crafty-core"
