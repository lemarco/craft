#!/usr/bin/env bash
# Rename crafty -> crafty across the workspace (crates, docs, scripts, deploy).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

rename_crate_dir() {
  local from=$1 to=$2
  if [[ -d "crates/$from" && ! -d "crates/$to" ]]; then
    git mv "crates/$from" "crates/$to"
  fi
}

for d in crafty-e2e-queue-client crafty-test-support crafty-store-redis \
  crafty-e2e-client crafty-dashboard crafty-storage crafty-proto crafty-macros \
  crafty-client crafty-actor crafty-core crafty-node crafty-net crafty-sim \
  crafty-ops crafty-fuzz; do
  rename_crate_dir "$d" "crafty-${d#crafty-}"
done
rename_crate_dir crafty crafty

for f in .cursor/rules/crafty-architecture.mdc .cursor/rules/crafty-quality-gate.mdc \
  .cursor/rules/crafty-testing.mdc .cursor/rules/crafty-commits.mdc; do
  if [[ -f $f ]]; then
    git mv "$f" "${f/crafty-/crafty-}"
  fi
done

for d in .cursor/skills/crafty-add-feature .cursor/skills/crafty-quality-gate \
  .cursor/skills/crafty-testing; do
  if [[ -d $d ]]; then
    git mv "$d" "${d/crafty-/crafty-}"
  fi
done

while IFS= read -r -d '' f; do
  perl -i -pe '
    s/crafty-/crafty-/g;
    s/CRAFTY_/CRAFTY_/g;
    s/Crafty(?!y)/Crafty/g;
    s/crafty(?!y)/crafty/g;
  ' "$f"
done < <(find . \( -path ./target -o -path ./.git \) -prune -o -type f \( \
  -name '*.rs' -o -name '*.toml' -o -name '*.md' -o -name '*.mdc' -o -name '*.sh' -o \
  -name '*.yml' -o -name '*.yaml' -o -name 'SKILL.md' -o -name 'AGENTS.md' -o \
  -name 'CHANGELOG.md' -o -name 'lefthook.yml' -o -name 'Dockerfile' -o -name '*.stderr' \
  \) -print0)

echo "rename-to-crafty: done"
