#!/usr/bin/env bash
# Rename trembita -> trembita across the workspace (crates, docs, scripts, deploy).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

rename_crate_dir() {
  local from=$1 to=$2
  if [[ -d "crates/$from" && ! -d "crates/$to" ]]; then
    git mv "crates/$from" "crates/$to"
  fi
}

for d in trembita-e2e-queue-client trembita-test-support trembita-store-redis \
  trembita-e2e-client trembita-dashboard trembita-storage trembita-proto trembita-macros \
  trembita-client trembita-actor trembita-core trembita-node trembita-net trembita-sim \
  trembita-ops trembita-fuzz; do
  rename_crate_dir "$d" "trembita-${d#trembita-}"
done
rename_crate_dir trembita trembita

for f in .cursor/rules/trembita-architecture.mdc .cursor/rules/trembita-quality-gate.mdc \
  .cursor/rules/trembita-testing.mdc .cursor/rules/trembita-commits.mdc; do
  if [[ -f $f ]]; then
    git mv "$f" "${f/trembita-/trembita-}"
  fi
done

for d in .cursor/skills/trembita-add-feature .cursor/skills/trembita-quality-gate \
  .cursor/skills/trembita-testing; do
  if [[ -d $d ]]; then
    git mv "$d" "${d/trembita-/trembita-}"
  fi
done

while IFS= read -r -d '' f; do
  perl -i -pe '
    s/trembita-/trembita-/g;
    s/TREMBITA_/TREMBITA_/g;
    s/Trembita(?!y)/Trembita/g;
    s/trembita(?!y)/trembita/g;
  ' "$f"
done < <(find . \( -path ./target -o -path ./.git \) -prune -o -type f \( \
  -name '*.rs' -o -name '*.toml' -o -name '*.md' -o -name '*.mdc' -o -name '*.sh' -o \
  -name '*.yml' -o -name '*.yaml' -o -name 'SKILL.md' -o -name 'AGENTS.md' -o \
  -name 'CHANGELOG.md' -o -name 'lefthook.yml' -o -name 'Dockerfile' -o -name '*.stderr' \
  \) -print0)

echo "rename-to-trembita: done"
