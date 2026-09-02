#!/usr/bin/env bash
# Verify internal markdown links resolve to existing files (docs/, README, AGENTS, examples).
# Skips http(s), mailto, and anchor-only targets. Anchors are not validated.

set -euo pipefail
cd "$(dirname "$0")/.."

python3 - <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(".").resolve()
LINK_RE = re.compile(r"\]\(([^)]+)\)")

SCAN_ROOTS = [
    ROOT / "docs",
    ROOT / "examples",
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / "CHANGELOG.md",
    ROOT / "CONTRIBUTING.md",
]

SKIP_PREFIXES = ("http://", "https://", "mailto:", "ftp://")
SKIP_TARGETS = {"", "#"}


def markdown_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if root.is_file():
            files.append(root)
        elif root.is_dir():
            files.extend(sorted(root.rglob("*.md")))
    for readme in (ROOT / "crates").glob("*/README.md"):
        files.append(readme)
    return sorted(set(files))


errors: list[str] = []

for md_file in markdown_files():
    text = md_file.read_text(encoding="utf-8")
    for match in LINK_RE.finditer(text):
        target = match.group(1).strip()
        if target.startswith(SKIP_PREFIXES) or target in SKIP_TARGETS or target.startswith("#"):
            continue
        path_part = target.split("#", 1)[0]
        if not path_part:
            continue
        resolved = (md_file.parent / path_part).resolve()
        if not resolved.exists() and not resolved.is_dir():
            rel = md_file.relative_to(ROOT)
            errors.append(f"{rel}: [{target}]")

if errors:
    print("Broken internal markdown links:", file=sys.stderr)
    for err in errors:
        print(f"  {err}", file=sys.stderr)
    sys.exit(1)

print(f"doc links ok ({len(markdown_files())} files)")
PY
