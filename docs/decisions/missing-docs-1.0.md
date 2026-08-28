# `missing_docs` at 1.0

**Status:** Accepted  
**Date:** 2026-08-28  
**Backlog:** B-11b

## Policy

| Phase | Workspace lint | CI / hooks |
|-------|----------------|------------|
| **Pre-1.0 (`0.x`)** | `missing_docs = "warn"` | `RUSTFLAGS=-A missing_docs` keeps gates green |
| **1.0 stabilization** | `missing_docs = "deny"` on **published** crates | No allow; `./scripts/docs-missing-audit.sh --workspace` must pass |

Published crates are those with `publish = true` in the workspace (see root
`Cargo.toml` `default-members` / publish scripts).

## Tracking

Run locally before a release candidate:

```bash
./scripts/docs-missing-audit.sh --workspace
```

Fix warnings crate-by-crate; prefer documenting public items on the **`crafty`**
facade first (see [public-api-1.0.md](public-api-1.0.md)).

## Exemptions

- `publish = false` crates (`crafty-test-support`, `crafty-e2e-*`, benchmarks)
- Private / `pub(crate)` items (no rustdoc required)
- Do **not** blanket-allow `missing_docs` at 1.0 — use targeted `#[expect(missing_docs)]`
  only for generated or truly internal modules, with a comment.

## Related

- [library-and-publishing.md](library-and-publishing.md)
- [public-api-1.0.md](public-api-1.0.md)
