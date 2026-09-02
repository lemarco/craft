---
name: trembita-publishing
description: >-
  Publish trembita workspace to crates.io — tag release, rate-limit-safe upload,
  resume partial publishes, post-publish docs. Use when releasing, publishing,
  tagging vX.Y.Z, crates.io 429 errors, or resume after failed publish.
---

# trembita publishing

See [docs/process.md](../../docs/process.md) for the full release diagram.

## Quick commands

| Step | Command |
|------|---------|
| Release gate | `./scripts/release.sh --dry-run` |
| Full release | `./scripts/release.sh <version> --publish` |
| Publish, no git push | `./scripts/release.sh <version> --publish --no-push` |
| Publish existing tag | `./scripts/release.sh <version> --publish-only` |
| Resume partial upload | `./scripts/publish-workspace.sh <version>` |

`--publish` defaults to **git push** + **release build**. Gate runs autofix, ci-fast-lane, MSRV strict.

## Rate limits

Never `cargo publish --workspace` for real uploads. Use `publish-workspace.sh` (order, skip, 429 retry).

## Partial publish recovery

```bash
./scripts/publish-workspace.sh 0.1.0   # idempotent; skips indexed crates
```

## Checklist

- [ ] CHANGELOG updated
- [ ] `./scripts/release.sh --dry-run` green (optional — release runs gate anyway)
- [ ] `./scripts/release.sh X.Y.Z --publish`
- [ ] All crates on crates.io; origin has commits + tag

## Troubleshooting

| Symptom | Action |
|---------|--------|
| `429 Too Many Requests` | Wait for script retry |
| `tag vX.Y.Z already exists` | `--publish-only` |
| `cargo doc` failed | Fix rustdoc; patch release — gate blocks |
| MSRV strict fail | `rustup toolchain install 1.90` |

## Related

- [docs/releasing.md](../../docs/releasing.md)
- Rule: `.cursor/rules/trembita-publishing.mdc`
