---
name: crafty-publishing
description: >-
  Publish crafty workspace to crates.io — tag release, rate-limit-safe upload,
  resume partial publishes, post-publish docs. Use when releasing, publishing,
  tagging vX.Y.Z, crates.io 429 errors, or resume after failed publish.
---

# crafty publishing

## Quick commands

| Step | Command |
|------|---------|
| Dry-run gate | `./scripts/release.sh --dry-run` |
| Prepare tag | `./scripts/release.sh <version>` |
| Push | `git push && git push origin v<version>` |
| Publish | `./scripts/release.sh <version> --publish-only` |
| Resume partial | `./scripts/publish-workspace.sh <version>` (same command) |
| Post-publish docs | `./scripts/post-publish-docs.sh <version>` |

Prepare + publish in one go (only when tag does **not** exist yet):

```bash
./scripts/release.sh <version> --publish
```

## Rate limits — what agents must know

**Problem we hit on v0.1.0:** `cargo publish --workspace` uploaded crafty-macros, crafty-proto, crafty-core, crafty-net, crafty-storage, then **429** on crafty-actor. Re-running `--workspace` failed with *already exists*.

**Root cause:** crates.io throttles **new crate** registrations per account/time window (not the same as API read limits). A 12-crate workspace first release exceeds the burst.

**Fix:** `./scripts/publish-workspace.sh`:

1. Publishes in **dependency order** (12 crates — see script `PUBLISH_ORDER`)
2. **Skips** crates where `GET /api/v1/crates/{name}/{version}` returns 200
3. **Waits** `CRAFTY_PUBLISH_DELAY_SECS` (default 30) after each successful upload
4. On **429**, parses `try again after … GMT`, waits until then + 5s buffer, retries

```bash
# Faster (riskier) — only if updating existing crates, not first release
CRAFTY_PUBLISH_DELAY_SECS=10 ./scripts/publish-workspace.sh 0.2.0
```

**Never** use `cargo publish --workspace` for real uploads. Dry-run is fine (CI fast lane).

## Partial publish recovery

```bash
# Check what's live (example)
curl -fsS -H 'User-Agent: crafty-check' https://crates.io/api/v1/crates/crafty-actor/0.1.0

# Resume — idempotent
./scripts/publish-workspace.sh 0.1.0
```

Expected skip lines: `skip … (already on crates.io)`.

## Full checklist

### Before tag

- [ ] CHANGELOG `[Unreleased]` moved under new version
- [ ] `./scripts/release.sh --dry-run` green
- [ ] `./scripts/quality-gate-pre-push.sh` green (or CI fast lane)
- [ ] Working tree clean

### Tag + push

```bash
./scripts/release.sh 0.2.0
git push && git push origin v0.2.0
```

If version already matches manifest (first release at 0.1.0): bump skipped; tag on current HEAD.

### Publish

```bash
./scripts/release.sh 0.2.0 --publish-only
```

Or GitLab: manual **`publish`** job on tag pipeline (uses `publish-workspace.sh`).

Allow **~10–15 min** for a 12-crate first release (delays + 429 waits).

### After publish

- [ ] All 12 crates on [crates.io](https://crates.io/crates/crafty)
- [ ] Commit post-publish doc changes from `post-publish-docs.sh`
- [ ] GitLab Release with CHANGELOG excerpt (manual)

```bash
git diff README.md docs/status.md
git commit -am "docs: mark crafty v0.2.0 published on crates.io"
git push
```

## Publish order (reference)

```
crafty-macros, crafty-proto → crafty-core, crafty-storage, crafty-net →
crafty-actor → crafty-client, crafty-dashboard, crafty-sim, crafty-store-redis →
crafty
```

## Troubleshooting

| Symptom | Action |
|---------|--------|
| `429 Too Many Requests` | Wait for script retry; do not parallelize publishes |
| `already exists on crates.io index` | Normal on resume; use `publish-workspace.sh` |
| `tag vX.Y.Z already exists` | Use `--publish-only`, not bare `release.sh` |
| `working tree not clean` | Commit/stash before prepare; `--publish-only` skips prepare |
| Token missing | `CARGO_REGISTRY_TOKEN` or `cargo login` |

## Related

- [docs/releasing.md](../../docs/releasing.md)
- [library-and-publishing ADR](../../docs/decisions/library-and-publishing.md)
- Rule: `.cursor/rules/crafty-publishing.mdc`
