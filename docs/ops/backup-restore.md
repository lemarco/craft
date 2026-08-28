# Backup and restore runbook

Operational guide for snapshotting a craft node's on-disk Raft layout and
restoring it after failure or migration. Uses the [`craft-ops`](../../crates/craft-ops/)
CLI (Tier 2 production reliability).

## When to use

- **Disaster recovery** — replace a lost VPS from object storage.
- **Migration** — copy `data_dir` to a new host before first start.
- **Pre-upgrade checkpoint** — tarball before a risky app semver bump.

This covers **persistence files** (`group-*.redb` under `data_dir`). It does
not replace cluster **membership** operations — restored nodes must still match
the committed voter set or join via `/cluster/join`.

## Layout

Multi-group clusters store one Redb file per Raft group:

```text
/data/craft/
  group-meta.redb # Meta-Raft coordinator (multi-Raft only)
  group-0.redb    # user raft group 0
  group-1.redb
  ...
```

Single-group nodes use only `group-0.redb`.

## Local export / import

**Export** (while the node is **stopped** — copying live Redb files risks
corruption):

```bash
craft-ops backup export \
  --data-dir /var/lib/craft/data \
  --archive /tmp/craft-backup.tar.gz
```

**Restore** on a fresh machine:

```bash
craft-ops backup import \
  --data-dir /var/lib/craft/data \
  --archive /tmp/craft-backup.tar.gz
```

Then start `craft-node` with the same `CRAFT_NODE_ID`, peers, and certs as
before the outage.

## Object storage (S3 / GCS)

Push after export:

```bash
craft-ops backup push \
  --archive /tmp/craft-backup.tar.gz \
  --dest s3://my-bucket/craft/node-2/2026-08-27.tar.gz
```

Pull before import:

```bash
craft-ops backup pull \
  --src s3://my-bucket/craft/node-2/2026-08-27.tar.gz \
  --archive /tmp/craft-backup.tar.gz
```

GCS uses the same URI shape: `gs://bucket/key`. Local smoke tests use
`file:///path/to/dir/object.tar.gz`.

Credentials follow the standard cloud SDK env vars (`AWS_*`, `GOOGLE_APPLICATION_CREDENTIALS`).
No vault integration — inject secrets via your orchestrator.

## Verification checklist

1. Stop the node (`systemctl stop craft-node` or container exit).
2. Export → push to object storage.
3. On a staging host: pull → import → start with matching identity.
4. Confirm `/ready` is `200`, `/introspect/cluster` shows expected `commit_index`.
5. Run a test propose/query through the client wire.

## Related

- [tier2-production-reliability.md](../decisions/tier2-production-reliability.md)
- [write-sharding-multi-raft.md](../decisions/write-sharding-multi-raft.md) — `data_dir` layout
- [cert-provisioning.md](../decisions/cert-provisioning.md) — mTLS material on restore
