# Backup and restore runbook

Operational guide for snapshotting a trembita node's on-disk Raft layout and
restoring it after failure or migration. Uses the [`trembita-ops`](../../crates/trembita-tools/)
CLI for snapshot backup/restore ([multi-raft](../decisions/multi-raft.md#production-reliability)).

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
/data/trembita/
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
trembita-ops backup export \
  --data-dir /var/lib/trembita/data \
  --archive /tmp/trembita-backup.tar.gz
```

**Restore** on a fresh machine:

```bash
trembita-ops backup import \
  --data-dir /var/lib/trembita/data \
  --archive /tmp/trembita-backup.tar.gz
```

Then start `trembita-node` with the same `TREMBITA_NODE_ID`, peers, and certs as
before the outage.

## Object storage (S3 / GCS)

Push after export:

```bash
trembita-ops backup push \
  --archive /tmp/trembita-backup.tar.gz \
  --dest s3://my-bucket/trembita/node-2/2026-08-27.tar.gz
```

Pull before import:

```bash
trembita-ops backup pull \
  --src s3://my-bucket/trembita/node-2/2026-08-27.tar.gz \
  --archive /tmp/trembita-backup.tar.gz
```

GCS uses the same URI shape: `gs://bucket/key`. Local smoke tests use
`file:///path/to/dir/object.tar.gz`.

Credentials follow the standard cloud SDK env vars (`AWS_*`, `GOOGLE_APPLICATION_CREDENTIALS`).
No vault integration — inject secrets via your orchestrator.

## Verification checklist

1. Stop the node (`systemctl stop trembita-node` or container exit).
2. Export → push to object storage.
3. On a staging host: pull → import → start with matching identity.
4. Confirm `/ready` is `200`, `/introspect/cluster` shows expected `commit_index`.
5. Run a test propose/query through the client wire.

## Related

- [multi-raft.md#production-reliability](../decisions/multi-raft.md#production-reliability)
- [multi-raft.md](../decisions/multi-raft.md) — `data_dir` layout
- [certificates.md](../decisions/certificates.md) — mTLS material on restore
