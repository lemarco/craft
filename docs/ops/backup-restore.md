# Backup and restore runbook (B-48)

Operational guide for **product** trembita nodes on VPS / bare metal: snapshot
[`TREMBITA_DATA_DIR`](../env.md), preserve [`TREMBITA_CERT_DIR`](../certs.md), and
recover after single-node loss without corrupting the Raft quorum.

**CLI:** [`trembita-ops`](../../crates/trembita-tools/) (`backup export|import|push|pull`).
**Wrapper:** [`scripts/backup-data-dir.sh`](../../scripts/backup-data-dir.sh) (stop-safe export).
**Deploy layout:** [deploy/](../../deploy/README.md) (B-45).

## What to back up

| Scope | Path (typical) | Notes |
|-------|----------------|-------|
| **Raft + product redb** | `TREMBITA_DATA_DIR` | `group-*.redb`, optional `group-meta.redb`, `queue-*.redb`, `mailbox-spool.redb`, app spools |
| **Assigned identity** | `{data_dir}/node-id` | Written on first boot / join — **must** travel with restore on same logical node |
| **mTLS material** | `TREMBITA_CERT_DIR` | `ca.pem`, `node-{id}.pem` (+ keys); backup **separately** from data (different rotation cadence) |
| **Env / secrets** | `/etc/myapp/trembita.env` | Not in `data_dir` — your config management (session secret, join seeds, gateway token) |

Multi-group clusters: one Redb file per Raft group ([multi-raft](../decisions/multi-raft.md)).

```text
/var/lib/myapp/data/          # TREMBITA_DATA_DIR
  node-id
  group-0.redb
  group-meta.redb             # multi-Raft coordinator
  group-1.redb
  queue-0.redb                # when job queues enabled
  mailbox-spool.redb          # optional B-41 durable mailbox
```

## What trembita does **not** do automatically

- Scheduled or incremental backups
- Cross-node replication of `data_dir` (each peer has its **own** Raft log)
- “Restore cluster” from one node's tarball — membership stays in **committed Raft config**
- Hot copy of live Redb files while the process is running (risk of torn pages)
- Backup of external Postgres/Redis cap-store backends (use those products' DR tools)

## Supported procedure (export while stopped)

1. **Drain and stop** the process (`systemctl stop myapp` — align `TimeoutStopSec` with [deploy/systemd](../../deploy/systemd/trembita.service)).
2. **Export** data (and optionally certs):

```bash
# From repo dev tree (builds trembita-ops if needed):
./scripts/backup-data-dir.sh \
  --data-dir /var/lib/myapp/data \
  --archive /var/backups/myapp-data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz \
  --cert-dir /var/lib/myapp/certs
```

Or directly:

```bash
trembita-ops backup export \
  --data-dir /var/lib/myapp/data \
  --archive /tmp/trembita-backup.tar.gz
```

3. **Push** to object storage (optional):

```bash
trembita-ops backup push \
  --archive /tmp/trembita-backup.tar.gz \
  --dest s3://my-bucket/myapp/node-a/2026-09-17.tar.gz
```

4. **Start** the node again if it was only a checkpoint (not a rebuild).

**RPO:** time since last successful export. **RTO:** import + start + `/ready` + smoke test.

## Restore scenarios

### A — Replace the same VPS (same role, same `node-id`)

Use when the disk died but this host is still **the same cluster member**.

1. Stop trembita on the replacement host (if anything was started).
2. Pull/import tarball into **empty** `TREMBITA_DATA_DIR` (or move aside old dir first).
3. Restore **matching** certs for that `node-id` (SAN must match assigned id — [certs.md](../certs.md)).
4. Restore the same `EnvironmentFile` (especially `TREMBITA_LISTEN`, **no** `TREMBITA_NODE_ID`).
5. Start service → `GET /ready` → verify `/introspect/cluster` commit index advances with peers.

Do **not** clone one node's `data_dir` onto a **second** live voter — two processes with duplicate Raft identity corrupt the group.

### B — Lost node, cluster still has quorum

If the failed machine was a **joiner** or you intend to **remove** it from membership:

1. Operate remaining voters normally; use cluster ops to remove the dead member if it was a voter ([cluster-membership](../decisions/cluster-membership.md)).
2. Provision a **fresh** host: empty `data_dir`, join via `TREMBITA_JOIN_SEEDS` ([joiner env example](../../deploy/env/joiner.env.example)).
3. Restore **only** if you must recover **that node's** historical local state **and** membership still expects that `node-id` — expert path; prefer fresh join when role was disposable.

### C — Migration to new hardware (planned)

1. Stop old node → export → import on new machine **before first start**.
2. Move certs + env; update DNS/LB targets.
3. Start once; confirm peers still reach the same `node-id` on wire.

### D — Catastrophic quorum loss

If **most** voters lost `data_dir` without backup, automatic recovery is **not** supported — restore from **per-node** backups taken when cluster was healthy, or rebuild cluster from scratch (new seed, empty catalogs). Document this in your DR plan.

## Cert material on restore

- Leaf cert must match **`node-id`** in `data_dir` and SAN in [certs.md](../certs.md).
- Join bootstrap cert `node-0.pem` is only for initial join — not a substitute for assigned `node-{id}.pem`.
- CA rotation: reissue leaves from new CA on all nodes; see [certs.md](../certs.md).

## Verification checklist

1. Node stopped → export → optional push.
2. Staging restore: import → start with production-equivalent env.
3. `GET /ready` returns **200**.
4. `/introspect/cluster` (or ops-summary) shows expected role and commit index.
5. Gateway smoke: session cookie or app-specific write/read.
6. After upgrade-related restore: confirm wire semver still compatible ([rolling-upgrade.md](rolling-upgrade.md)).

## Local import / object storage

**Import:**

```bash
trembita-ops backup import \
  --data-dir /var/lib/myapp/data \
  --archive /tmp/trembita-backup.tar.gz
```

**Pull** before import:

```bash
trembita-ops backup pull \
  --src s3://my-bucket/myapp/node-a/2026-09-17.tar.gz \
  --archive /tmp/trembita-backup.tar.gz
```

GCS: `gs://bucket/key`. Local smoke: `file:///path/dir/object.tar.gz`. Credentials: standard `AWS_*` / `GOOGLE_APPLICATION_CREDENTIALS`.

## Preflight

App repos with `deploy/`:

```bash
trembita doctor --preflight
```

Reminds you to document backup for `TREMBITA_DATA_DIR` + certs when deploy templates are present.

## Related

- [production-runbook § B-48](production-runbook.md#backup-restore-dr-b-48)
- [deploy/backup-restore.md](../../deploy/backup-restore.md) — operator cheat sheet
- [multi-raft § production reliability](../decisions/multi-raft.md#production-reliability)
- [rolling-upgrade.md](rolling-upgrade.md) — backup before risky upgrades
- [upgrade-coordinator.md](../decisions/upgrade-coordinator.md)
