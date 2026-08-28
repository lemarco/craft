# Production runbook

Operational checklist for running crafty on **N identical VPS or bare-metal nodes** — one binary, embedded redb, no mandatory Redis. Deep dives link out to focused guides.

**Deployment model:** [deployment-model](../decisions/deployment-model.md)  
**Certs:** [certs.md](../certs.md)

## VPS deployment checklist

1. **Build once** — same artifact on every node (`crafty-node` or your app embedding `crafty`).
2. **Unique node id** — set `CRAFTY_NODE_ID` (1…N) per host.
3. **Listen address** — `CRAFTY_LISTEN=0.0.0.0:7443` (QUIC/mTLS crafty wire).
4. **Data directory** — `CRAFTY_DATA_DIR=/var/lib/crafty/data` (redb: Raft groups, queues, actor store).
5. **Peers** — static `CRAFTY_PEERS=id@host:7443,...` **or** dynamic `CRAFTY_JOIN_SEEDS` on first boot.
6. **TLS** — `CRAFTY_NODE_CERT`, `CRAFTY_NODE_KEY`, `CRAFTY_CA_CERT` (see [certs.md](../certs.md)).
7. **Admin (optional)** — `CRAFTY_ADMIN=0.0.0.0:8080`; bind privately or firewall. Dashboard at `/dashboard`, Prometheus at `/metrics`.
8. **Firewall** — allow **UDP/TCP 7443** between cluster members; restrict admin port to ops networks only.

### First node (bootstrap)

- Set `CRAFTY_PEERS` to itself or leave join seeds empty for a single-node dev cluster.
- For multi-node: node 1 starts with voter config; subsequent nodes use `CRAFTY_JOIN_SEEDS` pointing at an existing member.

### Adding nodes

- New VPS gets the same binary, certs signed by the same CA, a new `CRAFTY_NODE_ID`, and join seeds / peer list.
- Wait for `/ready` (HTTP 200) before sending traffic.

## Operations (deep dives)

| Task | Guide |
|------|-------|
| Snapshot / restore `data_dir` | [backup-restore.md](backup-restore.md) |
| Rolling wire vs app semver upgrades | [rolling-upgrade.md](rolling-upgrade.md) |
| PKI generation and SAN naming | [certs.md](../certs.md) |
| `crafty-ops` CLI | [crafty-ops README](../../crates/crafty-ops/README.md) |

**Pre-upgrade:** export a backup before risky app semver bumps or catalog expansion.

## Multi-Raft

When write load exceeds a single Raft group:

- Enable `.raft_groups(n)` / multi-group catalog — see [multi-raft.md](../decisions/multi-raft.md).
- **Backup must include `group-meta.redb`** (Meta-Raft coordinator: saga journal, catalog).
- Rebalance and expansion are leader-driven; monitor `/introspect/raft-groups` on the admin port.

Start with **one group** until metrics or latency justify adding groups — premature sharding adds operational surface.

## Observability

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness (always 200) |
| `GET /ready` | Membership + not draining |
| `GET /metrics` | Prometheus (Raft, queue depth, saga counters) |
| `GET /dashboard` | HTML UI — cluster, actors, queues, workflows |
| `GET /introspect/queues` | Per-stream pending / leased depth |
| `GET /introspect/sagas` | Saga journal records (running / done / stuck) |

Scrape `/metrics` from a private network; do not expose the admin port on the public internet without TLS (`CRAFTY_ADMIN_TLS_*`) and firewall rules.

## Post-deploy verification

- [ ] `/ready` returns 200 on every node
- [ ] Sample `propose` / `query` or app-specific health check
- [ ] `./e2e/run.sh` or your integration smoke test
- [ ] Backup export tested to object storage (if DR required)

## Related

- [scenarios/README.md](../scenarios/README.md) — product patterns
- [getting-started.md](../getting-started.md) — CraftyApp tutorial
- [multi-raft.md § Production reliability](../decisions/multi-raft.md#production-reliability)
