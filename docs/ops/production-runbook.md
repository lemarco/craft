# Production runbook

Operational checklist for running trembita on **N identical VPS or bare-metal nodes** — one binary, embedded redb, no mandatory Redis. Deep dives link out to focused guides.

**Deployment model:** [deployment-model](../decisions/deployment-model.md)  
**Certs:** [certs.md](../certs.md)

## VPS deployment checklist

**Product app** ([env.md](../env.md)) — typical elastic cluster:

1. **Build once** — same artifact on every node.
2. **`TREMBITA_LISTEN`** — one port (QUIC + HTTP), e.g. `0.0.0.0:443`.
3. **`TREMBITA_DATA_DIR`** — redb + persisted `node-id` after join.
4. **`TREMBITA_CERT_DIR`** — shared PKI layout (`ca.pem`, `node-{id}.pem`); see [certs.md](../certs.md).
5. **Seed** — `TREMBITA_ALLOW_JOIN=1`; **joiners** — `TREMBITA_JOIN_SEEDS=1@seed:443` (no static `TREMBITA_PEERS`).
6. **Optional `GATEWAY_TOKEN`** — protect product HTTP in non-dev environments.
7. **Firewall** — allow **UDP + TCP** on the listen port between members; restrict HTTP to ingress/LB as needed.

Ops routes (`/health`, `/ready`, `/metrics`, `/dashboard`) are on the **same** TCP port as product APIs when using [`TrembitaApp::from_env`](../crates/trembita/src/app/runtime.rs).

### First node (bootstrap)

- Solo or seed: omit `TREMBITA_JOIN_SEEDS`; id `1` until join assigns others.

### Adding nodes

- Same binary + `TREMBITA_CERT_DIR` (join bootstrap cert `node-0.pem` in compose); set `TREMBITA_JOIN_SEEDS` to a live seed.
- Wait for `GET /ready` (HTTP 200) before traffic.

### Static clusters (`trembita-node`, e2e)

Legacy ops layout: explicit `TREMBITA_NODE_ID`, static `TREMBITA_PEERS`, per-file PEM paths — see [certs.md](../certs.md#static-multi-node-bootstrap).

### Join troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Join succeeds but node stays in `learners`, not `voters` | Default dynamic join requests `learner`. Set joiner `TREMBITA_JOIN_ROLE=voter` **and** seed `TREMBITA_ALLOW_VOTER_JOIN=1`, or bootstrap voters with `TREMBITA_PEERS`. |
| No leader for minutes after boot | Often mTLS mismatch (wrong CA, SAN, or cert for `TREMBITA_NODE_ID`). Check `RUST_LOG=trembita::net=warn,trembita::raft=debug` for QUIC handshake and pre-vote reject lines, then verify [certs.md](../certs.md), `/ready`, and UDP 7443 reachability. |
| `cluster rejected join: VoterJoinDisabled` | Seed has not enabled `TREMBITA_ALLOW_VOTER_JOIN=1`. |

## Operations (deep dives)

| Task | Guide |
|------|-------|
| Snapshot / restore `data_dir` | [backup-restore.md](backup-restore.md) |
| Rolling wire vs app semver upgrades | [rolling-upgrade.md](rolling-upgrade.md) |
| PKI generation and SAN naming | [certs.md](../certs.md) |
| `trembita-ops` CLI | [trembita-ops README](../../crates/trembita-tools/README.md) |

**Pre-upgrade:** export a backup before risky app semver bumps or catalog expansion.

## Multi-Raft

When write load exceeds a single Raft group:

- Enable `.raft_groups(n)` / multi-group catalog — see [multi-raft.md](../decisions/multi-raft.md).
- **Backup must include `group-meta.redb`** (Meta-Raft coordinator: saga journal, catalog).
- Rebalance and expansion are leader-driven; monitor `GET /introspect/raft-groups` on the ops HTTP bind (`TREMBITA_LISTEN` for product apps).

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

Scrape `/metrics` from a private network; do not expose the ops HTTP listener on the public internet without TLS (`TREMBITA_HTTP_TLS_*`) and firewall rules.

## Post-deploy verification

- [ ] `/ready` returns 200 on every node
- [ ] Sample `propose` / `query` or app-specific health check
- [ ] `./e2e/run.sh` or your integration smoke test
- [ ] Backup export tested to object storage (if DR required)

## Related

- [scenarios/README.md](../scenarios/README.md) — product patterns
- [getting-started.md](../getting-started.md) — TrembitaApp tutorial
- [multi-raft.md § Production reliability](../decisions/multi-raft.md#production-reliability)
