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
7. **`TREMBITA_GATEWAY_SESSION_SECRET`** — identical on every node when using cookie login behind a load balancer (B-29/B-40 — [gateway-cluster-auth](../decisions/gateway-cluster-auth.md)).
8. **Firewall** — allow **UDP + TCP** on the listen port between members; restrict HTTP to ingress/LB as needed ([ingress-lb.md](ingress-lb.md) — B-30).

Ops routes (`/health`, `/ready`, `/metrics`, `/dashboard`) are on the **same** TCP port as product APIs when using [`TrembitaApp::from_env`](../../crates/trembita/src/app/runtime.rs).

### First node (bootstrap)

- Solo or seed: omit `TREMBITA_JOIN_SEEDS`; id `1` until join assigns others.

### Adding nodes

- Same binary + `TREMBITA_CERT_DIR` (join bootstrap cert `node-0.pem` in compose); set `TREMBITA_JOIN_SEEDS` to a live seed.
- Wait for `GET /ready` (HTTP 200) before traffic.

### Static clusters (`trembita-node`, e2e)

Reference layout (not elastic product apps): explicit `TREMBITA_NODE_ID`, static `TREMBITA_PEERS`, per-file PEM paths — see [certs.md](../certs.md#static-multi-node-bootstrap).

### Join troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Join succeeds but node stays in `learners`, not `voters` | Default dynamic join requests `learner`. Set joiner `TREMBITA_JOIN_ROLE=voter` **and** seed `TREMBITA_ALLOW_VOTER_JOIN=1`, or bootstrap voters with `TREMBITA_PEERS`. |
| No leader for minutes after boot | Often mTLS mismatch (wrong CA, SAN, or cert for `TREMBITA_NODE_ID`). Check `RUST_LOG=trembita::net=warn,trembita::raft=debug` for QUIC handshake and pre-vote reject lines, then verify [certs.md](../certs.md), `/ready`, and **UDP on `TREMBITA_LISTEN`** (often port 443) between peers. |
| `cluster rejected join: VoterJoinDisabled` | Seed has not enabled `TREMBITA_ALLOW_VOTER_JOIN=1`. |

## Operations (deep dives)

| Task | Guide |
|------|-------|
| Public HTTP/WebSocket ingress (LB, health checks) | [ingress-lb.md](ingress-lb.md) |
| Snapshot / restore `data_dir` | [backup-restore.md](backup-restore.md) |
| Rolling wire vs app semver upgrades | [rolling-upgrade.md](rolling-upgrade.md) |
| PKI generation and SAN naming | [certs.md](../certs.md) |
| `trembita-ops` CLI | [trembita-ops README](../../crates/trembita-tools/README.md) |

**Pre-upgrade:** export a backup before risky app semver bumps or catalog expansion.

## Multi-Raft

When write load exceeds a single Raft group:

- **Product apps (B-32):** [`TrembitaConfigure::with_coordination_raft_groups`](../../crates/trembita/src/configure.rs), optional `TREMBITA_RAFT_GROUPS` / `TREMBITA_RAFT_SHARD_COUNT`, runtime [`add_raft_groups`](../../crates/trembita/src/app/runtime.rs) — see [capabilities § Coordination scale](../scenarios/capabilities.md#coordination-scale-b-32).
- **Growth presets (B-37):** `TREMBITA_COORDINATION_PROFILE` or `.with_coordination_growth_preset` — see [getting-started § B-37](../getting-started.md#when-to-enable-coordination-growth-b-37).
- **Assembly / custom SM:** `.raft_groups(n)` on [`TrembitaClusterBuilder`](../../crates/trembita-assembly/src/builder/mod.rs) — [multi-raft.md](../decisions/multi-raft.md).
- **Job enqueue hotspot:** sharded queue via [`QueueOpts::sharded`](../../crates/trembita/src/queue_opts.rs) or `TREMBITA_JOB_QUEUE_SHARDS` (B-32).
- **Backup must include `group-meta.redb`** (Meta-Raft coordinator: saga journal, catalog).
- Rebalance and expansion are leader-driven; monitor `GET /introspect/raft-groups` on the ops HTTP bind (`TREMBITA_LISTEN` for product apps).

Start with **one group** until metrics or latency justify adding groups — premature sharding adds operational surface.

## Observability

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness (always 200 while process runs) |
| `GET /ready` | **LB pool readiness** — 200 iff `join_phase` is `pool_ready` and not draining (B-35) |
| `GET /metrics` | Prometheus (Raft, queue depth, saga counters) |
| `GET /dashboard` | HTML UI — cluster, actors, queues, workflows |
| `GET /introspect/queues` | Per-stream pending / leased depth |
| `GET /introspect/sagas` | Saga journal records (running / done / stuck) |
| `GET /introspect/product-scale` | Capability `resolved_scale`, queue shard mode, coordination Raft groups (B-33) |
| `GET /introspect/directory-r3` | Directory RYW retry config, merge lag epochs, cumulative `NoTarget` by group (B-36) |
| `GET /introspect/join-status` | Elastic join pipeline phase + catch-up / auto-host flags (B-35) |
| `GET /introspect/ops-summary` | **Ops cockpit** — join + product scale + directory R3 + coordination preset + queue depths (B-43) |

### Ops cockpit introspect (B-43)

One JSON snapshot for deploy checks, CI preflight ([B-54](../backlog.md)), and on-call triage — same fields as the focused routes below, without chaining four curls:

```bash
curl -sf "https://node1/introspect/ops-summary" | jq '{
  join: .join | {phase, log_caught_up, hosts_wired},
  capability_groups: .product_scale.capability_groups,
  coordination: .product_scale.coordination,
  coordination_profile: .coordination_profile,
  merge_lag_epochs: .directory_r3.merge_lag_epochs,
  queue_depths
}'
```

| Section | Source epic | Dedicated route |
|---------|-------------|-----------------|
| `join` | B-35 | `/introspect/join-status` |
| `product_scale` | B-33 / B-32 | `/introspect/product-scale` |
| `directory_r3` | B-36 | `/introspect/directory-r3` |
| `coordination_profile` | B-37 | preset name + auto-shard hints (resolved Raft/queue layout under `product_scale`) |
| `queue_depths` | queues | `/introspect/queues` (pending / leased / oldest age only; excludes dead-letter / redelivered) |

**Contract:** nested sections are byte-for-byte the same JSON as the dedicated routes above (regression: `b43_ops_summary_nested_routes_match_dedicated_introspect_endpoints`). **`coordination_profile`** is only populated when a B-37 preset was set at boot; otherwise `{}`. Route is **GET-only**.

Public type: [`OpsSummary`](../../crates/trembita/src/app/ops_summary.rs) · in-process: [`TrembitaApp::ops_summary`](../../crates/trembita/src/app/ops_summary.rs).

Regression index (13 tests, `b43_*`): [capabilities § B-43 automated regression](../scenarios/capabilities.md#automated-regression-b-43).

### Join readiness (B-35)

Use **`GET /ready`** for load balancer registration — not **`GET /health`**. A joining learner may return **503** with `"join_phase":"catching_up"` or `"awaiting_hosts"` for minutes while it replicates the log and the leader supervisor spawns local cap/worker hosts.

Example (pool-ready seed):

```bash
curl -sf "https://node1/ready" | jq '{join_phase, log_caught_up, hosts_wired}'
```

Stuck joiner (private network):

```bash
curl -sf "https://stuck-node/introspect/join-status" | jq .
```

| `join_phase` | Typical fix |
|--------------|-------------|
| `awaiting_membership` | Check seed `TREMBITA_ALLOW_JOIN`, join RPC / firewall UDP between peers |
| `catching_up` | Wait or inspect Raft lag on seed; verify QUIC path on `TREMBITA_LISTEN` |
| `awaiting_hosts` | Ensure manifest registers workers/caps; supervisor reconcile on leader |
| `pool_ready` | Safe to add to LB pool |

Regression: [cluster-elasticity § B-35 automated regression](../decisions/cluster-elasticity.md#automated-regression-b-35).

### R3 directory visibility (B-36)

Use during **scale events**, **multi-Raft group changes**, or when clients report intermittent **`NoTarget`** / 503 on actor routes.

```bash
curl -sf "https://node1/introspect/directory-r3" | jq '{
  directory_policy,
  merge_lag_epochs,
  directory_retry_boost_active,
  deliver_no_target_totals
}'
```

Prometheus on the same node: `trembita_directory_merge_lag_epochs`, `trembita_directory_deliver_no_target_total`.

| Signal | Interpretation |
|--------|----------------|
| `merge_lag_epochs` elevated briefly | Normal during rebalance; wait for anti-entropy |
| `directory_retry_boost_active: true` | Post-rebalance RYW boost window (stronger retries) |
| Steady climb in `deliver_no_target_totals` for one `group` | Missing registrations, wrong group name, or clients holding stale sticky sessions — reopen sessions or inspect directory on dashboard |
| `directory_policy: eventual` | Non-default; product caps normally ship `read_your_writes` |

Sticky app code: [`ActorSession::reopen_str`](../../crates/trembita-runtime/src/session.rs) after worker migration. ADR + test commands: [actor-routing § B-36](../decisions/actor-routing.md#automated-regression-b-36).

### Advanced product surface (B-41)

| Need | Check |
|------|--------|
| Durable cross-node actor mailbox | `TREMBITA_DATA_DIR` set + `with_durable_mailbox(true)` — verify `{data_dir}/mailbox-spool.redb` exists after boot; **not** required for capability-only apps |
| Custom leader reconcile | `on_leader` body runs only while Raft leader — use `gate.first_in_term()` for one-shot bootstrap |
| Schedules in Postgres | `schedule-postgres` feature + [`PgScheduleSource`](../../crates/trembita-schedule-postgres/src/lib.rs); poll interval via `SchedulePoll` — leader reconciles into job queue redb |

Docs: [capabilities § B-41](../scenarios/capabilities.md#product-surface-gaps-b-41) · [schedule-source § B-41](../decisions/schedule-source.md#postgres-adapter-b-41).

### Gateway session rotation (B-40)

Rolling **`TREMBITA_GATEWAY_SESSION_SECRET`** without kicking all users immediately:

1. Generate new secret (≥16 bytes); keep the old value handy.
2. On **every** gateway node, set **`TREMBITA_GATEWAY_SESSION_SECRET`** = new, **`TREMBITA_GATEWAY_SESSION_SECRET_PREVIOUS`** = old ([env.md](../env.md)).
3. Restart nodes (or rolling restart behind drain). Existing cookies signed with the old key keep working until TTL; new logins get cookies signed with the primary secret.
4. After max session TTL, remove **`TREMBITA_GATEWAY_SESSION_SECRET_PREVIOUS`** on all nodes and restart again.

Verify: login on one node, `GET /me` (or app session route) on another — [B-39 local cluster smoke](../../dev/local-3node/README.md) or [B-34 elastic tests](../scenarios/capabilities.md#elastic-join--lb-b-34). Regression: [gateway-cluster-auth § B-40](../decisions/gateway-cluster-auth.md#automated-regression-b-40).

**Cap-store sessions:** revoke via [`revoke_capstore_session`](../../crates/trembita/src/gateway/cluster_session.rs) on logout; registry must be visible on all nodes (shared store adapter).

### Local 3-node cluster (B-39)

**Not a production deploy path** — use on a developer machine to validate **shared `TREMBITA_GATEWAY_SESSION_SECRET`** and **`GET /ready`** behind nginx before copying secrets into **`deploy/.env`** for VPS.

```bash
./scripts/local-cluster.sh setup && ./scripts/local-cluster.sh up
./scripts/local-cluster.sh session-smoke
```

Mirror the same secret on every production node when using cluster session cookies ([gateway-cluster-auth § B-29](../decisions/gateway-cluster-auth.md)). Full local guide: [local-3node](../../dev/local-3node/README.md) · [capabilities § B-39](../scenarios/capabilities.md#local-3-node-cluster-b-39).

### Scale & scaffold DX (B-38)

Before changing capability **scale** or adding **queued** routes in a scaffolded app:

```bash
trembita doctor --explain-scale   # scale narrative + foot-guns; no layout failures on partial trees
trembita doctor                   # full lint — fix [error] lines (suggestions may follow `→`)
trembita doctor --preflight       # deploy/env + B-33 join/session checks
```

| Finding class | Typical B-38 suggestion |
|---------------|-------------------------|
| `.instances(1)` + default queue | Add `key =` on handler or drop `.instances(1)` for PerNode |
| Marker-only `.instances(1)` | Delete `.instances(1)` when you meant stateless scale |
| `Route::Session` without `.per_node()` | Point to realtime template wiring |
| `store_get` / `store_set` without `require_store` | Reference jobs **`task.rs`** pattern |

New services: `trembita new my-app --profile jobs|realtime|api` (alias for `--template`). Scenario + test names: [capabilities § B-38](../scenarios/capabilities.md#scale-scaffold-dx-b-38).

### Coordination growth preset (B-37)

When deploy uses **`TREMBITA_COORDINATION_PROFILE`** (or code calls `.with_coordination_growth_preset`), verify the running node picked up the bundled B-32 layout:

```bash
curl -sf "https://node1/introspect/product-scale" | jq '{
  coordination_raft_groups,
  coordination_shard_count,
  job_queues
}'
```

| Expected (preset) | `coordination_raft_groups` | Typical queue `mode` |
|-------------------|----------------------------|----------------------|
| `standard` | 1 | `standard` |
| `jobs_backlog` | 1 | `auto_shard` |
| `write_sharding` | 2 | `standard` |
| `full` | 2 | `auto_shard` |

If values disagree with the table, check whether explicit `TREMBITA_RAFT_*` / `TREMBITA_JOB_QUEUE_AUTO_SHARD` override the profile, or whether the manifest queue was already `.sharded(n)` (preset does not replace fixed shards). Preset map + tests: [capabilities § B-37](../scenarios/capabilities.md#coordination-growth-presets-b-37).

### Product scale introspection (B-33)

After the cluster is ready, the product emits a structured log line (`target: trembita::product_scale`) with the same fields as **`GET /introspect/product-scale`** on the unified ops listener (also nested under **`product_scale`** in **`GET /introspect/ops-summary`** — [§ B-43](#ops-cockpit-introspect-b-43)). Use it to confirm multi-node cap pools, sharded queues, and multi-Raft coordination match the manifest/env before sending traffic.

- **Boot log:** `RUST_LOG=trembita::product_scale=info` (or `info` globally).
- **HTTP:** scrape from a private network; response is JSON (capability groups, queue mode, `coordination_raft_groups`).
- **Preflight:** `trembita doctor --preflight` errors when `deploy/.env.example` lists `TREMBITA_JOIN_SEEDS` without a documented `TREMBITA_GATEWAY_SESSION_SECRET` — see [capability-dx § B-33](../decisions/capability-dx.md#group-scale-b-28).

Scrape `/metrics` from a private network; do not expose the ops HTTP listener on the public internet without TLS (`TREMBITA_HTTP_TLS_*`) and firewall rules.

## Post-deploy verification

- [ ] `/ready` returns 200 with `"join_phase":"pool_ready"` on every backend in the LB pool (B-30/B-35 — not only `/health`)
- [ ] **`GET /introspect/ops-summary`** on each node: `join.phase` is `pool_ready`, `product_scale` matches manifest, `directory_r3.merge_lag_epochs` near zero when stable (B-43)
- [ ] Cookie login works via LB VIP when using cluster sessions (B-29/B-40 — issuer/verifier ports)
- [ ] `trembita doctor --preflight` clean on release manifest (B-31 scale foot-guns; B-33 join seeds + session secret in `deploy/.env.example`)
- [ ] Sample `propose` / `query` or app-specific health check
- [ ] `./e2e/run.sh` or your integration smoke test
- [ ] Backup export tested to object storage (if DR required)

## Related

- [scenarios/README.md](../scenarios/README.md) — product patterns
- [getting-started.md](../getting-started.md) — TrembitaApp tutorial
- [multi-raft.md § Production reliability](../decisions/multi-raft.md#production-reliability)
