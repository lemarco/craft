# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Design rationale: [decisions/](decisions/). Product vision: [decisions/product-scenarios.md](decisions/product-scenarios.md). Scenario guides: [scenarios/](scenarios/README.md).

When an item ships, remove its row here and update [status.md](status.md) (and the relevant ADR if needed).

---

## Open work

Product scale-out and founder UX — close the gap between «add VPS + same binary» and **automatic** HTTP + capability compute scale without manifest foot-guns.

**Pre-1.0 policy:** no live crates.io adopters to preserve — **breaking API/default changes are OK** when they match product intent (B-28 default scale, B-29 gateway auth shape, etc.).

Optional cap-store backends (Redis, Postgres) ship via `trembita` features — see [status.md](status.md).

| Id | Item | Status | Notes |
|----|------|--------|-------|
| B-28 | **Cap scale sensible defaults** | backlog | **Change runtime default:** [`CapGroupScale::PerNode`](../crates/trembita/src/capability/group.rs) for stateless groups (heuristic: no op with [`Route::Session`](../crates/trembita/src/capability/route.rs); optional: default `State` only). [`Fixed(1)`](../crates/trembita/src/capability/group.rs) via `.instances(1)` only. Auto-heuristic for manifest-only queued groups if needed. Update CLI, doctor, showcases (drop hard-coded `.instances(1)` where stateless). **Breaking OK.** **ADR:** extend [capability-dx](decisions/capability-dx.md). |
| B-29 | **Cluster-aware gateway session / auth** | backlog | **Composable configs, not one OAuth stack:** (1) **Cluster login cookie / bearer** — verify on any node (shared secret or cap-store session table); (2) **[`SessionGate`](../crates/trembita-http/src/lib.rs) + sticky cap [`Route::Session`](../crates/trembita/src/capability/route.rs) + WS** — cluster-valid session id, not in-memory `HashSet` per process; (3) **Optional OAuth/OIDC** — feature or app hook, off by default. Apps pick [`GatewayOpts`](../crates/trembita/src/gateway/mod.rs) / env profile (`cookie-only` vs `external-idp`). Migrate [realtime showcase](../examples/realtime/src/gateway_session.rs) to (1)+(2). **ADR:** new `gateway-cluster-auth.md` + [gateway-identity](decisions/gateway-identity.md). **Scenario:** [realtime-sessions](scenarios/realtime-sessions.md). |
| B-30 | **Happy-path load balancing (product recipe)** | backlog | One maintained **production ingress** guide: external LB / DNS / anycast / floating IP in front of homogeneous nodes on `TREMBITA_LISTEN`, health checks, optional QUIC vs HTTP split, mTLS client paths — not K8s-as-product ([deployment-model](decisions/deployment-model.md)). Link from [getting-started](getting-started.md) and [env](env.md). **Ops doc:** new or extend `docs/ops/` (e.g. `ingress-lb.md`). |
| B-31 | **Founder scale model (transparent split)** | backlog | Product-facing map: **stateless op → scales with nodes**; **keyed op → shard / single owner**; **session op → sticky**; **queued → queue consumers scale, handler placement follows cap scale**. Tooling: `trembita doctor` **errors** on foot-guns (e.g. `Queued` + explicit `.instances(1)` without keyed routing) — strict OK pre-1.0. **ADR:** [capability-dx](decisions/capability-dx.md), [product-terminology](decisions/product-terminology.md). |
| B-32 | **Product-level coordination scale (multi-Raft / queue shard)** | backlog | **Wave 1 ships both:** (1) sharded job queue — [`job_queue_sharded`](../crates/trembita-assembly/src/builder/cluster/products.rs) + optional leader [`job_queue_auto_shard`](../crates/trembita-assembly/src/builder/cluster/products.rs); (2) product **multi-Raft** — [`add_raft_groups`](../crates/trembita/src/app/runtime.rs) / catalog on **`EmptyStateMachine`** path (queue/topic/store coordination, not custom app SM). Wire through **`TrembitaApp` / `TrembitaConfigure` / `TREMBITA_*` env** — not assembly-only builder. **ADR:** [multi-raft](decisions/multi-raft.md), [job-queue](decisions/job-queue.md), [public-api-1.0](decisions/public-api-1.0.md). |

New feature epics after B-32: next **B-NN**; add a row with scenario + ADR links.

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md)
