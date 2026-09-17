# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Shipped epic history: [archive/backlog-shipped.md](archive/backlog-shipped.md) (B-01…B-27), [archive/backlog-wave-b28-b32.md](archive/backlog-wave-b28-b32.md). Design rationale: [decisions/](decisions/).

When an item ships, remove its row here, update [status.md](status.md), and move planning text to `docs/archive/` if it is no longer needed in live docs.

---

## Open work

**Pre-1.0 policy:** breaking API/default changes are OK when they match product intent. Optional cap-store backends ship via `trembita` features — see [status.md](status.md).

**Priority hint:** P0 → P1 → P2 within this table (top first). Next epic id after B-41 ships: **B-42**. **1.0 stabilization** — out of active backlog; see [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md).

| Id | Pri | Item | Status | Notes |
|----|-----|------|--------|-------|
| B-33 | P0 | **B-28–B-32 regression hardening** | backlog | Fix red pre-push tests: [`gateway_cluster_session`](../crates/trembita/tests/gateway_cluster_session.rs) (`MissingHosts` / test gateway wiring), [`integration/cap_scale`](../crates/trembita/src/integration/cap_scale.rs) (directory pool / supervisor timing). **Boot scale report:** structured log (+ optional ops route) listing cap groups [`resolved_scale`](../crates/trembita/src/capability/group.rs), queue shard mode, `coordination_raft_groups` after ready. **`doctor --preflight`:** validate synthetic `deploy/.env.example` (secret, data_dir, join seeds). **ADR:** extend [capability-dx](decisions/capability-dx.md) or short ops ADR. |
| B-34 | P0 | **E2E elastic + LB proof** | backlog | New or extend `e2e/`: 4th joiner, HTTP LB round-robin, [`ClusterSessionSecret`](../crates/trembita/src/gateway/cluster_session.rs) cookie on node B after login on node A; cap **PerNode** smoke. Link from [ops/ingress-lb.md](ops/ingress-lb.md). Heavy lane / MR label `run-heavy` if slow. |
| B-35 | P1 | **Join readiness pipeline** | backlog | Product-visible lifecycle: joined → learner caught up → supervisor auto hosts → gateway **`GET /ready`** true for pool membership. Document state machine in [cluster-elasticity](decisions/cluster-elasticity.md); optional `/cluster/join-status` or introspect field. |
| B-36 | P1 | **Rebalance visibility & sticky recovery (R3)** | backlog | Metrics/events: directory merge lag, cap deliver `NoTarget` (counter). Tune/document default [`directory_retry`](../crates/trembita/src/app/builder.rs). **Session/cap sticky:** ADR + helper for re-open [`ActorSession`](../crates/trembita-runtime/src/session.rs) after rebalance ([structural-limits](scenarios/structural-limits.md)). |
| B-37 | P1 | **Coordination growth presets** | backlog | **`TrembitaConfigure`** / env profile wiring [B-32](archive/backlog-wave-b28-b32.md): [`auto_shard`](../crates/trembita/src/queue_opts.rs), [`with_coordination_raft_groups`](../crates/trembita/src/configure.rs) + leader policy (depth / ceiling). getting-started «when to enable». |
| B-38 | P1 | **Founder DX v2 (doctor + scaffold)** | backlog | **`trembita doctor --explain-scale`**, lint v2 (suggestions), **`trembita new --profile jobs\|realtime\|api`**, queued cap idempotency template ([R4](scenarios/structural-limits.md)). **ADR:** [capability-dx](decisions/capability-dx.md), [framework-conventions](decisions/framework-conventions.md). |
| B-39 | P1 | **Local 3-node cluster (founder)** | backlog | **Phase 1:** script + doc in `dev/` or `examples/` (shared secret, LB curl). **Phase 2:** debug CLI `trembita dev cluster-up`. |
| B-40 | P2 | **Gateway auth: logic / storage split + OAuth optional** | backlog | **ADR** (extend [gateway-cluster-auth](decisions/gateway-cluster-auth.md)): separate **session logic** from **session storage** so adapters swap without touching HTTP routes. **Ports:** `SessionIssuer` + `SessionVerifier` (+ optional `revoke`); **`GatewaySessionStore`** (or reuse [`CapStateStore`](../crates/trembita-capstore/src/store.rs) behind a thin adapter). **Adapters:** signed cookie ([`ClusterSessionSecret`](../crates/trembita/src/gateway/cluster_session.rs) — stateless); cap-store registry (refactor today’s [`register_capstore_session`](../crates/trembita/src/gateway/cluster_session.rs)); optional Postgres/app store later. **HTTP unchanged:** [`SessionGate`](../crates/trembita-http/src/routing/auth.rs) calls `SessionVerifier` only; [`GatewayIdentity`](../crates/trembita/src/gateway/identity.rs) stays stateless edge. **OAuth:** optional feature / **`trembita-gateway-auth`** crate (OIDC flow only) → on success calls **`SessionIssuer`** — no storage inside OIDC module. **Also:** secret rotation (dual-verify), `examples/oauth-gateway`. **Not:** monolithic `trembita-auth` or mandatory IdP in core. |
| B-41 | P2 | **Product surface gaps (assembly → app)** | backlog | **Durable mailbox** on [`TrembitaApp`](../crates/trembita/src/app/mod.rs). **Leader hook** (product `LeaderTask` or doc-only). **`ScheduleSource` Postgres** optional feature/crate ([external-backlog](decisions/external-backlog.md) pattern). |

### Deferred decisions (ADR when epic starts)

| Topic | Options | Current lean |
|-------|---------|--------------|
| Session store port | Dedicated `GatewaySessionStore` vs `CapStateStore` adapter only | Thin **`GatewaySessionStore`** trait; default impl delegates to cap store keys (migrate B-29 helpers) |
| OAuth packaging | Feature on `trembita` vs `trembita-gateway-auth` crate | **Crate** for OIDC deps; facade feature re-exports; issuer/verifier traits live in `trembita` / `trembita-http` |
| Coordination profile | Env-only vs `TrembitaConfigure` | Both |

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md) · [structural-limits](scenarios/structural-limits.md)
