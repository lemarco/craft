# Shipped backlog wave B-33–B-41 (archive)

Product scale follow-on epics closed **2026-09-17**. **Current index:** [status § Product scale wave](../status.md#product-scale-wave-b-28b32). **Regression commands:** [testing-coverage § B-33–B-41](../testing-coverage.md#shipped-backlog-b-33b41).

**Context:** hardening and operator UX after the [B-28–B-32 wave](backlog-wave-b28-b32.md) — introspection, elastic proof, join readiness, R3 visibility, coordination presets, scaffold DX, local cluster, gateway auth ports, and advanced product surface (mailbox, leader hook, Postgres schedules). Later epics **B-42+** remain on [status](../status.md#product-scale-wave-b-28b32) and [testing-coverage § B-42+](../testing-coverage.md#shipped-backlog-b-42).

---

## Epic map (as shipped)

| Id | What shipped | Read first | Automated regression |
|----|--------------|------------|----------------------|
| **B-33** | **Regression hardening** — multi-node **CapHost** remote spawn, boot **`product_scale`** log + **`GET /introspect/product-scale`**, **`doctor --preflight`** join+session secret | [capability-dx § B-33](../decisions/capability-dx.md#group-scale-b-28) · [production-runbook § Product scale introspection](../ops/production-runbook.md#product-scale-introspection-b-33) | [`product_scale_report.rs`](../../crates/trembita/tests/product_scale_report.rs), [`gateway_cluster_session.rs`](../../crates/trembita/tests/gateway_cluster_session.rs), [`cap_scale.rs`](../../crates/trembita/src/integration/cap_scale.rs) |
| **B-34** | **E2E elastic + LB proof** — 4th joiner, nginx round-robin, cluster session cookie across nodes, **PerNode** cap | [ingress-lb § B-34](../ops/ingress-lb.md#elastic-join--http-lb-proof-b-34) · [capabilities § B-34](../scenarios/capabilities.md#elastic-join--lb-b-34) · [e2e § B-34](../../e2e/README.md#elastic-lb-b-34) | [`elastic_lb_product.rs`](../../crates/trembita/tests/elastic_lb_product.rs) (`b34_*`), [`e2e_elastic/cap.rs`](../../crates/trembita-tools/src/e2e_elastic/cap.rs), [`elastic_lb.sh`](../../e2e/elastic_lb.sh) |
| **B-35** | **Join readiness pipeline** — `join_phase` on **`GET /ready`**, **`GET /introspect/join-status`**, `ReadyOpts::pool_membership` for joiners | [cluster-elasticity § B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35) · [ingress-lb § B-35](../ops/ingress-lb.md#join-readiness-pipeline-b-35) | [`join_pipeline.rs`](../../crates/trembita-assembly/src/join_pipeline.rs) (`b35_*`), [`ingress_lb_ops.rs`](../../crates/trembita/tests/ingress_lb_ops.rs) (`b35_*`) |
| **B-36** | **R3 rebalance visibility & sticky recovery** — merge lag + `NoTarget` metrics/events, **`GET /introspect/directory-r3`**, **`ActorSession::reopen_*`** | [actor-routing § B-36](../decisions/actor-routing.md#r3-visibility--sticky-recovery-b-36) · [capabilities § B-36](../scenarios/capabilities.md#r3-directory-visibility-b-36) | [`directory_delivery.rs`](../../crates/trembita-runtime/src/directory_delivery.rs) (`b36_*`), [`directory_r3_report.rs`](../../crates/trembita/tests/directory_r3_report.rs) (`b36_*`) |
| **B-37** | **Coordination growth presets** — [`CoordinationGrowthPreset`](../../crates/trembita/src/configure.rs), **`TREMBITA_COORDINATION_PROFILE`**, manifest queue auto-shard wiring | [getting-started § B-37](../getting-started.md#when-to-enable-coordination-growth-b-37) · [capabilities § B-37](../scenarios/capabilities.md#coordination-growth-presets-b-37) | [`coordination_profile.rs`](../../crates/trembita-assembly/src/coordination_profile.rs) (`b37_*`), [`coordination_growth_preset.rs`](../../crates/trembita/tests/coordination_growth_preset.rs) (`b37_*`) |
| **B-38** | **Scale & scaffold DX** — `doctor --explain-scale`, lint v2 `suggestion`, `new --profile`, jobs **`task.rs`** R4 scaffold | [capability-dx § B-38](../decisions/capability-dx.md#scale-scaffold-dx-b-38) · [capabilities § B-38](../scenarios/capabilities.md#scale-scaffold-dx-b-38) | [`doctor.rs`](../../crates/trembita-cli/src/scaffold/doctor.rs) (`b38_*`), [`scaffold.rs`](../../crates/trembita-cli/tests/scaffold.rs) (`b38_*`) |
| **B-39** | **Local 3-node cluster** — `local-cluster.sh`, `dev cluster-up`, shared session secret, optional nginx **:18290** | [local-3node](../../dev/local-3node/README.md) · [capabilities § B-39](../scenarios/capabilities.md#local-3-node-cluster-b-39) | [`local_cluster.rs`](../../crates/trembita-cli/src/dev/local_cluster.rs) (`b39_*`), [`dev.rs`](../../crates/trembita-cli/tests/dev.rs), [`local-cluster.sh`](../../scripts/local-cluster.sh) |
| **B-40** | **Gateway auth split** — `SessionIssuer` / `SessionVerifier`, `SessionGate::from_verifier`, cap-store registry, secret rotation, `trembita-gateway-auth` | [gateway-cluster-auth § B-40](../decisions/gateway-cluster-auth.md#b-40--logic--storage-split) · [capabilities § B-40](../scenarios/capabilities.md#gateway-auth-split-b-40) | [`session_ports.rs`](../../crates/trembita-http/src/routing/session_ports.rs) (`b40_*`), [`gateway_cluster_session.rs`](../../crates/trembita/tests/gateway_cluster_session.rs) (`b40_*`) |
| **B-41** | **Product surface gaps** — durable mailbox spool, [`on_leader`](../../crates/trembita/src/app/builder.rs), `schedule-postgres` / [`PgScheduleSource`](../../crates/trembita-schedule-postgres/) | [capabilities § B-41](../scenarios/capabilities.md#product-surface-gaps-b-41) · [schedule-source § B-41](../decisions/schedule-source.md#postgres-adapter-b-41) | [`configure.rs`](../../crates/trembita/src/configure.rs) (`b41_*`), [`trembita-schedule-postgres`](../../crates/trembita-schedule-postgres/src/lib.rs) (`b41_*`) |

## Product outcomes (one line each)

| Id | Outcome |
|----|---------|
| B-33 | Boot and doctor expose **product scale** before traffic hits the cluster |
| B-34 | **Elastic join + HTTP LB** proven end-to-end with cluster sessions |
| B-35 | Load balancers see **join phase** on `/ready` and introspect |
| B-36 | Operators debug **R3 rebalance** and sticky session recovery |
| B-37 | **Coordination growth** is one preset instead of scattered env knobs |
| B-38 | **`trembita new` / doctor** guide scale-safe app layouts |
| B-39 | **Three nodes on a laptop** match production join/session patterns |
| B-40 | Gateway **session issue vs verify** is a clean port split |
| B-41 | **Mailbox, leader tasks, Postgres cron** close common product gaps |

**Prior wave:** [backlog-wave-b28-b32.md](backlog-wave-b28-b32.md) (B-28 … B-32). **Earlier epics:** [backlog-shipped.md](backlog-shipped.md) (B-01 … B-27). **Index hygiene (B-53):** this file + [testing-coverage § B-33–B-41](../testing-coverage.md#shipped-backlog-b-33b41) — [`deploy_pack.rs`](../../crates/trembita/tests/deploy_pack.rs) (`b53_*`).
