# Shipped backlog wave B-28–B-32 (archive)

Product scale epics closed **2026-09-17**. **Current index:** [status § Product scale wave](../status.md#product-scale-wave-b-28b32). **Regression commands:** [testing-coverage § B-28–B-32](../testing-coverage.md#shipped-backlog-b-28b32).

**Context:** homogeneous cluster growth — add VPS, same binary, `TREMBITA_JOIN_SEEDS` — without orchestrator-specific packaging. Follow-up **B-33** (regression hardening + boot scale report): [status § B-33](../status.md#product-scale-wave-b-28b32). Next open epic: **B-34** ([backlog](../backlog.md#open-work)).

---

## Epic map (as shipped)

| Id | What shipped | Read first | Automated regression |
|----|--------------|------------|----------------------|
| **B-28** | Auto **`resolved_scale`** on cap groups (marker → **PerNode**, shared RAM / session → **Fixed**) | [capabilities § Group scale](../scenarios/capabilities.md#group-scale-b-28) · [capability-dx § B-28](../decisions/capability-dx.md#group-scale-b-28) | [`cap_scale.rs`](../../crates/trembita/src/integration/cap_scale.rs), [`group.rs`](../../crates/trembita/src/capability/group.rs) |
| **B-29** | **Cluster session cookies** — shared secret on all gateway nodes | [gateway-cluster-auth](../decisions/gateway-cluster-auth.md) · [realtime § B-29](../scenarios/realtime-sessions.md#cluster-session-cookies-b-29) | [`gateway_cluster_session.rs`](../../crates/trembita/tests/gateway_cluster_session.rs) |
| **B-30** | **Ingress / LB** — liveness **`GET /health`** vs pool **`GET /ready`** on `TREMBITA_LISTEN` | [ops/ingress-lb.md](../ops/ingress-lb.md) | [`ingress_lb_ops.rs`](../../crates/trembita/tests/ingress_lb_ops.rs) |
| **B-31** | **Product scale** + **`trembita doctor`** foot-guns | [capability-dx § Product scale](../decisions/capability-dx.md#product-scale-model-b-31) | [`cap_scale_doctor.rs`](../../crates/trembita-cli/tests/cap_scale_doctor.rs) |
| **B-32** | **Coordination scale** — sharded / auto-shard job queue + multi-Raft on product API (`QueueOpts`, `TrembitaConfigure`, `TREMBITA_*`) | [capabilities § Coordination scale](../scenarios/capabilities.md#coordination-scale-b-32) · [env.md](../env.md) | [`product_coordination_scale.rs`](../../crates/trembita/tests/product_coordination_scale.rs) |

## Product outcomes (one line each)

| Id | Outcome |
|----|---------|
| B-28 | Stateless capability groups scale hosts **per node** by default |
| B-29 | Browser sessions verify on **any** gateway behind LB |
| B-30 | Operators health-check **`GET /ready`** for pool membership |
| B-31 | **`trembita doctor`** catches manifest scale foot-guns |
| B-32 | Sharded queues + product **multi-Raft** via manifest / env |

**Earlier shipped epics:** [backlog-shipped.md](backlog-shipped.md) (B-01 … B-27).
