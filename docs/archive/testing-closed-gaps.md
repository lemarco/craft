# Testing closed gaps (archive)

Historical log moved from [testing-coverage.md](../testing-coverage.md) on 2026-09-17. **Open gaps:** [Known gaps](../testing-coverage.md#known-gaps-prioritized).

| Closed | What | Where |
|--------|------|-------|
| 2026-09-17 | R1 client + leader wire `CommandTooLarge`; OpCtx consensus; R3 cap inline during `add_raft_groups` | `trembita-client/tests/command_size.rs`, `trembita-test-runtime/tests/runtime.rs` (`leader_rejects_oversized_propose_on_wire`), `trembita/src/integration/cap_rebalance.rs`, `capability/consensus.rs`, `trembita-assembly/.../assemble.rs` |
| 2026-09-15 | CLI **`new`** + read-only **`doctor`**; removed **`trembita add`**, **`doctor --fix`**, release **`dev`** | `trembita-cli`, `docs/{getting-started,decisions/framework-conventions}.md` |
| 2026-09 | Product **`AppManifest`** + scaffold **`manifest.rs`** (layout; manual edits in marker regions) | `trembita/src/app/manifest.rs`, `trembita-cli/src/scaffold/render.rs`, `tests/{scaffold,add_doctor}.rs` |
| 2026-09 | Doctor duplicate manifest ids + workflow wiring checks | `trembita-cli/src/scaffold/doctor.rs`, `tests/add_doctor.rs` |
| 2026-09 | Topic replicate auth rejects non-leader caller | `trembita/tests/topic.rs` |
| 2026-09 | Product gateway symmetry (topics/workflows HTTP + identity) | `trembita/tests/gateway_product_http.rs`, `gateway_product_symmetry.rs` |
| 2026-09 | B-31 founder scale — doctor table + integration + runtime defaults | `trembita-cli/src/scaffold/doctor.rs` (`founder_scale_b31_*`), `trembita-cli/tests/cap_scale_doctor.rs`, `trembita/src/capability/group.rs`, [capabilities § B-31 regression](../scenarios/capabilities.md#automated-regression-b-31) |
| 2026-09 | B-32 coordination scale — unit tables, env parse, product boot | `trembita/src/{configure,queue_opts,job_opts}.rs`, `trembita-assembly/src/env_config.rs` (`b32_*`), `trembita/tests/product_coordination_scale.rs`, [capabilities § B-32](../scenarios/capabilities.md#automated-regression-b-32) |
| 2026-09 | B-30 ingress/LB — `/health` liveness vs `/ready` pool contract | `trembita-http/src/ops_routes.rs`, `trembita-dashboard/src/views.rs`, `trembita/src/integration/ingress_lb.rs`, `trembita/tests/ingress_lb_ops.rs`, [ingress-lb](../ops/ingress-lb.md) |
| 2026-09 | Introspect `GET /introspect/topics` | `trembita-dashboard/tests/admin.rs`, `trembita/tests/gateway_product_http.rs` |
| 2026-09 | Gateway rate limit HTTP integration (`429`) | `trembita/tests/gateway_jobs_http.rs` |
| 2026-09 | Typed product wire errors (`ProductWireError` on queue/topic/store replies) | `trembita-proto/src/product.rs`, service handlers |
| 2026-09 | Gateway bearer identity header-only user + no query token | `trembita/src/gateway/identity.rs` (unit) |
| 2026-09 | Actor store in-memory contract tests | `trembita-capstore/tests/memory_contract.rs` |
| 2026-08 | Scenario soak per product path (actor store, saga resume, session restart) | `benchmarks/soak_{actor_store,saga,session}.rs`, `.gitlab-ci.yml` `bench` |
| 2026-08 | Gateway identity, `SessionHandle`, WS + HTTP E2E, gateway drain | `trembita/src/gateway/`, `trembita/tests/{gateway_identity,gateway_ws,gateway_http}.rs`, `examples/{realtime,stateful-workers}/`, `../decisions/gateway-identity.md` |
| 2026-09 | Introspect API on product gateway (`IntrospectApi`, default gateway surfaces, `AuthFn`) | `trembita-http/src/introspect_routes.rs`, `trembita/tests/gateway_introspect_http.rs`, `../decisions/introspect-api.md` |
| 2026-09 | Gateway virtual-host dispatch (`Gateway`/`Surface`, strict default, loopback dev fallback) | `trembita-http/src/gateway/`, `trembita-http/README.md`, `trembita/src/gateway/mod.rs` |
| 2026-09 | Durable event topics (`EventTopic`, min-cursor compaction, retention discard, voter replication) | `trembita-events/src/{topic,redb_topic,topic_service}`, `trembita-events/tests/topic_failover.rs`, `trembita/src/topic_opts.rs`, `../decisions/event-topics.md` |
| 2026-09 | Dynamic schedule source (`ScheduleSource`, diff reconcile, leader replication) | `trembita-jobs/src/schedule_source.rs`, `trembita-jobs/tests/schedule_source.rs`, `trembita/tests/schedule_source.rs`, `../decisions/schedule-source.md` |
| 2026-09 | Capability DX wave 1 (B-21) — inline ask, queued deliver, `CapManifest` | `trembita/src/capability/`, `trembita/tests/capability.rs`, `examples/stateful-workers/src/capabilities/`, `../decisions/capability-dx.md` |
| 2026-09 | Gateway capability adapters (`cap_fire`, `cap_invoke`) | `trembita/src/gateway/cap_handlers.rs`, `trembita/tests/gateway_cap_http.rs` |
| 2026-09 | B-21 complete — QueuedWait/Scheduled/Session, scaffold `capabilities/` | `trembita/src/capability/`, `trembita/tests/capability.rs`, `trembita-cli/templates/…/capabilities/` |
| 2026-09 | B-22 greenfield + `Route::Event` topic ingress / `publish_event` | `trembita/src/capability/event.rs`, `trembita/src/capability/call.rs`, `trembita/tests/capability.rs` (`capability_event_subscription_runs_inline`), `examples/stateful-workers/` |
| 2026-09 | B-25 — `cap_handler`, `{handler}_register`, `cap_register_chain!` | `trembita-macros/src/cap_handler.rs`, `trembita/tests/cap_request_macro.rs`, `examples/*/capabilities/` |
| 2026-09 | B-26a — `#[cap_handler]` on `async fn` | `trembita-macros/src/cap_handler.rs`, `examples/stateful-workers/src/capabilities/orders.rs` |
| 2026-09 | B-26c–d — typed WS mount, `{group}.{op}` on `CapRequest` | `gateway/ws/mod.rs`, `capability/call.rs`, showcases |
| 2026-09 | Async-only `#[cap_handler]`; jobs showcase → cap queue | `trembita-macros`, `examples/background-jobs/`, `capability_wiring.rs` |
| 2026-09 | Product gateway opt-out sugar — [`TrembitaAppBuilder::without_*`](../../crates/trembita/src/app/builder.rs), [`with_actors_api`](../../crates/trembita/src/app/builder.rs); `from_config` + `TREMBITA_LISTEN` enables ops/registration HTTP (not `/actors/*`) | `trembita/src/app/builder.rs` (`env_listen_gateway_tests`), `../decisions/product-terminology.md` |
| 2026-09 | B-22/B-23 scaffold greenfield — `.without_actors_api()`, `cap_invoke` `/ping`, infallible `from_config`, compile-ready `cargo check` | `trembita-cli/src/scaffold/{render.rs,main.rs}`, `trembita-cli/tests/scaffold.rs`, `trembita-cli/templates/trembita-app/` |
| 2026-09 | B-24 parity matrix + realtime cap session | `docs/scenarios/capability-parity.md`, `examples/realtime/src/capabilities/` |
| 2026-09 | B-24d realtime scaffold (`chat` cap + sticky WS) | `crates/trembita-cli/src/scaffold/render.rs`, `trembita-cli/tests/scaffold.rs` |
| 2026-09 | B-24c workflows saga → onboarding cap ops | `examples/workflows/src/capabilities/onboarding.rs`, `examples/workflows/src/onboarding.rs` |
| 2026-09 | CapGroup `per_node` + jobs ledger cap + workflows scaffold | `crates/trembita/src/capability/group.rs`, `examples/background-jobs/`, `trembita-cli/templates/…/onboarding.rs.tpl` |
| 2026-09 | B-28 auto `resolved_scale` (marker → PerNode, RAM/session → Fixed) | `crates/trembita/src/capability/group.rs` (`#[cfg(test)]`) |
| 2026-09 | B-28 3-node directory pool + shared RAM single host | `crates/trembita/src/integration/cap_scale.rs` |
| 2026-09 | B-29 cluster-signed `SessionGate` + cap-store registry | `crates/trembita/src/gateway/cluster_session.rs` (unit), `crates/trembita/tests/gateway_cluster_session.rs` (login cookie, dual gateway, wrong secret, cap-store HTTP) |
| 2026-09 | B-27i cap queued dedup + bridge idempotency | `trembita/src/capability/{call,queue}.rs`, `trembita/src/consumer.rs`, `trembita/tests/capability.rs` (`capability_enqueue_dedup_key_collapses`), `trembita/tests/gateway_cap_http.rs` (`gateway_cap_enqueue_honors_dedup_query`) |
| 2026-09 | Schedule admin HTTP + facade (B-20) | `trembita-http/src/schedule_routes.rs`, `trembita/tests/http_schedules.rs`, `docs/scenarios/triggers-and-pipelines.md` |
| 2026-09 | WorkTrigger + cron→workflow sugar | `trembita-jobs/src/work_trigger.rs`, `trembita/tests/work_trigger_dispatch.rs`, `docs/scenarios/cookbook-async-work.md` |
| 2026-09 | `.scheduled_workflows()` product preset | `trembita/src/scheduled_workflow_opts.rs`, `trembita/tests/scheduled_workflows_builder.rs` |
| 2026-09 | One-shot enqueue at T (`run_at_ms` / `delay_ms`, `enqueue_at`) | `trembita-jobs/src/queue/types.rs`, `trembita-http/src/routes.rs`, `trembita/tests/http_delayed_enqueue.rs` |
| 2026-08 | Transport + facade gaps: QUIC backoff, DNS discovery, queue compaction, auto-spawn sim, admin/leave E2E | `trembita-net/tests/quic.rs`, `trembita/tests/{discovery,queue}.rs`, `trembita-sim/tests/{auto_spawn,actor_scenarios}.rs`, `e2e/{run,leave}.sh` |
| 2026-08 | Runtime fatal-error observable path (`status()` → `None`, `Stopped`) | `trembita-runtime/tests/runtime.rs` |
| 2026-08 | Multi-Raft sim: shard routing + independent group safety | `trembita-sim/tests/multi_raft.rs` |
| 2026-08 | Group rebalance planner + sharded adopt/retire runtime | `trembita-runtime/tests/group_rebalance.rs`, `sharded.rs` |
| 2026-08 | Malformed persistence + backend error injection at driver | `trembita-runtime/tests/driver.rs` |
| 2026-08 | Cluster leave RPC + `TrembitaCluster::leave()` facade | `trembita-runtime/tests/runtime.rs`, `trembita/tests/multi_raft.rs`, `../decisions/cluster-membership.md#leave-rpc` |
| 2026-08 | Injectable Tokio clock in integration tests (`trembita-test-support::clock`, `start_paused`) | `trembita-test-support`, `trembita/tests/*`, `trembita-runtime/tests/*`, `trembita-client/tests/cluster.rs` |
| 2026-08 | Snapshot survives facade restart (`compact` + `data_dir`) | `trembita/tests/persistence.rs` |
| 2026-08 | 3-node majority survives one member restart | `trembita/tests/persistence.rs` |
| 2026-08 | Shared KV fixtures + harness helpers (dedupe ~8 copies) | `trembita-test-support` (`Kv`, `TrackedKv`, `find_keys_for_two_groups`, cluster polling) |
| 2026-08 | Stable shard router runtime (`StableShardRouter`, `activate_shards`, builder default) | `trembita-runtime/sharded`, `trembita/tests/multi_raft.rs`, `trembita-runtime/tests/sharded.rs` |
| 2026-08 | Linearizability E2E phase 2 (QUIC `trembita-e2e-client` + external checker) | `crates/trembita-e2e-client`, `e2e/linearizability.sh`, `e2e/docker-compose.yml` |
| 2026-08 | Hardening: graceful leave integration, ops HTTP TLS E2E | `trembita/tests/graceful_leave.rs`, `trembita/tests/facade.rs`, `trembita-dashboard/tests/admin.rs` |
| 2026-08 | Wire decode fuzz (`cargo-fuzz` wire_decode, scheduled CI) | `crates/trembita-fuzz/`, `.gitlab-ci.yml` `fuzz` job |
| 2026-08 | Multi-Raft modulus routing: learners planner, shard expansion, keyed batch, `/introspect/raft-groups` | `trembita-core`, `trembita-client`, `trembita-dashboard`, `trembita/tests/multi_raft.rs`, `../decisions/multi-raft.md#modulus-routing--keyed-batch` |
| 2026-08 | Client retry edge cases (`NoTargets`, timeout, `NotLeader`, unreachable) | `trembita-client/tests/retry.rs` |
| 2026-08 | Keyed client routing (multi-Raft propose/query) | `trembita/tests/client_keyed.rs` |
| 2026-08 | Stable shards & catalog: `catalog_version`, `switch_to_stable_shards`, saga hardening | `trembita-core/shard.rs`, `trembita/src/cluster.rs`, `trembita-client/src/saga.rs`, `trembita/tests/{multi_raft,saga}.rs` |
| 2026-08 | Cross-shard saga coordinator (`run_saga`, `StoreSagaJournal`) | `trembita-client/src/saga.rs`, `trembita/src/saga.rs`, `trembita/tests/saga.rs`, `../decisions/multi-raft.md#cross-shard-transactions` |
| 2026-08 | Saga hardening v2: group 0 journal fallback + coordinator restart resume | `trembita-proto/saga_journal`, `trembita-core/node`, `trembita-runtime`, `trembita/src/saga.rs`, `trembita/tests/saga.rs` |
| 2026-08 | Saga journal layered tests (proto/core/driver/runtime/facade) | `trembita-proto`, `trembita-core/tests/saga_journal.rs`, `trembita-runtime/tests/{driver,runtime}.rs`, `trembita/tests/saga.rs` |
| 2026-08 | Durable 2PC prepare timeout GC + `resume_cross_shard_2pc` | `trembita-runtime`, `trembita-client/two_phase`, `trembita/tests/two_phase.rs` |
| 2026-08 | 2PC facade + client journal + metrics + sim partition test | `trembita/src/two_phase.rs`, `trembita-proto/two_phase_journal`, `trembita-sim/tests/two_phase.rs`, `trembita/tests/two_phase.rs` |
| 2026-08 | Product showcases (4 standalone examples + cluster scripts) | `examples/{background-jobs,stateful-workers,realtime,workflows}/`, `dev/cluster-common.sh`, `./scripts/check-examples.sh` |
| 2026-08 | Reference KV in `trembita_core::kv` (single source, re-exported by facade) | `trembita-core/src/kv.rs`, `trembita-test-support` `TrackedKv` only |
| 2026-08 | Durable cross-shard 2PC (`durable_cross_shard_2pc`, `EntryPayload::TwoPhasePrepare/Abort`) | `trembita-proto/src/two_phase.rs`, `trembita-core/tests/two_phase_journal.rs`, `trembita/tests/two_phase.rs` |
| 2026-08 | Dynamic catalog runtime (`add_raft_groups`, `/cluster/catalog/add`) | `trembita-proto/catalog`, `trembita-runtime`, `trembita/tests/multi_raft.rs`, `../decisions/multi-raft.md` |
| 2026-08 | Multi-Raft architecture ADR + pure planners | `trembita-core/src/shard.rs`, `../decisions/multi-raft.md` |
| 2026-08 | Actor routing: ring, session, drain override, `ask_linearizable`, directory RYW | `trembita-runtime` (`ring`, `session`, `directory_policy`), `trembita-runtime/tests/{messaging,migration}.rs`, `../decisions/actor-routing.md` |
| 2026-08 | `trembita-node` env parsing unit tests | `trembita-tools/src/node/config.rs` (`#[cfg(test)]`) |
| 2026-08 | Actor store resume + idempotency after unreachable node | `trembita/tests/actor_store_resume.rs` |
| 2026-08 | Redis dual connection, idempotent worker, reconnect | `trembita-store-redis/tests/redis.rs` |
| 2026-08 | Redis TLS (`rediss://`) with private CA | `trembita-store-redis/tests/tls.rs` |
| 2026-08 | Facade `data_dir` stop → restart → state | `trembita/tests/persistence.rs` |
| 2026-07 | Pure Raft FSM + property sim | `trembita-core`, `trembita-sim` |
| 2026-07 | Store contract Memory ≡ Redb + reopen | `trembita-storage/tests/storage.rs` |
| 2026-07 | Driver-level restart + replay | `trembita-runtime/tests/driver.rs` |
| 2026-07 | E2E election + failover + chaos | `e2e/` |
| 2026-08 | E2E PEM hot reload (SIGHUP + poll) | `e2e/cert_renew.sh` |
| 2026-07 | Macro compile-fail suite | `trembita-runtime/tests/compile_fail.rs` |
| 2026-07 | Loopback QUIC + mTLS integration | `trembita-net/tests/quic.rs`, `trembita/tests/quic.rs` |
