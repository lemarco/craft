//! Linearizable Raft from capability handlers (B-52).
//!
//! | Need | API |
//! |------|-----|
//! | Idempotency / step marker (R4) | [`OpCtx::require_store`](https://docs.rs/trembita/latest/trembita/capability/struct.OpCtx.html) — see `task.rs` (jobs template) |
//! | Authoritative replicated fact | [`OpCtx::propose_keyed`](https://docs.rs/trembita/latest/trembita/capability/struct.OpCtx.html) / [`query_keyed_linearizable`](https://docs.rs/trembita/latest/trembita/capability/struct.OpCtx.html) |
//! | Cross-shard workflow | [`OpCtx::run_keyed_saga`](https://docs.rs/trembita/latest/trembita/capability/struct.OpCtx.html) |
//!
//! Showcase: [stateful-workers/process_order.rs]({{TREMBITA_DOC_BASE}}/examples/stateful-workers/src/capabilities/orders/process_order.rs).
//! Cheat sheet: [structural-limits § Domain patterns]({{TREMBITA_DOC_BASE}}/docs/scenarios/structural-limits.md#domain-patterns-b-52).

/// Reminder for code review — handlers must not use actor `ask` for authoritative reads.
pub const USE_RAFT_QUERY_NOT_ASK: &str = "Authoritative reads: OpCtx::query_keyed_linearizable";
