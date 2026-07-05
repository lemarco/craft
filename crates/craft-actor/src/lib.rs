//! `craft-actor` — the node runtime that ties consensus, storage, transport,
//! and the actor model together (backlog Wave 2).
//!
//! Hosts `RaftNodeActor`, the `ActorRegistry`, cross-node delivery/routing
//! (ADR 013, ADR 019), and the leader-only `ClusterSupervisor` (ADR 018).

pub use {craft_core, craft_net, craft_proto, craft_storage};
