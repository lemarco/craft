//! Cross-node actor messaging + directory wire types (cross-node-actors, cluster-routing).

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// A compile-time actor type tag. In v1 this is the Rust type name of the
/// `UserActor`, which is stable within a build; two nodes running the same
/// binary agree on it (cross-node-actors).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorTypeId(pub String);

/// A globally-unique address for a single actor instance in the cluster
/// (cross-node-actors). `generation` is bumped on respawn/migration so stale references
/// are detectable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorId {
    /// Node currently hosting the instance.
    pub node: NodeId,
    /// Logical group / pool name (e.g. `"workers"`).
    pub name: String,
    /// Instance index within the group (`0` for a singleton).
    pub instance: u32,
    /// Bumped on respawn / migration to invalidate stale references.
    pub generation: u64,
}

/// A directory entry describing one live actor instance, replicated across the
/// cluster via `/actor/register` (cross-node-actors).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorRegistration {
    /// The instance's address.
    pub id: ActorId,
    /// The actor's type tag.
    pub actor_type: ActorTypeId,
    /// Whether the actor carries migratable state (cross-node-actors migration).
    pub migratable: bool,
    /// Instantaneous mailbox depth on the hosting node (Observer / dashboard).
    #[serde(default)]
    pub mailbox_depth: u64,
    /// Seconds since the instance was spawned on the hosting node.
    #[serde(default)]
    pub uptime_secs: u64,
    /// Recent message rate (messages/s) on the hosting node (Observer / dashboard).
    #[serde(default)]
    pub messages_per_sec: f64,
}

impl ActorRegistration {
    /// Build a directory entry; runtime stats default to zero.
    #[must_use]
    pub fn new(id: ActorId, actor_type: ActorTypeId, migratable: bool) -> Self {
        Self {
            id,
            actor_type,
            migratable,
            mailbox_depth: 0,
            uptime_secs: 0,
            messages_per_sec: 0.0,
        }
    }
}

/// A state-based directory update: node `node`'s **complete** set of local
/// registrations at monotonic `epoch` (cross-node-actors publish/revoke). Receivers
/// replace everything they hold for `node`, applying an update only if its
/// `epoch` is newer — so updates are idempotent and reorder-safe. Publishing an
/// empty `registrations` revokes all of `node`'s entries (e.g. on leave).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectoryUpdate {
    /// The node whose local registrations this snapshot describes.
    pub node: NodeId,
    /// Monotonic per-node version; higher supersedes lower.
    pub epoch: u64,
    /// The node's full set of local registrations at this epoch.
    pub registrations: Vec<ActorRegistration>,
}

/// Acknowledgement for a [`DirectoryUpdate`] delivered to `/actor/register`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterAck {
    /// Whether the update was applied (`false` if it was stale/superseded).
    pub applied: bool,
}

/// A reference used to route a message to an actor or actor group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRef {
    /// Logical group / pool name (e.g. `"workers"`).
    pub group: String,
    /// Optional routing key for consistent-hash routing (cluster-routing); when
    /// `None`, round-robin routing is used.
    pub key: Option<String>,
    /// Pin to a specific node; when `None`, the registry chooses placement.
    pub node: Option<NodeId>,
}

/// An actor message crossing a node boundary via `/actor/deliver` (cross-node-actors).
///
/// The sender has already resolved the logical target (group + RR/keyed
/// selection) to a concrete instance `to` via the cluster directory (E7), so
/// the receiving node delivers straight to that instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorEnvelope {
    /// The concrete destination instance.
    pub to: ActorId,
    /// The sending instance, when the message originates from an actor (for
    /// replies / tracing); `None` for messages sent from outside the fabric.
    pub from: Option<ActorId>,
    /// The node that originated this envelope. Combined with [`req_id`](Self::req_id)
    /// it forms a cluster-unique key the receiver uses to deduplicate an
    /// at-least-once resend, so a side-effecting `ask` handler runs at most once
    /// per logical request. `None` disables dedup (legacy / intra-fabric sends).
    pub origin: Option<NodeId>,
    /// Per-sender correlation id, used to match a reply to its request and,
    /// with [`origin`](Self::origin), to deduplicate a resend.
    pub req_id: u64,
    /// Application-encoded (`postcard`) message body.
    pub payload: Vec<u8>,
    /// Whether the sender awaits a reply (`ask`) versus fire-and-forget
    /// (`cast`). When `true` the receiver decodes via
    /// `UserActor::decode_ask` and returns the reply in [`DeliverAck::reply`].
    pub reply_expected: bool,
}

/// Acknowledgement for an [`ActorEnvelope`] delivered to `/actor/deliver`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliverAck {
    /// Whether the message reached a live local mailbox.
    pub delivered: bool,
    /// A human-readable reason when `delivered` is `false` (unknown group,
    /// no such instance, closed mailbox, not remotely addressable).
    pub error: Option<String>,
    /// For an `ask` (`reply_expected`), the application-encoded (`postcard`)
    /// reply the handler produced; `None` for a fire-and-forget `cast` or when
    /// delivery failed.
    pub reply: Option<Vec<u8>>,
}

/// A request to spawn an actor on a target node's registry (`/actor/spawn`,
/// cross-node-actors). The target looks up a factory registered for `actor_type`,
/// decodes `config`, and starts the actor under `name`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnRequest {
    /// The group name to register the actor under.
    pub name: String,
    /// The actor's type tag; the target must have a factory registered for it.
    pub actor_type: ActorTypeId,
    /// `postcard`-encoded `A::Config`.
    pub config: Vec<u8>,
    /// Generation for the new instance (bumped on respawn/migration).
    pub generation: u64,
}

/// Reply to a [`SpawnRequest`]: the spawned instance's id, or an error string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnReply {
    /// The spawned instance's id on success.
    pub id: Option<ActorId>,
    /// A human-readable reason on failure (unknown type, config decode, name
    /// collision, start failure).
    pub error: Option<String>,
}

/// A request to drive a group to a cluster-wide instance count on the leader
/// (`/actor/scale`, cross-node-actors, supervisor-leader). Sent when `scale_cluster` is called on a
/// follower: the leader owns cluster-wide placement, so the follower forwards
/// the intent (with the committed voter set it observed) rather than planning
/// locally. The target reconstructs each placement via the `actor_type`
/// factory, exactly like a [`SpawnRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleRequest {
    /// The group name to scale.
    pub name: String,
    /// The actor's type tag; every hosting node must have a factory for it.
    pub actor_type: ActorTypeId,
    /// Desired cluster-wide instance count (one worker per node, one-worker-per-vps).
    pub total: u64,
    /// `postcard`-encoded `A::Config` used to construct new instances.
    pub config: Vec<u8>,
    /// The live/voter node set the requester observed (committed Raft
    /// membership), against which the plan is computed.
    pub live_nodes: Vec<NodeId>,
}

/// Reply to a [`ScaleRequest`]: `None` error on success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleReply {
    /// A human-readable reason on failure (planning error or a spawn failure);
    /// `None` when the scale was applied.
    pub error: Option<String>,
}

/// A request to stop a group on a target node (`/actor/stop`, cross-node-actors, supervisor-leader).
/// Sent by the leader when a scale-down (or reconcile) plans a *removal* on
/// another node: the one-worker-per-node model (one-worker-per-vps) means "remove on node
/// N" is "stop this group on node N". The target stops the named group
/// idempotently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopRequest {
    /// The group name to stop on the target node.
    pub name: String,
}

/// Reply to a [`StopRequest`]: `None` error on success (stopping an absent
/// group is a success — it is already gone).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopReply {
    /// A human-readable reason on failure; `None` when the group was stopped
    /// (or was already absent).
    pub error: Option<String>,
}

/// A request to migrate a stateful actor to a target node (`/actor/migrate`,
/// cross-node-actors). The departing node captures the instance's migration snapshot,
/// then asks the target to spawn a replacement under `name` and restore the
/// snapshot into it before it handles any message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrateRequest {
    /// The instance being migrated away (its current address on the source).
    pub from: ActorId,
    /// The group name to register the replacement under (usually `from.name`).
    pub name: String,
    /// The actor's type tag; the target must have a factory registered for it.
    pub actor_type: ActorTypeId,
    /// `postcard`-encoded `A::Config` for constructing the replacement.
    pub config: Vec<u8>,
    /// Migration snapshot from [`migration_snapshot`]; empty for a stateless
    /// actor (the target simply spawns a fresh instance).
    ///
    /// [`migration_snapshot`]: https://docs.rs/crafty-actor
    pub snapshot: Vec<u8>,
    /// Generation for the replacement instance (bumped past the source's).
    pub generation: u64,
}

/// Reply to a [`MigrateRequest`]: the replacement instance's id, or an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrateReply {
    /// The replacement instance's id on success.
    pub id: Option<ActorId>,
    /// A human-readable reason on failure (unknown type, config decode, restore
    /// failure, start failure).
    pub error: Option<String>,
}
