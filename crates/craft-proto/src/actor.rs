//! Cross-node actor messaging + directory wire types (ADR 013, ADR 019).

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// A compile-time actor type tag. In v1 this is the Rust type name of the
/// `UserActor`, which is stable within a build; two nodes running the same
/// binary agree on it (ADR 013).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorTypeId(pub String);

/// A globally-unique address for a single actor instance in the cluster
/// (ADR 013). `generation` is bumped on respawn/migration so stale references
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
/// cluster via `/actor/register` (ADR 013).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRegistration {
    /// The instance's address.
    pub id: ActorId,
    /// The actor's type tag.
    pub actor_type: ActorTypeId,
    /// Whether the actor carries migratable state (ADR 013 migration).
    pub migratable: bool,
}

/// A state-based directory update: node `node`'s **complete** set of local
/// registrations at monotonic `epoch` (ADR 013 publish/revoke). Receivers
/// replace everything they hold for `node`, applying an update only if its
/// `epoch` is newer — so updates are idempotent and reorder-safe. Publishing an
/// empty `registrations` revokes all of `node`'s entries (e.g. on leave).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Optional routing key for consistent-hash routing (ADR 019); when
    /// `None`, round-robin routing is used.
    pub key: Option<String>,
    /// Pin to a specific node; when `None`, the registry chooses placement.
    pub node: Option<NodeId>,
}

/// An actor message crossing a node boundary via `/actor/deliver` (ADR 013).
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
    /// Per-sender correlation id, used to match a reply to its request.
    pub req_id: u64,
    /// Application-encoded (`postcard`) message body.
    pub payload: Vec<u8>,
    /// Whether the sender awaits a reply (`ask`) versus fire-and-forget
    /// (`cast`). Cross-node `ask` is a later increment; E8 delivery is cast.
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
}

/// A request to spawn an actor on a target node's registry (`/actor/spawn`,
/// ADR 013). The target looks up a factory registered for `actor_type`,
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
