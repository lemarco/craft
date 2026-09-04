use std::time::Duration;

/// Why a spawn failed.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// An actor group with this name already exists.
    #[error("actor name `{0}` is already registered")]
    NameExists(String),
    /// A pool count `> 1` was requested in production mode, which allows at most
    /// one worker per node per name (one-worker-per-vps). Enable `--dev-multi-workers` for
    /// multiple local instances.
    #[error(
        "one worker per node in production (one-worker-per-vps); count {count} requires --dev-multi-workers"
    )]
    MultiWorkerDisabled {
        /// The rejected instance count.
        count: usize,
    },
    /// The requested instance count was zero.
    #[error("instance count must be at least 1")]
    ZeroCount,
    /// No spawn factory is registered for the requested actor type (a remote
    /// spawn arrived for a type the target node does not know, E9).
    #[error("no factory registered for actor type `{0}`")]
    UnknownType(String),
    /// The actor's config could not be decoded for a remote spawn (E9).
    #[error(transparent)]
    Config(#[from] ConfigCodecError),
    /// [`UserActor::start`] failed while constructing an instance.
    #[error("actor start failed: {0}")]
    Start(Box<dyn std::error::Error + Send + Sync>),
    /// [`UserActor::restore_migration`] failed while installing a migration
    /// snapshot on the target node (E12).
    #[error("migration restore failed: {0}")]
    Restore(MigrationError),
}

/// Why a `scale_local` failed.
#[derive(Debug, thiserror::Error)]
pub enum ScaleError {
    /// No actor group with this name exists.
    #[error("no actor named `{0}`")]
    NotFound(String),
    /// The group exists but holds a different actor type than requested.
    #[error("actor `{name}` is not of the requested type (registered as `{registered}`)")]
    TypeMismatch {
        /// The group name.
        name: String,
        /// The type the group was registered with.
        registered: &'static str,
    },
    /// A count `> 1` was requested in production mode, which allows at most one
    /// worker per node per name (one-worker-per-vps). Enable `--dev-multi-workers` to scale
    /// locally.
    #[error(
        "one worker per node in production (one-worker-per-vps); scaling to {count} requires --dev-multi-workers"
    )]
    MultiWorkerDisabled {
        /// The rejected instance count.
        count: usize,
    },
    /// A cluster-wide `total` cannot be placed one-per-node because there are
    /// fewer live nodes than instances requested (one-worker-per-vps, E9).
    #[error("cannot place {total} instances one-per-node across only {nodes} live node(s)")]
    InsufficientNodes {
        /// The requested cluster-wide total.
        total: usize,
        /// The number of live nodes available.
        nodes: usize,
    },
    /// The requested instance count was zero (use [`ActorRegistry::stop`]).
    #[error("instance count must be at least 1 (use `stop` to remove the group)")]
    ZeroCount,
    /// [`UserActor::start`] failed while growing the pool.
    #[error("actor start failed: {0}")]
    Start(Box<dyn std::error::Error + Send + Sync>),
}

/// Why a `stop` failed.
#[derive(Debug, thiserror::Error)]
pub enum StopError {
    /// No actor group with this name exists.
    #[error("no actor named `{0}`")]
    NotFound(String),
}

/// Why a message could not be routed to an instance.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SendError {
    /// The group currently has no live instances.
    #[error("no live actor instances")]
    NoInstances,
    /// The selected instance's mailbox is closed (it stopped).
    #[error("actor mailbox is closed")]
    Closed,
    /// The group is draining for stop/migration and rejects new messages
    /// (E12, [drain-timeout](../../../docs/decisions/drain-timeout.md)).
    #[error("actor is draining")]
    Draining,
}

/// OTP-style supervision policy for an actor instance whose
/// [`handle`](UserActor::handle) returns an error (E14,
/// [observability](../../../docs/decisions/observability.md) §5).
///
/// A handler error is trembita's notion of an actor *failure*. The policy decides
/// what the runtime does with the failing instance; a fresh instance is rebuilt
/// with [`UserActor::start`] from the original config (so supervised actors
/// require `Config: Clone`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartPolicy {
    /// Never restart: handler errors are non-fatal and the instance keeps its
    /// current state (the default — plain `spawn` behavior).
    #[default]
    Never,
    /// Restart on failure, bounded to `max_restarts` within a sliding `window`.
    /// Exceeding the budget escalates: the instance is stopped
    /// (`ActorStopped { reason: RestartLimit }` once telemetry lands).
    OnFailure {
        /// Maximum restarts allowed inside `window` before escalation.
        max_restarts: u32,
        /// Sliding window over which restarts are counted.
        window: Duration,
    },
    /// Always restart with fresh state on every handler error.
    Always,
}

/// The outcome of a graceful drain (E12, drain-timeout).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every in-flight and queued message finished before the timeout.
    Completed,
    /// The timeout elapsed with work still in flight; the actor was force
    /// stopped (`DrainIncomplete`).
    TimedOut,
}

/// An error while capturing a migration snapshot from a live actor (E12).
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The requested instance is not live in the group.
    #[error("no live instance {0}")]
    NoInstance(u32),
    /// The instance stopped before it could produce a snapshot.
    #[error("actor mailbox is closed")]
    Closed,
    /// [`UserActor::migration_snapshot`] failed.
    #[error(transparent)]
    Migration(#[from] MigrationError),
}

/// A failure capturing or applying a migratable actor's state (E12,
/// [cross-node-actors](../../../docs/decisions/cross-node-actors.md)).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MigrationError(pub String);

impl MigrationError {
    /// Build a migration error from any displayable cause.
    pub fn new(cause: impl std::fmt::Display) -> Self {
        Self(cause.to_string())
    }
}

/// Why an actor's config could not be (de)serialized for a remote spawn (E9).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigCodecError {
    /// The actor did not override [`UserActor::encode_config`] /
    /// [`decode_config`](UserActor::decode_config), so it can only be spawned
    /// locally.
    #[error("actor is not remotely spawnable")]
    NotSpawnable,
    /// The config could not be encoded/decoded.
    #[error("config codec failed: {0}")]
    Codec(String),
}

/// Why a wire payload could not be turned into a message (E8).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessageDecodeError {
    /// The actor did not override [`UserActor::decode_message`], so it accepts
    /// only in-process messages.
    #[error("actor is not remotely addressable")]
    NotAddressable,
    /// The payload could not be decoded into the actor's message type.
    #[error("wire payload decode failed: {0}")]
    Decode(String),
}

/// Why a cross-node delivery to a local instance failed (E8).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeliverError {
    /// No actor group with this name exists on this node.
    #[error("no actor named `{0}`")]
    NotFound(String),
    /// The payload could not be decoded into the actor's message type.
    #[error(transparent)]
    Decode(#[from] MessageDecodeError),
    /// The target instance id is not live in the group (stopped / migrated).
    #[error("no live instance {0} in the group")]
    NoInstance(u32),
    /// The target instance's mailbox is closed.
    #[error("actor mailbox is closed")]
    Closed,
    /// The group is draining for stop/migration and rejects new messages (E12).
    #[error("actor is draining")]
    Draining,
}

/// Why an `ask` failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AskError {
    /// The request could not be delivered.
    #[error(transparent)]
    Send(#[from] SendError),
    /// The actor handled (or dropped) the message without replying.
    #[error("actor dropped the reply")]
    NoReply,
    /// The message was delivered but no reply arrived within [`ASK_TIMEOUT`], so
    /// the caller stops waiting rather than blocking forever on a wedged handler.
    #[error("actor did not reply within {0:?}")]
    Timeout(Duration),
}
