//! `ActorRegistry` — local actor spawn / pool / scale / stop (backlog E6,
//! [cluster-elasticity](../../../docs/decisions/cluster-elasticity.md),
//! [cluster-elasticity#one-worker-per-vps-production](../../../docs/decisions/cluster-elasticity.md#one-worker-per-vps-production)).
//!
//! This is the **local** half of the actor fabric: named singletons and pools
//! of user actors running on the node, with round-robin and keyed message
//! routing. Cross-node addressing, the cluster directory, and remote
//! spawn/scale (cross-node-actors, cluster-routing) layer on top of these primitives in later
//! increments (E7–E9); the API here is shaped so they can.
//!
//! ## Actor model
//!
//! A [`UserActor`] owns some state built from a `Config` and handles one
//! `Message` at a time on its own tokio task (a serial mailbox — no interior
//! locking needed in user code). Request/response ("ask") is expressed by
//! carrying an [`RpcReplyPort`] inside a message, exactly like `ractor`'s
//! `RpcReplyPort`, so a single `Message` type covers both fire-and-forget and
//! call semantics.
//!
//! Like the node runtime (E1), this is built directly on tokio rather than an
//! external actor framework, keeping the dependency surface small and the whole
//! thing deterministic and unit-testable.
//!
//! ## Production vs development (one-worker-per-vps)
//!
//! Production runs **one worker per VPS per name**: [`spawn_pool`] and
//! [`scale_local`] with a count `> 1` are rejected unless the registry is built
//! with [`ActorRegistry::new_dev`]. Scale out by adding VPSes (E9
//! `scale_cluster`), not by stacking workers locally.
//!
//! [`spawn_pool`]: ActorRegistry::spawn_pool
//! [`scale_local`]: ActorRegistry::scale_local

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crafty_net::transport::BoxFuture;
use crafty_proto::{ActorId, ActorRegistration, ActorTypeId, NodeId};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Default graceful-drain timeout for stopping/migrating an actor instance
/// ([drain-timeout](../../../docs/decisions/drain-timeout.md)). Overridable per
/// call; the facade exposes `.drain_timeout(..)` / `CRAFTY_DRAIN_TIMEOUT`.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Caller-side deadline for `ask` (request/reply): if the target actor does not
/// answer within this window, [`ActorRef::ask`] / [`PoolRef::ask`] return
/// [`AskError::Timeout`] instead of blocking forever on a wedged or slow
/// handler. Mirrors the cross-node ask deadline so local and remote asks bound
/// the caller symmetrically.
pub const ASK_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// UserActor
// ---------------------------------------------------------------------------

/// A user-defined actor: state built from a `Config`, driven by a serial
/// mailbox of `Message`s.
///
/// Each actor instance runs on its own task and processes messages one at a
/// time, so `&mut self` handlers never race. For request/response, put an
/// [`RpcReplyPort`] in the message and reply to it from [`handle`](UserActor::handle).
pub trait UserActor: Send + Sized + 'static {
    /// Immutable configuration used to construct the actor's initial state.
    type Config: Send + 'static;
    /// The message type this actor accepts.
    type Message: Send + 'static;
    /// Error returned by [`start`](UserActor::start) / [`handle`](UserActor::handle).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Whether instances carry migratable state that should be snapshotted and
    /// transferred when their node leaves (cross-node-actors). Defaults to `false`
    /// (stateless — the supervisor simply respawns the same count elsewhere).
    const MIGRATABLE: bool = false;

    /// Build the actor's initial state from its configuration. Called once, on
    /// the actor's task, before any message is handled.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the actor cannot be initialized; the spawn
    /// fails and no task is left running.
    fn start(config: Self::Config) -> Result<Self, Self::Error>;

    /// Handle a single message. Returned errors are surfaced to the actor's
    /// task (currently logged as a dropped result); the actor keeps running.
    ///
    /// The returned future must be `Send` (it runs on a multi-threaded
    /// executor). Implement it with a plain `async fn handle`.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the message could not be processed.
    fn handle(
        &mut self,
        msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    /// Encode this actor's `Config` for a remote spawn (E9, cross-node-actors
    /// `/actor/spawn`). The default makes the actor **local-spawn-only**:
    /// `spawn_remote` / `scale_cluster` fail with
    /// [`ConfigCodecError::NotSpawnable`]. Override it (typically with
    /// `crafty_proto::encode`) to allow the control plane to place the actor on
    /// other nodes.
    ///
    /// # Errors
    /// Returns [`ConfigCodecError`] if the actor is not remotely spawnable or
    /// the config cannot be encoded.
    fn encode_config(_config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        Err(ConfigCodecError::NotSpawnable)
    }

    /// Decode a `Config` shipped for a remote spawn (E9). Must round-trip with
    /// [`encode_config`](UserActor::encode_config); the default rejects.
    ///
    /// # Errors
    /// Returns [`ConfigCodecError`] if the actor is not remotely spawnable or
    /// the bytes cannot be decoded.
    fn decode_config(_bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        Err(ConfigCodecError::NotSpawnable)
    }

    /// Capture this actor's migratable state as a byte snapshot, so it can be
    /// transferred to a replacement on another node when this node leaves
    /// (E12, [cross-node-actors](../../../docs/decisions/cross-node-actors.md)). The
    /// default is a **stateless** actor: an empty snapshot, meaning the
    /// supervisor simply respawns a fresh instance elsewhere. Stateful actors
    /// (`MIGRATABLE = true`) override this together with
    /// [`restore_migration`](UserActor::restore_migration).
    ///
    /// Runs on the actor's own task, ordered after any already-queued messages,
    /// so the snapshot reflects everything handled before the migration began.
    ///
    /// # Errors
    /// Returns [`MigrationError`] if the state cannot be captured.
    fn migration_snapshot(&self) -> Result<Vec<u8>, MigrationError> {
        Ok(Vec::new())
    }

    /// Restore migratable state from a snapshot produced by
    /// [`migration_snapshot`](UserActor::migration_snapshot) on the departing
    /// node (E12). Runs once, on the new instance's task, before it handles any
    /// message. The default ignores the (empty) snapshot.
    ///
    /// # Errors
    /// Returns [`MigrationError`] if the snapshot cannot be applied.
    fn restore_migration(&mut self, _snapshot: &[u8]) -> Result<(), MigrationError> {
        Ok(())
    }

    /// Decode a cross-node wire payload into a message for remote delivery
    /// (E8, cross-node-actors `/actor/deliver`). The default leaves the actor
    /// **local-only**: a remote `cast` to it fails with
    /// [`MessageDecodeError::NotAddressable`]. Override it (typically with
    /// `crafty_proto::decode`) to accept messages sent from other nodes.
    ///
    /// This decodes the fire-and-forget half; the request/reply half is
    /// [`decode_ask`](UserActor::decode_ask).
    ///
    /// # Errors
    /// Returns [`MessageDecodeError`] if the actor is not remotely addressable
    /// or the payload cannot be decoded into a [`Message`](UserActor::Message).
    fn decode_message(_payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        Err(MessageDecodeError::NotAddressable)
    }

    /// Decode a cross-node **ask** into a message carrying a reply port (E8,
    /// cross-node-actors, cluster-routing `/actor/deliver` with `reply_expected`). Build your ask
    /// message variant, converting the supplied [`WireReplyPort`] into the typed
    /// [`RpcReplyPort<R>`](RpcReplyPort) it expects via
    /// [`WireReplyPort::reply_port`]; whatever the handler replies is
    /// `postcard`-encoded back to the caller. The default rejects remote asks
    /// with [`MessageDecodeError::NotAddressable`].
    ///
    /// ```ignore
    /// fn decode_ask(payload: &[u8], reply: WireReplyPort)
    ///     -> Result<Self::Message, MessageDecodeError>
    /// {
    ///     let req: Req = crafty_proto::decode(payload)
    ///         .map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
    ///     Ok(Msg::Ask { req, reply: reply.reply_port::<Resp>() })
    /// }
    /// ```
    ///
    /// # Errors
    /// Returns [`MessageDecodeError`] if the actor does not support remote asks
    /// or the payload cannot be decoded.
    fn decode_ask(
        _payload: &[u8],
        _reply: WireReplyPort,
    ) -> Result<Self::Message, MessageDecodeError> {
        Err(MessageDecodeError::NotAddressable)
    }

    /// Called once after the mailbox closes (stop or scale-in), for cleanup.
    fn stopped(&mut self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}

/// A one-shot reply channel embedded in a message to implement "ask"
/// (request/response). The handler calls [`reply`](RpcReplyPort::reply) with
/// the response; the caller awaits it via [`ActorRef::ask`] / [`PoolRef::ask`].
///
/// A port is backed either by an **in-process** channel (local `ask`) or, for a
/// cross-node `ask` arriving over `/actor/deliver`, by a **wire** channel that
/// `postcard`-encodes the reply and returns it in the [`DeliverAck`]
/// (cross-node-actors, cluster-routing). A [`WireReplyPort`] is turned into a typed one via
/// [`WireReplyPort::reply_port`] inside [`UserActor::decode_ask`].
///
/// [`DeliverAck`]: crafty_proto::DeliverAck
pub struct RpcReplyPort<R> {
    sink: Reply<R>,
}

/// A cross-node reply: either the `postcard`-encoded bytes, or the reason the
/// handler's reply value could not be serialized. Carrying the failure lets the
/// serve side report a real encode error instead of a silent "no reply".
pub(crate) type WireReply = Result<Vec<u8>, String>;

/// Where an [`RpcReplyPort`]'s reply is delivered.
enum Reply<R> {
    /// In-process caller (local `ask`).
    Local(oneshot::Sender<R>),
    /// Cross-node caller: serialize `R` and hand the result back over the wire.
    Wire {
        tx: oneshot::Sender<WireReply>,
        encode: fn(&R) -> Result<Vec<u8>, crafty_proto::CodecError>,
    },
}

impl<R> RpcReplyPort<R> {
    /// A port backed by an in-process one-shot channel (local `ask`).
    fn local(tx: oneshot::Sender<R>) -> Self {
        Self {
            sink: Reply::Local(tx),
        }
    }

    /// Send the response back to the asker. Returns `Err(value)` if the caller
    /// already gave up (dropped the pending `ask`) or, for a cross-node reply,
    /// if the value could not be encoded.
    ///
    /// A cross-node encode failure is *also* signalled to the receiving node
    /// (over the wire channel) so the asker sees a real encode error rather than
    /// a reply that silently never arrives.
    ///
    /// # Errors
    /// Returns the unsent `value` if the receiving `ask` was dropped or the
    /// reply could not be serialized.
    pub fn reply(self, value: R) -> Result<(), R> {
        match self.sink {
            Reply::Local(tx) => tx.send(value),
            Reply::Wire { tx, encode } => match encode(&value) {
                Ok(bytes) => tx.send(Ok(bytes)).map_err(|_| value),
                Err(e) => {
                    // Surface the encode failure to the serve side; the handler
                    // still gets `value` back as undelivered.
                    let _ = tx.send(Err(e.to_string()));
                    Err(value)
                }
            },
        }
    }
}

/// An opaque reply channel for a cross-node `ask`, handed to
/// [`UserActor::decode_ask`]. Convert it to the typed [`RpcReplyPort`] your
/// message variant expects with [`reply_port`](WireReplyPort::reply_port); the
/// reply is `postcard`-encoded and returned in the `DeliverAck`.
pub struct WireReplyPort {
    tx: oneshot::Sender<WireReply>,
}

impl WireReplyPort {
    /// Adapt this wire channel into a typed [`RpcReplyPort<R>`] to embed in a
    /// message. `R` must be serializable so the reply can cross the node
    /// boundary.
    #[must_use]
    pub fn reply_port<R: serde::Serialize>(self) -> RpcReplyPort<R> {
        RpcReplyPort {
            sink: Reply::Wire {
                tx: self.tx,
                encode: crafty_proto::encode::<R>,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

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
/// A handler error is crafty's notion of an actor *failure*. The policy decides
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

// ---------------------------------------------------------------------------
// Instance + pool internals
// ---------------------------------------------------------------------------

/// An item on an instance's serial mailbox: either a user message or a control
/// request to capture the actor's migration snapshot (E12). Using one channel
/// keeps snapshot capture strictly ordered after already-queued messages.
enum Mailbox<A: UserActor> {
    User(A::Message),
    Snapshot(oneshot::Sender<Result<Vec<u8>, MigrationError>>),
}

/// Reconstructs a fresh actor `state` for a supervised restart (E14). Returns
/// `None` if [`UserActor::start`] fails, which escalates the instance to stop.
/// Only ever invoked from the owning instance task, so `Send` (no `Sync`).
type Rebuild<A> = Box<dyn Fn() -> Option<A> + Send>;

/// Build a [`Rebuild`] that reconstructs `A` from a retained config clone (E14).
fn make_rebuild<A: UserActor>(config: A::Config) -> Rebuild<A>
where
    A::Config: Clone,
{
    Box::new(move || A::start(config.clone()).ok())
}

/// Observes actor lifecycle transitions (E14 / observability Track H) so a telemetry
/// layer can surface spawns, stops, restarts, escalations, and per-message
/// latency as metrics/events **without** the registry depending on it. Install
/// one with [`ActorRegistry::set_observer`] *before spawning actors* (the facade
/// does this at build time); each instance task binds the observer once at
/// launch, so the hooks add no per-message locking.
///
/// The hooks fire from the instance's own task, so keep them cheap and
/// non-blocking — a slow or panicking observer stalls that actor. All hooks
/// except [`on_restart`](ActorObserver::on_restart) /
/// [`on_escalated`](ActorObserver::on_escalated) default to no-ops.
pub trait ActorObserver: Send + Sync {
    /// A fresh instance of `name` started (plain, supervised, or restored).
    fn on_spawned(&self, _name: &str, _instance: u32) {}
    /// An instance stopped for a non-escalation reason (explicit stop, drain,
    /// scale-in, or the source side of a migration).
    fn on_stopped(&self, _name: &str, _instance: u32) {}
    /// An instance finished handling one message in `elapsed`. Hot path —
    /// implementations should fast-path when no work is required.
    fn on_message_handled(&self, _name: &str, _instance: u32, _elapsed: Duration) {}
    /// A supervised instance rebuilt fresh state after a handler failure.
    /// `count` is the group's cumulative restart tally after this restart.
    fn on_restart(&self, name: &str, instance: u32, count: u32);
    /// A supervised instance exhausted its restart budget (or could not rebuild)
    /// and escalated: the instance stopped and deregistered itself.
    fn on_escalated(&self, name: &str, instance: u32);
}

/// A slot an [`ActorObserver`] can be installed into after construction (the
/// registry outlives the telemetry wiring in the builder). Read once per
/// instance task at launch.
type ObserverHook = Arc<Mutex<Option<Arc<dyn ActorObserver>>>>;

/// A point-in-time snapshot of one actor group's runtime counters, for metrics
/// sampling (observability §2). Cumulative counters (`messages`, `handle_nanos`) are
/// monotonic; the sampler derives rates/latency by differencing successive
/// reads. `mailbox_depth` is instantaneous (queued-but-unhandled messages).
#[derive(Debug, Clone)]
pub struct ActorGroupStats {
    /// The group's registered name.
    pub name: String,
    /// Live instance count.
    pub instances: usize,
    /// Cumulative messages handled across the group's instances.
    pub messages: u64,
    /// Cumulative wall-time spent in `handle`, in nanoseconds.
    pub handle_nanos: u64,
    /// Currently-queued (enqueued but not yet handled) messages.
    pub mailbox_depth: i64,
}

/// A single running actor instance within a named group.
#[allow(clippy::struct_field_names)] // `instance` is the actor id within the group.
struct Instance<A: UserActor> {
    instance: u32,
    tx: mpsc::UnboundedSender<Mailbox<A>>,
    join: JoinHandle<()>,
}

/// The shared state of a named actor group (one instance = a singleton).
struct PoolInner<A: UserActor> {
    name: String,
    /// Behind its own `Arc` so an escalating instance task can remove itself
    /// from the roster (E14) without holding the whole pool alive.
    instances: Arc<Mutex<Vec<Instance<A>>>>,
    /// Round-robin cursor for `send`.
    rr: AtomicUsize,
    /// Monotonic instance-id allocator (never reused within a group).
    next_instance: AtomicU32,
    /// Group-wide stop signal; flipping it to `true` ends every instance task.
    stop: watch::Sender<bool>,
    /// Set while the group is draining for stop/migration; new sends are
    /// rejected (E12, drain-timeout).
    draining: AtomicBool,
    /// Cumulative supervised restarts across the group's instances (E14). Held
    /// behind its own `Arc` so instance tasks can bump it without keeping the
    /// pool alive (which would break the drop-based stop path).
    restarts: Arc<AtomicU32>,
    /// Telemetry hook fired on lifecycle transitions + per message (Track H).
    observer: ObserverHook,
    /// Cumulative messages handled across the group's instances (Track H).
    messages: Arc<AtomicU64>,
    /// Cumulative nanoseconds spent in `handle` across instances (Track H).
    handle_nanos: Arc<AtomicU64>,
    /// Enqueued-but-unhandled messages (mailbox depth gauge, Track H). Signed
    /// so a transient dequeue-before-increment race can't underflow.
    queued: Arc<AtomicI64>,
    /// Per-group drain override; falls back to cluster default when unset.
    drain_timeout: Mutex<Option<Duration>>,
}

impl<A: UserActor> PoolInner<A> {
    fn new(name: &str, observer: ObserverHook) -> Arc<Self> {
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            name: name.to_string(),
            instances: Arc::new(Mutex::new(Vec::new())),
            rr: AtomicUsize::new(0),
            next_instance: AtomicU32::new(0),
            stop,
            draining: AtomicBool::new(false),
            restarts: Arc::new(AtomicU32::new(0)),
            observer,
            messages: Arc::new(AtomicU64::new(0)),
            handle_nanos: Arc::new(AtomicU64::new(0)),
            queued: Arc::new(AtomicI64::new(0)),
            drain_timeout: Mutex::new(None),
        })
    }

    fn set_drain_timeout(&self, timeout: Option<Duration>) {
        *self.drain_timeout.lock().unwrap() = timeout;
    }

    fn drain_timeout(&self) -> Option<Duration> {
        *self.drain_timeout.lock().unwrap()
    }

    /// Launch the mailbox task for an already-constructed `state`, register the
    /// instance, and return its id. Shared by fresh spawns and migration
    /// restores. `policy` + `rebuild` drive supervised restarts (E14); a plain
    /// spawn passes [`RestartPolicy::Never`] and `None`.
    fn launch(
        self: &Arc<Self>,
        mut state: A,
        policy: RestartPolicy,
        rebuild: Option<Rebuild<A>>,
    ) -> u32 {
        let instance = self.next_instance.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel::<Mailbox<A>>();
        let mut stop_rx = self.stop.subscribe();
        let restarts = Arc::clone(&self.restarts);
        let roster = Arc::clone(&self.instances);
        let messages = Arc::clone(&self.messages);
        let handle_nanos = Arc::clone(&self.handle_nanos);
        let queued = Arc::clone(&self.queued);
        // Bind the observer once (installed before any spawn, observability Track H),
        // so per-message hooks never touch the shared lock.
        let observer = self.observer.lock().unwrap().clone();
        let name = self.name.clone();
        let join = tokio::spawn(async move {
            if let Some(o) = &observer {
                o.on_spawned(&name, instance);
            }
            // Timestamps of recent restarts, for the `OnFailure` sliding window.
            let mut history: Vec<Instant> = Vec::new();
            // Whether we exited because supervision escalated (budget exhausted
            // or the rebuild failed): such an instance removes itself from the
            // roster so `is_alive` / routing stop seeing it (E14).
            let mut escalated = false;
            'run: loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        // Group dropped (Err) or stop signalled (true) → force out.
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    maybe = rx.recv() => match maybe {
                        Some(Mailbox::User(msg)) => {
                            queued.fetch_sub(1, Ordering::Relaxed);
                            let started = Instant::now();
                            let result = state.handle(msg).await;
                            let elapsed = started.elapsed();
                            messages.fetch_add(1, Ordering::Relaxed);
                            handle_nanos
                                .fetch_add(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX), Ordering::Relaxed);
                            if let Some(o) = &observer {
                                o.on_message_handled(&name, instance, elapsed);
                            }
                            if result.is_err() {
                                // A handler error is a failure; the policy decides
                                // whether to rebuild fresh state or escalate (E14).
                                match policy {
                                    RestartPolicy::Never => {}
                                    RestartPolicy::Always => {
                                        if let Some(fresh) = rebuild.as_ref().and_then(|rb| rb()) {
                                            state = fresh;
                                            let count =
                                                restarts.fetch_add(1, Ordering::Relaxed) + 1;
                                            if let Some(o) = &observer {
                                                o.on_restart(&name, instance, count);
                                            }
                                        } else {
                                            escalated = true;
                                            break 'run; // cannot restart
                                        }
                                    }
                                    RestartPolicy::OnFailure { max_restarts, window } => {
                                        let now = Instant::now();
                                        history.retain(|t| now.duration_since(*t) < window);
                                        if u32::try_from(history.len()).unwrap_or(u32::MAX) < max_restarts {
                                            if let Some(fresh) = rebuild.as_ref().and_then(|rb| rb()) {
                                                state = fresh;
                                                history.push(now);
                                                let count = restarts
                                                    .fetch_add(1, Ordering::Relaxed)
                                                    + 1;
                                                if let Some(o) = &observer {
                                                    o.on_restart(&name, instance, count);
                                                }
                                            } else {
                                                escalated = true;
                                                break 'run;
                                            }
                                        } else {
                                            escalated = true;
                                            break 'run; // budget exhausted → escalate
                                        }
                                    }
                                }
                            }
                        }
                        Some(Mailbox::Snapshot(reply)) => {
                            let _ = reply.send(state.migration_snapshot());
                        }
                        None => break, // mailbox closed (scaled in / drained)
                    }
                }
            }
            if escalated {
                roster.lock().unwrap().retain(|i| i.instance != instance);
                if let Some(o) = &observer {
                    o.on_escalated(&name, instance);
                }
            } else if let Some(o) = &observer {
                o.on_stopped(&name, instance);
            }
            state.stopped().await;
        });
        self.instances
            .lock()
            .unwrap()
            .push(Instance { instance, tx, join });
        instance
    }

    /// Start one instance and register it. On failure nothing is registered.
    fn spawn_instance(self: &Arc<Self>, config: A::Config) -> Result<u32, A::Error> {
        let state = A::start(config)?;
        Ok(self.launch(state, RestartPolicy::Never, None))
    }

    /// Start one supervised instance whose handler errors are governed by
    /// `policy`, rebuilding fresh state from `config` on restart (E14).
    fn spawn_instance_supervised(
        self: &Arc<Self>,
        config: A::Config,
        policy: RestartPolicy,
    ) -> Result<u32, A::Error>
    where
        A::Config: Clone,
    {
        let state = A::start(config.clone())?;
        let rebuild = make_rebuild::<A>(config);
        Ok(self.launch(state, policy, Some(rebuild)))
    }

    /// Start one instance and restore migratable state into it before it
    /// handles any message (E12).
    fn spawn_instance_restoring(
        self: &Arc<Self>,
        config: A::Config,
        snapshot: &[u8],
    ) -> Result<u32, SpawnError> {
        let mut state = A::start(config).map_err(|e| SpawnError::Start(Box::new(e)))?;
        state
            .restore_migration(snapshot)
            .map_err(SpawnError::Restore)?;
        Ok(self.launch(state, RestartPolicy::Never, None))
    }

    /// Cumulative supervised restarts across this group's instances (E14).
    fn restart_count(&self) -> u32 {
        self.restarts.load(Ordering::Relaxed)
    }

    /// A clone of the round-robin-selected instance's sender.
    fn pick_rr(&self) -> Option<mpsc::UnboundedSender<Mailbox<A>>> {
        let instances = self.instances.lock().unwrap();
        if instances.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % instances.len();
        Some(instances[i].tx.clone())
    }

    /// A clone of the instance selected by the consistent hash ring for `key`.
    fn pick_keyed(&self, key: u64) -> Option<mpsc::UnboundedSender<Mailbox<A>>> {
        let instances = self.instances.lock().unwrap();
        if instances.is_empty() {
            return None;
        }
        let index =
            crate::ring::pick_index(key, instances.len(), crate::ring::group_salt(&self.name));
        Some(instances[index].tx.clone())
    }

    fn send_rr(&self, msg: A::Message) -> Result<(), SendError> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(SendError::Draining);
        }
        let tx = self.pick_rr().ok_or(SendError::NoInstances)?;
        self.enqueue(&tx, msg).map_err(|_| SendError::Closed)
    }

    fn send_keyed(&self, key: u64, msg: A::Message) -> Result<(), SendError> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(SendError::Draining);
        }
        let tx = self.pick_keyed(key).ok_or(SendError::NoInstances)?;
        self.enqueue(&tx, msg).map_err(|_| SendError::Closed)
    }

    /// Enqueue a user message and bump the mailbox-depth gauge on success. The
    /// counter is decremented by the instance task when it dequeues (Track H).
    fn enqueue(
        &self,
        tx: &mpsc::UnboundedSender<Mailbox<A>>,
        msg: A::Message,
    ) -> Result<(), A::Message> {
        match tx.send(Mailbox::User(msg)) {
            Ok(()) => {
                self.queued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::SendError(Mailbox::User(msg))) => Err(msg),
            Err(_) => unreachable!("enqueue only sends Mailbox::User"),
        }
    }

    /// Deliver to a specific instance id (used by cross-node delivery, which
    /// has already selected the target instance via the directory, E8).
    fn send_to_instance(&self, instance: u32, msg: A::Message) -> Result<(), DeliverError> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(DeliverError::Draining);
        }
        let tx = {
            let instances = self.instances.lock().unwrap();
            instances
                .iter()
                .find(|i| i.instance == instance)
                .ok_or(DeliverError::NoInstance(instance))?
                .tx
                .clone()
        };
        self.enqueue(&tx, msg).map_err(|_| DeliverError::Closed)
    }

    /// Capture instance `instance`'s migration snapshot. The request rides the
    /// serial mailbox, so it observes every message queued before it (E12).
    async fn snapshot_instance(&self, instance: u32) -> Result<Vec<u8>, SnapshotError> {
        let tx = {
            let instances = self.instances.lock().unwrap();
            instances
                .iter()
                .find(|i| i.instance == instance)
                .ok_or(SnapshotError::NoInstance(instance))?
                .tx
                .clone()
        };
        let (reply, rx) = oneshot::channel();
        tx.send(Mailbox::Snapshot(reply))
            .map_err(|_| SnapshotError::Closed)?;
        rx.await
            .map_err(|_| SnapshotError::Closed)?
            .map_err(SnapshotError::Migration)
    }

    /// Gracefully drain every instance: reject new messages, let queued and
    /// in-flight work finish, and force-stop any instance still running when
    /// `timeout` elapses (E12, drain-timeout).
    async fn drain(&self, timeout: Duration) -> DrainOutcome {
        self.draining.store(true, Ordering::SeqCst);
        let drained: Vec<Instance<A>> = std::mem::take(&mut *self.instances.lock().unwrap());
        let mut outcome = DrainOutcome::Completed;
        for inst in drained {
            let Instance { tx, mut join, .. } = inst;
            // Close the mailbox so the task drains its queue then exits.
            drop(tx);
            if tokio::time::timeout(timeout, &mut join).await.is_ok() {
            } else {
                join.abort();
                outcome = DrainOutcome::TimedOut;
            }
        }
        outcome
    }

    fn len(&self) -> usize {
        self.instances.lock().unwrap().len()
    }

    /// The instance ids currently live in this group (ascending), for
    /// introspection and the forthcoming cross-node `ActorId` (E7).
    fn instance_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self
            .instances
            .lock()
            .unwrap()
            .iter()
            .map(|i| i.instance)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Signal every instance to stop and clear the roster. Synchronous and
    /// non-draining: tasks wind down on their own once signalled.
    fn signal_stop(&self) {
        let _ = self.stop.send(true);
        self.instances.lock().unwrap().clear();
    }

    /// Stop every instance and await their tasks (graceful drain).
    async fn stop(&self) {
        let _ = self.stop.send(true);
        let drained: Vec<Instance<A>> = std::mem::take(&mut *self.instances.lock().unwrap());
        for inst in drained {
            let _ = inst.join.await;
        }
    }

    /// Grow or shrink to exactly `count` instances, cloning `config` for new
    /// ones. Awaits the tasks of any instances removed on shrink.
    async fn scale_to(self: &Arc<Self>, count: usize, config: &A::Config) -> Result<(), A::Error>
    where
        A::Config: Clone,
    {
        let current = self.len();
        if count > current {
            for _ in current..count {
                self.spawn_instance(config.clone())?;
            }
        } else if count < current {
            let removed: Vec<Instance<A>> = {
                let mut instances = self.instances.lock().unwrap();
                instances.split_off(count)
            };
            for inst in removed {
                // Drop the sender *first* so the mailbox closes; only then can
                // the task observe `recv → None`, finish, and let `join` resolve.
                let Instance { tx, join, .. } = inst;
                drop(tx);
                let _ = join.await;
            }
        }
        Ok(())
    }
}

/// Type-erased lifecycle handle so the registry can stop / inspect a group
/// without knowing its actor type.
trait GroupLifecycle: Send + Sync {
    fn instance_count(&self) -> usize;
    fn instance_ids(&self) -> Vec<u32>;
    fn type_name(&self) -> &'static str;
    fn migratable(&self) -> bool;
    fn signal_stop(&self);
    /// Runtime counters `(instances, messages, handle_nanos, mailbox_depth)` for
    /// metrics sampling (Track H).
    fn runtime_stats(&self) -> (usize, u64, u64, i64);
    /// Gracefully drain and stop the group with `timeout` (E12, drain-timeout).
    fn drain(self: Arc<Self>, timeout: Duration) -> BoxFuture<'static, DrainOutcome>;
    /// Per-group graceful-drain override ([drain-timeout]).
    fn set_drain_timeout(&self, timeout: Option<Duration>);
    fn drain_timeout(&self) -> Option<Duration>;
    /// Capture a migration snapshot from instance `instance` (E12).
    fn snapshot(
        self: Arc<Self>,
        instance: u32,
    ) -> BoxFuture<'static, Result<Vec<u8>, SnapshotError>>;
}

impl<A: UserActor> GroupLifecycle for PoolInner<A> {
    fn instance_count(&self) -> usize {
        self.len()
    }
    fn instance_ids(&self) -> Vec<u32> {
        PoolInner::instance_ids(self)
    }
    fn type_name(&self) -> &'static str {
        std::any::type_name::<A>()
    }
    fn migratable(&self) -> bool {
        A::MIGRATABLE
    }
    fn signal_stop(&self) {
        PoolInner::signal_stop(self);
    }
    fn runtime_stats(&self) -> (usize, u64, u64, i64) {
        (
            self.len(),
            self.messages.load(Ordering::Relaxed),
            self.handle_nanos.load(Ordering::Relaxed),
            self.queued.load(Ordering::Relaxed),
        )
    }
    fn drain(self: Arc<Self>, timeout: Duration) -> BoxFuture<'static, DrainOutcome> {
        Box::pin(async move { PoolInner::drain(&self, timeout).await })
    }
    fn set_drain_timeout(&self, timeout: Option<Duration>) {
        PoolInner::set_drain_timeout(self, timeout);
    }
    fn drain_timeout(&self) -> Option<Duration> {
        PoolInner::drain_timeout(self)
    }
    fn snapshot(
        self: Arc<Self>,
        instance: u32,
    ) -> BoxFuture<'static, Result<Vec<u8>, SnapshotError>> {
        Box::pin(async move { PoolInner::snapshot_instance(&self, instance).await })
    }
}

/// Type-erased byte ingress so the registry can deliver a cross-node
/// [`ActorEnvelope`](crafty_proto::ActorEnvelope) payload to a group without
/// knowing its actor type (E8): the payload is decoded via
/// [`UserActor::decode_message`] and routed to the selected instance.
trait WireIngress: Send + Sync {
    fn deliver(&self, instance: u32, payload: &[u8]) -> Result<(), DeliverError>;
    /// Deliver a cross-node **ask**: decode via [`UserActor::decode_ask`] with a
    /// wire reply port and return the channel the encoded reply arrives on.
    fn deliver_ask(
        &self,
        instance: u32,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<WireReply>, DeliverError>;
}

impl<A: UserActor> WireIngress for PoolInner<A> {
    fn deliver(&self, instance: u32, payload: &[u8]) -> Result<(), DeliverError> {
        let msg = A::decode_message(payload)?;
        self.send_to_instance(instance, msg)
    }

    fn deliver_ask(
        &self,
        instance: u32,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<WireReply>, DeliverError> {
        let (tx, rx) = oneshot::channel();
        let msg = A::decode_ask(payload, WireReplyPort { tx })?;
        self.send_to_instance(instance, msg)?;
        Ok(rx)
    }
}

// ---------------------------------------------------------------------------
// Public handles
// ---------------------------------------------------------------------------

/// A handle to a single named actor (a group of one). Cheap to clone.
#[derive(Clone)]
pub struct ActorRef<A: UserActor> {
    pool: Arc<PoolInner<A>>,
}

impl<A: UserActor> std::fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("name", &self.pool.name)
            .field("alive", &(self.pool.len() > 0))
            .finish()
    }
}

impl<A: UserActor> ActorRef<A> {
    /// Deliver a fire-and-forget message.
    ///
    /// # Errors
    /// Returns [`SendError`] if the actor has stopped.
    pub fn send(&self, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_rr(msg)
    }

    /// Send a request and await its reply. `build` receives an [`RpcReplyPort`]
    /// to embed in the message; the handler replies through it.
    ///
    /// # Errors
    /// Returns [`AskError`] if the message cannot be delivered or the actor
    /// drops the reply without answering.
    pub async fn ask<R, F>(&self, build: F) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnOnce(RpcReplyPort<R>) -> A::Message,
    {
        let (tx, rx) = oneshot::channel();
        self.pool.send_rr(build(RpcReplyPort::local(tx)))?;
        match tokio::time::timeout(ASK_TIMEOUT, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(AskError::NoReply),
            Err(_) => Err(AskError::Timeout(ASK_TIMEOUT)),
        }
    }

    /// Whether the actor still has a live instance.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.pool.len() > 0
    }

    /// The registered name of this actor.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.pool.name
    }

    /// How many supervised restarts this actor has undergone (E14, observability §5).
    /// Always `0` for an unsupervised (`RestartPolicy::Never`) actor.
    #[must_use]
    pub fn restart_count(&self) -> u32 {
        self.pool.restart_count()
    }

    /// Stop the actor and await its task.
    pub async fn stop(&self) {
        self.pool.stop().await;
    }
}

/// A handle to a named pool of actors, routing messages across its instances.
/// Cheap to clone.
#[derive(Clone)]
pub struct PoolRef<A: UserActor> {
    pool: Arc<PoolInner<A>>,
}

impl<A: UserActor> std::fmt::Debug for PoolRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolRef")
            .field("name", &self.pool.name)
            .field("instances", &self.pool.len())
            .finish()
    }
}

impl<A: UserActor> PoolRef<A> {
    /// Deliver a message to the next instance (round-robin).
    ///
    /// # Errors
    /// Returns [`SendError`] if the pool has no live instances.
    pub fn send(&self, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_rr(msg)
    }

    /// Deliver a message to the instance chosen by hashing `key`, so all
    /// messages for the same key reach the same instance (stable within a run;
    /// true consistent hashing across nodes arrives with E8/cluster-routing).
    ///
    /// # Errors
    /// Returns [`SendError`] if the pool has no live instances.
    pub fn send_keyed<K: Hash>(&self, key: &K, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_keyed(crate::ring::hash_key(key), msg)
    }

    /// Ask the next instance (round-robin). See [`ActorRef::ask`].
    ///
    /// # Errors
    /// Returns [`AskError`] if the message cannot be delivered or is dropped.
    pub async fn ask<R, F>(&self, build: F) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnOnce(RpcReplyPort<R>) -> A::Message,
    {
        let (tx, rx) = oneshot::channel();
        self.pool.send_rr(build(RpcReplyPort::local(tx)))?;
        match tokio::time::timeout(ASK_TIMEOUT, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(AskError::NoReply),
            Err(_) => Err(AskError::Timeout(ASK_TIMEOUT)),
        }
    }

    /// Number of live instances.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pool.len()
    }

    /// Whether the pool has no live instances.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pool.len() == 0
    }

    /// The registered name of this pool.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.pool.name
    }

    /// The instance ids currently live in this pool (ascending).
    #[must_use]
    pub fn instance_ids(&self) -> Vec<u32> {
        self.pool.instance_ids()
    }

    /// Stop every instance and await their tasks.
    pub async fn stop(&self) {
        self.pool.stop().await;
    }
}
// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

struct GroupEntry {
    /// `Arc<PoolInner<A>>` erased for typed downcast in `pool`/`get`/`scale`.
    handle: Arc<dyn Any + Send + Sync>,
    /// The same pool, erased for type-agnostic lifecycle/inspection.
    lifecycle: Arc<dyn GroupLifecycle>,
    /// The same pool, erased for cross-node byte delivery (E8).
    wire: Arc<dyn WireIngress>,
}

/// How the registry places worker instances on this node (one-worker-per-vps).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementMode {
    /// Production default: at most **one** worker per node per name. Scale out
    /// by adding VPSes, not by stacking workers on one machine.
    Production,
    /// Development (`--dev-multi-workers` / `RAFT_DEV_MULTI_WORKERS=1`): multiple
    /// local instances per name are permitted, at the user's responsibility.
    DevelopmentMulti,
}

/// A node-local registry of named user actors and pools (backlog E6).
///
/// Clone it freely — every clone shares the same underlying registry.
#[derive(Clone)]
pub struct ActorRegistry {
    groups: Arc<Mutex<HashMap<String, GroupEntry>>>,
    dev_multi_workers: bool,
    /// Shared with every spawned pool so a later-installed observer still fires.
    observer: ObserverHook,
}

impl Default for ActorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorRegistry {
    /// Create a production registry: at most one instance per name (one-worker-per-vps).
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            dev_multi_workers: false,
            observer: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a development registry that permits local pools / `scale_local`
    /// with more than one instance (`--dev-multi-workers`, one-worker-per-vps).
    #[must_use]
    pub fn new_dev() -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            dev_multi_workers: true,
            observer: Arc::new(Mutex::new(None)),
        }
    }

    /// Install an [`ActorObserver`] to receive lifecycle + per-message telemetry
    /// (Track H). Install *before spawning actors* (the facade does this at build
    /// time): each instance task binds the observer once at launch, so an
    /// observer set after a spawn does not retroactively attach to it.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub fn set_observer(&self, observer: Arc<dyn ActorObserver>) {
        *self.observer.lock().unwrap() = Some(observer);
    }

    /// Snapshot per-group runtime counters for metrics sampling (Track H). One
    /// entry per registered group; cumulative fields are monotonic.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn stats(&self) -> Vec<ActorGroupStats> {
        let groups = self.groups.lock().unwrap();
        groups
            .iter()
            .map(|(name, entry)| {
                let (instances, messages, handle_nanos, mailbox_depth) =
                    entry.lifecycle.runtime_stats();
                ActorGroupStats {
                    name: name.clone(),
                    instances,
                    messages,
                    handle_nanos,
                    mailbox_depth,
                }
            })
            .collect()
    }

    /// Whether local multi-instance pools are permitted.
    #[must_use]
    pub fn dev_multi_workers(&self) -> bool {
        self.dev_multi_workers
    }

    /// The registry's placement mode (one-worker-per-vps). Production enforces one worker
    /// per node per name; development permits multiple local instances.
    #[must_use]
    pub fn placement_mode(&self) -> PlacementMode {
        if self.dev_multi_workers {
            PlacementMode::DevelopmentMulti
        } else {
            PlacementMode::Production
        }
    }

    /// Names of all registered actor groups.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.groups.lock().unwrap().keys().cloned().collect()
    }

    /// Whether a group with `name` exists.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.groups.lock().unwrap().contains_key(name)
    }

    /// Spawn a single named actor (a singleton).
    ///
    /// # Errors
    /// Returns [`SpawnError::NameExists`] if `name` is taken or
    /// [`SpawnError::Start`] if the actor fails to initialize.
    pub fn spawn<A: UserActor>(
        &self,
        name: &str,
        config: A::Config,
    ) -> Result<ActorRef<A>, SpawnError> {
        self.reserve(name)?;
        let pool = PoolInner::<A>::new(name, self.observer.clone());
        if let Err(e) = pool.spawn_instance(config) {
            return Err(SpawnError::Start(Box::new(e)));
        }
        self.insert(name, &pool);
        Ok(ActorRef { pool })
    }

    /// Spawn a supervised singleton whose handler errors are governed by
    /// `policy` (E14, observability §5). On a supervised restart the actor is rebuilt
    /// with [`UserActor::start`] from `config`, so a supervised `Config` must be
    /// `Clone`. Read the running restart tally via [`ActorRef::restart_count`].
    ///
    /// # Errors
    /// Returns [`SpawnError::NameExists`] if `name` is taken or
    /// [`SpawnError::Start`] if the actor fails to initialize.
    pub fn spawn_supervised<A: UserActor>(
        &self,
        name: &str,
        config: A::Config,
        policy: RestartPolicy,
    ) -> Result<ActorRef<A>, SpawnError>
    where
        A::Config: Clone,
    {
        self.reserve(name)?;
        let pool = PoolInner::<A>::new(name, self.observer.clone());
        if let Err(e) = pool.spawn_instance_supervised(config, policy) {
            return Err(SpawnError::Start(Box::new(e)));
        }
        self.insert(name, &pool);
        Ok(ActorRef { pool })
    }

    /// Spawn a single named actor and restore migratable state into it from a
    /// snapshot before it handles any message (E12, cross-node-actors). Used by the
    /// `/actor/migrate` target side.
    ///
    /// # Errors
    /// Returns [`SpawnError::NameExists`] if `name` is taken, [`SpawnError::Start`]
    /// if the actor fails to initialize, or [`SpawnError::Restore`] if the
    /// snapshot cannot be applied.
    pub fn spawn_restoring<A: UserActor>(
        &self,
        name: &str,
        config: A::Config,
        snapshot: &[u8],
    ) -> Result<ActorRef<A>, SpawnError> {
        self.reserve(name)?;
        let pool = PoolInner::<A>::new(name, self.observer.clone());
        pool.spawn_instance_restoring(config, snapshot)?;
        self.insert(name, &pool);
        Ok(ActorRef { pool })
    }

    /// Spawn a pool of `count` identical actors under `name`.
    ///
    /// # Errors
    /// Returns [`SpawnError::ZeroCount`] for `count == 0`,
    /// [`SpawnError::MultiWorkerDisabled`] for `count > 1` in production,
    /// [`SpawnError::NameExists`] if `name` is taken, or [`SpawnError::Start`]
    /// if an instance fails to initialize.
    pub fn spawn_pool<A: UserActor>(
        &self,
        name: &str,
        count: usize,
        config: A::Config,
    ) -> Result<PoolRef<A>, SpawnError>
    where
        A::Config: Clone,
    {
        if count == 0 {
            return Err(SpawnError::ZeroCount);
        }
        if count > 1 && !self.dev_multi_workers {
            return Err(SpawnError::MultiWorkerDisabled { count });
        }
        self.reserve(name)?;
        let pool = PoolInner::<A>::new(name, self.observer.clone());
        for _ in 0..count {
            if let Err(e) = pool.spawn_instance(config.clone()) {
                pool.signal_stop();
                return Err(SpawnError::Start(Box::new(e)));
            }
        }
        self.insert(name, &pool);
        Ok(PoolRef { pool })
    }

    /// Grow or shrink the pool `name` to exactly `count` instances.
    ///
    /// # Errors
    /// Returns [`ScaleError::NotFound`] / [`ScaleError::TypeMismatch`] if the
    /// group is missing or a different type, [`ScaleError::ZeroCount`] for
    /// `count == 0`, [`ScaleError::MultiWorkerDisabled`] for `count > 1` in
    /// production, or [`ScaleError::Start`] if a new instance fails to start.
    pub async fn scale_local<A: UserActor>(
        &self,
        name: &str,
        count: usize,
        config: A::Config,
    ) -> Result<PoolRef<A>, ScaleError>
    where
        A::Config: Clone,
    {
        if count == 0 {
            return Err(ScaleError::ZeroCount);
        }
        if count > 1 && !self.dev_multi_workers {
            return Err(ScaleError::MultiWorkerDisabled { count });
        }
        let pool = self.lookup::<A>(name)?;
        pool.scale_to(count, &config)
            .await
            .map_err(|e| ScaleError::Start(Box::new(e)))?;
        Ok(PoolRef { pool })
    }

    /// Get a handle to the singleton actor `name`, if registered as `A`.
    #[must_use]
    pub fn get<A: UserActor>(&self, name: &str) -> Option<ActorRef<A>> {
        self.downcast::<A>(name).map(|pool| ActorRef { pool })
    }

    /// Get a handle to the pool `name`, if registered as `A`.
    #[must_use]
    pub fn pool<A: UserActor>(&self, name: &str) -> Option<PoolRef<A>> {
        self.downcast::<A>(name).map(|pool| PoolRef { pool })
    }

    /// Stop and remove the actor group `name`.
    ///
    /// The instances are signalled to stop and dropped from the roster; their
    /// tasks wind down asynchronously (graceful drain-with-timeout is E12). To
    /// await a specific group's tasks, use [`ActorRef::stop`] / [`PoolRef::stop`].
    ///
    /// # Errors
    /// Returns [`StopError::NotFound`] if no such group exists.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub fn stop(&self, name: &str) -> Result<(), StopError> {
        let entry = self
            .groups
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| StopError::NotFound(name.to_string()))?;
        entry.lifecycle.signal_stop();
        Ok(())
    }

    /// Gracefully stop and remove the actor group `name`: reject new messages,
    /// let queued and in-flight work finish, and force-stop anything still
    /// running when `default_timeout` elapses (E12, drain-timeout). Uses the
    /// group's per-actor override when set via [`Self::set_group_drain_timeout`].
    ///
    /// # Errors
    /// Returns [`StopError::NotFound`] if no such group exists.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub async fn stop_graceful(
        &self,
        name: &str,
        default_timeout: Duration,
    ) -> Result<DrainOutcome, StopError> {
        let entry = self
            .groups
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| StopError::NotFound(name.to_string()))?;
        let timeout = entry.lifecycle.drain_timeout().unwrap_or(default_timeout);
        Ok(entry.lifecycle.drain(timeout).await)
    }

    /// Override the graceful-drain timeout for group `name` (per-actor drain).
    ///
    /// # Errors
    /// Returns [`StopError::NotFound`] if no such group exists.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub fn set_group_drain_timeout(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> Result<(), StopError> {
        let groups = self.groups.lock().unwrap();
        let entry = groups
            .get(name)
            .ok_or_else(|| StopError::NotFound(name.to_string()))?;
        entry.lifecycle.set_drain_timeout(timeout);
        Ok(())
    }

    /// Effective drain timeout for `name`, if a per-group override is set.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn group_drain_timeout(&self, name: &str) -> Option<Duration> {
        let groups = self.groups.lock().unwrap();
        groups.get(name).and_then(|e| e.lifecycle.drain_timeout())
    }

    /// Gracefully stop and remove the actor group `name` (deprecated alias).
    ///
    /// # Errors
    /// Returns [`StopError::NotFound`] if no such group exists.
    pub async fn stop_graceful_with_timeout(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<DrainOutcome, StopError> {
        self.stop_graceful(name, timeout).await
    }

    /// Capture a migration snapshot from instance `instance` of local group
    /// `name` by asking the live actor (E12, cross-node-actors). The request is ordered
    /// after any already-queued messages.
    ///
    /// # Errors
    /// Returns [`SnapshotError`] if the group / instance is gone or the actor
    /// fails to produce a snapshot.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub async fn snapshot_local(
        &self,
        name: &str,
        instance: u32,
    ) -> Result<Vec<u8>, SnapshotError> {
        let lifecycle = {
            let groups = self.groups.lock().unwrap();
            groups
                .get(name)
                .ok_or(SnapshotError::NoInstance(instance))?
                .lifecycle
                .clone()
        };
        lifecycle.snapshot(instance).await
    }

    /// Number of live instances in group `name` (0 if absent).
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn instance_count(&self, name: &str) -> usize {
        self.groups
            .lock()
            .unwrap()
            .get(name)
            .map_or(0, |e| e.lifecycle.instance_count())
    }

    /// Snapshot every locally-hosted actor instance as an [`ActorRegistration`]
    /// owned by `node_id`, for publication into the cluster directory (E7,
    /// cross-node-actors). Generation is `0` (bumped on respawn/migration in E12).
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn local_registrations(&self, node_id: NodeId) -> Vec<ActorRegistration> {
        let groups = self.groups.lock().unwrap();
        let mut out = Vec::new();
        for (name, entry) in groups.iter() {
            let actor_type = ActorTypeId(entry.lifecycle.type_name().to_string());
            let migratable = entry.lifecycle.migratable();
            for instance in entry.lifecycle.instance_ids() {
                out.push(ActorRegistration {
                    id: ActorId {
                        node: node_id,
                        name: name.clone(),
                        instance,
                        generation: 0,
                    },
                    actor_type: actor_type.clone(),
                    migratable,
                });
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Deliver a cross-node payload to instance `instance` of local group
    /// `name` (E8, cross-node-actors). The payload is decoded via the actor's
    /// [`UserActor::decode_message`] and enqueued on the target instance's
    /// mailbox. Called by the `/actor/deliver` handler.
    ///
    /// # Errors
    /// Returns [`DeliverError`] if the group is unknown, the actor is not
    /// remotely addressable, the payload cannot be decoded, or the instance is
    /// gone / closed.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub fn deliver_local(
        &self,
        name: &str,
        instance: u32,
        payload: &[u8],
    ) -> Result<(), DeliverError> {
        let wire = {
            let groups = self.groups.lock().unwrap();
            groups
                .get(name)
                .ok_or_else(|| DeliverError::NotFound(name.to_string()))?
                .wire
                .clone()
        };
        wire.deliver(instance, payload)
    }

    /// Deliver a cross-node **ask** to instance `instance` of local group
    /// `name` and return the channel its `postcard`-encoded reply will arrive
    /// on (E8, cross-node-actors, cluster-routing). The payload is decoded via
    /// [`UserActor::decode_ask`]. Called by the `/actor/deliver` handler when
    /// `reply_expected` is set.
    ///
    /// # Errors
    /// Returns [`DeliverError`] if the group is unknown, the actor does not
    /// support remote asks, the payload cannot be decoded, or the instance is
    /// gone / closed.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub fn deliver_local_ask(
        &self,
        name: &str,
        instance: u32,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<WireReply>, DeliverError> {
        let wire = {
            let groups = self.groups.lock().unwrap();
            groups
                .get(name)
                .ok_or_else(|| DeliverError::NotFound(name.to_string()))?
                .wire
                .clone()
        };
        wire.deliver_ask(instance, payload)
    }

    // ---- internals -------------------------------------------------------

    fn reserve(&self, name: &str) -> Result<(), SpawnError> {
        if self.groups.lock().unwrap().contains_key(name) {
            return Err(SpawnError::NameExists(name.to_string()));
        }
        Ok(())
    }

    fn insert<A: UserActor>(&self, name: &str, pool: &Arc<PoolInner<A>>) {
        let entry = GroupEntry {
            handle: pool.clone(),
            lifecycle: pool.clone(),
            wire: pool.clone(),
        };
        self.groups.lock().unwrap().insert(name.to_string(), entry);
    }

    fn downcast<A: UserActor>(&self, name: &str) -> Option<Arc<PoolInner<A>>> {
        let groups = self.groups.lock().unwrap();
        let entry = groups.get(name)?;
        entry.handle.clone().downcast::<PoolInner<A>>().ok()
    }

    fn lookup<A: UserActor>(&self, name: &str) -> Result<Arc<PoolInner<A>>, ScaleError> {
        let groups = self.groups.lock().unwrap();
        let entry = groups
            .get(name)
            .ok_or_else(|| ScaleError::NotFound(name.to_string()))?;
        let registered = entry.lifecycle.type_name();
        entry
            .handle
            .clone()
            .downcast::<PoolInner<A>>()
            .map_err(|_| ScaleError::TypeMismatch {
                name: name.to_string(),
                registered,
            })
    }
}
