//! `ActorRegistry` — local actor spawn / pool / scale / stop (backlog E6,
//! [ADR 012](../../../docs/decisions/012-elastic-cluster.md),
//! [ADR 014](../../../docs/decisions/014-one-worker-per-vps.md)).
//!
//! This is the **local** half of the actor fabric: named singletons and pools
//! of user actors running on the node, with round-robin and keyed message
//! routing. Cross-node addressing, the cluster directory, and remote
//! spawn/scale (ADR 013, ADR 019) layer on top of these primitives in later
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
//! ## Production vs development (ADR 014)
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
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

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

    /// Called once after the mailbox closes (stop or scale-in), for cleanup.
    fn stopped(&mut self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}

/// A one-shot reply channel embedded in a message to implement "ask"
/// (request/response). The handler calls [`reply`](RpcReplyPort::reply) with
/// the response; the caller awaits it via [`ActorRef::ask`] / [`PoolRef::ask`].
#[derive(Debug)]
pub struct RpcReplyPort<R> {
    tx: oneshot::Sender<R>,
}

impl<R> RpcReplyPort<R> {
    /// Send the response back to the asker. Returns `Err(value)` if the caller
    /// already gave up (dropped the pending `ask`).
    ///
    /// # Errors
    /// Returns the unsent `value` if the receiving `ask` was dropped.
    pub fn reply(self, value: R) -> Result<(), R> {
        self.tx.send(value)
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
    /// A pool count `> 1` was requested outside development mode (ADR 014).
    #[error("pools with count > 1 require development mode (--dev-multi-workers); got {count}")]
    DevModeRequired {
        /// The rejected instance count.
        count: usize,
    },
    /// The requested instance count was zero.
    #[error("instance count must be at least 1")]
    ZeroCount,
    /// [`UserActor::start`] failed while constructing an instance.
    #[error("actor start failed: {0}")]
    Start(Box<dyn std::error::Error + Send + Sync>),
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
    /// A count `> 1` was requested outside development mode (ADR 014).
    #[error("scaling above 1 requires development mode (--dev-multi-workers); got {count}")]
    DevModeRequired {
        /// The rejected instance count.
        count: usize,
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
}

// ---------------------------------------------------------------------------
// Instance + pool internals
// ---------------------------------------------------------------------------

/// A single running actor instance within a named group.
struct Instance<A: UserActor> {
    instance: u32,
    tx: mpsc::UnboundedSender<A::Message>,
    join: JoinHandle<()>,
}

/// The shared state of a named actor group (one instance = a singleton).
struct PoolInner<A: UserActor> {
    name: String,
    instances: Mutex<Vec<Instance<A>>>,
    /// Round-robin cursor for `send`.
    rr: AtomicUsize,
    /// Monotonic instance-id allocator (never reused within a group).
    next_instance: AtomicU32,
    /// Group-wide stop signal; flipping it to `true` ends every instance task.
    stop: watch::Sender<bool>,
}

impl<A: UserActor> PoolInner<A> {
    fn new(name: &str) -> Arc<Self> {
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            name: name.to_string(),
            instances: Mutex::new(Vec::new()),
            rr: AtomicUsize::new(0),
            next_instance: AtomicU32::new(0),
            stop,
        })
    }

    /// Start one instance and register it. On failure nothing is registered.
    fn spawn_instance(self: &Arc<Self>, config: A::Config) -> Result<u32, A::Error> {
        let mut state = A::start(config)?;
        let instance = self.next_instance.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel::<A::Message>();
        let mut stop_rx = self.stop.subscribe();
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        // Group dropped (Err) or stop signalled (true) → drain out.
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    maybe = rx.recv() => match maybe {
                        Some(msg) => {
                            // Handler errors keep the actor alive (supervision
                            // policies arrive with E14); the result is dropped.
                            let _ = state.handle(msg).await;
                        }
                        None => break, // all senders dropped (scaled in)
                    }
                }
            }
            state.stopped().await;
        });
        self.instances
            .lock()
            .unwrap()
            .push(Instance { instance, tx, join });
        Ok(instance)
    }

    /// A clone of the round-robin-selected instance's sender.
    fn pick_rr(&self) -> Option<mpsc::UnboundedSender<A::Message>> {
        let instances = self.instances.lock().unwrap();
        if instances.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % instances.len();
        Some(instances[i].tx.clone())
    }

    /// A clone of the instance selected by hashing `key` (stable within a run).
    fn pick_keyed(&self, key: u64) -> Option<mpsc::UnboundedSender<A::Message>> {
        let instances = self.instances.lock().unwrap();
        if instances.is_empty() {
            return None;
        }
        let i = (key % instances.len() as u64) as usize;
        Some(instances[i].tx.clone())
    }

    fn send_rr(&self, msg: A::Message) -> Result<(), SendError> {
        let tx = self.pick_rr().ok_or(SendError::NoInstances)?;
        tx.send(msg).map_err(|_| SendError::Closed)
    }

    fn send_keyed(&self, key: u64, msg: A::Message) -> Result<(), SendError> {
        let tx = self.pick_keyed(key).ok_or(SendError::NoInstances)?;
        tx.send(msg).map_err(|_| SendError::Closed)
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
    fn type_name(&self) -> &'static str;
    fn signal_stop(&self);
}

impl<A: UserActor> GroupLifecycle for PoolInner<A> {
    fn instance_count(&self) -> usize {
        self.len()
    }
    fn type_name(&self) -> &'static str {
        std::any::type_name::<A>()
    }
    fn signal_stop(&self) {
        PoolInner::signal_stop(self);
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
        self.pool.send_rr(build(RpcReplyPort { tx }))?;
        rx.await.map_err(|_| AskError::NoReply)
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
    /// true consistent hashing across nodes arrives with E8/ADR 019).
    ///
    /// # Errors
    /// Returns [`SendError`] if the pool has no live instances.
    pub fn send_keyed<K: Hash>(&self, key: &K, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_keyed(hash_key(key), msg)
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
        self.pool.send_rr(build(RpcReplyPort { tx }))?;
        rx.await.map_err(|_| AskError::NoReply)
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

fn hash_key<K: Hash>(key: &K) -> u64 {
    // `DefaultHasher::new()` is seeded deterministically (unlike `RandomState`),
    // so keyed routing is stable across the process.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

struct GroupEntry {
    /// `Arc<PoolInner<A>>` erased for typed downcast in `pool`/`get`/`scale`.
    handle: Arc<dyn Any + Send + Sync>,
    /// The same pool, erased for type-agnostic lifecycle/inspection.
    lifecycle: Arc<dyn GroupLifecycle>,
}

/// A node-local registry of named user actors and pools (backlog E6).
///
/// Clone it freely — every clone shares the same underlying registry.
#[derive(Clone)]
pub struct ActorRegistry {
    groups: Arc<Mutex<HashMap<String, GroupEntry>>>,
    dev_multi_workers: bool,
}

impl Default for ActorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorRegistry {
    /// Create a production registry: at most one instance per name (ADR 014).
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            dev_multi_workers: false,
        }
    }

    /// Create a development registry that permits local pools / `scale_local`
    /// with more than one instance (`--dev-multi-workers`, ADR 014).
    #[must_use]
    pub fn new_dev() -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            dev_multi_workers: true,
        }
    }

    /// Whether local multi-instance pools are permitted.
    #[must_use]
    pub fn dev_multi_workers(&self) -> bool {
        self.dev_multi_workers
    }

    /// Names of all registered actor groups.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.groups.lock().unwrap().keys().cloned().collect()
    }

    /// Whether a group with `name` exists.
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
        let pool = PoolInner::<A>::new(name);
        if let Err(e) = pool.spawn_instance(config) {
            return Err(SpawnError::Start(Box::new(e)));
        }
        self.insert(name, &pool);
        Ok(ActorRef { pool })
    }

    /// Spawn a pool of `count` identical actors under `name`.
    ///
    /// # Errors
    /// Returns [`SpawnError::ZeroCount`] for `count == 0`,
    /// [`SpawnError::DevModeRequired`] for `count > 1` in production,
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
            return Err(SpawnError::DevModeRequired { count });
        }
        self.reserve(name)?;
        let pool = PoolInner::<A>::new(name);
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
    /// `count == 0`, [`ScaleError::DevModeRequired`] for `count > 1` in
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
            return Err(ScaleError::DevModeRequired { count });
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

    /// Number of live instances in group `name` (0 if absent).
    #[must_use]
    pub fn instance_count(&self, name: &str) -> usize {
        self.groups
            .lock()
            .unwrap()
            .get(name)
            .map_or(0, |e| e.lifecycle.instance_count())
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
