use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use trembita_net::transport::BoxFuture;
use trembita_proto::{ActorId, ActorRegistration, ActorTypeId, NodeId};

use super::actor::UserActor;
use super::errors::{
    AskError, ConfigCodecError, DeliverError, DrainOutcome, MessageDecodeError, MigrationError,
    RestartPolicy, ScaleError, SendError, SnapshotError, SpawnError, StopError,
};
use super::lifecycle::{GroupLifecycle, WireIngress};
use super::observer::{
    ActorGroupStats, ActorObserver, ComputeTokenHook, LocalActorIntrospection, ObserverHook,
    mailbox_depth_u64,
};
use super::pool::PoolInner;
use super::refs::{ActorRef, PoolRef};
use super::reply::{WireReply, WireReplyPort};
use super::{ASK_TIMEOUT, DEFAULT_DRAIN_TIMEOUT};

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
    /// Optional compute pool for typed [`ActorRef::ask`] (workload governor).
    compute_tokens: ComputeTokenHook,
    /// Previous cumulative message counts for per-group rate derivation.
    message_rate_sampler: Arc<Mutex<HashMap<String, (u64, Instant)>>>,
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
            compute_tokens: Arc::new(Mutex::new(None)),
            message_rate_sampler: Arc::new(Mutex::new(HashMap::new())),
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
            compute_tokens: Arc::new(Mutex::new(None)),
            message_rate_sampler: Arc::new(Mutex::new(HashMap::new())),
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

    /// Attach the process-wide compute token pool for typed [`ActorRef::ask`].
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    pub fn set_compute_tokens(&self, pool: Arc<crate::ComputeTokenPool>) {
        *self.compute_tokens.lock().unwrap() = Some(pool);
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

    /// Derive per-group message rates (messages/s) from cumulative counters.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn group_message_rates(&self) -> HashMap<String, f64> {
        let stats = self.stats();
        let now = Instant::now();
        let mut sampler = self.message_rate_sampler.lock().unwrap();
        stats
            .iter()
            .map(|stat| {
                let rate = sampler
                    .get(&stat.name)
                    .map_or(0.0, |(prev_messages, prev_at)| {
                        let elapsed = now.duration_since(*prev_at).as_secs_f64();
                        if elapsed > 0.0 {
                            #[allow(clippy::cast_precision_loss)]
                            {
                                stat.messages.saturating_sub(*prev_messages) as f64 / elapsed
                            }
                        } else {
                            0.0
                        }
                    });
                sampler.insert(stat.name.clone(), (stat.messages, now));
                (stat.name.clone(), rate)
            })
            .collect()
    }

    /// Snapshot locally hosted instances for Observer / dashboard introspection.
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn local_actor_introspection(&self) -> Vec<LocalActorIntrospection> {
        let groups = self.groups.lock().unwrap();
        groups
            .iter()
            .flat_map(|(name, entry)| {
                entry.lifecycle.instance_introspection().into_iter().map(
                    |(instance, uptime_secs, mailbox_depth)| LocalActorIntrospection {
                        name: name.clone(),
                        instance,
                        mailbox_depth,
                        uptime_secs,
                    },
                )
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
        Ok(ActorRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
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
        Ok(ActorRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
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
        Ok(ActorRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
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
        Ok(PoolRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
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
        Ok(PoolRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
    }

    /// Get a handle to the singleton actor `name`, if registered as `A`.
    #[must_use]
    pub fn get<A: UserActor>(&self, name: &str) -> Option<ActorRef<A>> {
        self.downcast::<A>(name).map(|pool| ActorRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
    }

    /// Get a handle to the pool `name`, if registered as `A`.
    #[must_use]
    pub fn pool<A: UserActor>(&self, name: &str) -> Option<PoolRef<A>> {
        self.downcast::<A>(name).map(|pool| PoolRef {
            pool,
            compute_tokens: Arc::clone(&self.compute_tokens),
        })
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
    /// When `group_rates` is `Some`, those per-group msg/s values are stamped on
    /// every instance (avoids a second sampler tick when the caller already sampled).
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn local_registrations(
        &self,
        node_id: NodeId,
        group_rates: Option<&HashMap<String, f64>>,
    ) -> Vec<ActorRegistration> {
        let group_rates = match group_rates {
            Some(rates) => rates.clone(),
            None => self.group_message_rates(),
        };
        let stats: HashMap<(String, u32), (u64, u64)> = self
            .local_actor_introspection()
            .into_iter()
            .map(|i| {
                (
                    (i.name, i.instance),
                    (mailbox_depth_u64(i.mailbox_depth), i.uptime_secs),
                )
            })
            .collect();
        let groups = self.groups.lock().unwrap();
        let mut out = Vec::new();
        for (name, entry) in groups.iter() {
            let actor_type = ActorTypeId(entry.lifecycle.type_name().to_string());
            let migratable = entry.lifecycle.migratable();
            let messages_per_sec = *group_rates.get(name).unwrap_or(&0.0);
            for instance in entry.lifecycle.instance_ids() {
                let (mailbox_depth, uptime_secs) = stats
                    .get(&(name.clone(), instance))
                    .copied()
                    .unwrap_or((0, 0));
                out.push(ActorRegistration {
                    id: ActorId {
                        node: node_id,
                        name: name.clone(),
                        instance,
                        generation: 0,
                    },
                    actor_type: actor_type.clone(),
                    migratable,
                    mailbox_depth,
                    uptime_secs,
                    messages_per_sec,
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
