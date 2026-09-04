use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::actor::UserActor;
use super::errors::{
    DeliverError, DrainOutcome, MigrationError, RestartPolicy, SendError, SnapshotError, SpawnError,
};
use super::observer::ObserverHook;

/// An item on an instance's serial mailbox: either a user message or a control
/// request to capture the actor's migration snapshot (E12).
pub(super) enum Mailbox<A: UserActor> {
    User(A::Message),
    Snapshot(oneshot::Sender<Result<Vec<u8>, MigrationError>>),
}

type Rebuild<A> = Box<dyn Fn() -> Option<A> + Send>;

fn make_rebuild<A: UserActor>(config: A::Config) -> Rebuild<A>
where
    A::Config: Clone,
{
    Box::new(move || A::start(config.clone()).ok())
}

#[allow(clippy::struct_field_names)]
struct Instance<A: UserActor> {
    instance: u32,
    tx: mpsc::UnboundedSender<Mailbox<A>>,
    join: JoinHandle<()>,
    spawned_at: Instant,
    /// Enqueued-but-unhandled messages for this instance (Track H / Observer).
    queued: Arc<AtomicI64>,
}

/// The shared state of a named actor group (one instance = a singleton).
pub(super) struct PoolInner<A: UserActor> {
    pub(super) name: String,
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
    pub(super) fn new(name: &str, observer: ObserverHook) -> Arc<Self> {
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

    pub(super) fn set_drain_timeout(&self, timeout: Option<Duration>) {
        *self.drain_timeout.lock().unwrap() = timeout;
    }

    pub(super) fn drain_timeout(&self) -> Option<Duration> {
        *self.drain_timeout.lock().unwrap()
    }

    /// Launch the mailbox task for an already-constructed `state`, register the
    /// instance, and return its id. Shared by fresh spawns and migration
    /// restores. `policy` + `rebuild` drive supervised restarts (E14); a plain
    /// spawn passes [`RestartPolicy::Never`] and `None`.
    #[allow(clippy::too_many_lines)] // spawn + mailbox wiring
    pub(super) fn launch(
        self: &Arc<Self>,
        mut state: A,
        policy: RestartPolicy,
        rebuild: Option<Rebuild<A>>,
    ) -> u32 {
        let instance = self.next_instance.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel::<Mailbox<A>>();
        let instance_queued = Arc::new(AtomicI64::new(0));
        let mut stop_rx = self.stop.subscribe();
        let restarts = Arc::clone(&self.restarts);
        let roster = Arc::clone(&self.instances);
        let messages = Arc::clone(&self.messages);
        let handle_nanos = Arc::clone(&self.handle_nanos);
        let group_queued = Arc::clone(&self.queued);
        let inst_queued = Arc::clone(&instance_queued);
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
                            group_queued.fetch_sub(1, Ordering::Relaxed);
                            inst_queued.fetch_sub(1, Ordering::Relaxed);
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
        let spawned_at = Instant::now();
        self.instances.lock().unwrap().push(Instance {
            instance,
            tx,
            join,
            spawned_at,
            queued: instance_queued,
        });
        instance
    }

    pub(super) fn instance_introspection(&self) -> Vec<(u32, u64, i64)> {
        let now = Instant::now();
        self.instances
            .lock()
            .unwrap()
            .iter()
            .map(|i| {
                (
                    i.instance,
                    now.duration_since(i.spawned_at).as_secs(),
                    i.queued.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// Start one instance and register it. On failure nothing is registered.
    pub(super) fn spawn_instance(self: &Arc<Self>, config: A::Config) -> Result<u32, A::Error> {
        let state = A::start(config)?;
        Ok(self.launch(state, RestartPolicy::Never, None))
    }

    /// Start one supervised instance whose handler errors are governed by
    /// `policy`, rebuilding fresh state from `config` on restart (E14).
    pub(super) fn spawn_instance_supervised(
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
    pub(super) fn spawn_instance_restoring(
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
    pub(super) fn restart_count(&self) -> u32 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub(super) fn runtime_stats(&self) -> (usize, u64, u64, i64) {
        (
            self.len(),
            self.messages.load(Ordering::Relaxed),
            self.handle_nanos.load(Ordering::Relaxed),
            self.queued.load(Ordering::Relaxed),
        )
    }

    /// A clone of the round-robin-selected instance's sender and depth counter.
    pub(super) fn pick_rr(&self) -> Option<(mpsc::UnboundedSender<Mailbox<A>>, Arc<AtomicI64>)> {
        let instances = self.instances.lock().unwrap();
        if instances.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % instances.len();
        Some((instances[i].tx.clone(), Arc::clone(&instances[i].queued)))
    }

    /// A clone of the instance selected by the consistent hash ring for `key`.
    pub(super) fn pick_keyed(
        &self,
        key: u64,
    ) -> Option<(mpsc::UnboundedSender<Mailbox<A>>, Arc<AtomicI64>)> {
        let instances = self.instances.lock().unwrap();
        if instances.is_empty() {
            return None;
        }
        let index =
            crate::ring::pick_index(key, instances.len(), crate::ring::group_salt(&self.name));
        Some((
            instances[index].tx.clone(),
            Arc::clone(&instances[index].queued),
        ))
    }

    pub(super) fn send_rr(&self, msg: A::Message) -> Result<(), SendError> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(SendError::Draining);
        }
        let (tx, queued) = self.pick_rr().ok_or(SendError::NoInstances)?;
        self.enqueue(&tx, &queued, msg)
            .map_err(|_| SendError::Closed)
    }

    pub(super) fn send_keyed(&self, key: u64, msg: A::Message) -> Result<(), SendError> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(SendError::Draining);
        }
        let (tx, queued) = self.pick_keyed(key).ok_or(SendError::NoInstances)?;
        self.enqueue(&tx, &queued, msg)
            .map_err(|_| SendError::Closed)
    }

    /// Enqueue a user message and bump the mailbox-depth gauges on success. The
    /// counters are decremented by the instance task when it dequeues (Track H).
    pub(super) fn enqueue(
        &self,
        tx: &mpsc::UnboundedSender<Mailbox<A>>,
        instance_queued: &Arc<AtomicI64>,
        msg: A::Message,
    ) -> Result<(), A::Message> {
        match tx.send(Mailbox::User(msg)) {
            Ok(()) => {
                self.queued.fetch_add(1, Ordering::Relaxed);
                instance_queued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::SendError(Mailbox::User(msg))) => Err(msg),
            Err(_) => unreachable!("enqueue only sends Mailbox::User"),
        }
    }

    /// Deliver to a specific instance id (used by cross-node delivery, which
    /// has already selected the target instance via the directory, E8).
    pub(super) fn send_to_instance(
        &self,
        instance: u32,
        msg: A::Message,
    ) -> Result<(), DeliverError> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(DeliverError::Draining);
        }
        let (tx, queued) = {
            let instances = self.instances.lock().unwrap();
            instances
                .iter()
                .find(|i| i.instance == instance)
                .map(|i| (i.tx.clone(), Arc::clone(&i.queued)))
                .ok_or(DeliverError::NoInstance(instance))?
        };
        self.enqueue(&tx, &queued, msg)
            .map_err(|_| DeliverError::Closed)
    }

    /// Capture instance `instance`'s migration snapshot. The request rides the
    /// serial mailbox, so it observes every message queued before it (E12).
    pub(super) async fn snapshot_instance(&self, instance: u32) -> Result<Vec<u8>, SnapshotError> {
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
    pub(super) async fn drain(&self, timeout: Duration) -> DrainOutcome {
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

    pub(super) fn len(&self) -> usize {
        self.instances.lock().unwrap().len()
    }

    /// The instance ids currently live in this group (ascending), for
    /// introspection and the forthcoming cross-node `ActorId` (E7).
    pub(super) fn instance_ids(&self) -> Vec<u32> {
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
    pub(super) fn signal_stop(&self) {
        let _ = self.stop.send(true);
        self.instances.lock().unwrap().clear();
    }

    /// Stop every instance and await their tasks (graceful drain).
    pub(super) async fn stop(&self) {
        let _ = self.stop.send(true);
        let drained: Vec<Instance<A>> = std::mem::take(&mut *self.instances.lock().unwrap());
        for inst in drained {
            let _ = inst.join.await;
        }
    }

    /// Grow or shrink to exactly `count` instances, cloning `config` for new
    /// ones. Awaits the tasks of any instances removed on shrink.
    pub(super) async fn scale_to(
        self: &Arc<Self>,
        count: usize,
        config: &A::Config,
    ) -> Result<(), A::Error>
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
