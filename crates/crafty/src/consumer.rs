//! Queue consumer helpers ([`JobConsumer`], [`ConsumerOpts`], [`CraftyApp::spawn_consumer`]).

use std::sync::Arc;
use std::time::Duration;

use crafty_actor::{ActorStateStore, JobContext, StoreError, WorkerId, run_queue_consumer};

use crate::CraftyApp;

pub(crate) type ConsumerSpawnFn = Box<
    dyn FnOnce(Arc<CraftyApp>, tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()>
        + Send,
>;

/// Spawn one or more [`JobConsumer`] loops with a shared stop channel.
#[derive(Default)]
pub struct ConsumerGroup {
    streams: Vec<String>,
    spawners: Vec<ConsumerSpawnFn>,
}

impl ConsumerGroup {
    /// Empty group — chain [`.add`](Self::add) then [`.spawn`](Self::spawn).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a consumer (pass the generated unit struct for type inference).
    #[must_use]
    pub fn add<C: JobConsumer>(mut self, consumer: C, opts: ConsumerOpts) -> Self {
        self.streams.push(C::STREAM.to_string());
        self.spawners.push(Box::new(move |app, stop| {
            app.spawn_consumer(consumer, opts, stop)
        }));
        self
    }

    /// Split into stream names and spawn closures for [`crate::CraftyAppBuilder::consumers`].
    #[must_use]
    pub fn into_parts(self) -> (Vec<String>, Vec<ConsumerSpawnFn>) {
        (self.streams, self.spawners)
    }

    /// Start every registered consumer; returns `(stop_tx, join handles)`.
    pub fn spawn(
        self,
        app: &Arc<CraftyApp>,
    ) -> (
        tokio::sync::watch::Sender<bool>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let handles = self
            .spawners
            .into_iter()
            .map(|spawn| spawn(Arc::clone(app), stop_rx.clone()))
            .collect();
        (stop_tx, handles)
    }
}

/// Marker written before the handler runs.
const MARK_PROCESSING: &[u8] = b"processing";
/// Marker written after the handler succeeds, before the ack.
const MARK_DONE: &[u8] = b"done";

/// Derives the idempotency key for a delivery. `None` opts the job out of the guard.
pub type IdempotencyKeyFn =
    Arc<dyn Fn(&[u8], JobContext<'_>) -> Option<String> + Send + Sync + 'static>;

/// Effectively-once guard for a consumer — see
/// [background-jobs](../../../docs/scenarios/background-jobs.md#effectively-once-recipe).
///
/// This is a recipe wired up for you, not an exactly-once delivery mode: the queue
/// is still at-least-once, and the guarantee is only as good as the store and the key.
#[derive(Clone)]
pub struct IdempotencyOpts {
    store: Arc<dyn ActorStateStore>,
    prefix: String,
    key_fn: IdempotencyKeyFn,
    ttl: Option<Duration>,
}

impl std::fmt::Debug for IdempotencyOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdempotencyOpts")
            .field("prefix", &self.prefix)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl IdempotencyOpts {
    /// Guard deliveries with markers in `store`, keyed by `prefix` + `key_fn`.
    ///
    /// `key_fn` should derive the key from the *business event* (`order-4711:charge`),
    /// not from payload bytes. Returning `None` runs the handler unguarded.
    #[must_use]
    pub fn new(
        store: Arc<dyn ActorStateStore>,
        prefix: impl Into<String>,
        key_fn: impl Fn(&[u8], JobContext<'_>) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            key_fn: Arc::new(key_fn),
            ttl: None,
        }
    }

    /// Expire markers after `ttl`.
    ///
    /// A marker that outlives every possible redelivery is dead weight; one that
    /// expires *before* the last redelivery silently reopens the duplicate window.
    /// Size it against the lease timeout and attempt ceiling.
    #[must_use]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Key a delivery by [`JobContext::dedup_key`], falling back to the job id.
    ///
    /// Convenience for the common case where the enqueue side already set a
    /// `dedup_key`.
    #[must_use]
    pub fn by_dedup_key(store: Arc<dyn ActorStateStore>, prefix: impl Into<String>) -> Self {
        Self::new(store, prefix, |_payload, ctx| {
            ctx.dedup_key
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .or_else(|| Some(format!("job-{}", ctx.job_id.0)))
        })
    }
}

/// Options for [`CraftyApp::spawn_consumer`].
#[derive(Debug, Clone)]
pub struct ConsumerOpts {
    /// Worker instance id on this node (distinct consumers on the same stream).
    pub instance: u32,
    /// Maximum jobs leased per poll ([`run_queue_consumer`] batch size).
    pub batch: usize,
    /// Sleep between polls when the queue is empty.
    pub idle_sleep: Duration,
    /// Optional effectively-once guard around the handler.
    pub idempotency: Option<IdempotencyOpts>,
}

impl Default for ConsumerOpts {
    fn default() -> Self {
        Self {
            instance: 0,
            batch: 1,
            idle_sleep: Duration::from_millis(100),
            idempotency: None,
        }
    }
}

impl ConsumerOpts {
    /// Guard this consumer with the effectively-once recipe.
    #[must_use]
    pub fn idempotency(mut self, opts: IdempotencyOpts) -> Self {
        self.idempotency = Some(opts);
        self
    }
}

/// Why a delivery was not acked. Any variant nacks the job for redelivery.
enum ConsumeError<E> {
    /// The user handler rejected the payload.
    Handler(E),
    /// The idempotency store was unreachable — retry rather than risk a duplicate.
    Store(StoreError),
    /// Another worker holds the `processing` marker for this key.
    Contended,
}

impl<E: std::fmt::Debug> std::fmt::Debug for ConsumeError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handler(e) => write!(f, "handler error: {e:?}"),
            Self::Store(e) => write!(f, "idempotency store error: {e:?}"),
            Self::Contended => write!(f, "job already being processed by another worker"),
        }
    }
}

/// Handler registered via [`macro@crate::consumer`].
///
/// Implementations are generated by the attribute macro; spawn with
/// [`CraftyApp::spawn_consumer`].
pub trait JobConsumer: Send + Sync + 'static {
    /// Job stream name passed to `#[consumer("…")]`.
    const STREAM: &'static str;

    /// Error returned when the handler rejects a payload (job is nacked).
    type Error: std::fmt::Debug + Send + 'static;

    /// Process one leased job.
    ///
    /// `ctx` carries what is known about *this delivery* — notably
    /// [`JobContext::attempts`] and [`JobContext::is_redelivery`], since delivery is
    /// at-least-once. Handlers written as `async fn f(payload: &[u8])` ignore it;
    /// declare a second argument to receive it.
    fn handle(
        payload: &[u8],
        ctx: JobContext<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl CraftyApp {
    /// Spawn a background task that leases from `C::STREAM`, invokes the handler, and ack/nacks.
    ///
    /// Requires the stream to be registered via [`crate::CraftyAppBuilder::queue`].
    /// Pass a [`tokio::sync::watch`] stop receiver to shut the loop down cleanly.
    /// The `_consumer` value is only used for type inference (see [`macro@crate::consumer`]).
    ///
    /// # Panics
    /// If `C::STREAM` was not registered on the cluster.
    pub fn spawn_consumer<C: JobConsumer>(
        self: &Arc<Self>,
        _consumer: C,
        opts: ConsumerOpts,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let app = Arc::clone(self);
        let ConsumerOpts {
            instance,
            batch,
            idle_sleep,
            idempotency,
        } = opts;
        tokio::spawn(async move {
            let queue = app
                .job_queue(C::STREAM)
                .expect("consumer stream must be registered via .queue");
            let worker = WorkerId {
                node: app.node_id(),
                instance,
            };
            run_queue_consumer(queue, worker, batch, idle_sleep, stop, |job| {
                let payload = job.payload.clone();
                let dedup_key = job.dedup_key.clone();
                let job_id = job.job_id;
                let lease_id = job.lease_id;
                let attempts = job.attempts;
                let idem = idempotency.clone();
                async move {
                    let ctx = JobContext {
                        job_id,
                        lease_id,
                        stream: C::STREAM,
                        attempts,
                        dedup_key: dedup_key.as_deref(),
                    };
                    run_guarded::<C>(&payload, ctx, idem.as_ref()).await
                }
            })
            .await;
        })
    }
}

/// Run `C::handle`, wrapped in the effectively-once guard when one is configured.
///
/// Order is load-bearing: check `done` → claim `processing` → handler → mark `done`
/// → ack. The `done` mark lands *before* the ack, so a crash in the redelivery
/// window is caught by the first check next time round.
async fn run_guarded<C: JobConsumer>(
    payload: &[u8],
    ctx: JobContext<'_>,
    idem: Option<&IdempotencyOpts>,
) -> Result<(), ConsumeError<C::Error>> {
    let Some(idem) = idem else {
        return C::handle(payload, ctx).await.map_err(ConsumeError::Handler);
    };
    let Some(suffix) = (idem.key_fn)(payload, ctx) else {
        // No key for this job — nothing to guard against.
        return C::handle(payload, ctx).await.map_err(ConsumeError::Handler);
    };
    let key = format!("{}{suffix}", idem.prefix);

    match idem.store.get(&key).await {
        Ok(Some(mark)) if mark == MARK_DONE => {
            // Redelivery of a job whose side effect already landed — ack it.
            tracing::debug!(stream = C::STREAM, key = %key, "skipping duplicate delivery");
            return Ok(());
        }
        Ok(_) => {}
        Err(e) => return Err(ConsumeError::Store(e)),
    }

    // Claim the key. Absent → processing; a stale `processing` from a dead worker
    // is retaken once its marker TTL lapses.
    let claimed = idem
        .store
        .compare_and_set(&key, None, MARK_PROCESSING, idem.ttl)
        .await
        .map_err(ConsumeError::Store)?;
    if !claimed {
        match idem.store.get(&key).await {
            Ok(Some(mark)) if mark == MARK_DONE => return Ok(()),
            Ok(_) => return Err(ConsumeError::Contended),
            Err(e) => return Err(ConsumeError::Store(e)),
        }
    }

    C::handle(payload, ctx)
        .await
        .map_err(ConsumeError::Handler)?;

    // Durable before the ack.
    idem.store
        .set(&key, MARK_DONE, idem.ttl)
        .await
        .map_err(ConsumeError::Store)?;
    Ok(())
}
