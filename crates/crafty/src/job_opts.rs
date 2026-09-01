//! Declarative job registration for [`CraftyAppBuilder`](super::app::CraftyAppBuilder).

use std::time::Duration;

use crafty_actor::DEFAULT_QUEUE_PREFETCH;

use crate::consumer::{ConsumerOpts, ConsumerSpawnFn, JobConsumer};
use crate::queue_opts::QueueOpts;

/// One durable job stream with optional handler, scaling, and HTTP enqueue.
///
/// Combines [`.queue`](super::app::CraftyAppBuilder::queue) + [`.consumer`](super::app::CraftyAppBuilder::consumer)
/// (+ optional gateway `/jobs/*`) in a single declaration.
///
/// ```
/// # use std::time::Duration;
/// # use crafty::{CraftyApp, JobOpts, RunOpts, consumer};
/// #
/// # #[consumer("emails")]
/// # async fn send_email(_payload: &[u8]) -> Result<(), ()> { Ok(()) }
/// #
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// CraftyApp::builder()
///     .data_dir("/tmp/app")
///     .jobs([JobOpts::new("emails")
///         .lease(Duration::from_secs(300))
///         .consumer(SendEmailConsumer)
///         .instances(2)
///         .batch(4)
///         .http_enqueue(true)])
///     .run(RunOpts::default().with_wait_queue("emails"))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct JobOpts {
    name: String,
    lease: Duration,
    prefetch: usize,
    instances: u32,
    batch: usize,
    idle_sleep: Duration,
    http_enqueue: bool,
    spawners: Vec<ConsumerSpawnFn>,
    config_error: Option<String>,
}

impl std::fmt::Debug for JobOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobOpts")
            .field("name", &self.name)
            .field("lease", &self.lease)
            .field("prefetch", &self.prefetch)
            .field("instances", &self.instances)
            .field("batch", &self.batch)
            .field("idle_sleep", &self.idle_sleep)
            .field("http_enqueue", &self.http_enqueue)
            .field("consumers", &self.spawners.len())
            .field("config_error", &self.config_error)
            .finish()
    }
}

impl JobOpts {
    /// Register a job stream named `name` (creates `queue-{name}.redb` under `data_dir`).
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lease: Duration::from_secs(60),
            prefetch: DEFAULT_QUEUE_PREFETCH,
            instances: 1,
            batch: 1,
            idle_sleep: Duration::from_millis(100),
            http_enqueue: false,
            spawners: Vec::new(),
            config_error: None,
        }
    }

    /// Lease visibility timeout for workers holding jobs from this stream.
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Leader prefetch depth (`0` disables). Default: framework prefetch.
    #[must_use]
    pub fn prefetch(mut self, prefetch: usize) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Number of consumer loops on **this** node (`instance` 0..N-1).
    #[must_use]
    pub fn instances(mut self, count: u32) -> Self {
        self.instances = count.max(1);
        self
    }

    /// Max jobs leased per poll for each consumer instance.
    #[must_use]
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch.max(1);
        self
    }

    /// Sleep between polls when the queue is empty.
    #[must_use]
    pub fn idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.idle_sleep = idle_sleep;
        self
    }

    /// Mount `POST /jobs/{stream}` on the product gateway (`with_jobs_api`).
    #[must_use]
    pub fn http_enqueue(mut self, enabled: bool) -> Self {
        self.http_enqueue = enabled;
        self
    }

    /// [`JobConsumer`] from `#[consumer("…")]` — `C::STREAM` must match [`.new`](Self::new).
    ///
    /// Pass the generated unit type (e.g. `SendEmailConsumer`), not the async fn.
    #[must_use]
    pub fn consumer<C>(mut self, consumer: C) -> Self
    where
        C: JobConsumer + Clone + 'static,
    {
        if C::STREAM != self.name {
            self.config_error = Some(format!(
                "JobOpts::new({:?}).consumer(): stream mismatch — handler is registered for {:?}, expected {:?}",
                self.name, C::STREAM, self.name
            ));
            return self;
        }
        let instances = self.instances;
        let batch = self.batch;
        let idle_sleep = self.idle_sleep;
        for instance in 0..instances {
            let consumer = consumer.clone();
            let opts = ConsumerOpts {
                instance,
                batch,
                idle_sleep,
            };
            self.spawners.push(Box::new(move |app, stop| {
                app.spawn_consumer(consumer.clone(), opts, stop)
            }));
        }
        self
    }

    pub(crate) fn into_registration(self) -> JobRegistration {
        JobRegistration {
            queue: QueueOpts {
                name: self.name.clone(),
                lease: self.lease,
                prefetch: self.prefetch,
            },
            stream: self.name,
            spawners: self.spawners,
            http_enqueue: self.http_enqueue,
            config_error: self.config_error,
        }
    }
}

pub(crate) struct JobRegistration {
    pub queue: QueueOpts,
    pub stream: String,
    pub spawners: Vec<ConsumerSpawnFn>,
    pub http_enqueue: bool,
    pub config_error: Option<String>,
}
