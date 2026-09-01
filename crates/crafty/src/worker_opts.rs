//! Declarative actor (worker) registration for [`CraftyAppBuilder`](super::app::CraftyAppBuilder).

use std::marker::PhantomData;
use std::time::Duration;

use crafty_actor::{AutoscalePolicy, UserActor};

use crate::app::CraftyAppBuilder;

/// How many instances of a worker actor group to run in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerScale {
    /// Fixed pool size cluster-wide ([`manage`](crate::cluster::CraftyClusterBuilder::manage)).
    Fixed(usize),
    /// One instance per live node ([`manage_auto`](crate::cluster::CraftyClusterBuilder::manage_auto)).
    PerNode,
    /// Queue-depth autoscale between `min` and `max` ([`job_queue_autoscale`](crate::cluster::CraftyClusterBuilder::job_queue_autoscale)).
    Auto {
        /// Minimum worker instances.
        min: usize,
        /// Maximum worker instances (also capped by reachable nodes).
        max: usize,
    },
}

/// One managed actor group with explicit scale and optional queue autoscale.
///
/// Combines [`.actors`](super::app::CraftyAppBuilder::actors) with optional
/// [`job_queue_autoscale`](crate::cluster::CraftyClusterBuilder::job_queue_autoscale).
///
/// Register several heterogeneous worker types via [`WorkerGroup`] or the [`workers!`](crate::workers) macro:
///
/// ```
/// # use crafty::{CraftyApp, RunOpts, WorkerGroup, WorkerOpts, WorkerScale, workers};
/// #
/// # struct OrderProcessor;
/// # impl crafty::actor::UserActor for OrderProcessor {
/// #     type Config = ();
/// #     type Message = ();
/// #     type Error = std::convert::Infallible;
/// #     fn start(_: Self::Config) -> Result<Self, Self::Error> { Ok(Self) }
/// #     fn handle(&mut self, _: Self::Message) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
/// #         std::future::ready(Ok(()))
/// #     }
/// # }
/// #
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// CraftyApp::builder()
///     .data_dir("/tmp/app")
///     .workers(workers![
///         WorkerOpts::<OrderProcessor>::new("orders")
///             .config(())
///             .scale(WorkerScale::Fixed(1)),
///     ])
///     .run(RunOpts::default())
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct WorkerOpts<A: UserActor> {
    name: String,
    config: Option<A::Config>,
    scale: WorkerScale,
    autoscale_from: Option<String>,
    autoscale_target_pending: u64,
    autoscale_cooldown: Duration,
    autoscale_poll: Duration,
    http_cast: bool,
    config_error: Option<String>,
    _actor: PhantomData<A>,
}

impl<A: UserActor> std::fmt::Debug for WorkerOpts<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerOpts")
            .field("name", &self.name)
            .field("config", &self.config.is_some())
            .field("scale", &self.scale)
            .field("autoscale_from", &self.autoscale_from)
            .field("autoscale_target_pending", &self.autoscale_target_pending)
            .field("autoscale_cooldown", &self.autoscale_cooldown)
            .field("autoscale_poll", &self.autoscale_poll)
            .field("http_cast", &self.http_cast)
            .field("config_error", &self.config_error)
            .finish()
    }
}

impl<A: UserActor> WorkerOpts<A> {
    /// Register a worker group named `name` (directory group + cast/ask routing key).
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        WorkerOpts {
            name: name.into(),
            config: None,
            scale: WorkerScale::PerNode,
            autoscale_from: None,
            autoscale_target_pending: 10,
            autoscale_cooldown: Duration::from_secs(30),
            autoscale_poll: Duration::from_secs(1),
            http_cast: false,
            config_error: None,
            _actor: PhantomData::<A>,
        }
    }

    /// Actor constructor config passed to [`UserActor::start`].
    #[must_use]
    pub fn config(mut self, config: A::Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Fixed pool, per-node, or queue-driven autoscale.
    #[must_use]
    pub fn scale(mut self, scale: WorkerScale) -> Self {
        self.scale = scale;
        self
    }

    /// Job stream that drives [`WorkerScale::Auto`] depth sampling (defaults to the worker group name).
    #[must_use]
    pub fn autoscale_from(mut self, stream: impl Into<String>) -> Self {
        self.autoscale_from = Some(stream.into());
        self
    }

    /// Target pending jobs per worker when using [`WorkerScale::Auto`].
    #[must_use]
    pub fn autoscale_target_pending(mut self, target: u64) -> Self {
        self.autoscale_target_pending = target.max(1);
        self
    }

    /// Minimum time between autoscale decisions.
    #[must_use]
    pub fn autoscale_cooldown(mut self, cooldown: Duration) -> Self {
        self.autoscale_cooldown = cooldown;
        self
    }

    /// How often queue depth is sampled for autoscale.
    #[must_use]
    pub fn autoscale_poll(mut self, poll: Duration) -> Self {
        self.autoscale_poll = poll;
        self
    }

    /// Mount cast/ask HTTP routes on the product gateway (`with_actors_api`).
    #[must_use]
    pub fn http_cast(mut self, enabled: bool) -> Self {
        self.http_cast = enabled;
        self
    }

    pub(crate) fn into_entry(self) -> WorkerEntry
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        if let Some(err) = self.config_error {
            return WorkerEntry {
                apply: Box::new(|mut builder| {
                    builder.config_errors.push(err);
                    builder
                }),
            };
        }

        let Some(config) = self.config else {
            let name = self.name.clone();
            return WorkerEntry {
                apply: Box::new(move |mut builder| {
                    builder.config_errors.push(format!(
                        "WorkerOpts::new({name:?}): call .config(...) before registering"
                    ));
                    builder
                }),
            };
        };

        let name = self.name.clone();
        let scale = self.scale;
        let autoscale_from = self.autoscale_from.unwrap_or_else(|| self.name.clone());
        let autoscale_target_pending = self.autoscale_target_pending;
        let autoscale_cooldown = self.autoscale_cooldown;
        let autoscale_poll = self.autoscale_poll;
        let http_cast = self.http_cast;

        WorkerEntry {
            apply: Box::new(move |mut builder: CraftyAppBuilder| {
                builder.registration.actors = true;
                if http_cast {
                    builder.gateway_api.actors = true;
                    if let Some(gateway) = builder.gateway.as_mut() {
                        gateway.actors_api = true;
                    }
                }
                match scale {
                    WorkerScale::Fixed(total) => {
                        builder.inner = builder.inner.manage::<A>(&name, total, config.clone());
                    }
                    WorkerScale::PerNode => {
                        builder.inner = builder.inner.manage_auto::<A>(&name, config.clone());
                    }
                    WorkerScale::Auto { min, max } => {
                        let min = min.max(1);
                        let max = max.max(min);
                        builder
                            .worker_autoscale_streams
                            .push(autoscale_from.clone());
                        builder.inner = builder.inner.manage::<A>(&name, min, config.clone());
                        let policy = AutoscalePolicy {
                            worker_group: name.clone(),
                            target_pending_per_worker: autoscale_target_pending,
                            min_workers: min,
                            max_workers: max,
                            cooldown: autoscale_cooldown,
                            poll_interval: autoscale_poll,
                        };
                        builder.inner = builder.inner.job_queue_autoscale::<A>(
                            &autoscale_from,
                            &policy,
                            config,
                        );
                    }
                }
                builder
            }),
        }
    }
}

pub(crate) type WorkerApplyFn = Box<dyn FnOnce(CraftyAppBuilder) -> CraftyAppBuilder + Send>;

pub(crate) struct WorkerEntry {
    pub apply: WorkerApplyFn,
}

/// Type-erased collection of [`WorkerOpts`] for [`.workers`](super::app::CraftyAppBuilder::workers).
#[derive(Default)]
pub struct WorkerGroup {
    entries: Vec<WorkerEntry>,
}

impl WorkerGroup {
    /// Empty group — chain [`.with_worker`](Self::with_worker) or use the [`workers!`](crate::workers) macro.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one worker actor group (heterogeneous types allowed).
    #[must_use]
    pub fn with_worker<A>(mut self, opts: WorkerOpts<A>) -> Self
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        self.entries.push(opts.into_entry());
        self
    }

    pub(crate) fn into_entries(self) -> Vec<WorkerEntry> {
        self.entries
    }
}

/// Register several heterogeneous [`WorkerOpts`] as one [`WorkerGroup`].
///
/// ```
/// # use crafty::{WorkerGroup, WorkerOpts, WorkerScale, workers};
/// # struct A;
/// # impl crafty::actor::UserActor for A {
/// #     type Config = ();
/// #     type Message = ();
/// #     type Error = std::convert::Infallible;
/// #     fn start(_: Self::Config) -> Result<Self, Self::Error> { Ok(Self) }
/// #     fn handle(&mut self, _: Self::Message) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
/// #         std::future::ready(Ok(()))
/// #     }
/// # }
/// let group: WorkerGroup = workers![WorkerOpts::<A>::new("a").config(()).scale(WorkerScale::PerNode)];
/// let _ = group;
/// ```
#[macro_export]
macro_rules! workers {
    ($($opt:expr),* $(,)?) => {{
        let mut group = $crate::WorkerGroup::new();
        $(group = group.with_worker($opt);)*
        group
    }};
}
