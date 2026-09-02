//! Local API vs job fairness — [`ComputeTokenPool`] and [`run_workload_governor`].
//!
//! See [workload-governor ADR](../../../docs/decisions/workload-governor.md).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::JobQueue;
use trembita_runtime::{ComputeTokenPool, ExternalLoad};

/// Consumer knobs published by [`run_workload_governor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerTune {
    /// Maximum jobs leased per poll.
    pub batch: usize,
    /// Sleep between polls when the queue is empty.
    pub idle_sleep: Duration,
}

impl Default for ConsumerTune {
    fn default() -> Self {
        Self {
            batch: 1,
            idle_sleep: Duration::from_millis(100),
        }
    }
}

/// Builder knobs for the per-node workload governor.
#[derive(Clone)]
pub struct WorkloadOpts {
    /// Upper bound on concurrent compute holders (gateway + consumers).
    pub max_compute_tokens: usize,
    /// Floor when ingress is hot.
    pub min_compute_tokens: usize,
    /// Active gateway connections at or above which API protection kicks in.
    pub api_protect_connections: usize,
    /// Governor tick interval.
    pub tick: Duration,
    /// Tune when ingress is quiet and work is waiting.
    pub when_opportunistic: ConsumerTune,
    /// Default middle-ground tune.
    pub when_balanced: ConsumerTune,
    /// Tune when ingress is hot.
    pub when_protective: ConsumerTune,
    /// Optional subprocess / shell-out load the token pool cannot observe.
    pub external_load: Option<Arc<dyn ExternalLoad>>,
}

impl std::fmt::Debug for WorkloadOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadOpts")
            .field("max_compute_tokens", &self.max_compute_tokens)
            .field("min_compute_tokens", &self.min_compute_tokens)
            .field("api_protect_connections", &self.api_protect_connections)
            .field("tick", &self.tick)
            .field("when_opportunistic", &self.when_opportunistic)
            .field("when_balanced", &self.when_balanced)
            .field("when_protective", &self.when_protective)
            .field(
                "external_load",
                &self.external_load.as_ref().map(|_| "<external>"),
            )
            .finish()
    }
}

impl WorkloadOpts {
    /// Default preset: protect API when hot; jobs use slack when ingress is quiet.
    #[must_use]
    pub fn balanced() -> Self {
        Self {
            max_compute_tokens: std::thread::available_parallelism()
                .map_or(4, std::num::NonZero::get),
            min_compute_tokens: 1,
            api_protect_connections: 32,
            tick: Duration::from_millis(500),
            when_opportunistic: ConsumerTune {
                batch: 16,
                idle_sleep: Duration::from_millis(10),
            },
            when_balanced: ConsumerTune {
                batch: 4,
                idle_sleep: Duration::from_millis(100),
            },
            when_protective: ConsumerTune {
                batch: 1,
                idle_sleep: Duration::from_millis(500),
            },
            external_load: None,
        }
    }

    /// Favour API latency — lower ceilings and tighter consumer tuning sooner.
    #[must_use]
    pub fn api_first() -> Self {
        let mut opts = Self::balanced();
        opts.api_protect_connections = 8;
        opts.when_protective = ConsumerTune {
            batch: 1,
            idle_sleep: Duration::from_secs(1),
        };
        opts
    }

    /// Favour background throughput when ingress is quiet.
    #[must_use]
    pub fn jobs_opportunistic() -> Self {
        let mut opts = Self::balanced();
        opts.api_protect_connections = 64;
        opts.when_opportunistic = ConsumerTune {
            batch: 32,
            idle_sleep: Duration::from_millis(5),
        };
        opts
    }

    /// Set the process-wide compute token ceiling.
    #[must_use]
    pub fn max_compute_tokens(mut self, max: usize) -> Self {
        self.max_compute_tokens = max.max(1);
        self
    }

    /// Set the connection count threshold for API protection.
    #[must_use]
    pub fn api_protect_connections(mut self, connections: usize) -> Self {
        self.api_protect_connections = connections.max(1);
        self
    }

    /// Attach a port that reports compute load outside the cooperative token pool.
    #[must_use]
    pub fn external_load(mut self, load: Arc<dyn ExternalLoad>) -> Self {
        self.external_load = Some(load);
        self
    }
}

/// Snapshot emitted to metrics hooks on each governor tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadMetricsSnapshot {
    /// Tokens currently held.
    pub tokens_in_use: usize,
    /// Current token ceiling after the last governor decision.
    pub token_ceiling: usize,
    /// Active gateway connections.
    pub active_connections: usize,
    /// Sum of `pending` across registered job queues.
    pub queue_depth: u64,
    /// Subprocess / shell-out units from [`ExternalLoad`], if wired.
    pub external_load_units: usize,
    /// Consumer tune after this tick.
    pub tune: ConsumerTune,
    /// `true` when consumer tune changed this tick.
    pub tune_changed: bool,
}

/// Optional metrics callback from [`run_workload_governor`].
pub type WorkloadMetricsHook = Arc<dyn Fn(WorkloadMetricsSnapshot) + Send + Sync>;

/// Per-node background loop: read ingress + queue depth, adjust token ceiling and consumer tune.
pub async fn run_workload_governor(
    pool: Arc<ComputeTokenPool>,
    tune_tx: watch::Sender<ConsumerTune>,
    mut stop: watch::Receiver<bool>,
    opts: WorkloadOpts,
    connections: Arc<dyn Fn() -> usize + Send + Sync>,
    queues: Vec<Arc<dyn JobQueue>>,
    metrics: Option<WorkloadMetricsHook>,
) {
    let _ = tune_tx.send(opts.when_balanced);
    let mut interval = tokio::time::interval(opts.tick);
    let mut last_tune = opts.when_balanced;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
        }

        let active_connections = connections();
        let external_load_units = opts.external_load.as_ref().map_or(0, |load| load.units());
        let mut queue_depth = 0u64;
        for queue in &queues {
            if let Ok(m) = queue.metrics().await {
                queue_depth += m.pending;
            }
        }

        let (token_ceiling, tune) = decide(
            &opts,
            active_connections,
            queue_depth,
            pool.in_use(),
            external_load_units,
        );
        pool.set_max(token_ceiling);
        let tune_changed = tune != last_tune;
        if tune_changed {
            let _ = tune_tx.send(tune);
            last_tune = tune;
        }

        if let Some(hook) = &metrics {
            hook(WorkloadMetricsSnapshot {
                tokens_in_use: pool.in_use(),
                token_ceiling,
                active_connections,
                queue_depth,
                external_load_units,
                tune,
                tune_changed,
            });
        }
    }
}

fn decide(
    opts: &WorkloadOpts,
    connections: usize,
    queue_depth: u64,
    tokens_in_use: usize,
    external_units: usize,
) -> (usize, ConsumerTune) {
    let external_pressure = external_units.saturating_mul(opts.api_protect_connections)
        / opts.max_compute_tokens.max(1);
    let effective_connections = connections.saturating_add(external_pressure);
    if effective_connections >= opts.api_protect_connections {
        return (opts.min_compute_tokens.max(1), opts.when_protective);
    }
    if effective_connections == 0 && queue_depth > 0 {
        return (opts.max_compute_tokens.max(1), opts.when_opportunistic);
    }
    let span = opts.api_protect_connections.saturating_sub(1).max(1);
    let pressure = effective_connections.min(span);
    let headroom = opts
        .max_compute_tokens
        .saturating_sub(opts.min_compute_tokens);
    let mut token_ceiling = opts
        .min_compute_tokens
        .saturating_add(headroom.saturating_mul(span - pressure) / span)
        .max(1);
    // Do not raise the ceiling above what in-flight holders + external load already consume.
    let floor = tokens_in_use.saturating_add(external_units).max(1);
    token_ceiling = token_ceiling.max(floor);
    (token_ceiling, opts.when_balanced)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_hot_protects_api() {
        let opts = WorkloadOpts::balanced();
        let (tokens, tune) = decide(&opts, opts.api_protect_connections, 100, 0, 0);
        assert_eq!(tokens, opts.min_compute_tokens.max(1));
        assert_eq!(tune, opts.when_protective);
    }

    #[test]
    fn decide_idle_with_depth_boosts_jobs() {
        let opts = WorkloadOpts::balanced();
        let (tokens, tune) = decide(&opts, 0, 5, 0, 0);
        assert_eq!(tokens, opts.max_compute_tokens.max(1));
        assert_eq!(tune, opts.when_opportunistic);
    }

    #[test]
    fn decide_moderate_load_balances() {
        let opts = WorkloadOpts::balanced();
        let (tokens, tune) = decide(&opts, 4, 0, 0, 0);
        assert!(tokens > opts.min_compute_tokens);
        assert!(tokens <= opts.max_compute_tokens);
        assert_eq!(tune, opts.when_balanced);
    }

    #[test]
    fn decide_external_load_protects_api() {
        let opts = WorkloadOpts::balanced();
        let heavy_external = opts.max_compute_tokens;
        let (tokens, tune) = decide(&opts, 0, 0, 0, heavy_external);
        assert_eq!(tokens, opts.min_compute_tokens.max(1));
        assert_eq!(tune, opts.when_protective);
    }

    #[tokio::test]
    async fn governor_publishes_opportunistic_tune() {
        let pool = ComputeTokenPool::new(4);
        let (tune_tx, mut tune_rx) = watch::channel(ConsumerTune::default());
        let (stop_tx, stop_rx) = watch::channel(false);
        let opts = WorkloadOpts {
            tick: Duration::from_millis(20),
            ..WorkloadOpts::balanced()
        };
        let queue = Arc::new(crate::InMemoryJobQueue::new(Duration::from_secs(30)));
        queue.enqueue(b"x").await.unwrap();
        let queues: Vec<Arc<dyn JobQueue>> = vec![queue];
        let governor = tokio::spawn(run_workload_governor(
            pool,
            tune_tx,
            stop_rx,
            opts.clone(),
            Arc::new(|| 0usize),
            queues,
            None,
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while tune_rx.borrow().batch != opts.when_opportunistic.batch {
                tune_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("governor should publish opportunistic tune");
        let _ = stop_tx.send(true);
        governor.await.unwrap();
    }
}
