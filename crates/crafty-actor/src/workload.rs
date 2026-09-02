//! Local API vs job fairness — [`ComputeTokenPool`] and [`run_workload_governor`].
//!
//! See [workload-governor ADR](../../../docs/decisions/workload-governor.md).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::compute_token::ComputeTokenPool;
use crate::queue::JobQueue;

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
#[derive(Debug, Clone)]
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
        let mut queue_depth = 0u64;
        for queue in &queues {
            if let Ok(m) = queue.metrics().await {
                queue_depth += m.pending;
            }
        }

        let (token_ceiling, tune) = decide(&opts, active_connections, queue_depth);
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
                tune,
                tune_changed,
            });
        }
    }
}

fn decide(opts: &WorkloadOpts, connections: usize, queue_depth: u64) -> (usize, ConsumerTune) {
    if connections >= opts.api_protect_connections {
        return (opts.min_compute_tokens.max(1), opts.when_protective);
    }
    if connections == 0 && queue_depth > 0 {
        return (opts.max_compute_tokens.max(1), opts.when_opportunistic);
    }
    let span = opts.api_protect_connections.saturating_sub(1).max(1);
    let pressure = connections.min(span);
    let headroom = opts
        .max_compute_tokens
        .saturating_sub(opts.min_compute_tokens);
    let token_ceiling = opts
        .min_compute_tokens
        .saturating_add(headroom.saturating_mul(span - pressure) / span)
        .max(1);
    (token_ceiling, opts.when_balanced)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_hot_protects_api() {
        let opts = WorkloadOpts::balanced();
        let (tokens, tune) = decide(&opts, opts.api_protect_connections, 100);
        assert_eq!(tokens, opts.min_compute_tokens.max(1));
        assert_eq!(tune, opts.when_protective);
    }

    #[test]
    fn decide_idle_with_depth_boosts_jobs() {
        let opts = WorkloadOpts::balanced();
        let (tokens, tune) = decide(&opts, 0, 5);
        assert_eq!(tokens, opts.max_compute_tokens.max(1));
        assert_eq!(tune, opts.when_opportunistic);
    }

    #[test]
    fn decide_moderate_load_balances() {
        let opts = WorkloadOpts::balanced();
        let (tokens, tune) = decide(&opts, 4, 0);
        assert!(tokens > opts.min_compute_tokens);
        assert!(tokens <= opts.max_compute_tokens);
        assert_eq!(tune, opts.when_balanced);
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
