use std::future::Future;
use std::time::Duration;

use super::port::JobQueue;
use super::types::LeasedJob;
use trembita_proto::WorkerId;

/// Optional workload governor integration for [`run_queue_consumer`].
pub struct QueueConsumerWorkload {
    /// Shared compute token pool (acquired per handler invocation).
    pub tokens: std::sync::Arc<trembita_runtime::ComputeTokenPool>,
    /// Live consumer tuning from [`crate::run_workload_governor`].
    pub tune: tokio::sync::watch::Receiver<crate::ConsumerTune>,
}

/// Poll a [`JobQueue`], invoke `handle` on each leased job, then ack or nack.
///
/// Leases up to `batch` jobs per poll and acknowledges successes with
/// [`JobQueue::ack_batch`] (one leader transaction when using [`crate::ClusterJobQueue`]).
///
/// Runs until `stop` is set. When the queue is empty, sleeps `idle_sleep` between polls.
///
/// When `workload` is set, `batch` / `idle_sleep` come from the governor's
/// [`crate::ConsumerTune`] watch channel and each handler acquires `compute_cost`
/// token units from the pool (default 1).
#[allow(clippy::too_many_arguments)]
pub async fn run_queue_consumer<Q, F, Fut, E>(
    queue: std::sync::Arc<Q>,
    worker: WorkerId,
    batch: usize,
    idle_sleep: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut handle: F,
    workload: Option<QueueConsumerWorkload>,
    compute_cost: usize,
) where
    Q: JobQueue + ?Sized,
    F: FnMut(&LeasedJob) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let mut tune_rx = workload.as_ref().map(|w| w.tune.clone());
    loop {
        if *stop.borrow() {
            break;
        }
        let (batch, idle_sleep) = tune_rx.as_ref().map_or((batch.max(1), idle_sleep), |rx| {
            let tune = *rx.borrow();
            (tune.batch.max(1), tune.idle_sleep)
        });
        let Ok(jobs) = queue.lease(worker, batch).await else {
            tokio::time::sleep(idle_sleep).await;
            continue;
        };
        if jobs.is_empty() {
            if let Some(rx) = tune_rx.as_mut() {
                tokio::select! {
                    () = tokio::time::sleep(idle_sleep) => {}
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = rx.changed() => {}
                }
            } else {
                tokio::select! {
                    () = tokio::time::sleep(idle_sleep) => {}
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                }
            }
            continue;
        }
        let mut acks = Vec::with_capacity(jobs.len());
        let mut nacks = Vec::new();
        for job in jobs {
            let _token = if let Some(wl) = &workload {
                Some(wl.tokens.acquire_weighted(compute_cost).await)
            } else {
                None
            };
            match handle(&job).await {
                Ok(()) => acks.push(job.lease_id),
                Err(_) => nacks.push(job.lease_id),
            }
        }
        if !acks.is_empty() {
            let _ = queue.ack_batch(worker, &acks).await;
        }
        for lease_id in nacks {
            let _ = queue.nack(worker, lease_id).await;
        }
        if *stop.borrow() {
            break;
        }
    }
}
