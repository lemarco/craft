//! Process-wide **compute tokens** — cap concurrent gateway + job + actor work
//! ([workload-governor](../../../docs/decisions/workload-governor.md)).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Shared semaphore-like pool with a dynamically adjustable ceiling.
#[derive(Debug)]
pub struct ComputeTokenPool {
    max: AtomicUsize,
    in_use: AtomicUsize,
    notify: Notify,
}

impl ComputeTokenPool {
    /// Create a pool allowing up to `max_tokens` concurrent holders (minimum 1).
    #[must_use]
    pub fn new(max_tokens: usize) -> Arc<Self> {
        Arc::new(Self {
            max: AtomicUsize::new(max_tokens.max(1)),
            in_use: AtomicUsize::new(0),
            notify: Notify::new(),
        })
    }

    /// Current ceiling (may change at runtime via [`Self::set_max`]).
    #[must_use]
    pub fn max_tokens(&self) -> usize {
        self.max.load(Ordering::Acquire)
    }

    /// Tokens currently held.
    #[must_use]
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }

    /// Adjust the ceiling; waiting acquirers are woken.
    pub fn set_max(&self, max_tokens: usize) {
        self.max.store(max_tokens.max(1), Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait until one token is available.
    pub async fn acquire(self: &Arc<Self>) -> ComputeGuard {
        self.acquire_weighted(1).await
    }

    /// Wait until `weight` token units are available (minimum 1).
    ///
    /// Used when a handler reserves capacity for subprocess work the pool cannot
    /// observe directly ([`JobOpts::compute_cost`](../../crafty/src/job_opts.rs)).
    pub async fn acquire_weighted(self: &Arc<Self>, weight: usize) -> ComputeGuard {
        let weight = weight.max(1);
        loop {
            let max = self.max.load(Ordering::Acquire);
            let cur = self.in_use.load(Ordering::Acquire);
            if cur.saturating_add(weight) <= max
                && self
                    .in_use
                    .compare_exchange(cur, cur + weight, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return ComputeGuard {
                    pool: Arc::clone(self),
                    weight,
                };
            }
            self.notify.notified().await;
        }
    }
}

/// RAII holder — releases held token units on drop.
pub struct ComputeGuard {
    pool: Arc<ComputeTokenPool>,
    weight: usize,
}

impl Drop for ComputeGuard {
    fn drop(&mut self) {
        self.pool
            .in_use
            .fetch_sub(self.weight, Ordering::AcqRel);
        self.pool.notify.notify_waiters();
    }
}

/// Run `work` while holding an optional compute token (no-op when `pool` is `None`).
pub async fn with_compute_guard<F, T>(pool: Option<&Arc<ComputeTokenPool>>, work: F) -> T
where
    F: std::future::Future<Output = T>,
{
    with_compute_guard_weighted(pool, 1, work).await
}

/// Run `work` while holding `weight` token units when `pool` is set.
pub async fn with_compute_guard_weighted<F, T>(
    pool: Option<&Arc<ComputeTokenPool>>,
    weight: usize,
    work: F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    match pool {
        Some(pool) => {
            let _guard = pool.acquire_weighted(weight).await;
            work.await
        }
        None => work.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_and_release() {
        let pool = ComputeTokenPool::new(2);
        let _a = pool.acquire().await;
        let _b = pool.acquire().await;
        assert_eq!(pool.in_use(), 2);
    }

    #[tokio::test]
    async fn weighted_acquire_reserves_multiple_units() {
        let pool = ComputeTokenPool::new(4);
        let _a = pool.acquire_weighted(3).await;
        assert_eq!(pool.in_use(), 3);
        let pool2 = Arc::clone(&pool);
        let waiter = tokio::spawn(async move {
            let guard = pool2.acquire_weighted(2).await;
            (pool2.in_use(), guard)
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        drop(_a);
        let (in_use, _guard) = tokio::time::timeout(std::time::Duration::from_millis(200), waiter)
            .await
            .expect("waiter should acquire")
            .unwrap();
        assert_eq!(in_use, 2);
        assert_eq!(pool.in_use(), 2);
    }

    #[tokio::test]
    async fn set_max_unblocks_waiters() {
        let pool = ComputeTokenPool::new(1);
        let _a = pool.acquire().await;
        let pool2 = Arc::clone(&pool);
        let waiter = tokio::spawn(async move { pool2.acquire().await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        pool.set_max(2);
        tokio::time::timeout(std::time::Duration::from_millis(200), waiter)
            .await
            .expect("waiter should acquire")
            .unwrap();
    }
}
