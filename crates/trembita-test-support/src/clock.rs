//! Virtual time helpers for async integration tests.
//!
//! Mark tests with `#[tokio::test(start_paused = true)]` (requires Tokio
//! `test-util` in the test crate's dev-dependencies). Polling helpers advance
//! the Tokio clock instead of waiting on wall time so Raft `interval` ticks and
//! client timeouts run deterministically.

use std::future::Future;
use std::time::Duration;

use crate::harness::TICK_PERIOD;

/// Default polling step — one Raft runtime tick in fast integration tests.
pub const POLL_STEP: Duration = TICK_PERIOD;

/// Advance the paused Tokio clock and yield so timer-driven tasks (Raft ticks,
/// `sleep`, `timeout`) can run.
pub async fn advance(duration: Duration) {
    tokio::time::advance(duration).await;
    tokio::task::yield_now().await;
}

/// Poll a sync predicate, advancing `step` between attempts.
///
/// # Panics
/// If `cond` never returns true within `max_steps` attempts.
pub async fn eventually<F>(what: &str, max_steps: usize, step: Duration, mut cond: F)
where
    F: FnMut() -> bool,
{
    for _ in 0..max_steps {
        if cond() {
            return;
        }
        advance(step).await;
    }
    panic!("condition not met: {what}");
}

/// Poll an async predicate, advancing `step` between attempts.
///
/// # Panics
/// If `cond` never returns true within `max_steps` attempts.
pub async fn eventually_async<F, Fut>(what: &str, max_steps: usize, step: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..max_steps {
        if cond().await {
            return;
        }
        advance(step).await;
    }
    panic!("condition not met: {what}");
}

/// Shorthand: ~5s of polls at [`POLL_STEP`].
pub async fn eventually_default<F>(what: &str, cond: F)
where
    F: FnMut() -> bool,
{
    eventually(what, 500, POLL_STEP, cond).await;
}

/// Shorthand: ~5s of async polls at [`POLL_STEP`].
pub async fn eventually_async_default<F, Fut>(what: &str, cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    eventually_async(what, 500, POLL_STEP, cond).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn advance_wakes_a_sleeping_task() {
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = done.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        advance(Duration::from_millis(50)).await;
        task.await.expect("task");
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
    }
}
