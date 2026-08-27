//! Exponential reconnect backoff for the peer connection pool (backlog C5,
//! [ADR 027](../../../docs/decisions/027-future-work-and-risks.md) R2).
//!
//! [`BackoffPolicy`] is a pure, deterministic delay schedule; `BackoffState`
//! tracks a single endpoint's consecutive failures and the earliest time a
//! redial is allowed. Both take an explicit `now: Instant`, so the reconnect
//! logic is unit-testable without sleeping or a real clock.

use std::time::{Duration, Instant};

/// An exponential backoff schedule: `base`, doubling by `factor` per
/// consecutive failure, capped at `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    /// Delay after the first failure.
    pub base: Duration,
    /// Upper bound on the delay.
    pub max: Duration,
    /// Growth factor applied per additional consecutive failure.
    pub factor: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(100),
            max: Duration::from_secs(5),
            factor: 2,
        }
    }
}

impl BackoffPolicy {
    /// The delay to wait after `failures` consecutive failures (1-based).
    /// `failures == 0` is [`Duration::ZERO`] (ready immediately).
    #[must_use]
    pub fn delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        let mut delay = self.base;
        // Apply the growth `failures - 1` times, saturating at `max`.
        for _ in 1..failures {
            if delay >= self.max {
                break;
            }
            delay = delay.saturating_mul(self.factor);
        }
        delay.min(self.max)
    }
}

/// Per-endpoint reconnect state: how many consecutive dials have failed and the
/// earliest instant the next dial is permitted.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BackoffState {
    failures: u32,
    /// `None` means "ready now" (no failure recorded yet, or reset).
    ready_at: Option<Instant>,
}

impl BackoffState {
    /// Whether a (re)connect attempt is allowed at `now`.
    #[must_use]
    pub fn ready(&self, now: Instant) -> bool {
        self.ready_at.is_none_or(|t| now >= t)
    }

    /// Number of consecutive failures recorded.
    #[cfg(test)]
    pub(crate) fn failures(&self) -> u32 {
        self.failures
    }

    /// Record a failed attempt at `now`, arming the backoff window per `policy`.
    pub fn record_failure(&mut self, policy: &BackoffPolicy, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.ready_at = Some(now + policy.delay(self.failures));
    }

    /// Clear the backoff after a successful connect.
    pub fn reset(&mut self) {
        self.failures = 0;
        self.ready_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_exponentially_and_caps() {
        let p = BackoffPolicy {
            base: Duration::from_millis(100),
            max: Duration::from_secs(1),
            factor: 2,
        };
        assert_eq!(p.delay(0), Duration::ZERO);
        assert_eq!(p.delay(1), Duration::from_millis(100));
        assert_eq!(p.delay(2), Duration::from_millis(200));
        assert_eq!(p.delay(3), Duration::from_millis(400));
        assert_eq!(p.delay(4), Duration::from_millis(800));
        assert_eq!(p.delay(5), Duration::from_secs(1), "capped at max");
        assert_eq!(p.delay(100), Duration::from_secs(1), "stays capped");
    }

    #[test]
    fn fresh_state_is_ready() {
        let s = BackoffState::default();
        assert!(s.ready(Instant::now()));
        assert_eq!(s.failures(), 0);
    }

    #[test]
    fn failure_arms_the_window_and_reset_clears_it() {
        let p = BackoffPolicy::default();
        let now = Instant::now();
        let mut s = BackoffState::default();

        s.record_failure(&p, now);
        assert_eq!(s.failures(), 1);
        assert!(!s.ready(now), "not ready immediately after a failure");
        assert!(
            s.ready(now + p.base + Duration::from_millis(1)),
            "ready once the window elapses"
        );

        s.record_failure(&p, now);
        assert_eq!(s.failures(), 2, "consecutive failures accumulate");

        s.reset();
        assert_eq!(s.failures(), 0);
        assert!(s.ready(now), "reset makes it ready again");
    }
}
