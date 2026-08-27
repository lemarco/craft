//! Traffic-class priority tuning (ADR 027 R2 — "full tuning" follow-up to the
//! v1 per-class connection isolation).
//!
//! QUIC connections are independent at the transport layer, so there is no
//! cross-connection stream priority to lean on: the lever that actually
//! prevents bulk client/actor payloads from starving latency-sensitive Raft
//! heartbeats on the shared UDP socket (ADR 027 R2) is **admission control** —
//! rate-limiting the bulk [`TrafficClass`]es while consensus traffic flows
//! unthrottled.
//!
//! A [`TrafficPolicy`] maps each class to an optional token-bucket rate limiter.
//! The default policy is *unlimited* (no behavior change);
//! operators opt into limits per class when a node is client/actor-heavy.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use crate::route::TrafficClass;

/// A token-bucket rate limiter: refills at `rate` tokens per second up to a
/// `burst` ceiling. `acquire` consumes one token,
/// sleeping only when the bucket is empty. Tokens may go transiently negative so
/// concurrent acquirers queue rather than all racing through an empty bucket.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    rate: f64,
    burst: f64,
    bucket: Mutex<Bucket>,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    /// A limiter refilling `rate` tokens/second, holding at most `burst` tokens
    /// (the largest instantaneous burst allowed). Both must be positive.
    #[must_use]
    pub fn new(rate: f64, burst: f64) -> Self {
        Self {
            rate: rate.max(f64::MIN_POSITIVE),
            burst: burst.max(1.0),
            bucket: Mutex::new(Bucket {
                tokens: burst.max(1.0),
                last: Instant::now(),
            }),
        }
    }

    /// Refill according to elapsed time, reserve one token, and return how long
    /// the caller must wait before that token is available (`ZERO` if a token
    /// was already banked). Split out from [`acquire`](Self::acquire) with an
    /// explicit `now` so the refill maths are unit-testable without real time.
    fn reserve(&self, now: Instant) -> Duration {
        let mut b = self.bucket.lock().unwrap();
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * self.rate).min(self.burst);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Duration::ZERO
        } else {
            // Reserve this slot (tokens go negative); the wait is how long until
            // the bucket refills back to zero.
            let deficit = 1.0 - b.tokens;
            b.tokens -= 1.0;
            Duration::from_secs_f64(deficit / self.rate)
        }
    }

    /// Consume one token, awaiting the refill delay if the bucket is empty.
    pub async fn acquire(&self) {
        let wait = self.reserve(Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Per-[`TrafficClass`] admission control (ADR 027 R2). Classes without a
/// configured limiter are unthrottled — notably [`TrafficClass::Peer`], so Raft
/// consensus is never rate-limited.
#[derive(Clone, Default)]
pub struct TrafficPolicy {
    limiters: HashMap<TrafficClass, Arc<RateLimiter>>,
}

impl TrafficPolicy {
    /// A policy that throttles nothing (the default).
    #[must_use]
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Rate-limit `class` to `rate` requests/second with a `burst` ceiling.
    /// Chainable; re-setting a class replaces its limiter. Limiting
    /// [`TrafficClass::Peer`] is allowed but discouraged (it can induce
    /// spurious elections).
    #[must_use]
    pub fn with_rate(mut self, class: TrafficClass, rate: f64, burst: f64) -> Self {
        self.limiters
            .insert(class, Arc::new(RateLimiter::new(rate, burst)));
        self
    }

    /// Block until a request of `class` is admitted (immediate for unthrottled
    /// classes).
    pub async fn admit(&self, class: TrafficClass) {
        if let Some(limiter) = self.limiters.get(&class) {
            limiter.acquire().await;
        }
    }

    /// Whether `class` is currently rate-limited.
    #[must_use]
    pub fn is_limited(&self, class: TrafficClass) -> bool {
        self.limiters.contains_key(&class)
    }
}

impl std::fmt::Debug for TrafficPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut classes: Vec<_> = self.limiters.keys().copied().collect();
        classes.sort_by_key(|c| format!("{c:?}"));
        f.debug_struct("TrafficPolicy")
            .field("limited_classes", &classes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_bucket_admits_without_waiting() {
        let limiter = RateLimiter::new(10.0, 5.0);
        let now = Instant::now();
        // Five banked tokens → five immediate admissions.
        for _ in 0..5 {
            assert_eq!(limiter.reserve(now), Duration::ZERO);
        }
        // Sixth in the same instant must wait ~1/rate for a refill.
        let wait = limiter.reserve(now);
        assert!(wait > Duration::ZERO, "empty bucket should impose a wait");
        assert!(wait <= Duration::from_secs_f64(1.0 / 10.0) + Duration::from_millis(1));
    }

    #[test]
    fn tokens_refill_over_time() {
        let limiter = RateLimiter::new(100.0, 1.0);
        let start = Instant::now();
        assert_eq!(limiter.reserve(start), Duration::ZERO);
        // Immediately empty; but 50ms later 100/s has refilled ~5 tokens.
        let later = start + Duration::from_millis(50);
        assert_eq!(limiter.reserve(later), Duration::ZERO);
    }

    #[test]
    fn unlimited_policy_admits_every_class() {
        let policy = TrafficPolicy::unlimited();
        for class in [
            TrafficClass::Peer,
            TrafficClass::Client,
            TrafficClass::Cluster,
            TrafficClass::Actor,
        ] {
            assert!(!policy.is_limited(class));
        }
    }

    #[test]
    fn with_rate_limits_only_the_named_class() {
        let policy = TrafficPolicy::unlimited().with_rate(TrafficClass::Actor, 1000.0, 10.0);
        assert!(policy.is_limited(TrafficClass::Actor));
        assert!(!policy.is_limited(TrafficClass::Peer));
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_paces_requests_past_the_burst() {
        // rate 100/s, burst 2: the first two are instant, the third waits.
        let limiter = RateLimiter::new(100.0, 2.0);
        limiter.acquire().await;
        limiter.acquire().await;
        let start = Instant::now();
        limiter.acquire().await;
        assert!(
            start.elapsed() >= Duration::from_millis(9),
            "third acquire should have paced ~10ms, waited {:?}",
            start.elapsed()
        );
    }
}
