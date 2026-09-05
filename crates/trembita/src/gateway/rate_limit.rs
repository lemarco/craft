//! Simple per-gateway request rate limiting (fixed one-second window).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Token-bucket-like limiter: at most `max_per_sec` acquisitions per rolling second.
#[derive(Debug, Clone)]
pub struct GatewayRateLimiter {
    inner: Arc<GatewayRateLimiterInner>,
}

#[derive(Debug)]
struct GatewayRateLimiterInner {
    max_per_sec: u32,
    window_start: Mutex<Instant>,
    count: Mutex<u32>,
}

impl GatewayRateLimiter {
    /// Create a limiter allowing `max_per_sec` requests per second (gateway-wide).
    #[must_use]
    pub fn new(max_per_sec: u32) -> Self {
        Self {
            inner: Arc::new(GatewayRateLimiterInner {
                max_per_sec: max_per_sec.max(1),
                window_start: Mutex::new(Instant::now()),
                count: Mutex::new(0),
            }),
        }
    }

    /// Returns `false` when the per-second budget is exhausted.
    pub(crate) fn try_acquire(&self) -> bool {
        let inner = &self.inner;
        let now = Instant::now();
        let mut window = inner.window_start.lock().expect("poisoned");
        let mut count = inner.count.lock().expect("poisoned");
        if now.duration_since(*window) >= Duration::from_secs(1) {
            *window = now;
            *count = 0;
        }
        if *count >= inner.max_per_sec {
            return false;
        }
        *count += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unchecked_time_subtraction)]
    fn limiter_resets_after_window() {
        let limiter = GatewayRateLimiter::new(2);
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        {
            let mut window = limiter.inner.window_start.lock().expect("poisoned");
            *window = Instant::now() - Duration::from_secs(2);
        }
        assert!(limiter.try_acquire());
    }
}
