//! Simple per-gateway request rate limiting (fixed one-second window).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Token-bucket-like limiter: at most `max_per_sec` acquisitions per rolling second.
#[derive(Debug)]
pub struct GatewayRateLimiter {
    max_per_sec: u32,
    window_start: Mutex<Instant>,
    count: Mutex<u32>,
}

impl GatewayRateLimiter {
    /// Create a limiter allowing `max_per_sec` requests per second (gateway-wide).
    #[must_use]
    pub fn new(max_per_sec: u32) -> Self {
        Self {
            max_per_sec: max_per_sec.max(1),
            window_start: Mutex::new(Instant::now()),
            count: Mutex::new(0),
        }
    }

    fn try_acquire(&self) -> bool {
        let now = Instant::now();
        let mut window = self.window_start.lock().expect("poisoned");
        let mut count = self.count.lock().expect("poisoned");
        if now.duration_since(*window) >= Duration::from_secs(1) {
            *window = now;
            *count = 0;
        }
        if *count >= self.max_per_sec {
            return false;
        }
        *count += 1;
        true
    }
}

/// Reject with `429 Too Many Requests` when the per-second budget is exhausted.
pub async fn rate_limit_middleware(
    State(limiter): State<Arc<GatewayRateLimiter>>,
    _connect: Option<ConnectInfo<SocketAddr>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if limiter.try_acquire() {
        next.run(request).await
    } else {
        (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_resets_after_window() {
        let limiter = GatewayRateLimiter::new(2);
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        {
            let mut window = limiter.window_start.lock().expect("poisoned");
            *window = Instant::now() - Duration::from_secs(2);
        }
        assert!(limiter.try_acquire());
    }
}
