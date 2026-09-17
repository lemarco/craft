//! Shared gateway connection counting (used by cluster assembly and product gateway).

use std::sync::atomic::{AtomicUsize, Ordering};

/// Tracks live gateway connections (WebSocket, long-poll, …).
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    active: AtomicUsize,
}

impl ConnectionTracker {
    /// Increment active connection count; decrements when the guard drops.
    #[must_use]
    pub fn track(&self) -> ConnectionGuard<'_> {
        self.active.fetch_add(1, Ordering::SeqCst);
        ConnectionGuard { tracker: self }
    }

    /// Number of connections still open.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

/// RAII guard — decrements [`ConnectionTracker`] on drop.
pub struct ConnectionGuard<'a> {
    tracker: &'a ConnectionTracker,
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.tracker.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Counts HTTP handlers currently executing (finer than connection count alone).
#[derive(Debug, Default)]
pub struct HttpInFlight {
    active: AtomicUsize,
}

impl HttpInFlight {
    /// Increment in-flight count; decrements when the guard drops.
    #[must_use]
    pub fn track(&self) -> InFlightGuard<'_> {
        self.active.fetch_add(1, Ordering::SeqCst);
        InFlightGuard { counter: self }
    }

    /// Handlers currently running on this node.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

/// RAII guard for [`HttpInFlight`].
pub struct InFlightGuard<'a> {
    counter: &'a HttpInFlight,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.counter.active.fetch_sub(1, Ordering::SeqCst);
    }
}
