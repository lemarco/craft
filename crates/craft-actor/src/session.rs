//! Sticky actor sessions — pin traffic to one instance for a TTL ([cluster-routing]).
//!
//! Useful for in-memory workflow state on a worker without Redis: open a session
//! after the first keyed pick, then route subsequent casts/asks to the same
//! [`ActorId`] until the lease expires or the instance disappears.
//!
//! [client-and-routing#cluster-actor-routing]: ../../../docs/decisions/client-and-routing.md#cluster-actor-routing

use std::time::{Duration, Instant};

use craft_proto::{ActorId, ActorRegistration};

use crate::directory::ActorDirectory;

/// A lease on a specific actor instance, obtained from a keyed pick or explicit resolve.
#[derive(Debug, Clone)]
pub struct ActorSession {
    target: ActorId,
    expires_at: Option<Instant>,
}

impl ActorSession {
    /// Pin to `registration` with optional time-to-live.
    #[must_use]
    pub fn new(registration: &ActorRegistration, ttl: Option<Duration>) -> Self {
        Self {
            target: registration.id.clone(),
            expires_at: ttl.map(|d| Instant::now() + d),
        }
    }

    /// The pinned actor id.
    #[must_use]
    pub fn target(&self) -> &ActorId {
        &self.target
    }

    /// Whether the lease has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|t| Instant::now() >= t)
    }

    /// Resolve the current registration for this session, if still live.
    #[must_use]
    pub fn resolve(&self, directory: &ActorDirectory) -> Option<ActorRegistration> {
        if self.is_expired() {
            return None;
        }
        directory.resolve(&self.target)
    }
}
