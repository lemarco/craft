//! Sticky actor sessions — pin traffic to one instance for a TTL ([cluster-routing]).
//!
//! Useful for in-memory workflow state on a worker without Redis: open a session
//! after the first keyed pick, then route subsequent casts/asks to the same
//! [`ActorId`] until the lease expires or the instance disappears.
//!
//! [client-and-routing#cluster-actor-routing]: ../../../docs/decisions/client-and-routing.md#cluster-actor-routing

use std::hash::Hash;
use std::time::{Duration, Instant};

use trembita_proto::{ActorId, ActorRegistration};

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

    /// Re-pin `key` in `group` after rebalance or worker loss (R3 sticky recovery).
    ///
    /// When `previous` is still valid in the directory, returns a clone. Otherwise
    /// performs a fresh keyed pick and opens a new lease with `ttl`.
    #[must_use]
    pub fn reopen_keyed<K: Hash>(
        directory: &ActorDirectory,
        group: &str,
        key: &K,
        ttl: Option<Duration>,
        previous: Option<&Self>,
    ) -> Option<Self> {
        if let Some(prev) = previous
            && prev.resolve(directory).is_some()
        {
            return Some(prev.clone());
        }
        directory.pick_keyed(group, key).map(|r| Self::new(&r, ttl))
    }

    /// Like [`Self::reopen_keyed`] with a string routing key.
    #[must_use]
    pub fn reopen_str(
        directory: &ActorDirectory,
        group: &str,
        key: &str,
        ttl: Option<Duration>,
        previous: Option<&Self>,
    ) -> Option<Self> {
        let key = key.to_string();
        Self::reopen_keyed(directory, group, &key, ttl, previous)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trembita_proto::{ActorGroupName, ActorId, ActorRegistration, ActorTypeId, NodeId};

    fn reg(group: &str, node: u64, instance: u32) -> ActorRegistration {
        ActorRegistration {
            id: ActorId {
                name: ActorGroupName::try_from(group).expect("group name"),
                node: NodeId(node),
                instance,
                generation: 1,
            },
            actor_type: ActorTypeId::try_new("test").expect("actor type"),
            migratable: false,
            mailbox_depth: 0,
            uptime_secs: 0,
            messages_per_sec: 0.0,
        }
    }

    #[test]
    fn b36_reopen_reuses_live_session_before_rekeying() {
        let directory = ActorDirectory::new();
        let update = trembita_proto::DirectoryUpdate {
            node: NodeId(1),
            epoch: 1,
            registrations: vec![reg("workers", 1, 0)],
        };
        directory.apply(&update);
        let session = ActorSession::new(&reg("workers", 1, 0), None);
        let reopened =
            ActorSession::reopen_str(&directory, "workers", "user-1", None, Some(&session));
        assert_eq!(
            reopened.as_ref().map(|s| s.target()),
            Some(session.target())
        );
    }

    #[test]
    fn b36_reopen_keyed_picks_after_target_gone() {
        let directory = ActorDirectory::new();
        directory.apply(&trembita_proto::DirectoryUpdate {
            node: NodeId(1),
            epoch: 1,
            registrations: vec![reg("workers", 1, 0), reg("workers", 1, 1)],
        });
        let old = ActorSession::new(&reg("workers", 1, 0), None);
        directory.apply(&trembita_proto::DirectoryUpdate {
            node: NodeId(1),
            epoch: 2,
            registrations: vec![reg("workers", 1, 1)],
        });
        let reopened = ActorSession::reopen_str(&directory, "workers", "user-1", None, Some(&old));
        assert!(reopened.is_some());
        assert_ne!(reopened.as_ref().map(|s| s.target()), Some(old.target()));
    }

    #[test]
    fn b36_reopen_returns_none_when_group_has_no_registrations() {
        let directory = ActorDirectory::new();
        let old = ActorSession::new(&reg("workers", 1, 0), None);
        let reopened = ActorSession::reopen_str(&directory, "workers", "user-1", None, Some(&old));
        assert!(reopened.is_none());
    }
}
