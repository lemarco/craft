//! BEAM `:telemetry`-style event stream (ADR 026 §3).
//!
//! The runtime emits [`CraftEvent`]s onto an [`EventBus`] (a bounded broadcast
//! channel). Subscribers — the live dashboard's SSE feed, user sinks via
//! `cluster.events()`, log forwarders — receive them without ever blocking the
//! actor mailbox or Raft loop: a slow subscriber's lagged events are dropped
//! and counted (never backpressured onto producers).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// Why an actor stopped (surfaced in [`CraftEvent::ActorStopped`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    /// Normal, requested shutdown.
    Normal,
    /// The actor's `handle` returned an error.
    Failure,
    /// The restart budget was exhausted (ADR 026 §5) and the actor escalated.
    RestartLimit,
    /// Stopped because it migrated to another node (ADR 013).
    Migrated,
}

/// A telemetry event describing something that happened in the cluster.
///
/// Serializable so it can be streamed as JSON over SSE and to user sinks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CraftEvent {
    /// An actor was spawned.
    ActorSpawned {
        /// Actor identity (registry key / instance).
        id: String,
    },
    /// An actor stopped.
    ActorStopped {
        /// Actor identity.
        id: String,
        /// Why it stopped.
        reason: StopReason,
    },
    /// An actor was restarted by its supervisor.
    ActorRestarted {
        /// Actor identity.
        id: String,
        /// Cumulative restart count.
        count: u32,
    },
    /// An actor migrated between nodes.
    ActorMigrated {
        /// Actor identity.
        id: String,
        /// Source node.
        from: u64,
        /// Destination node.
        to: u64,
    },
    /// A mailbox depth sample.
    MailboxDepth {
        /// Actor identity.
        id: String,
        /// Queued message count.
        len: u64,
    },
    /// A message was handled, with its latency in milliseconds.
    MessageHandled {
        /// Actor identity.
        id: String,
        /// Handling latency in milliseconds.
        latency_ms: u64,
    },
    /// A node joined the cluster.
    NodeJoined {
        /// Joining node id.
        node_id: u64,
    },
    /// A node left the cluster.
    NodeLeft {
        /// Departing node id.
        node_id: u64,
        /// Whether the departure was graceful.
        graceful: bool,
    },
    /// Leadership changed.
    LeaderChanged {
        /// New term.
        term: u64,
        /// New leader.
        leader: u64,
    },
    /// An opt-in per-message trace record (ADR 026 §7).
    MessageTraced {
        /// Traced actor identity.
        id: String,
        /// Human-readable message summary.
        message: String,
    },
    /// Multi-Raft group placement was recomputed on the leader (ADR 031).
    RaftGroupsRebalanced {
        /// Groups adopted on this node.
        adopt: Vec<u32>,
        /// Groups retired on this node.
        retire: Vec<u32>,
    },
}

/// Options for opt-in per-message tracing (ADR 026 §7). Off by default; when
/// enabled it auto-expires after `duration` so tracing never runs cluster-wide
/// indefinitely.
#[derive(Debug, Clone)]
pub struct TraceOpts {
    /// Emit a [`CraftEvent::MessageTraced`] per handled message.
    pub messages: bool,
    /// How long the trace stays enabled before auto-expiring.
    pub duration: Duration,
}

impl Default for TraceOpts {
    fn default() -> Self {
        Self {
            messages: true,
            duration: Duration::from_secs(30),
        }
    }
}

/// A bounded broadcast bus for [`CraftEvent`]s.
///
/// Cloning shares the same channel. [`emit`](EventBus::emit) never blocks; if
/// the ring buffer is full for a lagging subscriber, that subscriber observes a
/// lag error on `recv` and the [`dropped`](EventBus::dropped) counter advances.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<CraftEvent>,
    dropped: Arc<AtomicU64>,
}

impl EventBus {
    /// Create a bus buffering up to `capacity` events per subscriber.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(1));
        Self {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Emit an event to all current subscribers. Never blocks; returns the
    /// number of subscribers that received it (0 if none are listening).
    pub fn emit(&self, event: CraftEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Subscribe to future events.
    #[must_use]
    pub fn subscribe(&self) -> EventSubscription {
        EventSubscription {
            rx: self.tx.subscribe(),
            dropped: Arc::clone(&self.dropped),
        }
    }

    /// Total events dropped so far due to slow subscribers.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(1024)
    }
}

/// A single subscription to an [`EventBus`]. Counts its own lagged drops into
/// the shared [`EventBus::dropped`] counter.
pub struct EventSubscription {
    rx: broadcast::Receiver<CraftEvent>,
    dropped: Arc<AtomicU64>,
}

impl EventSubscription {
    /// Receive the next event, transparently accounting for (and skipping past)
    /// lagged/dropped events. Returns `None` once the bus is closed.
    pub async fn recv(&mut self) -> Option<CraftEvent> {
        loop {
            match self.rx.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    self.dropped.fetch_add(n, Ordering::Relaxed);
                    // Skip the gap and keep reading current events.
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribers_receive_emitted_events() {
        let bus = EventBus::new(8);
        let mut sub = bus.subscribe();
        assert_eq!(bus.emit(CraftEvent::NodeJoined { node_id: 3 }), 1);
        assert_eq!(
            sub.recv().await,
            Some(CraftEvent::NodeJoined { node_id: 3 })
        );
    }

    #[tokio::test]
    async fn slow_subscriber_drops_are_counted() {
        let bus = EventBus::new(2);
        let mut sub = bus.subscribe();
        // Overflow the 2-slot ring without draining.
        for id in 0..5 {
            bus.emit(CraftEvent::NodeJoined { node_id: id });
        }
        // The next recv skips the lagged gap and yields the most recent events.
        let ev = sub.recv().await.expect("event after lag");
        assert!(matches!(ev, CraftEvent::NodeJoined { .. }));
        assert!(bus.dropped() >= 1, "dropped events should be counted");
    }

    #[test]
    fn events_serialize_to_tagged_json() {
        let json =
            serde_json::to_string(&CraftEvent::LeaderChanged { term: 4, leader: 2 }).unwrap();
        assert_eq!(json, r#"{"event":"leader_changed","term":4,"leader":2}"#);
    }
}
