//! Durable event topic wire types ([event-topics](../../../docs/decisions/event-topics.md)).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Monotonic event id within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TopicEventId(pub u64);

/// Lease id for an in-flight event delivery on a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicLeaseId(pub u64);

/// Publish one event (`POST /raft/v1/topic/publish`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicPublishRequest {
    /// Topic name.
    pub topic: String,
    /// Opaque event body.
    pub payload: Vec<u8>,
}

/// Response to [`TopicPublishRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicPublishReply {
    /// Assigned event id when successful.
    pub event_id: u64,
    /// Set when publish failed.
    pub error: Option<String>,
}

/// Lease events for a subscription (`POST /raft/v1/topic/lease`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicLeaseRequest {
    /// Topic name.
    pub topic: String,
    /// Named subscription within the topic.
    pub subscription: String,
    /// Worker node id.
    pub worker_node: u64,
    /// Worker instance index on the node.
    pub worker_instance: u32,
    /// Maximum events to lease.
    pub max: u32,
}

/// One leased event in [`TopicLeaseReply::events`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicLeasedEventWire {
    /// Lease handle for ack/nack.
    pub lease_id: u64,
    /// Event id.
    pub event_id: u64,
    /// Event body.
    pub payload: Vec<u8>,
    /// Delivery attempts so far (including this lease).
    pub attempts: u32,
}

/// Response to [`TopicLeaseRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicLeaseReply {
    /// Leased events (may be empty).
    pub events: Vec<TopicLeasedEventWire>,
    /// Set when lease failed.
    pub error: Option<String>,
}

/// Acknowledge a leased event (`POST /raft/v1/topic/ack`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicAckRequest {
    /// Topic name.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
    /// Worker node id.
    pub worker_node: u64,
    /// Worker instance index.
    pub worker_instance: u32,
    /// Lease to acknowledge.
    pub lease_id: u64,
}

/// Response to [`TopicAckRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicAckReply {
    /// Set when ack failed.
    pub error: Option<String>,
}

/// Negative-acknowledge a leased event (`POST /raft/v1/topic/nack`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicNackRequest {
    /// Topic name.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
    /// Worker node id.
    pub worker_node: u64,
    /// Worker instance index.
    pub worker_instance: u32,
    /// Lease to return.
    pub lease_id: u64,
}

/// Response to [`TopicNackRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicNackReply {
    /// Set when nack failed.
    pub error: Option<String>,
}

/// Per-subscription gauges in [`TopicMetricsReply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSubscriptionMetricsWire {
    /// Subscription name.
    pub subscription: String,
    /// Committed cursor (last ack'd event id).
    pub cursor: u64,
    /// Events behind the log head (`head - cursor`).
    pub lag: u64,
    /// Ready but not yet leased.
    pub pending: u64,
    /// Currently leased to workers.
    pub leased: u64,
    /// Events discarded by retention for this subscription.
    pub retention_discards: u64,
}

/// Topic depth gauges (`POST /raft/v1/topic/metrics`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMetricsRequest {
    /// Topic name.
    pub topic: String,
}

/// Response to [`TopicMetricsRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMetricsReply {
    /// Total retained events.
    pub event_count: u64,
    /// Log head (last assigned event id, `0` when empty).
    pub head: u64,
    /// Compaction floor — events at or below this id are removed.
    pub compact_head: u64,
    /// Age of the oldest retained event.
    pub oldest_event_age_ms: u64,
    /// Per-subscription lag and counters.
    pub subscriptions: Vec<TopicSubscriptionMetricsWire>,
    /// Set when metrics collection failed.
    pub error: Option<String>,
}

/// Idempotent topic mutation replicated from the leader (`POST /raft/v1/topic/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicReplicateOp {
    /// Append one event and fan out to every registered subscription pending set.
    Publish {
        /// Assigned event id.
        event_id: u64,
        /// Event body.
        payload: Vec<u8>,
        /// Publish timestamp (unix ms).
        published_at_ms: u64,
        /// Next event id after this publish.
        next_event_id: u64,
    },
    /// Move an event from pending to leased for one subscription.
    Lease {
        /// Subscription name.
        subscription: String,
        /// Lease id.
        lease_id: u64,
        /// Event id.
        event_id: u64,
        /// Worker node.
        worker_node: u64,
        /// Worker instance.
        worker_instance: u32,
        /// Lease expiry (unix ms).
        expires_at_ms: u64,
        /// Next lease id counter.
        next_lease_id: u64,
        /// Attempt count for this delivery.
        attempts: u32,
    },
    /// Terminal success — advance subscription cursor.
    Ack {
        /// Subscription name.
        subscription: String,
        /// Released lease id.
        lease_id: u64,
        /// Acknowledged event id.
        event_id: u64,
        /// New committed cursor for the subscription.
        cursor: u64,
    },
    /// Retry or dead-letter one leased event.
    Nack {
        /// Subscription name.
        subscription: String,
        /// Released lease id.
        lease_id: u64,
        /// Event id.
        event_id: u64,
        /// Updated attempt count.
        attempts: u32,
        /// When true the event is not requeued.
        dead_letter: bool,
    },
    /// Visibility timeout expired — requeue or dead-letter.
    Reclaim {
        /// Subscription name.
        subscription: String,
        /// Released lease id.
        lease_id: u64,
        /// Event id.
        event_id: u64,
        /// Updated attempt count.
        attempts: u32,
        /// When true the event is not requeued.
        dead_letter: bool,
    },
    /// Register a subscription at boot with an initial cursor.
    RegisterSubscription {
        /// Subscription name.
        name: String,
        /// Initial cursor (`0` = before first event; `head` = only new events).
        cursor: u64,
        /// Retry ceiling (`0` = unlimited).
        max_attempts: u32,
    },
    /// Remove a subscription and advance compaction floor if it was the minimum cursor.
    RemoveSubscription {
        /// Subscription name.
        name: String,
    },
    /// Retention forced cursor forward for a lagging subscription.
    RetentionDiscard {
        /// Subscription name.
        subscription: String,
        /// New cursor after discarding lagging events.
        cursor: u64,
        /// Number of events discarded in this operation.
        discarded: u64,
    },
    /// Physical delete of events at or below `compact_head`.
    CompactHead {
        /// New compaction floor.
        compact_head: u64,
    },
}

/// Batch replication from the topic leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicReplicateRequest {
    /// Target topic.
    pub topic: String,
    /// Idempotent mutations in order.
    pub ops: Vec<TopicReplicateOp>,
    /// Declared Raft leader id.
    pub leader_id: u64,
}

/// Response to [`TopicReplicateRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicReplicateReply {
    /// Set when apply failed.
    pub error: Option<String>,
}

/// Default retention when not configured (7 days).
pub const DEFAULT_TOPIC_MAX_EVENT_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Default max retained events per topic (1M).
pub const DEFAULT_TOPIC_MAX_RETAINED_EVENTS: u64 = 1_000_000;

/// Convert a duration to milliseconds for wire metrics.
#[must_use]
pub fn duration_to_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
