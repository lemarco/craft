//! Durable event topic port ([event-topics](../../../docs/decisions/event-topics.md)).
//!
//! [`EventTopic`] is durable pub/sub: one publish, many named subscriptions with
//! independent cursors and lease/ack. Distinct from [`JobQueue`](super::queue::JobQueue)
//! (point-to-point). [`InMemoryEventTopic`] backs tests; production uses
//! [`RedbEventTopic`](super::redb_topic::RedbEventTopic).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crafty_proto::{NodeId, TopicReplicateOp};

pub use crate::queue::WorkerId;
use crate::queue::after_failed_attempt;
pub use crate::store::BoxFuture;

/// Monotonic event id within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

/// Lease token for an in-flight event delivery on a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicLeaseId(pub u64);

/// Batch of idempotent topic mutations replicated from the leader.
pub type TopicReplicationOps = Vec<TopicReplicateOp>;

/// An event handed to a subscriber under lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedEvent {
    /// Token required for ack/nack.
    pub lease_id: TopicLeaseId,
    /// Event id within the topic log.
    pub event_id: EventId,
    /// Opaque payload from publish.
    pub payload: Vec<u8>,
    /// Delivery attempts including this one (`1` on first delivery).
    pub attempts: u32,
}

/// Where a subscription starts reading when registered after events exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubscriptionStart {
    /// From the first retained event (`cursor = 0`, fan out backlog).
    Earliest,
    /// Only events published after registration (`cursor = head`).
    #[default]
    Latest,
}

/// Boot-time subscription declaration ([`EventTopic::register_subscriptions`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSubscriptionDef {
    /// Stable subscription name within the topic.
    pub name: String,
    /// Initial cursor when the subscription is first seen.
    pub start: SubscriptionStart,
    /// Retry ceiling (`0` = unlimited).
    pub max_attempts: u32,
}

/// Per-subscription gauges in [`TopicMetrics`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSubscriptionMetrics {
    /// Subscription name.
    pub subscription: String,
    /// Committed cursor (last ack'd event id).
    pub cursor: u64,
    /// Events behind log head (`head.saturating_sub(cursor)`).
    pub lag: u64,
    /// Ready but not yet leased.
    pub pending: u64,
    /// Currently leased to workers.
    pub leased: u64,
    /// Events discarded by retention for this subscription.
    pub retention_discards: u64,
}

/// Topic depth and lag gauges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetrics {
    /// Retained events in the log.
    pub event_count: u64,
    /// Log head (last assigned event id, `0` when empty).
    pub head: u64,
    /// Compaction floor — events at or below this id are removed.
    pub compact_head: u64,
    /// Age of the oldest retained event.
    pub oldest_event_age: Duration,
    /// Per-subscription lag and counters.
    pub subscriptions: Vec<TopicSubscriptionMetrics>,
}

/// Retention limits; exceeding them force-advances a lagging subscription cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRetentionOpts {
    /// Maximum age of the oldest retained event before forced discard.
    pub max_event_age: Duration,
    /// Maximum events retained in the log (approximate pressure via min cursor).
    pub max_retained_events: u64,
}

impl Default for TopicRetentionOpts {
    fn default() -> Self {
        Self {
            max_event_age: Duration::from_millis(crafty_proto::DEFAULT_TOPIC_MAX_EVENT_AGE_MS),
            max_retained_events: crafty_proto::DEFAULT_TOPIC_MAX_RETAINED_EVENTS,
        }
    }
}

/// What a topic subscriber knows about the delivery it is handling.
#[derive(Debug, Clone)]
pub struct TopicContext<'a> {
    /// Event id within the topic.
    pub event_id: EventId,
    /// Lease token backing this delivery.
    pub lease_id: TopicLeaseId,
    /// Topic name.
    pub topic: &'a str,
    /// Named subscription within the topic.
    pub subscription: &'a str,
    /// Delivery attempts including this one.
    pub attempts: u32,
}

impl TopicContext<'_> {
    /// Build a delivery context for tests and manual handler invocation.
    #[must_use]
    pub fn new<'a>(
        event_id: EventId,
        lease_id: TopicLeaseId,
        topic: &'a str,
        subscription: &'a str,
        attempts: u32,
    ) -> TopicContext<'a> {
        TopicContext {
            event_id,
            lease_id,
            topic,
            subscription,
            attempts,
        }
    }

    /// `true` when this is not the first delivery of the event on this subscription.
    #[must_use]
    pub fn is_redelivery(&self) -> bool {
        self.attempts > 1
    }
}

/// Errors from topic operations.
#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    /// Unknown topic or subscription.
    #[error("topic not found: {0}")]
    NotFound(String),
    /// Unknown subscription within a topic.
    #[error("subscription not found: {0}")]
    SubscriptionNotFound(String),
    /// Lease expired or belongs to another worker.
    #[error("invalid lease")]
    InvalidLease,
    /// Storage or wire failure.
    #[error("backend: {0}")]
    Backend(String),
    /// Encode/decode failure.
    #[error("codec: {0}")]
    Codec(String),
    /// Caller is not the Raft leader.
    #[error("not leader")]
    NotLeader,
}

/// Durable pub/sub with named subscriptions ([event-topics](../../../docs/decisions/event-topics.md)).
pub trait EventTopic: Send + Sync {
    /// Append one event and return replication ops for voters.
    ///
    /// # Errors
    /// Returns [`TopicError`] when the leader backend fails.
    fn publish_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(EventId, TopicReplicationOps), TopicError>>;

    /// Apply one replicated mutation (followers and idempotent replay).
    fn apply_replicate<'a>(
        &'a self,
        op: &'a TopicReplicateOp,
    ) -> BoxFuture<'a, Result<(), TopicError>>;

    /// Register subscriptions declared at build time.
    fn register_subscriptions<'a>(
        &'a self,
        subs: &'a [TopicSubscriptionDef],
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>>;

    /// Lease up to `max` events for `subscription`.
    fn lease_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<(Vec<LeasedEvent>, TopicReplicationOps), TopicError>>;

    /// Acknowledge one leased event (terminal success for that subscription cursor).
    fn ack_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>>;

    /// Return a leased event to pending or dead-letter.
    fn nack_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>>;

    /// Collect depth, lag, and age gauges.
    ///
    /// # Errors
    /// Returns [`TopicError`] on backend failure.
    fn metrics(&self) -> BoxFuture<'_, Result<TopicMetrics, TopicError>>;

    /// Enforce retention thresholds; may emit [`TopicReplicateOp::RetentionDiscard`].
    ///
    /// # Errors
    /// Returns [`TopicError`] on backend failure.
    fn enforce_retention_replicated(
        &self,
    ) -> BoxFuture<'_, Result<TopicReplicationOps, TopicError>>;

    /// Convenience publish for tests and single-node backends.
    fn publish(&self, payload: &[u8]) -> BoxFuture<'_, Result<EventId, TopicError>> {
        let payload = payload.to_vec();
        Box::pin(async move {
            let (id, _ops) = self.publish_replicated(&payload).await?;
            Ok(id)
        })
    }

    /// Convenience lease without exposing replication ops.
    fn lease<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<Vec<LeasedEvent>, TopicError>> {
        Box::pin(async move {
            let (events, _ops) = self.lease_replicated(subscription, worker, max).await?;
            Ok(events)
        })
    }

    /// Convenience ack without exposing replication ops.
    fn ack<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<(), TopicError>> {
        Box::pin(async move {
            let _ops = self.ack_replicated(subscription, worker, lease_id).await?;
            Ok(())
        })
    }

    /// Convenience nack without exposing replication ops.
    fn nack<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<(), TopicError>> {
        Box::pin(async move {
            let _ops = self.nack_replicated(subscription, worker, lease_id).await?;
            Ok(())
        })
    }
}

fn unix_ms_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn failed_delivery(attempts: u32, max_attempts: u32) -> (u32, bool) {
    let outcome = after_failed_attempt(attempts, max_attempts, unix_ms_now());
    (outcome.attempts, outcome.dead_letter)
}

fn instant_from_unix_ms(ms: u64) -> Instant {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if ms <= now_ms {
        now
    } else {
        now + Duration::from_millis(ms - now_ms)
    }
}

#[derive(Debug, Clone)]
struct StoredEvent {
    payload: Vec<u8>,
    published_at_ms: u64,
}

#[derive(Debug, Clone)]
struct SubState {
    cursor: u64,
    max_attempts: u32,
    retention_discards: u64,
    pending: VecDeque<u64>,
    leases: HashMap<u64, (u64, WorkerId, u32, Instant)>,
    dead_letter: BTreeMap<u64, u32>,
}

/// In-memory [`EventTopic`] for unit tests.
#[derive(Debug)]
pub struct InMemoryEventTopic {
    lease_timeout: Duration,
    retention: TopicRetentionOpts,
    next_event_id: Mutex<u64>,
    next_lease_id: Mutex<u64>,
    compact_head: Mutex<u64>,
    events: Mutex<BTreeMap<u64, StoredEvent>>,
    subs: Mutex<HashMap<String, SubState>>,
}

impl InMemoryEventTopic {
    /// Create a topic with the given lease timeout and retention defaults.
    #[must_use]
    pub fn new(lease_timeout: Duration) -> Self {
        Self {
            lease_timeout,
            retention: TopicRetentionOpts::default(),
            next_event_id: Mutex::new(1),
            next_lease_id: Mutex::new(1),
            compact_head: Mutex::new(0),
            events: Mutex::new(BTreeMap::new()),
            subs: Mutex::new(HashMap::new()),
        }
    }

    /// Override retention thresholds.
    #[must_use]
    pub fn retention(mut self, retention: TopicRetentionOpts) -> Self {
        self.retention = retention;
        self
    }

    fn head(&self) -> u64 {
        self.next_event_id
            .lock()
            .expect("poisoned")
            .saturating_sub(1)
    }

    fn min_cursor(subs: &HashMap<String, SubState>) -> u64 {
        subs.values().map(|s| s.cursor).min().unwrap_or(0)
    }

    fn compact_locked(
        events: &mut BTreeMap<u64, StoredEvent>,
        compact_head: &mut u64,
        subs: &mut HashMap<String, SubState>,
    ) -> Option<TopicReplicateOp> {
        let floor = Self::min_cursor(subs);
        if floor <= *compact_head {
            return None;
        }
        events.retain(|id, _| *id > floor);
        for sub in subs.values_mut() {
            sub.pending.retain(|id| *id > floor);
            sub.dead_letter.retain(|id, _| *id > floor);
        }
        *compact_head = floor;
        Some(TopicReplicateOp::CompactHead {
            compact_head: floor,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn apply_op_inner(&self, op: &TopicReplicateOp) -> Result<(), TopicError> {
        match op {
            TopicReplicateOp::Publish {
                event_id,
                payload,
                published_at_ms,
                next_event_id,
            } => {
                let mut events = self.events.lock().expect("poisoned");
                let mut subs = self.subs.lock().expect("poisoned");
                if !events.contains_key(event_id) {
                    events.insert(
                        *event_id,
                        StoredEvent {
                            payload: payload.clone(),
                            published_at_ms: *published_at_ms,
                        },
                    );
                    for sub in subs.values_mut() {
                        if *event_id > sub.cursor {
                            sub.pending.push_back(*event_id);
                        }
                    }
                }
                *self.next_event_id.lock().expect("poisoned") = *next_event_id;
            }
            TopicReplicateOp::RegisterSubscription {
                name,
                cursor,
                max_attempts,
            } => {
                let mut subs = self.subs.lock().expect("poisoned");
                if subs.contains_key(name) {
                    return Ok(());
                }
                let mut sub = SubState {
                    cursor: *cursor,
                    max_attempts: *max_attempts,
                    retention_discards: 0,
                    pending: VecDeque::new(),
                    leases: HashMap::new(),
                    dead_letter: BTreeMap::new(),
                };
                if *cursor == 0 {
                    let events = self.events.lock().expect("poisoned");
                    for id in events.keys() {
                        if *id > sub.cursor {
                            sub.pending.push_back(*id);
                        }
                    }
                }
                subs.insert(name.clone(), sub);
            }
            TopicReplicateOp::RemoveSubscription { name } => {
                let mut subs = self.subs.lock().expect("poisoned");
                subs.remove(name);
            }
            TopicReplicateOp::Lease {
                subscription,
                lease_id,
                event_id,
                worker_node,
                worker_instance,
                expires_at_ms,
                next_lease_id,
                attempts,
            } => {
                let mut subs = self.subs.lock().expect("poisoned");
                let sub = subs
                    .get_mut(subscription)
                    .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.clone()))?;
                if !sub.leases.contains_key(lease_id) {
                    sub.pending.retain(|id| id != event_id);
                    sub.leases.insert(
                        *lease_id,
                        (
                            *event_id,
                            WorkerId {
                                node: NodeId(*worker_node),
                                instance: *worker_instance,
                            },
                            *attempts,
                            instant_from_unix_ms(*expires_at_ms),
                        ),
                    );
                }
                *self.next_lease_id.lock().expect("poisoned") = *next_lease_id;
            }
            TopicReplicateOp::Ack {
                subscription,
                lease_id,
                event_id,
                cursor,
            } => {
                let mut subs = self.subs.lock().expect("poisoned");
                let sub = subs
                    .get_mut(subscription)
                    .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.clone()))?;
                sub.leases.remove(lease_id);
                sub.dead_letter.remove(event_id);
                sub.cursor = sub.cursor.max(*cursor).max(*event_id);
            }
            TopicReplicateOp::Nack {
                subscription,
                lease_id,
                event_id,
                attempts,
                dead_letter,
            }
            | TopicReplicateOp::Reclaim {
                subscription,
                lease_id,
                event_id,
                attempts,
                dead_letter,
            } => {
                let mut subs = self.subs.lock().expect("poisoned");
                let sub = subs
                    .get_mut(subscription)
                    .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.clone()))?;
                sub.leases.remove(lease_id);
                if *dead_letter {
                    sub.dead_letter.insert(*event_id, *attempts);
                } else {
                    sub.pending.push_back(*event_id);
                }
            }
            TopicReplicateOp::RetentionDiscard {
                subscription,
                cursor,
                discarded,
            } => {
                let mut subs = self.subs.lock().expect("poisoned");
                let sub = subs
                    .get_mut(subscription)
                    .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.clone()))?;
                sub.pending.retain(|id| *id > *cursor);
                sub.leases.retain(|_, (eid, _, _, _)| *eid > *cursor);
                sub.dead_letter.retain(|id, _| *id > *cursor);
                sub.cursor = sub.cursor.max(*cursor);
                sub.retention_discards = sub.retention_discards.saturating_add(*discarded);
            }
            TopicReplicateOp::CompactHead { compact_head } => {
                let mut events = self.events.lock().expect("poisoned");
                let mut subs = self.subs.lock().expect("poisoned");
                let mut head = self.compact_head.lock().expect("poisoned");
                events.retain(|id, _| *id > *compact_head);
                for sub in subs.values_mut() {
                    sub.pending.retain(|id| *id > *compact_head);
                    sub.dead_letter.retain(|id, _| *id > *compact_head);
                }
                *head = *compact_head;
            }
        }
        Ok(())
    }

    fn reclaim_expired_ops(&self) -> TopicReplicationOps {
        let mut ops = Vec::new();
        let mut subs = self.subs.lock().expect("poisoned");
        let sub_names: Vec<String> = subs.keys().cloned().collect();
        for sub_name in sub_names {
            let Some(sub) = subs.get_mut(&sub_name) else {
                continue;
            };
            let max_attempts = sub.max_attempts;
            let expired: Vec<(u64, u64, u32)> = sub
                .leases
                .iter()
                .filter_map(|(lease_id, (event_id, _, attempts, expires))| {
                    if *expires <= Instant::now() {
                        Some((*lease_id, *event_id, *attempts))
                    } else {
                        None
                    }
                })
                .collect();
            for (lease_id, event_id, attempts) in expired {
                sub.leases.remove(&lease_id);
                let (attempts, dead) = failed_delivery(attempts, max_attempts);
                if dead {
                    sub.dead_letter.insert(event_id, attempts);
                } else {
                    sub.pending.push_back(event_id);
                }
                ops.push(TopicReplicateOp::Reclaim {
                    subscription: sub_name.clone(),
                    lease_id,
                    event_id,
                    attempts,
                    dead_letter: dead,
                });
            }
        }
        ops
    }
}

impl EventTopic for InMemoryEventTopic {
    fn publish_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(EventId, TopicReplicationOps), TopicError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops();
            let event_id = {
                let mut next = self.next_event_id.lock().expect("poisoned");
                let id = *next;
                *next = next.saturating_add(1);
                id
            };
            let published_at_ms = unix_ms_now();
            let next_event_id = *self.next_event_id.lock().expect("poisoned");
            ops.push(TopicReplicateOp::Publish {
                event_id,
                payload: payload.to_vec(),
                published_at_ms,
                next_event_id,
            });
            for op in &ops {
                self.apply_op_inner(op)?;
            }
            let mut subs = self.subs.lock().expect("poisoned");
            let mut events = self.events.lock().expect("poisoned");
            let mut compact_head = self.compact_head.lock().expect("poisoned");
            if let Some(op) = Self::compact_locked(&mut events, &mut compact_head, &mut subs) {
                ops.push(op);
            }
            Ok((EventId(event_id), ops))
        })
    }

    fn apply_replicate<'a>(
        &'a self,
        op: &'a TopicReplicateOp,
    ) -> BoxFuture<'a, Result<(), TopicError>> {
        Box::pin(async move {
            self.apply_op_inner(op)?;
            if matches!(
                op,
                TopicReplicateOp::Ack { .. }
                    | TopicReplicateOp::RetentionDiscard { .. }
                    | TopicReplicateOp::RemoveSubscription { .. }
            ) {
                let mut subs = self.subs.lock().expect("poisoned");
                let mut events = self.events.lock().expect("poisoned");
                let mut compact_head = self.compact_head.lock().expect("poisoned");
                if let Some(compact) =
                    Self::compact_locked(&mut events, &mut compact_head, &mut subs)
                {
                    let _ = compact;
                }
            }
            Ok(())
        })
    }

    fn register_subscriptions<'a>(
        &'a self,
        defs: &'a [TopicSubscriptionDef],
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let head = self.head();
            let mut ops = Vec::new();
            for def in defs {
                let cursor = match def.start {
                    SubscriptionStart::Earliest => 0,
                    SubscriptionStart::Latest => head,
                };
                ops.push(TopicReplicateOp::RegisterSubscription {
                    name: def.name.clone(),
                    cursor,
                    max_attempts: def.max_attempts,
                });
            }
            for op in &ops {
                self.apply_op_inner(op)?;
            }
            Ok(ops)
        })
    }

    fn lease_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<(Vec<LeasedEvent>, TopicReplicationOps), TopicError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops();
            for op in &ops {
                self.apply_op_inner(op)?;
            }
            let mut subs = self.subs.lock().expect("poisoned");
            let sub = subs
                .get_mut(subscription)
                .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.to_string()))?;
            let expires_at_ms = unix_ms_now()
                .saturating_add(u64::try_from(self.lease_timeout.as_millis()).unwrap_or(u64::MAX));
            let mut out = Vec::new();
            let mut next_lease = *self.next_lease_id.lock().expect("poisoned");
            let events_map = self.events.lock().expect("poisoned");
            for _ in 0..max {
                let Some(event_id) = sub.pending.pop_front() else {
                    break;
                };
                let Some(stored) = events_map.get(&event_id) else {
                    continue;
                };
                let attempts = sub
                    .dead_letter
                    .get(&event_id)
                    .map_or(1, |a| a.saturating_add(1));
                let lease_id = next_lease;
                next_lease = next_lease.saturating_add(1);
                sub.leases.insert(
                    lease_id,
                    (
                        event_id,
                        worker,
                        attempts,
                        instant_from_unix_ms(expires_at_ms),
                    ),
                );
                ops.push(TopicReplicateOp::Lease {
                    subscription: subscription.to_string(),
                    lease_id,
                    event_id,
                    worker_node: worker.node.0,
                    worker_instance: worker.instance,
                    expires_at_ms,
                    next_lease_id: next_lease,
                    attempts,
                });
                out.push(LeasedEvent {
                    lease_id: TopicLeaseId(lease_id),
                    event_id: EventId(event_id),
                    payload: stored.payload.clone(),
                    attempts,
                });
            }
            *self.next_lease_id.lock().expect("poisoned") = next_lease;
            Ok((out, ops))
        })
    }

    fn ack_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let mut subs = self.subs.lock().expect("poisoned");
            let sub = subs
                .get_mut(subscription)
                .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.to_string()))?;
            let Some((event_id, owner, _, _)) = sub.leases.remove(&lease_id.0) else {
                return Err(TopicError::InvalidLease);
            };
            if owner != worker {
                sub.leases
                    .insert(lease_id.0, (event_id, owner, 0, Instant::now()));
                return Err(TopicError::InvalidLease);
            }
            let cursor = sub.cursor.max(event_id);
            sub.cursor = cursor;
            sub.dead_letter.remove(&event_id);
            let mut ops = vec![TopicReplicateOp::Ack {
                subscription: subscription.to_string(),
                lease_id: lease_id.0,
                event_id,
                cursor,
            }];
            drop(subs);
            for op in &ops {
                self.apply_op_inner(op)?;
            }
            let mut subs = self.subs.lock().expect("poisoned");
            let mut events = self.events.lock().expect("poisoned");
            let mut compact_head = self.compact_head.lock().expect("poisoned");
            if let Some(op) = Self::compact_locked(&mut events, &mut compact_head, &mut subs) {
                ops.push(op);
            }
            Ok(ops)
        })
    }

    fn nack_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let mut subs = self.subs.lock().expect("poisoned");
            let sub = subs
                .get_mut(subscription)
                .ok_or_else(|| TopicError::SubscriptionNotFound(subscription.to_string()))?;
            let Some((event_id, owner, attempts, _)) = sub.leases.remove(&lease_id.0) else {
                return Err(TopicError::InvalidLease);
            };
            if owner != worker {
                sub.leases
                    .insert(lease_id.0, (event_id, owner, attempts, Instant::now()));
                return Err(TopicError::InvalidLease);
            }
            let (attempts, dead) = failed_delivery(attempts, sub.max_attempts);
            if dead {
                sub.dead_letter.insert(event_id, attempts);
            } else {
                sub.pending.push_back(event_id);
            }
            let ops = vec![TopicReplicateOp::Nack {
                subscription: subscription.to_string(),
                lease_id: lease_id.0,
                event_id,
                attempts,
                dead_letter: dead,
            }];
            drop(subs);
            for op in &ops {
                self.apply_op_inner(op)?;
            }
            Ok(ops)
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<TopicMetrics, TopicError>> {
        Box::pin(async move {
            let events = self.events.lock().expect("poisoned");
            let subs = self.subs.lock().expect("poisoned");
            let compact_head = *self.compact_head.lock().expect("poisoned");
            let head = self.head();
            let oldest_age = events
                .values()
                .map(|e| unix_ms_now().saturating_sub(e.published_at_ms))
                .max()
                .unwrap_or(0);
            let subscriptions = subs
                .iter()
                .map(|(name, sub)| TopicSubscriptionMetrics {
                    subscription: name.clone(),
                    cursor: sub.cursor,
                    lag: head.saturating_sub(sub.cursor),
                    pending: u64::try_from(sub.pending.len()).unwrap_or(u64::MAX),
                    leased: u64::try_from(sub.leases.len()).unwrap_or(u64::MAX),
                    retention_discards: sub.retention_discards,
                })
                .collect();
            Ok(TopicMetrics {
                event_count: u64::try_from(events.len()).unwrap_or(u64::MAX),
                head,
                compact_head,
                oldest_event_age: Duration::from_millis(oldest_age),
                subscriptions,
            })
        })
    }

    fn enforce_retention_replicated(
        &self,
    ) -> BoxFuture<'_, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let head = self.head();
            let events = self.events.lock().expect("poisoned");
            let oldest_age_ms = events
                .values()
                .map(|e| unix_ms_now().saturating_sub(e.published_at_ms))
                .max()
                .unwrap_or(0);
            let event_count = events.len();
            drop(events);
            let max_age_ms =
                u64::try_from(self.retention.max_event_age.as_millis()).unwrap_or(u64::MAX);
            let mut ops = Vec::new();
            if oldest_age_ms > max_age_ms
                || u64::try_from(event_count).unwrap_or(u64::MAX)
                    > self.retention.max_retained_events
            {
                let mut subs = self.subs.lock().expect("poisoned");
                for (name, sub) in subs.iter_mut() {
                    let lag = head.saturating_sub(sub.cursor);
                    if lag == 0 {
                        continue;
                    }
                    let new_cursor = head;
                    let discarded = lag;
                    sub.pending.clear();
                    sub.leases.retain(|_, (eid, _, _, _)| *eid > new_cursor);
                    sub.dead_letter.retain(|id, _| *id > new_cursor);
                    sub.cursor = new_cursor;
                    sub.retention_discards = sub.retention_discards.saturating_add(discarded);
                    ops.push(TopicReplicateOp::RetentionDiscard {
                        subscription: name.clone(),
                        cursor: new_cursor,
                        discarded,
                    });
                }
            }
            for op in &ops {
                self.apply_op_inner(op)?;
            }
            let mut subs = self.subs.lock().expect("poisoned");
            let mut events = self.events.lock().expect("poisoned");
            let mut compact_head = self.compact_head.lock().expect("poisoned");
            if let Some(op) = Self::compact_locked(&mut events, &mut compact_head, &mut subs) {
                ops.push(op);
            }
            Ok(ops)
        })
    }
}

/// Poll an [`EventTopic`], invoke `handle` on each leased event, then ack or nack.
///
/// Runs until `stop` is set. When empty, sleeps `idle_sleep` between polls.
#[allow(clippy::too_many_arguments)]
pub async fn run_topic_subscriber<T, F, Fut, E>(
    topic: std::sync::Arc<T>,
    _topic_name: &str,
    subscription: &str,
    worker: WorkerId,
    batch: usize,
    idle_sleep: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut handle: F,
) where
    T: EventTopic + ?Sized,
    F: FnMut(&LeasedEvent) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    loop {
        if *stop.borrow() {
            break;
        }
        let Ok(events) = topic.lease(subscription, worker, batch.max(1)).await else {
            tokio::time::sleep(idle_sleep).await;
            continue;
        };
        if events.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(idle_sleep) => {}
                _ = stop.changed() => {
                    if *stop.borrow() {
                        break;
                    }
                }
            }
            continue;
        }
        for event in events {
            match handle(&event).await {
                Ok(()) => {
                    let _ = topic.ack(subscription, worker, event.lease_id).await;
                }
                Err(_) => {
                    let _ = topic.nack(subscription, worker, event.lease_id).await;
                }
            }
        }
        if *stop.borrow() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: u64) -> WorkerId {
        WorkerId {
            node: NodeId(id),
            instance: 0,
        }
    }

    async fn register_three(topic: &InMemoryEventTopic) {
        topic
            .register_subscriptions(&[
                TopicSubscriptionDef {
                    name: "a".into(),
                    start: SubscriptionStart::Earliest,
                    max_attempts: 0,
                },
                TopicSubscriptionDef {
                    name: "b".into(),
                    start: SubscriptionStart::Earliest,
                    max_attempts: 0,
                },
                TopicSubscriptionDef {
                    name: "c".into(),
                    start: SubscriptionStart::Earliest,
                    max_attempts: 0,
                },
            ])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn three_subscriptions_see_all_events() {
        let topic = InMemoryEventTopic::new(Duration::from_secs(30));
        register_three(&topic).await;
        for i in 0..5 {
            topic.publish(format!("e{i}").as_bytes()).await.unwrap();
        }
        for sub in ["a", "b", "c"] {
            let mut seen = Vec::new();
            for _ in 0..5 {
                let batch = topic.lease(sub, worker(1), 10).await.unwrap();
                for ev in batch {
                    seen.push(ev.payload);
                    topic.ack(sub, worker(1), ev.lease_id).await.unwrap();
                }
            }
            assert_eq!(seen.len(), 5);
        }
    }

    #[tokio::test]
    async fn slow_subscription_does_not_block_fast_ones() {
        let topic = InMemoryEventTopic::new(Duration::from_secs(30));
        register_three(&topic).await;
        topic.publish(b"1").await.unwrap();
        topic.publish(b"2").await.unwrap();
        let slow = topic.lease("a", worker(1), 2).await.unwrap();
        assert_eq!(slow.len(), 2);
        let fast = topic.lease("b", worker(2), 10).await.unwrap();
        assert_eq!(fast.len(), 2);
        for ev in fast {
            topic.ack("b", worker(2), ev.lease_id).await.unwrap();
        }
        let metrics = topic.metrics().await.unwrap();
        let b = metrics
            .subscriptions
            .iter()
            .find(|s| s.subscription == "b")
            .unwrap();
        assert_eq!(b.cursor, 2);
        let a = metrics
            .subscriptions
            .iter()
            .find(|s| s.subscription == "a")
            .unwrap();
        assert_eq!(a.leased, 2);
    }

    #[tokio::test]
    async fn late_subscription_starts_at_latest() {
        let topic = InMemoryEventTopic::new(Duration::from_secs(30));
        topic
            .register_subscriptions(&[TopicSubscriptionDef {
                name: "early".into(),
                start: SubscriptionStart::Earliest,
                max_attempts: 0,
            }])
            .await
            .unwrap();
        topic.publish(b"before").await.unwrap();
        let ev = topic.lease("early", worker(1), 1).await.unwrap();
        topic.ack("early", worker(1), ev[0].lease_id).await.unwrap();
        topic
            .register_subscriptions(&[TopicSubscriptionDef {
                name: "late".into(),
                start: SubscriptionStart::Latest,
                max_attempts: 0,
            }])
            .await
            .unwrap();
        topic.publish(b"after").await.unwrap();
        let late = topic.lease("late", worker(1), 10).await.unwrap();
        assert_eq!(late.len(), 1);
        assert_eq!(late[0].payload, b"after");
    }

    #[tokio::test]
    async fn retention_threshold_discards_lagging_subscription() {
        let topic =
            InMemoryEventTopic::new(Duration::from_secs(30)).retention(TopicRetentionOpts {
                max_event_age: Duration::from_millis(0),
                max_retained_events: 1,
            });
        register_three(&topic).await;
        topic.publish(b"1").await.unwrap();
        topic.publish(b"2").await.unwrap();
        let _leased = topic.lease("a", worker(1), 2).await.unwrap();
        topic.enforce_retention_replicated().await.unwrap();
        let metrics = topic.metrics().await.unwrap();
        let a = metrics
            .subscriptions
            .iter()
            .find(|s| s.subscription == "a")
            .unwrap();
        assert!(a.retention_discards >= 1);
        assert_eq!(a.cursor, 2);
    }

    #[tokio::test]
    async fn removing_subscription_advances_compact_head() {
        let topic = InMemoryEventTopic::new(Duration::from_secs(30));
        register_three(&topic).await;
        topic.publish(b"1").await.unwrap();
        let ev = topic.lease("a", worker(1), 1).await.unwrap();
        topic.ack("a", worker(1), ev[0].lease_id).await.unwrap();
        topic
            .apply_replicate(&TopicReplicateOp::RemoveSubscription { name: "b".into() })
            .await
            .unwrap();
        topic
            .apply_replicate(&TopicReplicateOp::RemoveSubscription { name: "c".into() })
            .await
            .unwrap();
        let metrics = topic.metrics().await.unwrap();
        assert_eq!(metrics.compact_head, 1);
        assert_eq!(metrics.event_count, 0);
    }
}
