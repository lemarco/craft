//! Durable [`EventTopic`](super::topic::EventTopic) backed by `redb`
//! ([event-topics](../../../docs/decisions/event-topics.md)).
//!
//! One `{data_dir}/topic-{name}.redb` file per topic.

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use trembita_proto::{TopicReplicateOp, decode, encode};

use super::topic::{
    EventId, EventTopic, LeasedEvent, SubscriptionStart, TopicContext, TopicError, TopicLeaseId,
    TopicMetrics, TopicReplicationOps, TopicRetentionOpts, TopicSubscriptionDef,
    TopicSubscriptionMetrics,
};
use {
    trembita_actor_store::BoxFuture,
    trembita_jobs::{WorkerId, after_failed_attempt},
};

const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("topic_events");
const SUBS: TableDefinition<&str, &[u8]> = TableDefinition::new("topic_subs");
const SUB_PENDING: TableDefinition<&[u8], ()> = TableDefinition::new("topic_sub_pending");
const SUB_LEASES: TableDefinition<u64, &[u8]> = TableDefinition::new("topic_sub_leases");
const SUB_DEAD: TableDefinition<&[u8], &[u8]> = TableDefinition::new("topic_sub_dead");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("topic_meta");

const K_NEXT_EVENT: &str = "next_event_id";
const K_NEXT_LEASE: &str = "next_lease_id";
const K_COMPACT_HEAD: &str = "compact_head";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredEvent {
    payload: Vec<u8>,
    published_at_ms: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredSub {
    cursor: u64,
    max_attempts: u32,
    retention_discards: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredLease {
    subscription: String,
    event_id: u64,
    worker_node: u64,
    worker_instance: u32,
    expires_at_ms: u64,
    attempts: u32,
}

fn backend(e: impl std::fmt::Display) -> TopicError {
    TopicError::Backend(e.to_string())
}

fn codec(e: impl std::fmt::Display) -> TopicError {
    TopicError::Codec(e.to_string())
}

fn pending_key(sub: &str, event_id: u64) -> Vec<u8> {
    let mut key = sub.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&event_id.to_be_bytes());
    key
}

fn parse_pending_key(key: &[u8]) -> Option<(String, u64)> {
    let sep = key.iter().position(|b| *b == 0)?;
    let sub = std::str::from_utf8(&key[..sep]).ok()?.to_string();
    let id_bytes: [u8; 8] = key.get(sep + 1..)?.try_into().ok()?;
    Some((sub, u64::from_be_bytes(id_bytes)))
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Crash-safe [`EventTopic`] in a dedicated `redb` file.
#[derive(Debug)]
pub struct RedbEventTopic {
    lease_timeout: Duration,
    retention: TopicRetentionOpts,
    db: Mutex<Database>,
}

impl RedbEventTopic {
    /// Open or create the topic database at `path`.
    ///
    /// # Errors
    /// Returns [`TopicError::Backend`] if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>, lease_timeout: Duration) -> Result<Self, TopicError> {
        let db = Mutex::new(Database::create(path).map_err(backend)?);
        let topic = Self {
            lease_timeout,
            retention: TopicRetentionOpts::default(),
            db,
        };
        topic.bootstrap()?;
        Ok(topic)
    }

    /// Override retention thresholds.
    #[must_use]
    pub fn retention(mut self, retention: TopicRetentionOpts) -> Self {
        self.retention = retention;
        self
    }

    fn bootstrap(&self) -> Result<(), TopicError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            txn.open_table(EVENTS).map_err(backend)?;
            txn.open_table(SUBS).map_err(backend)?;
            txn.open_table(SUB_PENDING).map_err(backend)?;
            txn.open_table(SUB_LEASES).map_err(backend)?;
            txn.open_table(SUB_DEAD).map_err(backend)?;
            let mut meta = txn.open_table(META).map_err(backend)?;
            if meta.get(K_NEXT_EVENT).map_err(backend)?.is_none() {
                meta.insert(K_NEXT_EVENT, encode(&1u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
            if meta.get(K_NEXT_LEASE).map_err(backend)?.is_none() {
                meta.insert(K_NEXT_LEASE, encode(&1u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
            if meta.get(K_COMPACT_HEAD).map_err(backend)?.is_none() {
                meta.insert(K_COMPACT_HEAD, encode(&0u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn read_meta_u64(&self, key: &str) -> Result<u64, TopicError> {
        let txn = self
            .db
            .lock()
            .expect("poisoned")
            .begin_read()
            .map_err(backend)?;
        let table = txn.open_table(META).map_err(backend)?;
        match table.get(key).map_err(backend)? {
            Some(v) => decode(v.value()).map_err(codec),
            None => Err(backend(format!("missing meta key {key}"))),
        }
    }

    fn bump_meta_u64(
        meta: &mut redb::Table<'_, &str, &[u8]>,
        key: &str,
        at_least: u64,
    ) -> Result<(), TopicError> {
        let current = match meta.get(key).map_err(backend)? {
            Some(v) => decode(v.value()).map_err(codec)?,
            None => 0,
        };
        if at_least > current {
            meta.insert(key, encode(&at_least).map_err(codec)?.as_slice())
                .map_err(backend)?;
        }
        Ok(())
    }

    fn head(&self) -> Result<u64, TopicError> {
        Ok(self.read_meta_u64(K_NEXT_EVENT)?.saturating_sub(1))
    }

    #[allow(dead_code)]
    fn min_cursor(subs: &redb::ReadOnlyTable<&str, &[u8]>) -> Result<u64, TopicError> {
        let mut min = None;
        for row in subs.iter().map_err(backend)? {
            let (_, bytes) = row.map_err(backend)?;
            let stored: StoredSub = decode(bytes.value()).map_err(codec)?;
            min = Some(min.map_or(stored.cursor, |m: u64| m.min(stored.cursor)));
        }
        Ok(min.unwrap_or(0))
    }

    fn min_cursor_write(subs: &redb::Table<'_, &str, &[u8]>) -> Result<u64, TopicError> {
        let mut min = None;
        for row in subs.iter().map_err(backend)? {
            let (_, bytes) = row.map_err(backend)?;
            let stored: StoredSub = decode(bytes.value()).map_err(codec)?;
            min = Some(min.map_or(stored.cursor, |m: u64| m.min(stored.cursor)));
        }
        Ok(min.unwrap_or(0))
    }

    fn fanout_pending(txn: &redb::WriteTransaction, event_id: u64) -> Result<(), TopicError> {
        let subs = txn.open_table(SUBS).map_err(backend)?;
        let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
        for row in subs.iter().map_err(backend)? {
            let (name, bytes) = row.map_err(backend)?;
            let stored: StoredSub = decode(bytes.value()).map_err(codec)?;
            if event_id > stored.cursor {
                pending
                    .insert(pending_key(name.value(), event_id).as_slice(), ())
                    .map_err(backend)?;
            }
        }
        Ok(())
    }

    fn compact_if_needed(
        txn: &redb::WriteTransaction,
    ) -> Result<Option<TopicReplicateOp>, TopicError> {
        let subs = txn.open_table(SUBS).map_err(backend)?;
        let floor = Self::min_cursor_write(&subs)?;
        let mut meta = txn.open_table(META).map_err(backend)?;
        let current = match meta.get(K_COMPACT_HEAD).map_err(backend)? {
            Some(v) => decode(v.value()).map_err(codec)?,
            None => 0,
        };
        if floor <= current {
            return Ok(None);
        }
        let mut events = txn.open_table(EVENTS).map_err(backend)?;
        let to_remove: Vec<u64> = events
            .iter()
            .map_err(backend)?
            .filter_map(|row| {
                let (id, _) = row.ok()?;
                (id.value() <= floor).then_some(id.value())
            })
            .collect();
        for id in to_remove {
            events.remove(id).map_err(backend)?;
        }
        let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
        let pending_remove: Vec<Vec<u8>> = pending
            .iter()
            .map_err(backend)?
            .filter_map(|row| {
                let (key, _) = row.ok()?;
                let (_, eid) = parse_pending_key(key.value())?;
                (eid <= floor).then_some(key.value().to_vec())
            })
            .collect();
        for key in pending_remove {
            pending.remove(key.as_slice()).map_err(backend)?;
        }
        let mut dead = txn.open_table(SUB_DEAD).map_err(backend)?;
        let dead_remove: Vec<Vec<u8>> = dead
            .iter()
            .map_err(backend)?
            .filter_map(|row| {
                let (key, _) = row.ok()?;
                let (_, eid) = parse_pending_key(key.value())?;
                (eid <= floor).then_some(key.value().to_vec())
            })
            .collect();
        for key in dead_remove {
            dead.remove(key.as_slice()).map_err(backend)?;
        }
        meta.insert(K_COMPACT_HEAD, encode(&floor).map_err(codec)?.as_slice())
            .map_err(backend)?;
        Ok(Some(TopicReplicateOp::CompactHead {
            compact_head: floor,
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn apply_replicate_inner(
        &self,
        op: &TopicReplicateOp,
    ) -> Result<Option<TopicReplicateOp>, TopicError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        let compact = {
            match op {
                TopicReplicateOp::Publish {
                    event_id,
                    payload,
                    published_at_ms,
                    next_event_id,
                } => {
                    let mut events = txn.open_table(EVENTS).map_err(backend)?;
                    let mut meta = txn.open_table(META).map_err(backend)?;
                    if events.get(*event_id).map_err(backend)?.is_none() {
                        let stored = StoredEvent {
                            payload: payload.clone(),
                            published_at_ms: *published_at_ms,
                        };
                        events
                            .insert(*event_id, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        Self::fanout_pending(&txn, *event_id)?;
                    }
                    Self::bump_meta_u64(&mut meta, K_NEXT_EVENT, *next_event_id)?;
                }
                TopicReplicateOp::RegisterSubscription {
                    name,
                    cursor,
                    max_attempts,
                } => {
                    let mut subs = txn.open_table(SUBS).map_err(backend)?;
                    if subs.get(name.as_str()).map_err(backend)?.is_some() {
                        return Ok(None);
                    }
                    let stored = StoredSub {
                        cursor: *cursor,
                        max_attempts: *max_attempts,
                        retention_discards: 0,
                    };
                    subs.insert(name.as_str(), encode(&stored).map_err(codec)?.as_slice())
                        .map_err(backend)?;
                    if *cursor == 0 {
                        let events = txn.open_table(EVENTS).map_err(backend)?;
                        let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
                        for row in events.iter().map_err(backend)? {
                            let (id, _) = row.map_err(backend)?;
                            if id.value() > *cursor {
                                pending
                                    .insert(pending_key(name, id.value()).as_slice(), ())
                                    .map_err(backend)?;
                            }
                        }
                    }
                }
                TopicReplicateOp::RemoveSubscription { name } => {
                    let mut subs = txn.open_table(SUBS).map_err(backend)?;
                    subs.remove(name.as_str()).map_err(backend)?;
                    let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
                    let keys: Vec<Vec<u8>> = pending
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (key, _) = row.ok()?;
                            let (sub, _) = parse_pending_key(key.value())?;
                            (sub == *name).then_some(key.value().to_vec())
                        })
                        .collect();
                    for key in keys {
                        pending.remove(key.as_slice()).map_err(backend)?;
                    }
                    let mut leases = txn.open_table(SUB_LEASES).map_err(backend)?;
                    let lease_remove: Vec<u64> = leases
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (id, bytes) = row.ok()?;
                            let lease: StoredLease = decode(bytes.value()).ok()?;
                            (lease.subscription == *name).then_some(id.value())
                        })
                        .collect();
                    for id in lease_remove {
                        leases.remove(id).map_err(backend)?;
                    }
                    let mut dead = txn.open_table(SUB_DEAD).map_err(backend)?;
                    let dead_remove: Vec<Vec<u8>> = dead
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (key, _) = row.ok()?;
                            let (sub, _) = parse_pending_key(key.value())?;
                            (sub == *name).then_some(key.value().to_vec())
                        })
                        .collect();
                    for key in dead_remove {
                        dead.remove(key.as_slice()).map_err(backend)?;
                    }
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
                    let mut leases = txn.open_table(SUB_LEASES).map_err(backend)?;
                    let mut meta = txn.open_table(META).map_err(backend)?;
                    if leases.get(*lease_id).map_err(backend)?.is_none() {
                        let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
                        pending
                            .remove(pending_key(subscription, *event_id).as_slice())
                            .map_err(backend)?;
                        let lease = StoredLease {
                            subscription: subscription.clone(),
                            event_id: *event_id,
                            worker_node: *worker_node,
                            worker_instance: *worker_instance,
                            expires_at_ms: *expires_at_ms,
                            attempts: *attempts,
                        };
                        leases
                            .insert(*lease_id, encode(&lease).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                    }
                    Self::bump_meta_u64(&mut meta, K_NEXT_LEASE, *next_lease_id)?;
                }
                TopicReplicateOp::Ack {
                    subscription,
                    lease_id,
                    event_id,
                    cursor,
                } => {
                    let mut leases = txn.open_table(SUB_LEASES).map_err(backend)?;
                    leases.remove(*lease_id).map_err(backend)?;
                    let mut subs = txn.open_table(SUBS).map_err(backend)?;
                    let mut stored: Option<StoredSub> = subs
                        .get(subscription.as_str())
                        .map_err(backend)?
                        .map(|b| decode(b.value()).map_err(codec))
                        .transpose()?;
                    if let Some(stored) = stored.as_mut() {
                        stored.cursor = stored.cursor.max(*cursor).max(*event_id);
                        subs.insert(
                            subscription.as_str(),
                            encode(stored).map_err(codec)?.as_slice(),
                        )
                        .map_err(backend)?;
                    }
                    let mut dead = txn.open_table(SUB_DEAD).map_err(backend)?;
                    dead.remove(pending_key(subscription, *event_id).as_slice())
                        .map_err(backend)?;
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
                    let mut leases = txn.open_table(SUB_LEASES).map_err(backend)?;
                    leases.remove(*lease_id).map_err(backend)?;
                    if *dead_letter {
                        let mut dead = txn.open_table(SUB_DEAD).map_err(backend)?;
                        dead.insert(
                            pending_key(subscription, *event_id).as_slice(),
                            encode(attempts).map_err(codec)?.as_slice(),
                        )
                        .map_err(backend)?;
                    } else {
                        let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
                        pending
                            .insert(pending_key(subscription, *event_id).as_slice(), ())
                            .map_err(backend)?;
                    }
                }
                TopicReplicateOp::RetentionDiscard {
                    subscription,
                    cursor,
                    discarded,
                } => {
                    let mut subs = txn.open_table(SUBS).map_err(backend)?;
                    let mut stored: Option<StoredSub> = subs
                        .get(subscription.as_str())
                        .map_err(backend)?
                        .map(|b| decode(b.value()).map_err(codec))
                        .transpose()?;
                    if let Some(stored) = stored.as_mut() {
                        stored.cursor = stored.cursor.max(*cursor);
                        stored.retention_discards =
                            stored.retention_discards.saturating_add(*discarded);
                        subs.insert(
                            subscription.as_str(),
                            encode(stored).map_err(codec)?.as_slice(),
                        )
                        .map_err(backend)?;
                    }
                    let mut pending = txn.open_table(SUB_PENDING).map_err(backend)?;
                    let keys: Vec<Vec<u8>> = pending
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (key, _) = row.ok()?;
                            let (sub, eid) = parse_pending_key(key.value())?;
                            (sub == *subscription && eid <= *cursor).then_some(key.value().to_vec())
                        })
                        .collect();
                    for key in keys {
                        pending.remove(key.as_slice()).map_err(backend)?;
                    }
                    let mut leases = txn.open_table(SUB_LEASES).map_err(backend)?;
                    let lease_remove: Vec<u64> = leases
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (id, bytes) = row.ok()?;
                            let lease: StoredLease = decode(bytes.value()).ok()?;
                            (lease.subscription == *subscription && lease.event_id <= *cursor)
                                .then_some(id.value())
                        })
                        .collect();
                    for id in lease_remove {
                        leases.remove(id).map_err(backend)?;
                    }
                    let mut dead = txn.open_table(SUB_DEAD).map_err(backend)?;
                    let dead_remove: Vec<Vec<u8>> = dead
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (key, _) = row.ok()?;
                            let (sub, eid) = parse_pending_key(key.value())?;
                            (sub == *subscription && eid <= *cursor).then_some(key.value().to_vec())
                        })
                        .collect();
                    for key in dead_remove {
                        dead.remove(key.as_slice()).map_err(backend)?;
                    }
                }
                TopicReplicateOp::CompactHead { compact_head } => {
                    let mut meta = txn.open_table(META).map_err(backend)?;
                    meta.insert(
                        K_COMPACT_HEAD,
                        encode(compact_head).map_err(codec)?.as_slice(),
                    )
                    .map_err(backend)?;
                    let mut events = txn.open_table(EVENTS).map_err(backend)?;
                    let to_remove: Vec<u64> = events
                        .iter()
                        .map_err(backend)?
                        .filter_map(|row| {
                            let (id, _) = row.ok()?;
                            (id.value() <= *compact_head).then_some(id.value())
                        })
                        .collect();
                    for id in to_remove {
                        events.remove(id).map_err(backend)?;
                    }
                }
            }
            match op {
                TopicReplicateOp::Ack { .. }
                | TopicReplicateOp::RetentionDiscard { .. }
                | TopicReplicateOp::RemoveSubscription { .. } => Self::compact_if_needed(&txn)?,
                _ => None,
            }
        };
        txn.commit().map_err(backend)?;
        Ok(compact)
    }

    fn reclaim_expired_ops(&self) -> Result<TopicReplicationOps, TopicError> {
        let now = now_ms();
        let mut ops = Vec::new();
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_read().map_err(backend)?;
        let leases = txn.open_table(SUB_LEASES).map_err(backend)?;
        let expired: Vec<(u64, StoredLease)> = leases
            .iter()
            .map_err(backend)?
            .filter_map(|row| {
                let (id, bytes) = row.ok()?;
                let lease: StoredLease = decode(bytes.value()).ok()?;
                (lease.expires_at_ms <= now).then_some((id.value(), lease))
            })
            .collect();
        drop(txn);
        for (lease_id, lease) in expired {
            let max_attempts = {
                let txn = db.begin_read().map_err(backend)?;
                let subs = txn.open_table(SUBS).map_err(backend)?;
                subs.get(lease.subscription.as_str())
                    .map_err(backend)?
                    .and_then(|b| decode::<StoredSub>(b.value()).ok().map(|s| s.max_attempts))
                    .unwrap_or(0)
            };
            let (attempts, dead) = {
                let o = after_failed_attempt(lease.attempts, max_attempts, now);
                (o.attempts, o.dead_letter)
            };
            ops.push(TopicReplicateOp::Reclaim {
                subscription: lease.subscription,
                lease_id,
                event_id: lease.event_id,
                attempts,
                dead_letter: dead,
            });
        }
        Ok(ops)
    }
}

impl EventTopic for RedbEventTopic {
    fn publish_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(EventId, TopicReplicationOps), TopicError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops()?;
            for op in &ops {
                self.apply_replicate_inner(op)?;
            }
            let event_id = self.read_meta_u64(K_NEXT_EVENT)?;
            let published_at_ms = now_ms();
            let next_event_id = event_id.saturating_add(1);
            ops.push(TopicReplicateOp::Publish {
                event_id,
                payload: payload.to_vec(),
                published_at_ms,
                next_event_id,
            });
            for op in &ops {
                let _ = self.apply_replicate_inner(op)?;
            }
            Ok((EventId(event_id), ops))
        })
    }

    fn apply_replicate<'a>(
        &'a self,
        op: &'a TopicReplicateOp,
    ) -> BoxFuture<'a, Result<(), TopicError>> {
        Box::pin(async move {
            if let Some(compact) = self.apply_replicate_inner(op)? {
                self.apply_replicate_inner(&compact)?;
            }
            Ok(())
        })
    }

    fn register_subscriptions<'a>(
        &'a self,
        defs: &'a [TopicSubscriptionDef],
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let head = self.head()?;
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
                let _ = self.apply_replicate_inner(op)?;
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
            let mut ops = self.reclaim_expired_ops()?;
            for op in &ops {
                self.apply_replicate_inner(op)?;
            }
            let reclaim_count = ops.len();
            let out = {
                let db = self.db.lock().expect("poisoned");
                let txn = db.begin_read().map_err(backend)?;
                let subs = txn.open_table(SUBS).map_err(backend)?;
                if subs.get(subscription).map_err(backend)?.is_none() {
                    return Err(TopicError::SubscriptionNotFound(subscription.to_string()));
                }
                let pending = txn.open_table(SUB_PENDING).map_err(backend)?;
                let mut candidates: Vec<u64> = pending
                    .iter()
                    .map_err(backend)?
                    .filter_map(|row| {
                        let (key, _) = row.ok()?;
                        let (sub, eid) = parse_pending_key(key.value())?;
                        (sub == subscription).then_some(eid)
                    })
                    .collect();
                candidates.sort_unstable();
                candidates.truncate(max);
                let events = txn.open_table(EVENTS).map_err(backend)?;
                let expires_at_ms = now_ms().saturating_add(
                    u64::try_from(self.lease_timeout.as_millis()).unwrap_or(u64::MAX),
                );
                let meta = txn.open_table(META).map_err(backend)?;
                let mut next_lease: u64 = match meta.get(K_NEXT_LEASE).map_err(backend)? {
                    Some(v) => decode(v.value()).map_err(codec)?,
                    None => return Err(backend(format!("missing meta key {K_NEXT_LEASE}"))),
                };
                let mut out = Vec::new();
                for event_id in candidates {
                    let Some(bytes) = events.get(event_id).map_err(backend)? else {
                        continue;
                    };
                    let stored: StoredEvent = decode(bytes.value()).map_err(codec)?;
                    let attempts = {
                        let dead = txn.open_table(SUB_DEAD).map_err(backend)?;
                        dead.get(pending_key(subscription, event_id).as_slice())
                            .map_err(backend)?
                            .map_or(1, |b| {
                                decode::<u32>(b.value()).unwrap_or(1).saturating_add(1)
                            })
                    };
                    let lease_id = next_lease;
                    next_lease = next_lease.saturating_add(1);
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
                        payload: stored.payload,
                        attempts,
                    });
                }
                out
            };
            for op in ops.iter().skip(reclaim_count) {
                self.apply_replicate_inner(op)?;
            }
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
            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_read().map_err(backend)?;
            let leases = txn.open_table(SUB_LEASES).map_err(backend)?;
            let Some(bytes) = leases.get(lease_id.0).map_err(backend)? else {
                return Err(TopicError::InvalidLease);
            };
            let lease: StoredLease = decode(bytes.value()).map_err(codec)?;
            if lease.subscription != subscription
                || lease.worker_node != worker.node.0
                || lease.worker_instance != worker.instance
            {
                return Err(TopicError::InvalidLease);
            }
            let subs = txn.open_table(SUBS).map_err(backend)?;
            let cursor = subs
                .get(subscription)
                .map_err(backend)?
                .map_or(0, |b| {
                    decode::<StoredSub>(b.value()).map_or(0, |s| s.cursor)
                })
                .max(lease.event_id);
            drop(txn);
            drop(db);
            let ops = vec![TopicReplicateOp::Ack {
                subscription: subscription.to_string(),
                lease_id: lease_id.0,
                event_id: lease.event_id,
                cursor,
            }];
            if let Some(compact) = self.apply_replicate_inner(&ops[0])? {
                self.apply_replicate_inner(&compact)?;
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
            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_read().map_err(backend)?;
            let leases = txn.open_table(SUB_LEASES).map_err(backend)?;
            let Some(bytes) = leases.get(lease_id.0).map_err(backend)? else {
                return Err(TopicError::InvalidLease);
            };
            let lease: StoredLease = decode(bytes.value()).map_err(codec)?;
            if lease.subscription != subscription
                || lease.worker_node != worker.node.0
                || lease.worker_instance != worker.instance
            {
                return Err(TopicError::InvalidLease);
            }
            let max_attempts = txn
                .open_table(SUBS)
                .map_err(backend)?
                .get(subscription)
                .map_err(backend)?
                .map_or(0, |b| {
                    decode::<StoredSub>(b.value()).map_or(0, |s| s.max_attempts)
                });
            let (attempts, dead) = {
                let o = after_failed_attempt(lease.attempts, max_attempts, now_ms());
                (o.attempts, o.dead_letter)
            };
            drop(txn);
            drop(db);
            let ops = vec![TopicReplicateOp::Nack {
                subscription: subscription.to_string(),
                lease_id: lease_id.0,
                event_id: lease.event_id,
                attempts,
                dead_letter: dead,
            }];
            self.apply_replicate_inner(&ops[0])?;
            Ok(ops)
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<TopicMetrics, TopicError>> {
        Box::pin(async move {
            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_read().map_err(backend)?;
            let meta = txn.open_table(META).map_err(backend)?;
            let head = meta
                .get(K_NEXT_EVENT)
                .map_err(backend)?
                .map(|v| decode::<u64>(v.value()).map_err(codec))
                .transpose()?
                .unwrap_or(1)
                .saturating_sub(1);
            let compact_head = meta
                .get(K_COMPACT_HEAD)
                .map_err(backend)?
                .map(|v| decode::<u64>(v.value()).map_err(codec))
                .transpose()?
                .unwrap_or(0);
            let events = txn.open_table(EVENTS).map_err(backend)?;
            let event_count = events.iter().map_err(backend)?.count() as u64;
            let oldest_age = events
                .iter()
                .map_err(backend)?
                .filter_map(|row| {
                    let (_, bytes) = row.ok()?;
                    let stored: StoredEvent = decode(bytes.value()).ok()?;
                    Some(now_ms().saturating_sub(stored.published_at_ms))
                })
                .max()
                .unwrap_or(0);
            let subs = txn.open_table(SUBS).map_err(backend)?;
            let pending = txn.open_table(SUB_PENDING).map_err(backend)?;
            let leases = txn.open_table(SUB_LEASES).map_err(backend)?;
            let mut subscriptions = Vec::new();
            for row in subs.iter().map_err(backend)? {
                let (name, bytes) = row.map_err(backend)?;
                let stored: StoredSub = decode(bytes.value()).map_err(codec)?;
                let sub_name = name.value();
                let pending_count = pending
                    .iter()
                    .map_err(backend)?
                    .filter(|row| {
                        row.as_ref()
                            .ok()
                            .and_then(|(key, _)| parse_pending_key(key.value()))
                            .is_some_and(|(sub, _)| sub == sub_name)
                    })
                    .count();
                let leased_count = leases
                    .iter()
                    .map_err(backend)?
                    .filter(|row| {
                        row.as_ref()
                            .ok()
                            .and_then(|(_, bytes)| decode::<StoredLease>(bytes.value()).ok())
                            .is_some_and(|l| l.subscription == sub_name)
                    })
                    .count();
                subscriptions.push(TopicSubscriptionMetrics {
                    subscription: sub_name.to_string(),
                    cursor: stored.cursor,
                    lag: head.saturating_sub(stored.cursor),
                    pending: u64::try_from(pending_count).unwrap_or(u64::MAX),
                    leased: u64::try_from(leased_count).unwrap_or(u64::MAX),
                    retention_discards: stored.retention_discards,
                });
            }
            Ok(TopicMetrics {
                event_count,
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
            let head = self.head()?;
            let metrics = self.metrics().await?;
            let max_age_ms =
                u64::try_from(self.retention.max_event_age.as_millis()).unwrap_or(u64::MAX);
            let over_age =
                u64::try_from(metrics.oldest_event_age.as_millis()).unwrap_or(0) > max_age_ms;
            let over_count = metrics.event_count > self.retention.max_retained_events;
            let mut ops = Vec::new();
            if over_age || over_count {
                for sub in metrics.subscriptions {
                    let lag = head.saturating_sub(sub.cursor);
                    if lag == 0 {
                        continue;
                    }
                    ops.push(TopicReplicateOp::RetentionDiscard {
                        subscription: sub.subscription,
                        cursor: head,
                        discarded: lag,
                    });
                }
            }
            for op in &ops {
                if let Some(compact) = self.apply_replicate_inner(op)? {
                    self.apply_replicate_inner(&compact)?;
                }
            }
            Ok(ops)
        })
    }
}

// Silence unused import warning when TopicContext is only referenced in docs.
#[allow(unused_imports)]
use TopicContext as _;
