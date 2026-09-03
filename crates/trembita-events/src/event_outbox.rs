//! Transactional event outbox port — leader drainer from application DB into
//! [`EventTopic`](super::topic::EventTopic)
//! ([event-outbox](../../../docs/decisions/event-outbox.md)).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use trembita_runtime::{ClusterState, LeaderLoopOpts, run_leader_loop};

use crate::{BoxFuture, EventTopic, TopicError};

const CURSORS: TableDefinition<&str, &[u8]> = TableDefinition::new("event_outbox_cursors");

/// Why an outbox poll or cursor operation failed.
#[derive(Debug, thiserror::Error)]
pub enum EventOutboxError {
    /// Backend (database, disk) error.
    #[error("event outbox backend error: {0}")]
    Backend(String),
}

fn backend(e: impl std::fmt::Display) -> EventOutboxError {
    EventOutboxError::Backend(e.to_string())
}

/// One row from the application outbox store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    /// Stable row id — used for idempotent [`EventOutboxSource::mark_published`].
    pub id: Vec<u8>,
    /// Payload to publish to the topic.
    pub payload: Vec<u8>,
}

/// Application-owned transactional outbox ([event-outbox](../../../docs/decisions/event-outbox.md)).
pub trait EventOutboxSource: Send + Sync {
    /// Return up to `max` unpublished events strictly after `after` (exclusive).
    ///
    /// Implementations order by stable id. `after = None` starts from the beginning.
    fn poll(
        &self,
        after: Option<&[u8]>,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<OutboxEvent>, EventOutboxError>>;

    /// Mark rows published after a successful topic publish — idempotent for repeated ids.
    fn mark_published(&self, ids: &[Vec<u8>]) -> BoxFuture<'_, Result<(), EventOutboxError>>;
}

/// Leader poll interval for [`run_event_outbox_drainer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventOutboxPoll(Duration);

impl EventOutboxPoll {
    /// Poll every `secs` seconds on the topic leader.
    #[must_use]
    pub fn secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs.max(1)))
    }

    /// Poll every `millis` milliseconds (minimum 100 ms).
    #[must_use]
    pub fn millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis.max(100)))
    }

    /// Inner duration.
    #[must_use]
    pub fn duration(self) -> Duration {
        self.0
    }
}

/// Tunables for [`run_event_outbox_drainer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOutboxDrainOpts {
    /// Leader poll interval.
    pub poll_interval: Duration,
    /// Max outbox rows drained per tick.
    pub max_batch: usize,
}

impl Default for EventOutboxDrainOpts {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_batch: 64,
        }
    }
}

impl EventOutboxDrainOpts {
    /// Leader poll interval.
    #[must_use]
    pub fn poll(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Max rows per poll.
    #[must_use]
    pub fn max_batch(mut self, n: usize) -> Self {
        self.max_batch = n.max(1);
        self
    }
}

/// Persist drainer cursor per topic between leadership terms.
pub trait EventOutboxCursor: Send + Sync {
    /// Last published outbox id for `topic`, if any.
    fn load(&self, topic: &str) -> BoxFuture<'_, Result<Option<Vec<u8>>, EventOutboxError>>;

    /// Store cursor after `mark_published` succeeds.
    fn store(&self, topic: &str, cursor: &[u8]) -> BoxFuture<'_, Result<(), EventOutboxError>>;
}

/// In-process cursor store for unit tests.
#[derive(Default)]
pub struct InMemoryEventOutboxCursor {
    inner: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryEventOutboxCursor {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventOutboxCursor for InMemoryEventOutboxCursor {
    fn load(&self, topic: &str) -> BoxFuture<'_, Result<Option<Vec<u8>>, EventOutboxError>> {
        let topic = topic.to_string();
        Box::pin(async move {
            Ok(self
                .inner
                .lock()
                .map_err(|_| backend("poisoned InMemoryEventOutboxCursor"))?
                .get(&topic)
                .cloned())
        })
    }

    fn store(&self, topic: &str, cursor: &[u8]) -> BoxFuture<'_, Result<(), EventOutboxError>> {
        let topic = topic.to_string();
        let cursor = cursor.to_vec();
        Box::pin(async move {
            self.inner
                .lock()
                .map_err(|_| backend("poisoned InMemoryEventOutboxCursor"))?
                .insert(topic, cursor);
            Ok(())
        })
    }
}

/// Crash-safe cursor checkpoint at `{data_dir}/event-outbox-cursors.redb`.
pub struct RedbEventOutboxCursor {
    db: Database,
}

impl RedbEventOutboxCursor {
    /// Open or create the cursor database at `path`.
    ///
    /// # Errors
    /// Returns [`EventOutboxError::Backend`] when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EventOutboxError> {
        let db = Database::create(path.as_ref()).map_err(|e| backend(e))?;
        let store = Self { db };
        store.bootstrap()?;
        Ok(store)
    }

    fn bootstrap(&self) -> Result<(), EventOutboxError> {
        let write = self.db.begin_write().map_err(|e| backend(e))?;
        {
            let _ = write.open_table(CURSORS).map_err(|e| backend(e))?;
        }
        write.commit().map_err(|e| backend(e))?;
        Ok(())
    }
}

impl EventOutboxCursor for RedbEventOutboxCursor {
    fn load(&self, topic: &str) -> BoxFuture<'_, Result<Option<Vec<u8>>, EventOutboxError>> {
        let topic = topic.to_string();
        Box::pin(async move {
            let read = self.db.begin_read().map_err(|e| backend(e))?;
            let table = read.open_table(CURSORS).map_err(|e| backend(e))?;
            Ok(table
                .get(topic.as_str())
                .map_err(|e| backend(e))?
                .map(|v| v.value().to_vec()))
        })
    }

    fn store(&self, topic: &str, cursor: &[u8]) -> BoxFuture<'_, Result<(), EventOutboxError>> {
        let topic = topic.to_string();
        let cursor = cursor.to_vec();
        Box::pin(async move {
            let write = self.db.begin_write().map_err(|e| backend(e))?;
            {
                let mut table = write.open_table(CURSORS).map_err(|e| backend(e))?;
                table
                    .insert(topic.as_str(), cursor.as_slice())
                    .map_err(|e| backend(e))?;
            }
            write.commit().map_err(|e| backend(e))?;
            Ok(())
        })
    }
}

/// Leader-only loop: poll unpublished rows, publish to [`EventTopic`], mark published.
pub async fn run_event_outbox_drainer(
    topic_name: String,
    topic: std::sync::Arc<dyn EventTopic>,
    source: std::sync::Arc<dyn EventOutboxSource>,
    cursor: std::sync::Arc<dyn EventOutboxCursor>,
    state: std::sync::Arc<dyn ClusterState>,
    opts: EventOutboxDrainOpts,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let topic_tick = topic_name.clone();
    let topic_client = std::sync::Arc::clone(&topic);
    let source_tick = std::sync::Arc::clone(&source);
    let cursor_tick = std::sync::Arc::clone(&cursor);
    let opts_tick = opts.clone();
    run_leader_loop(
        state,
        LeaderLoopOpts::new(opts.poll_interval),
        stop,
        move |_| {
            let topic_name = topic_tick.clone();
            let topic = std::sync::Arc::clone(&topic_client);
            let source = std::sync::Arc::clone(&source_tick);
            let cursor = std::sync::Arc::clone(&cursor_tick);
            let opts = opts_tick.clone();
            async move {
                drain_event_outbox_once(
                    &topic_name,
                    topic.as_ref(),
                    source.as_ref(),
                    cursor.as_ref(),
                    &opts,
                )
                .await;
            }
        },
    )
    .await;
}

async fn drain_event_outbox_once(
    topic_name: &str,
    topic: &dyn EventTopic,
    source: &dyn EventOutboxSource,
    cursor_store: &dyn EventOutboxCursor,
    opts: &EventOutboxDrainOpts,
) {
    let after = cursor_store.load(topic_name).await.ok().flatten();
    let Ok(events) = source.poll(after.as_deref(), opts.max_batch).await else {
        return;
    };
    if events.is_empty() {
        return;
    }
    let mut published_ids = Vec::new();
    let mut last_id = None;
    for event in events {
        match topic.publish(&event.payload).await {
            Ok(_) => {
                published_ids.push(event.id.clone());
                last_id = Some(event.id);
            }
            Err(TopicError::NotLeader) => break,
            Err(_) => break,
        }
    }
    if published_ids.is_empty() {
        return;
    }
    if source.mark_published(&published_ids).await.is_err() {
        return;
    }
    if let Some(id) = last_id {
        let _ = cursor_store.store(topic_name, &id).await;
    }
}

/// In-memory outbox for unit tests and single-node dev.
#[derive(Default)]
pub struct InMemoryEventOutboxSource {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    rows: BTreeMap<Vec<u8>, Row>,
}

#[derive(Clone)]
struct Row {
    payload: Vec<u8>,
    published: bool,
}

impl InMemoryEventOutboxSource {
    /// Empty outbox.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one unpublished row (test helper).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn push(&self, id: impl Into<Vec<u8>>, payload: impl Into<Vec<u8>>) {
        let id = id.into();
        let payload = payload.into();
        self.inner.lock().expect("poisoned").rows.insert(
            id,
            Row {
                payload,
                published: false,
            },
        );
    }

    /// Whether `id` is marked published (test helper).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    #[must_use]
    pub fn is_published(&self, id: &[u8]) -> bool {
        self.inner
            .lock()
            .expect("poisoned")
            .rows
            .get(id)
            .is_some_and(|r| r.published)
    }
}

impl EventOutboxSource for InMemoryEventOutboxSource {
    fn poll(
        &self,
        after: Option<&[u8]>,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<OutboxEvent>, EventOutboxError>> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .map_err(|_| backend("poisoned InMemoryEventOutboxSource"))?;
            let mut out = Vec::new();
            for (id, row) in &inner.rows {
                if row.published {
                    continue;
                }
                if after.is_some_and(|cursor| id.as_slice() <= cursor) {
                    continue;
                }
                out.push(OutboxEvent {
                    id: id.clone(),
                    payload: row.payload.clone(),
                });
                if out.len() >= max {
                    break;
                }
            }
            Ok(out)
        })
    }

    fn mark_published(&self, ids: &[Vec<u8>]) -> BoxFuture<'_, Result<(), EventOutboxError>> {
        let ids: Vec<Vec<u8>> = ids.to_vec();
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| backend("poisoned InMemoryEventOutboxSource"))?;
            for id in ids {
                if let Some(row) = inner.rows.get_mut(&id) {
                    row.published = true;
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use trembita_proto::NodeId;

    use super::*;
    use crate::InMemoryEventTopic;

    struct MockState {
        leader: bool,
    }

    impl ClusterState for MockState {
        fn is_leader(&self) -> bool {
            self.leader
        }

        fn live_nodes(&self) -> Vec<NodeId> {
            vec![NodeId(1)]
        }

        fn leader_id(&self) -> Option<NodeId> {
            self.leader.then_some(NodeId(1))
        }

        fn reachable_nodes(&self) -> Vec<NodeId> {
            vec![NodeId(1)]
        }
    }

    #[tokio::test]
    async fn drainer_publishes_and_marks_rows() {
        let topic = Arc::new(InMemoryEventTopic::new(Duration::from_secs(60)));
        let source = Arc::new(InMemoryEventOutboxSource::new());
        source.push(b"1", b"alpha");
        source.push(b"2", b"beta");
        let cursor = Arc::new(InMemoryEventOutboxCursor::new());
        let state = Arc::new(MockState { leader: true });
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let drainer = tokio::spawn(run_event_outbox_drainer(
            "events".into(),
            topic.clone(),
            source.clone(),
            cursor,
            state,
            EventOutboxDrainOpts {
                poll_interval: Duration::from_millis(20),
                max_batch: 8,
            },
            stop_rx,
        ));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(source.is_published(b"1"));
        assert!(source.is_published(b"2"));
        let metrics = topic.metrics().await.unwrap();
        assert_eq!(metrics.event_count, 2);
        drainer.abort();
    }

    #[tokio::test]
    async fn drainer_skips_when_not_leader() {
        let topic = Arc::new(InMemoryEventTopic::new(Duration::from_secs(60)));
        let source = Arc::new(InMemoryEventOutboxSource::new());
        source.push(b"1", b"alpha");
        let cursor = Arc::new(InMemoryEventOutboxCursor::new());
        let state = Arc::new(MockState { leader: false });
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let drainer = tokio::spawn(run_event_outbox_drainer(
            "events".into(),
            topic.clone(),
            source.clone(),
            cursor,
            state,
            EventOutboxDrainOpts {
                poll_interval: Duration::from_millis(20),
                max_batch: 8,
            },
            stop_rx,
        ));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!source.is_published(b"1"));
        assert_eq!(topic.metrics().await.unwrap().event_count, 0);
        drainer.abort();
    }

    #[test]
    fn redb_cursor_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cursors.redb");
        {
            let store = RedbEventOutboxCursor::open(&path).unwrap();
            store.store("events", b"42").await.unwrap();
        }
        let store = RedbEventOutboxCursor::open(&path).unwrap();
        assert_eq!(store.load("events").await.unwrap(), Some(b"42".to_vec()));
    }
}
