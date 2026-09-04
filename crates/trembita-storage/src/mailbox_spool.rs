//! Durable outbox/inbox for cross-node `/actor/deliver` (mailbox spool).
//!
//! When enabled, outbound envelopes are write-ahead logged before send and
//! removed only after the peer acks delivery; inbound envelopes are persisted
//! before they reach the actor mailbox and removed after enqueue succeeds.
//! A background drainer replays pending entries after restarts or transport
//! failures ([job-queue](../../../docs/decisions/job-queue.md)).

use std::path::Path;
use std::sync::Mutex;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use trembita_proto::{ActorEnvelope, decode, encode};

use crate::redb_util::{self, open_mutex_database};

const OUTBOX: TableDefinition<u64, &[u8]> = TableDefinition::new("mailbox_outbox");
const INBOX: TableDefinition<u64, &[u8]> = TableDefinition::new("mailbox_inbox");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("mailbox_spool_meta");
const K_NEXT_ID: &str = "next_id";

/// Stable row id in the spool tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MailboxSpoolId(pub u64);

/// Why a mailbox spool read or write failed.
#[derive(Debug, thiserror::Error)]
pub enum MailboxSpoolError {
    /// Disk / redb backend failure.
    #[error("backend: {0}")]
    Backend(String),
    /// Envelope encode/decode failure.
    #[error("codec: {0}")]
    Codec(String),
}

fn backend(e: impl std::fmt::Display) -> MailboxSpoolError {
    MailboxSpoolError::Backend(redb_util::error_string(e))
}

fn codec(e: impl std::fmt::Display) -> MailboxSpoolError {
    MailboxSpoolError::Codec(redb_util::error_string(e))
}

/// Write-ahead log for cross-node actor envelopes.
pub trait MailboxSpool: Send + Sync {
    /// Persist an outbound envelope before transport send.
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError`] if the backend write fails.
    fn push_outbox(&self, envelope: &ActorEnvelope) -> Result<MailboxSpoolId, MailboxSpoolError>;
    /// Drop a delivered outbound row.
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError`] if the backend delete fails.
    fn remove_outbox(&self, id: MailboxSpoolId) -> Result<(), MailboxSpoolError>;
    /// Oldest pending outbound rows (FIFO by id).
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError`] if the backend read fails.
    fn list_outbox(
        &self,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError>;

    /// Persist an inbound envelope before handing it to a local mailbox.
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError`] if the backend write fails.
    fn push_inbox(&self, envelope: &ActorEnvelope) -> Result<MailboxSpoolId, MailboxSpoolError>;
    /// Drop an inbound row after the mailbox accepted the message.
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError`] if the backend delete fails.
    fn remove_inbox(&self, id: MailboxSpoolId) -> Result<(), MailboxSpoolError>;
    /// Oldest pending inbound rows (FIFO by id).
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError`] if the backend read fails.
    fn list_inbox(
        &self,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError>;
}

/// In-memory spool for unit tests.
#[derive(Default)]
pub struct InMemoryMailboxSpool {
    outbox: Mutex<Vec<(MailboxSpoolId, ActorEnvelope)>>,
    inbox: Mutex<Vec<(MailboxSpoolId, ActorEnvelope)>>,
    next_id: Mutex<u64>,
}

impl InMemoryMailboxSpool {
    /// Empty in-memory spool with monotonic row ids starting at 1.
    #[must_use]
    pub fn new() -> Self {
        Self {
            outbox: Mutex::new(Vec::new()),
            inbox: Mutex::new(Vec::new()),
            next_id: Mutex::new(1),
        }
    }

    fn alloc_id(&self) -> MailboxSpoolId {
        let mut next = self.next_id.lock().expect("poisoned");
        let id = MailboxSpoolId(*next);
        *next += 1;
        id
    }
}

impl MailboxSpool for InMemoryMailboxSpool {
    fn push_outbox(&self, envelope: &ActorEnvelope) -> Result<MailboxSpoolId, MailboxSpoolError> {
        let id = self.alloc_id();
        self.outbox
            .lock()
            .expect("poisoned")
            .push((id, envelope.clone()));
        Ok(id)
    }

    fn remove_outbox(&self, id: MailboxSpoolId) -> Result<(), MailboxSpoolError> {
        let mut rows = self.outbox.lock().expect("poisoned");
        if let Some(pos) = rows.iter().position(|(row, _)| *row == id) {
            rows.remove(pos);
            Ok(())
        } else {
            Err(backend(format!("outbox id {} missing", id.0)))
        }
    }

    fn list_outbox(
        &self,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError> {
        let rows = self.outbox.lock().expect("poisoned");
        Ok(rows.iter().take(max).cloned().collect())
    }

    fn push_inbox(&self, envelope: &ActorEnvelope) -> Result<MailboxSpoolId, MailboxSpoolError> {
        let id = self.alloc_id();
        self.inbox
            .lock()
            .expect("poisoned")
            .push((id, envelope.clone()));
        Ok(id)
    }

    fn remove_inbox(&self, id: MailboxSpoolId) -> Result<(), MailboxSpoolError> {
        let mut rows = self.inbox.lock().expect("poisoned");
        if let Some(pos) = rows.iter().position(|(row, _)| *row == id) {
            rows.remove(pos);
            Ok(())
        } else {
            Err(backend(format!("inbox id {} missing", id.0)))
        }
    }

    fn list_inbox(
        &self,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError> {
        let rows = self.inbox.lock().expect("poisoned");
        Ok(rows.iter().take(max).cloned().collect())
    }
}

/// Crash-safe outbox/inbox in a dedicated `redb` file (`{data_dir}/mailbox-spool.redb`).
#[derive(Debug)]
pub struct RedbMailboxSpool {
    db: Mutex<Database>,
}

impl RedbMailboxSpool {
    /// Open or create the spool database at `path`.
    ///
    /// # Errors
    /// Returns [`MailboxSpoolError::Backend`] when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MailboxSpoolError> {
        let db = open_mutex_database(path).map_err(backend)?;
        let spool = Self { db };
        spool.bootstrap()?;
        Ok(spool)
    }

    fn bootstrap(&self) -> Result<(), MailboxSpoolError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            txn.open_table(OUTBOX).map_err(backend)?;
            txn.open_table(INBOX).map_err(backend)?;
            let mut meta = txn.open_table(META).map_err(backend)?;
            if meta.get(K_NEXT_ID).map_err(backend)?.is_none() {
                meta.insert(K_NEXT_ID, encode(&1u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn alloc_id(&self) -> Result<MailboxSpoolId, MailboxSpoolError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        let id = {
            let mut meta = txn.open_table(META).map_err(backend)?;
            let current: u64 = match meta.get(K_NEXT_ID).map_err(backend)? {
                Some(v) => decode(v.value()).map_err(codec)?,
                None => 1,
            };
            meta.insert(K_NEXT_ID, encode(&(current + 1)).map_err(codec)?.as_slice())
                .map_err(backend)?;
            MailboxSpoolId(current)
        };
        txn.commit().map_err(backend)?;
        Ok(id)
    }

    fn push_table(
        &self,
        table: TableDefinition<u64, &[u8]>,
        envelope: &ActorEnvelope,
    ) -> Result<MailboxSpoolId, MailboxSpoolError> {
        let id = self.alloc_id()?;
        let bytes = encode(envelope).map_err(codec)?;
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut rows = txn.open_table(table).map_err(backend)?;
            rows.insert(id.0, bytes.as_slice()).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(id)
    }

    fn remove_table(
        &self,
        table: TableDefinition<u64, &[u8]>,
        id: MailboxSpoolId,
    ) -> Result<(), MailboxSpoolError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut rows = txn.open_table(table).map_err(backend)?;
            rows.remove(id.0).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn list_table(
        &self,
        table: TableDefinition<u64, &[u8]>,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_read().map_err(backend)?;
        let rows = txn.open_table(table).map_err(backend)?;
        let mut out = Vec::new();
        for row in rows.iter().map_err(backend)? {
            let (id, bytes) = row.map_err(backend)?;
            let envelope: ActorEnvelope = decode(bytes.value()).map_err(codec)?;
            out.push((MailboxSpoolId(id.value()), envelope));
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }
}

impl MailboxSpool for RedbMailboxSpool {
    fn push_outbox(&self, envelope: &ActorEnvelope) -> Result<MailboxSpoolId, MailboxSpoolError> {
        self.push_table(OUTBOX, envelope)
    }

    fn remove_outbox(&self, id: MailboxSpoolId) -> Result<(), MailboxSpoolError> {
        self.remove_table(OUTBOX, id)
    }

    fn list_outbox(
        &self,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError> {
        self.list_table(OUTBOX, max)
    }

    fn push_inbox(&self, envelope: &ActorEnvelope) -> Result<MailboxSpoolId, MailboxSpoolError> {
        self.push_table(INBOX, envelope)
    }

    fn remove_inbox(&self, id: MailboxSpoolId) -> Result<(), MailboxSpoolError> {
        self.remove_table(INBOX, id)
    }

    fn list_inbox(
        &self,
        max: usize,
    ) -> Result<Vec<(MailboxSpoolId, ActorEnvelope)>, MailboxSpoolError> {
        self.list_table(INBOX, max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trembita_proto::ActorId;

    fn sample_envelope() -> ActorEnvelope {
        ActorEnvelope {
            to: ActorId {
                node: trembita_proto::NodeId(2),
                name: "w".into(),
                instance: 0,
                generation: 1,
            },
            from: None,
            origin: Some(trembita_proto::NodeId(1)),
            req_id: 42,
            payload: b"hi".to_vec(),
            reply_expected: false,
        }
    }

    #[test]
    fn in_memory_outbox_fifo() {
        let spool = InMemoryMailboxSpool::new();
        let e1 = sample_envelope();
        let id1 = spool.push_outbox(&e1).expect("push");
        let listed = spool.list_outbox(10).expect("list");
        assert_eq!(listed.len(), 1);
        spool.remove_outbox(id1).expect("remove");
        assert!(spool.list_outbox(10).expect("list").is_empty());
    }

    #[test]
    fn redb_spool_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        let id = {
            let spool = RedbMailboxSpool::open(&path).unwrap();
            spool.push_inbox(&sample_envelope()).unwrap()
        };
        let spool = RedbMailboxSpool::open(&path).unwrap();
        let rows = spool.list_inbox(10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, id);
    }
}
