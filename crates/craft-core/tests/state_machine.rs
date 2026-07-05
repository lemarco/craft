//! Reference [`StateMachine`] implementation (an in-memory key/value store) and
//! its behavioural tests. This doubles as the documented example of how a user
//! wires their own machine into craft (ADR 001).

use std::collections::BTreeMap;
use std::fmt;

use craft_core::proto::{LogIndex, decode, encode};
use craft_core::{Command, Query, StateMachine};
use serde::{Deserialize, Serialize};

// --- The user-defined command/query/response/error types -------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KvCommand {
    /// Insert or overwrite a key, returning the previous value.
    Set { key: String, value: String },
    /// Remove a key, reporting whether it existed.
    Delete { key: String },
    /// Append to an existing key; errors if the key is absent.
    Append { key: String, suffix: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KvQuery {
    Get { key: String },
    Len,
    AppliedThrough,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KvResponse {
    Previous(Option<String>),
    Existed(bool),
    Value(String),
    Get(Option<String>),
    Len(u64),
    AppliedThrough(u64),
}

#[derive(Debug, PartialEq, Eq)]
enum KvError {
    MissingKey(String),
    BadSnapshot,
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::MissingKey(k) => write!(f, "missing key: {k}"),
            KvError::BadSnapshot => write!(f, "malformed snapshot"),
        }
    }
}

impl std::error::Error for KvError {}

// --- The machine ------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct KvMachine {
    data: BTreeMap<String, String>,
    /// Highest log index applied so far (proves `apply` receives the index).
    applied_through: u64,
}

impl StateMachine for KvMachine {
    type Command = KvCommand;
    type Query = KvQuery;
    type Response = KvResponse;
    type Error = KvError;

    fn apply(
        &mut self,
        index: LogIndex,
        command: &Self::Command,
    ) -> Result<Self::Response, Self::Error> {
        // Validate before mutating (per the trait's error contract), then bump
        // the applied watermark once the command is known to succeed.
        let response = match command {
            KvCommand::Set { key, value } => {
                let previous = self.data.insert(key.clone(), value.clone());
                KvResponse::Previous(previous)
            }
            KvCommand::Delete { key } => KvResponse::Existed(self.data.remove(key).is_some()),
            KvCommand::Append { key, suffix } => {
                let current = self
                    .data
                    .get_mut(key)
                    .ok_or_else(|| KvError::MissingKey(key.clone()))?;
                current.push_str(suffix);
                KvResponse::Value(current.clone())
            }
        };
        self.applied_through = index.0;
        Ok(response)
    }

    fn query(&self, query: &Self::Query) -> Result<Self::Response, Self::Error> {
        Ok(match query {
            KvQuery::Get { key } => KvResponse::Get(self.data.get(key).cloned()),
            KvQuery::Len => KvResponse::Len(self.data.len() as u64),
            KvQuery::AppliedThrough => KvResponse::AppliedThrough(self.applied_through),
        })
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        encode(self).map_err(|_| KvError::BadSnapshot)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        *self = decode(snapshot).map_err(|_| KvError::BadSnapshot)?;
        Ok(())
    }
}

// --- Helpers ----------------------------------------------------------------

fn set(m: &mut KvMachine, index: u64, key: &str, value: &str) -> KvResponse {
    m.apply(
        LogIndex(index),
        &KvCommand::Set {
            key: key.into(),
            value: value.into(),
        },
    )
    .unwrap()
}

// --- Tests ------------------------------------------------------------------

#[test]
fn set_returns_previous_and_get_reads_it_back() {
    let mut m = KvMachine::default();
    assert_eq!(set(&mut m, 1, "a", "1"), KvResponse::Previous(None));
    assert_eq!(
        set(&mut m, 2, "a", "2"),
        KvResponse::Previous(Some("1".into()))
    );
    assert_eq!(
        m.query(&KvQuery::Get { key: "a".into() }).unwrap(),
        KvResponse::Get(Some("2".into()))
    );
    assert_eq!(
        m.query(&KvQuery::Get {
            key: "missing".into()
        })
        .unwrap(),
        KvResponse::Get(None)
    );
}

#[test]
fn delete_reports_existence() {
    let mut m = KvMachine::default();
    set(&mut m, 1, "a", "1");
    assert_eq!(
        m.apply(LogIndex(2), &KvCommand::Delete { key: "a".into() })
            .unwrap(),
        KvResponse::Existed(true)
    );
    assert_eq!(
        m.apply(LogIndex(3), &KvCommand::Delete { key: "a".into() })
            .unwrap(),
        KvResponse::Existed(false)
    );
    assert_eq!(m.query(&KvQuery::Len).unwrap(), KvResponse::Len(0));
}

#[test]
fn append_errors_on_missing_key_without_mutating() {
    let mut m = KvMachine::default();
    let err = m
        .apply(
            LogIndex(1),
            &KvCommand::Append {
                key: "ghost".into(),
                suffix: "x".into(),
            },
        )
        .unwrap_err();
    assert_eq!(err, KvError::MissingKey("ghost".into()));
    // A failed command must not advance the applied watermark or create the key.
    assert_eq!(
        m.query(&KvQuery::AppliedThrough).unwrap(),
        KvResponse::AppliedThrough(0)
    );
    assert_eq!(m.query(&KvQuery::Len).unwrap(), KvResponse::Len(0));
}

#[test]
fn append_extends_existing_value() {
    let mut m = KvMachine::default();
    set(&mut m, 1, "greeting", "hel");
    assert_eq!(
        m.apply(
            LogIndex(2),
            &KvCommand::Append {
                key: "greeting".into(),
                suffix: "lo".into(),
            },
        )
        .unwrap(),
        KvResponse::Value("hello".into())
    );
}

#[test]
fn apply_records_the_log_index() {
    let mut m = KvMachine::default();
    set(&mut m, 7, "a", "1");
    set(&mut m, 42, "b", "2");
    assert_eq!(
        m.query(&KvQuery::AppliedThrough).unwrap(),
        KvResponse::AppliedThrough(42)
    );
}

#[test]
fn snapshot_then_restore_reproduces_state() {
    let mut original = KvMachine::default();
    set(&mut original, 1, "a", "1");
    set(&mut original, 2, "b", "2");
    set(&mut original, 3, "c", "3");
    let image = original.snapshot().unwrap();

    let mut restored = KvMachine::default();
    // Put some junk in first to prove restore *replaces* rather than merges.
    set(&mut restored, 99, "stale", "x");
    restored.restore(&image).unwrap();

    assert_eq!(restored, original);
    assert_eq!(
        restored.query(&KvQuery::AppliedThrough).unwrap(),
        KvResponse::AppliedThrough(3)
    );
    assert_eq!(
        restored
            .query(&KvQuery::Get {
                key: "stale".into()
            })
            .unwrap(),
        KvResponse::Get(None)
    );
}

#[test]
fn restore_rejects_malformed_snapshot() {
    let mut m = KvMachine::default();
    let err = m.restore(&[0xff, 0xff, 0xff, 0xff]).unwrap_err();
    assert_eq!(err, KvError::BadSnapshot);
}

#[test]
fn applying_the_same_sequence_is_deterministic() {
    let script = [("a", "1"), ("b", "2"), ("a", "3"), ("c", "4")];
    let mut left = KvMachine::default();
    let mut right = KvMachine::default();
    for (i, (k, v)) in script.iter().enumerate() {
        let index = i as u64 + 1;
        set(&mut left, index, k, v);
        set(&mut right, index, k, v);
    }
    // Identical inputs -> identical state -> byte-identical snapshots.
    assert_eq!(left.snapshot().unwrap(), right.snapshot().unwrap());
}

#[test]
fn command_codec_round_trips_through_the_command_trait() {
    let cmd = KvCommand::Set {
        key: "k".into(),
        value: "v".into(),
    };
    let bytes = Command::to_bytes(&cmd).unwrap();
    assert_eq!(<KvCommand as Command>::from_bytes(&bytes).unwrap(), cmd);
}

#[test]
fn query_codec_round_trips_through_the_query_trait() {
    let q = KvQuery::Get { key: "k".into() };
    let bytes = Query::to_bytes(&q).unwrap();
    assert_eq!(<KvQuery as Query>::from_bytes(&bytes).unwrap(), q);
}
