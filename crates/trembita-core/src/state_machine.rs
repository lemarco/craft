//! Application state-machine API (state-machine).
//!
//! A cluster replicates an opaque log; the *application* decides what those
//! entries mean by implementing [`StateMachine`]. The Raft core commits and
//! orders entries, then the runtime feeds each committed command to
//! [`StateMachine::apply`] exactly once, in index order (architecture-style applier loop).
//!
//! ## Encode/decode glue (state-machine)
//!
//! state-machine called for macros to generate `Encode`/`Decode` glue and to check
//! that command types are *owned* and *clone-safe* for replication. In this
//! stack that glue is already provided generically by `serde` + `postcard`, so
//! instead of a bespoke derive we expose the [`Command`] and [`Query`] marker
//! traits with **blanket implementations** over any `serde` type that also
//! satisfies the replication bounds. The bounds (`Clone + Send + 'static`) are
//! exactly the "owned & clone-safe" compile-time check state-machine asked for — a
//! type borrowing a lifetime, or one that is not `Clone`, simply will not
//! satisfy [`Command`] and the code will not compile.

use serde::Serialize;
use serde::de::DeserializeOwned;
use trembita_proto::{CodecError, LogIndex, decode, encode};

/// A replicated command applied to the [`StateMachine`].
///
/// Commands are serialized into the Raft log, shipped to peers, and later
/// decoded and applied, so they must be self-owned (`'static`), `Clone`able
/// (a leader may retry replication), and `Send` (they cross task/actor
/// boundaries). Any type meeting those bounds plus `serde` gets a [`Command`]
/// implementation for free via the blanket impl below — derive
/// `#[derive(Clone, Serialize, Deserialize)]` and you are done.
pub trait Command: Clone + Send + 'static {
    /// Encode this command to `postcard` bytes for the log/wire.
    ///
    /// # Errors
    /// Returns [`CodecError`] if serialization fails.
    fn to_bytes(&self) -> Result<Vec<u8>, CodecError>;

    /// Decode a command from `postcard` bytes read back from the log/wire.
    ///
    /// # Errors
    /// Returns [`CodecError`] if deserialization fails.
    fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError>
    where
        Self: Sized;
}

impl<T> Command for T
where
    T: Clone + Send + 'static + Serialize + DeserializeOwned,
{
    fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        encode(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        decode(bytes)
    }
}

/// A read-only query served by the [`StateMachine`].
///
/// Queries never enter the log; they are answered locally after the `ReadIndex`
/// protocol confirms the leader is current (read-consistency). They still cross task
/// boundaries and may be sent to a remote leader, hence `Send + 'static` +
/// `serde`. Unlike a [`Command`], a query need not be `Clone`.
pub trait Query: Send + 'static {
    /// Encode this query to `postcard` bytes for the wire.
    ///
    /// # Errors
    /// Returns [`CodecError`] if serialization fails.
    fn to_bytes(&self) -> Result<Vec<u8>, CodecError>;

    /// Decode a query from `postcard` bytes.
    ///
    /// # Errors
    /// Returns [`CodecError`] if deserialization fails.
    fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError>
    where
        Self: Sized;
}

impl<T> Query for T
where
    T: Send + 'static + Serialize + DeserializeOwned,
{
    fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        encode(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        decode(bytes)
    }
}

/// The user-defined, deterministic application state machine (state-machine).
///
/// Implementations must be **deterministic**: applying the same sequence of
/// commands from the same snapshot must always yield the same state and the
/// same per-command [`Response`](StateMachine::Response). This is what lets a
/// lagging follower rebuild identical state purely from the replicated log, and
/// what lets the deterministic simulator (testing-strategy) reproduce runs. Avoid wall
/// clocks, RNGs, and external I/O inside [`apply`](StateMachine::apply); feed
/// any such inputs in through the command instead.
pub trait StateMachine: Send + 'static {
    /// The command type applied by [`apply`](StateMachine::apply).
    type Command: Command;
    /// The read-only query type answered by [`query`](StateMachine::query).
    type Query: Query;
    /// The value returned to the client from an `apply` or `query`.
    type Response: Send + 'static + Serialize + DeserializeOwned;
    /// The error type surfaced when a command or query cannot be handled.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Apply a committed command at log `index`, mutating state and returning a
    /// response for the client.
    ///
    /// The runtime calls this exactly once per committed command, in ascending
    /// index order. `index` is provided so implementations can persist an
    /// applied-through watermark for idempotent external side effects
    /// (actor-state-redis), though the in-log state itself needs no such bookkeeping.
    ///
    /// # Errors
    /// Returns [`Self::Error`](StateMachine::Error) if the command is invalid
    /// for the current state. Note that a returned error does **not** roll back
    /// the log entry — it is reported to the client while the command remains
    /// committed, so implementations should validate before mutating.
    fn apply(
        &mut self,
        index: LogIndex,
        command: &Self::Command,
    ) -> Result<Self::Response, Self::Error>;

    /// Answer a read-only query against the current applied state.
    ///
    /// Must not mutate state. Linearizability is guaranteed by the caller via
    /// `ReadIndex` (read-consistency), not by this method.
    ///
    /// # Errors
    /// Returns [`Self::Error`](StateMachine::Error) if the query is invalid.
    fn query(&self, query: &Self::Query) -> Result<Self::Response, Self::Error>;

    /// Serialize the entire machine state into a snapshot image for log
    /// compaction (Raft §7). The bytes are opaque to the core; only
    /// [`restore`](StateMachine::restore) interprets them.
    ///
    /// # Errors
    /// Returns [`Self::Error`](StateMachine::Error) if the state cannot be
    /// serialized.
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error>;

    /// Replace the machine state with the one encoded in `snapshot`, discarding
    /// any current state. Called when a follower installs a leader snapshot or
    /// a node restarts from disk.
    ///
    /// # Errors
    /// Returns [`Self::Error`](StateMachine::Error) if the snapshot is
    /// malformed.
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error>;
}
