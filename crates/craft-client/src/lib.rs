//! `craft-client` — client handles for talking to a craft cluster (client-api).
//!
//! Two layers over a shared [`craft_proto`] wire contract (client-api):
//!
//! * **In-process (L1):** embed a node and use its `craft_actor::NodeHandle`
//!   directly (`propose`/`query`) — no serialization, no network.
//! * **Remote (L2):** [`RemoteClient`] speaks `postcard` over any
//!   [`craft_net`] transport (live QUIC/HTTP/3 with client mTLS, or the
//!   in-memory `LocalNetwork` in tests). A follower transparently forwards to
//!   the leader server-side (client-routing), so a client can connect to any node; the
//!   built-in [`RetryPolicy`] adds failover + leader-follow for elections and
//!   downed nodes.
//!
//! [`TypedClient`] wraps either layer with a
//! [`StateMachine`](craft_core::StateMachine)'s command/query/response types.

pub use {craft_core, craft_net, craft_proto};

mod batch;
mod error;
mod remote;
mod saga;
mod two_phase;
mod typed;

pub use batch::{BatchError, KeyedBatchStep, propose_keyed_batch};
pub use error::ClientError;
pub use remote::{Client, KeyedClient, RemoteClient, RetryPolicy};
pub use saga::{
    InMemorySagaJournal, ResumeSagaOpts, RunSagaOpts, SagaError, SagaEvent, SagaJournal,
    SagaJournalError, SagaJournalPhase, SagaJournalRecord, SagaOutcome, SagaPlan, SagaStep,
    decode_journal_record, encode_journal_record, resume_saga, run_saga,
};
pub use two_phase::{TwoPhaseClient, TwoPhaseError, propose_cross_shard_2pc};
pub use typed::TypedClient;
