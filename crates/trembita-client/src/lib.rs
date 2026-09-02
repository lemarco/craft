//! `trembita-client` — client handles for talking to a trembita cluster (client-api).
//!
//! Two layers over a shared [`trembita_proto`] wire contract (client-api):
//!
//! * **In-process (L1):** embed a node and use its `trembita_runtime::NodeHandle`
//!   directly (`propose`/`query`) — no serialization, no network.
//! * **Remote (L2):** [`RemoteClient`] speaks `postcard` over any
//!   [`trembita_net`] transport (live QUIC/HTTP/3 with client mTLS, or the
//!   in-memory `LocalNetwork` in tests). A follower transparently forwards to
//!   the leader server-side (client-routing), so a client can connect to any node; the
//!   built-in [`RetryPolicy`] adds failover + leader-follow for elections and
//!   downed nodes.
//!
//! [`TypedClient`] wraps either layer with a
//! [`StateMachine`](trembita_core::StateMachine)'s command/query/response types.

pub use {trembita_core, trembita_net, trembita_proto};

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
pub use two_phase::{
    InMemoryTwoPhaseJournal, ResumeTwoPhaseOpts, RunTwoPhaseOpts, TwoPhaseClient, TwoPhaseError,
    TwoPhaseEvent, TwoPhaseJournal, TwoPhaseJournalError, TwoPhaseJournalRecord,
    decode_two_phase_journal_record, encode_two_phase_journal_record, propose_cross_shard_2pc,
    propose_cross_shard_2pc_with_opts, resume_cross_shard_2pc,
};
pub use typed::TypedClient;
