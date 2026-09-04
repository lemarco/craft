//! Shared imports for [`RaftNode`] impl submodules (internal; not re-exported).

#![allow(unused_imports)]

pub(crate) use std::collections::{BTreeMap, BTreeSet};

pub(crate) use trembita_proto::{
    AppendEntries, AppendEntriesReply, CatalogCommand, EntryPayload, InstallSnapshot,
    InstallSnapshotReply, LogEntry, LogId, LogIndex, Membership, NodeId,
    QueueAutoscalePolicyCommand, RaftRpc, RaftRpcReply, RequestVote, RequestVoteReply, Round,
    SagaJournalCommand, Term, TwoPhaseAbortCommand, TwoPhaseJournalCommand, TwoPhasePrepareCommand,
};

pub(crate) use crate::config::Configuration;
pub(crate) use crate::failure_detector::{
    AckWindowLiveness, FailureDetectorKind, PhiAccrualLiveness,
};
pub(crate) use crate::log::Log;
pub(crate) use crate::rng::Rng;

pub(crate) use super::types::{
    CatalogProposeError, Committed, Config, MembershipError, NotLeader, Output, Persist, ReadId,
    Role, SnapshotState,
};
