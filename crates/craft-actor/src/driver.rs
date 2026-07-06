//! [`RaftDriver`] — the synchronous heart of the node runtime (Wave 2, E3).
//!
//! `craft-core`'s [`RaftNode`] is a pure, I/O-free state machine: it accepts
//! inputs (ticks, RPCs, client proposals) and emits [`Output`] *effects* that
//! some runtime must execute. `RaftDriver` is the piece that composes that core
//! with the user's application [`StateMachine`] and turns those effects into
//! concrete work:
//!
//! * [`Output::Apply`] → decode the command and feed it to
//!   [`StateMachine::apply`], in strict index order, exactly once.
//! * [`Output::ReadReady`] → the ReadIndex protocol confirmed the leader is
//!   current (ADR 005), so a previously registered linearizable query is run
//!   against the applied state via [`StateMachine::query`].
//! * [`Output::ReadFailed`] → leadership was lost before the read could be
//!   served; the pending query is dropped and reported so the client retries.
//! * [`Output::LoadSnapshot`] → restore application state from a leader-shipped
//!   snapshot (Raft §7) via [`StateMachine::restore`].
//! * [`Output::Send`] / [`Output::Reply`] → surfaced as [`NetEffect`]s for the
//!   caller (an actor task wrapping a `craft-net` transport) to dispatch.
//! * [`Output::RoleChanged`] → recorded and surfaced for observability.
//!
//! The driver itself performs **no network or timer I/O**, so it stays fully
//! deterministic and unit-testable: a caller drives it with `tick`/`deliver_*`
//! and routes the returned [`NetEffect`]s. The async actor + `craft-net`
//! transport wiring (E1) layers on top of this.
//!
//! ## Durability (backlog B4)
//!
//! The driver owns a [`RaftStorage`] backend and persists **before** it acts on
//! any effect from a step: at the top of [`drain`](RaftDriver::drain) it takes
//! the core's [`Persist`](craft_core::Persist) delta and writes the hard state
//! and log synchronously. Because a command is only reported to its client via
//! the returned [`Step`] *after* that fsync, the node never acknowledges a
//! commit or reveals a vote that is not yet durable (Raft §5.1–§5.3). On
//! restart, [`RaftDriver::recover`] rebuilds the core from the stored hard
//! state and log; the state machine is then rebuilt by replaying the log as the
//! recovered node re-establishes its commit index.
//!
//! Nodes that opt out of durability (the simulator, in-memory tests) use
//! [`RaftDriver::new`], which installs a [`NullStorage`] that discards writes.
//! Snapshot durability (compaction / `InstallSnapshot`) is a later increment.

use std::collections::HashMap;

use craft_core::Command as _;
use craft_core::{
    Committed, Config, MembershipError, NotLeader, Output, RaftNode, ReadId, Role, StateMachine,
};
use craft_proto::{CodecError, LogIndex, NodeId, RaftRpc, RaftRpcReply};
use craft_storage::{HardState, NullStorage, RaftStorage, StorageError};

/// A network effect the driver produced that the caller must dispatch through
/// a transport. Kept separate from application results so a runtime can send
/// these immediately without blocking on state-machine work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetEffect {
    /// Send a request RPC to `peer`.
    Send {
        /// Destination node.
        peer: NodeId,
        /// The request to send.
        rpc: RaftRpc,
    },
    /// Reply to a peer's earlier request RPC.
    Reply {
        /// Destination node (the original requester).
        peer: NodeId,
        /// The reply to send.
        reply: RaftRpcReply,
    },
}

/// The outcome of a linearizable read once the ReadIndex round resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome<R> {
    /// The read was confirmed and answered against applied state.
    Ready {
        /// The client's read token.
        id: ReadId,
        /// The query response.
        response: R,
    },
    /// The read could not be honored (leadership lost); retry it.
    Failed {
        /// The client's read token.
        id: ReadId,
    },
}

/// Everything produced by draining the core after one input step.
///
/// A caller typically dispatches `effects` over the network, delivers
/// `applied` responses to the clients that proposed those commands, and
/// resolves `reads` to their waiting query clients.
#[derive(Debug)]
pub struct Step<M: StateMachine> {
    /// Network effects to dispatch (in emitted order).
    pub effects: Vec<NetEffect>,
    /// Responses from committed commands applied this step, in index order.
    pub applied: Vec<(LogIndex, M::Response)>,
    /// Linearizable reads that resolved this step.
    pub reads: Vec<ReadOutcome<M::Response>>,
    /// Role transitions observed this step (for observability).
    pub role_changes: Vec<Role>,
}

impl<M: StateMachine> Default for Step<M> {
    fn default() -> Self {
        Self {
            effects: Vec::new(),
            applied: Vec::new(),
            reads: Vec::new(),
            role_changes: Vec::new(),
        }
    }
}

impl<M: StateMachine> Step<M> {
    /// `true` if nothing at all happened this step.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
            && self.applied.is_empty()
            && self.reads.is_empty()
            && self.role_changes.is_empty()
    }
}

/// Errors that can arise while executing core effects against the state
/// machine. These indicate a corrupted log or a broken state machine
/// invariant — not routine client-facing errors, which are carried inside
/// `M::Response`/`M::Error` at the application layer.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    /// This node is not the leader; the client should be redirected.
    #[error("not leader (leader hint: {leader:?})")]
    NotLeader {
        /// Best-known leader for redirection, if any.
        leader: Option<NodeId>,
    },
    /// A command/query could not be encoded or a committed entry could not be
    /// decoded back into the application command type.
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    /// [`StateMachine::apply`] failed on a committed command.
    #[error("state machine apply at index {index:?}: {source}")]
    Apply {
        /// The offending log index.
        index: LogIndex,
        /// The state-machine error, stringified.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// [`StateMachine::query`] failed while serving a confirmed read.
    #[error("state machine query: {0}")]
    Query(Box<dyn std::error::Error + Send + Sync>),
    /// [`StateMachine::restore`] failed while installing a snapshot.
    #[error("state machine restore: {0}")]
    Restore(Box<dyn std::error::Error + Send + Sync>),
    /// A durable-storage read or write failed. This is fatal: the node cannot
    /// safely continue once it can no longer persist Raft state.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

impl From<NotLeader> for DriverError {
    fn from(e: NotLeader) -> Self {
        Self::NotLeader { leader: e.leader }
    }
}

/// Composes a Raft [`RaftNode`] with a user [`StateMachine`], executing the
/// core's committed-command / read / snapshot effects and surfacing network
/// effects for a transport to dispatch. See the module docs for the full
/// contract.
pub struct RaftDriver<M: StateMachine> {
    node: RaftNode,
    machine: M,
    /// Durable backend for the hard state and log (backlog B4). Defaults to a
    /// [`NullStorage`] for nodes that opt out of persistence.
    storage: Box<dyn RaftStorage>,
    /// Queries awaiting their ReadIndex confirmation, keyed by read token.
    pending_queries: HashMap<ReadId, M::Query>,
}

impl<M: StateMachine> RaftDriver<M> {
    /// Create a non-durable driver over an existing `node` and `machine`.
    ///
    /// Writes are discarded through a [`NullStorage`]; nothing survives a
    /// restart. Use [`with_storage`](RaftDriver::with_storage) or
    /// [`recover`](RaftDriver::recover) for durability.
    ///
    /// The `machine` is assumed to already reflect everything the `node` has
    /// marked applied (both are typically fresh, or both restored together).
    pub fn new(node: RaftNode, machine: M) -> Self {
        Self::with_storage(node, machine, Box::new(NullStorage))
    }

    /// Create a durable driver backed by `storage`.
    ///
    /// The `node` and `machine` must already be consistent with `storage` — in
    /// practice they are either both fresh or both produced by
    /// [`recover`](RaftDriver::recover).
    pub fn with_storage(node: RaftNode, machine: M, storage: Box<dyn RaftStorage>) -> Self {
        Self {
            node,
            machine,
            storage,
            pending_queries: HashMap::new(),
        }
    }

    /// Rebuild a driver from durably persisted state after a restart (backlog
    /// B4): loads the hard state and log from `storage` and reconstructs the
    /// core via [`RaftNode::restore`]. `machine` must be a *fresh* state
    /// machine — the recovered node comes back with `last_applied` at 0 and
    /// replays its committed log to rebuild the machine as it re-establishes a
    /// commit index.
    ///
    /// `members` is the bootstrap voter set, used only if the recovered log
    /// carries no membership entry.
    ///
    /// # Errors
    /// Returns [`DriverError::Storage`] if the backend cannot be read.
    pub fn recover(
        id: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        config: Config,
        machine: M,
        storage: Box<dyn RaftStorage>,
    ) -> Result<Self, DriverError> {
        let hard = storage.load_hard_state()?;
        let entries = storage.read_from(LogIndex(1))?;
        let node = RaftNode::restore(
            id,
            members,
            config,
            hard.current_term,
            hard.voted_for,
            entries,
        );
        Ok(Self::with_storage(node, machine, storage))
    }

    /// Borrow the underlying consensus node (state inspection, tests).
    #[must_use]
    pub fn node(&self) -> &RaftNode {
        &self.node
    }

    /// Borrow the application state machine.
    #[must_use]
    pub fn machine(&self) -> &M {
        &self.machine
    }

    /// `true` if this node currently believes it is the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    /// Advance the logical clock by one tick and execute resulting effects.
    ///
    /// # Errors
    /// Returns [`DriverError`] if draining an effect fails (see [`DriverError`]).
    pub fn tick(&mut self) -> Result<Step<M>, DriverError> {
        self.node.tick();
        self.drain()
    }

    /// Start a leader election on this node (test/bootstrap helper) and execute
    /// resulting effects.
    ///
    /// # Errors
    /// Returns [`DriverError`] if draining an effect fails.
    pub fn campaign(&mut self) -> Result<Step<M>, DriverError> {
        self.node.campaign();
        self.drain()
    }

    /// Deliver a peer's request RPC and execute resulting effects.
    ///
    /// # Errors
    /// Returns [`DriverError`] if draining an effect fails.
    pub fn deliver_rpc(&mut self, from: NodeId, rpc: RaftRpc) -> Result<Step<M>, DriverError> {
        self.node.receive(from, rpc);
        self.drain()
    }

    /// Deliver a peer's reply to one of our earlier request RPCs and execute
    /// resulting effects.
    ///
    /// # Errors
    /// Returns [`DriverError`] if draining an effect fails.
    pub fn deliver_reply(
        &mut self,
        from: NodeId,
        reply: RaftRpcReply,
    ) -> Result<Step<M>, DriverError> {
        self.node.receive_reply(from, reply);
        self.drain()
    }

    /// Propose an application command. Succeeds only on the leader.
    ///
    /// Returns the log index the command was appended at together with the
    /// effects produced (replication sends, plus the command's own `apply`
    /// result if it committed synchronously, e.g. in a single-node cluster).
    ///
    /// # Errors
    /// Returns [`DriverError::NotLeader`] if this node is not the leader,
    /// [`DriverError::Codec`] if the command cannot be encoded, or a drain
    /// error if applying a resulting committed command fails.
    pub fn propose(&mut self, command: &M::Command) -> Result<(LogIndex, Step<M>), DriverError> {
        let bytes = command.to_bytes()?;
        let index = self.node.propose(bytes)?;
        let step = self.drain()?;
        Ok((index, step))
    }

    /// Register a linearizable read (ReadIndex, ADR 005). Succeeds only on the
    /// leader. The query is held until the core confirms the read is safe, at
    /// which point it is answered and surfaced as [`ReadOutcome::Ready`] in a
    /// later [`Step`] (possibly this one for a single-node cluster).
    ///
    /// # Errors
    /// Returns [`DriverError::NotLeader`] if this node is not the leader, or a
    /// drain error if serving a resulting confirmed read fails.
    pub fn query(&mut self, id: ReadId, query: M::Query) -> Result<Step<M>, DriverError> {
        // Register the query first so a synchronously-confirmed read (single
        // node) finds it during the drain below.
        self.pending_queries.insert(id, query);
        match self.node.read_index(id) {
            Ok(()) => self.drain(),
            Err(e) => {
                self.pending_queries.remove(&id);
                Err(e.into())
            }
        }
    }

    /// Begin a joint-consensus membership change to `new_voters` (+ optional
    /// `learners`) — the log entry underpinning a cluster join/leave (ADR 016).
    ///
    /// The outer `Result` reports a fatal drain failure (which stops the node);
    /// the inner `Result` reports whether the change was accepted (returning the
    /// log index of the joint-config entry and the effects to dispatch) or
    /// rejected by the core with a [`MembershipError`] (not leader, a change
    /// already in flight, or an empty voter set).
    ///
    /// # Errors
    /// Returns [`DriverError`] only if draining a resulting committed command
    /// fails; membership rejections are carried in the inner `Result`.
    pub fn propose_membership(
        &mut self,
        new_voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<Result<(LogIndex, Step<M>), MembershipError>, DriverError> {
        match self.node.propose_membership(new_voters, learners) {
            Ok(index) => Ok(Ok((index, self.drain()?))),
            Err(e) => Ok(Err(e)),
        }
    }

    /// Persist the core's durable delta (hard state + log) for this step.
    ///
    /// Run before any effect is surfaced so a follower never ack's an entry it
    /// has not fsync'd and a node never reveals a vote it has not recorded
    /// (Raft §5.1–§5.3).
    fn persist(&mut self) -> Result<(), DriverError> {
        let Some(delta) = self.node.take_persist() else {
            return Ok(());
        };
        if delta.hard_state_dirty {
            self.storage.save_hard_state(&HardState {
                current_term: delta.term,
                voted_for: delta.voted_for,
            })?;
        }
        if let Some(from) = delta.truncate_from {
            self.storage.truncate_suffix(from)?;
        }
        if !delta.entries.is_empty() {
            self.storage.append(&delta.entries)?;
        }
        Ok(())
    }

    /// Drain and execute every pending core [`Output`].
    fn drain(&mut self) -> Result<Step<M>, DriverError> {
        self.persist()?;
        let mut step = Step::default();
        for output in self.node.take_outputs() {
            match output {
                Output::Send(peer, rpc) => step.effects.push(NetEffect::Send { peer, rpc }),
                Output::Reply(peer, reply) => step.effects.push(NetEffect::Reply { peer, reply }),
                Output::Apply(committed) => {
                    let (index, response) = self.apply_committed(committed)?;
                    step.applied.push((index, response));
                }
                Output::ReadReady { id, index } => {
                    if let Some(outcome) = self.serve_read(id, index)? {
                        step.reads.push(outcome);
                    }
                }
                Output::ReadFailed { id } => {
                    if self.pending_queries.remove(&id).is_some() {
                        step.reads.push(ReadOutcome::Failed { id });
                    }
                }
                Output::LoadSnapshot { data, .. } => {
                    self.machine
                        .restore(&data)
                        .map_err(|e| DriverError::Restore(Box::new(e)))?;
                }
                Output::RoleChanged(role) => step.role_changes.push(role),
            }
        }
        Ok(step)
    }

    /// Decode and apply a single committed command to the state machine.
    fn apply_committed(
        &mut self,
        committed: Committed,
    ) -> Result<(LogIndex, M::Response), DriverError> {
        let index = committed.index;
        let command = M::Command::from_bytes(&committed.command)?;
        let response = self
            .machine
            .apply(index, &command)
            .map_err(|e| DriverError::Apply {
                index,
                source: Box::new(e),
            })?;
        Ok((index, response))
    }

    /// Serve a query whose ReadIndex round was just confirmed.
    fn serve_read(
        &mut self,
        id: ReadId,
        _index: LogIndex,
    ) -> Result<Option<ReadOutcome<M::Response>>, DriverError> {
        let Some(query) = self.pending_queries.remove(&id) else {
            // No registered query for this token (already resolved or foreign);
            // nothing to serve.
            return Ok(None);
        };
        let response = self
            .machine
            .query(&query)
            .map_err(|e| DriverError::Query(Box::new(e)))?;
        Ok(Some(ReadOutcome::Ready { id, response }))
    }
}
