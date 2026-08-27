//! The node runtime — an async event loop (spawned by [`spawn`]) that turns a
//! [`RaftDriver`] into a live, networked node (backlog E1/E2/E4).
//!
//! [`RaftDriver`] is synchronous and I/O-free: it must be *driven*. This module
//! supplies the drive train:
//!
//! * A **tokio task** owns the driver and selects over a periodic tick (the
//!   election/heartbeat clock, E2) and an inbound mailbox.
//! * Outbound [`NetEffect::Send`](crate::NetEffect)s are dispatched over a
//!   [`craft_net`] [`Transport`]; each peer reply is fed back into the mailbox,
//!   so request/response transports drive the core's fire-and-forget model.
//! * Client **proposals** and **queries** are correlated to their results:
//!   a proposal's `oneshot` responder is keyed by the log index it lands at and
//!   fired when that index applies; a query's responder is keyed by its
//!   [`ReadId`] and fired when the ReadIndex round confirms.
//! * A [`NodeService`] adapter implements [`craft_net`]'s [`RequestHandler`] so
//!   a `QuicServer` (or the in-memory `LocalNetwork`) can route inbound
//!   `/peer/wire` and `/client/wire` requests into the running node.
//!
//! The loop holds an `Arc<dyn Transport>`, so the exact same runtime runs over
//! the deterministic `LocalNetwork` in tests and over live QUIC in production
//! (wire-transport) with no code changes.
//!
//! ## Not yet wired (tracked in the backlog)
//!
//! * **Durable persistence** (B4): the in-memory core log is the source of
//!   truth; hard state and the log are not yet flushed through `craft-storage`,
//!   so a restart loses state.
//! * **Log compaction / snapshots** (Track G): leaders can compact via
//!   [`NodeHandle::compact`]; inbound `InstallSnapshot` restore is handled via
//!   the driver. Automatic background compaction is not wired yet.
//! * **Per-connection identity** (C5): [`NodeService`] trusts the sender id
//!   declared inside a peer RPC instead of the presented client certificate.
//! * **Fatal errors are silent**: a corrupt-log / state-machine failure stops
//!   the loop with no diagnostic until `tracing` lands (Track H).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use craft_core::{Command as _, MembershipError, Query as _, ReadId, Role, StateMachine};
use craft_net::transport::{Body, BoxFuture};
use craft_net::{
    RequestHandler, Route, Transport, TransportError, decode_body, encode_body,
    send_client_request, send_join_request, send_leave_request, send_peer_rpc,
};
use craft_proto::{
    ClientRequest, ClientResponse, JoinRejection, JoinRequest, JoinResponse, LeaveRejection,
    LeaveRequest, LeaveResponse, LogIndex, NodeId, PROTOCOL_VERSION, RaftRpc, RaftRpcReply, Term,
    protocol_version_compatible,
};
use tokio::sync::{mpsc, oneshot};

use crate::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};

/// An error returned to a client whose request could not be completed.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// This node is not the leader; retry against `leader` (transparent
    /// forwarding is a later increment, client-routing).
    #[error("not leader (leader hint: {leader:?})")]
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// The node runtime has stopped (shut down or crashed); the result will
    /// never arrive.
    #[error("node runtime stopped")]
    Stopped,
    /// A driver-level failure while servicing the request.
    #[error("{0}")]
    Driver(String),
}

/// A point-in-time snapshot of a node's consensus status (observability).
#[derive(Debug, Clone)]
pub struct NodeStatus {
    /// This node's id.
    pub id: NodeId,
    /// Current role.
    pub role: Role,
    /// Current term.
    pub term: Term,
    /// Best-known leader, if any.
    pub leader: Option<NodeId>,
    /// Highest committed index.
    pub commit_index: LogIndex,
    /// Highest applied index.
    pub last_applied: LogIndex,
    /// The current committed voter set (Raft membership), sorted.
    pub voters: Vec<NodeId>,
    /// Non-voting learners in the committed configuration, sorted.
    pub learners: Vec<NodeId>,
    /// The voters this node currently considers **reachable** — a liveness
    /// signal distinct from committed membership (liveness-vs-membership). On the leader this
    /// drops voters that have stopped acking heartbeats (crashed / partitioned)
    /// even though they remain committed voters; a follower reports all voters.
    pub reachable: Vec<NodeId>,
}

/// Internal mailbox messages processed by the runtime loop.
enum Envelope<M: StateMachine> {
    Rpc {
        from: NodeId,
        rpc: RaftRpc,
        respond: oneshot::Sender<RaftRpcReply>,
    },
    Reply {
        from: NodeId,
        reply: RaftRpcReply,
    },
    Propose {
        command: M::Command,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    Query {
        query: M::Query,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    /// Leader-only: confirm a ReadIndex without executing a query.
    ConfirmReadIndex {
        respond: oneshot::Sender<Result<(LogIndex, Term), ClientError>>,
    },
    /// Follower-only: query local state after the apply barrier.
    LocalQuery {
        query: M::Query,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    Join {
        request: JoinRequest,
        respond: oneshot::Sender<JoinResponse>,
    },
    Leave {
        request: LeaveRequest,
        respond: oneshot::Sender<LeaveResponse>,
    },
    /// Leader-only: begin a joint-consensus membership change (per-group-raft-membership).
    ProposeMembership {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    Campaign,
    Status {
        respond: oneshot::Sender<NodeStatus>,
    },
    ExportMigration {
        respond: oneshot::Sender<Result<craft_proto::GroupMigrationBundle, ClientError>>,
    },
    /// Compact the applied log prefix into a durable snapshot (Raft §7).
    Compact {
        respond: oneshot::Sender<Result<bool, ClientError>>,
    },
    Shutdown {
        done: Option<oneshot::Sender<()>>,
    },
}

/// Tunables for the runtime loop.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Wall-clock duration of one logical Raft tick. The core's timeouts are in
    /// ticks (see [`craft_core::Config`]); this maps them onto real time.
    pub tick_period: Duration,
    /// Whether this node accepts cluster joins (`--allow-join`, join-rpc). When
    /// `false`, `/cluster/join` requests are rejected with
    /// [`JoinRejection::JoinsDisabled`].
    pub allow_join: bool,
    /// Whether this node accepts cluster leaves (`--allow-leave`). When `false`,
    /// `/cluster/leave` requests are rejected with
    /// [`LeaveRejection::LeavesDisabled`].
    pub allow_leave: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            tick_period: Duration::from_millis(50),
            allow_join: false,
            allow_leave: false,
        }
    }
}

/// A cloneable handle to a running node (see [`spawn`]). Dropping every handle
/// does not stop the node; call [`shutdown`](NodeHandle::shutdown) for that.
pub struct NodeHandle<M: StateMachine> {
    id: NodeId,
    tx: mpsc::UnboundedSender<Envelope<M>>,
}

impl<M: StateMachine> Clone for NodeHandle<M> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            tx: self.tx.clone(),
        }
    }
}

impl<M: StateMachine> NodeHandle<M> {
    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Propose an application command and await its applied response.
    ///
    /// Resolves once the command commits and applies on this node (which
    /// requires it to be, and remain, the leader for the round).
    ///
    /// # Errors
    /// [`ClientError::NotLeader`] if this node is not the leader when the
    /// proposal is made **or** if it loses leadership before the command
    /// commits (in the latter case the command may still commit under the new
    /// leader, so commands should be idempotent — actor-state-redis), or
    /// [`ClientError::Stopped`] if the runtime shut down before the command
    /// applied.
    pub async fn propose(&self, command: M::Command) -> Result<M::Response, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Propose { command, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Run a linearizable query (ReadIndex, read-consistency) and await its result.
    ///
    /// # Errors
    /// [`ClientError::NotLeader`] if this node is not the leader, or
    /// [`ClientError::Stopped`] if the runtime shut down first.
    pub async fn query(&self, query: M::Query) -> Result<M::Response, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Query { query, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Confirm a linearizable read index on the leader (follower-read setup).
    pub async fn confirm_read_index(&self) -> Result<(LogIndex, Term), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::ConfirmReadIndex { respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Run a query against local applied state (after a confirmed read index
    /// and apply barrier on a follower).
    pub async fn local_query(&self, query: M::Query) -> Result<M::Response, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::LocalQuery { query, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Export durable Raft state for cross-node group migration (write-sharding-multi-raft).
    pub async fn export_migration(&self) -> Result<craft_proto::GroupMigrationBundle, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::ExportMigration { respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Etcd-style follower read: confirm with the leader, wait for the apply
    /// barrier, then serve from local state.
    pub async fn follower_query_bytes(
        &self,
        query_bytes: Vec<u8>,
        route_key: Option<Vec<u8>>,
        leader: NodeId,
        transport: &Arc<dyn Transport>,
        timeout: Duration,
    ) -> Result<M::Response, ClientError> {
        let query = M::Query::from_bytes(&query_bytes)
            .map_err(|e| ClientError::Driver(format!("decode query: {e}")))?;
        let confirm = tokio::time::timeout(
            timeout,
            send_client_request(
                &**transport,
                leader,
                &ClientRequest::ReadIndexConfirm {
                    route_key: route_key.clone(),
                },
            ),
        )
        .await
        .map_err(|_| ClientError::Driver("read index confirm timed out".to_string()))?
        .map_err(|e| ClientError::Driver(format!("read index confirm failed: {e}")))?;
        let ClientResponse::ReadIndexConfirmed { index, .. } = confirm else {
            return Err(ClientError::Driver(format!(
                "leader rejected read index confirm: {confirm:?}"
            )));
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let Some(status) = self.status().await else {
                return Err(ClientError::Stopped);
            };
            if status.last_applied >= index {
                return self.local_query(query).await;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClientError::Driver(
                    "apply barrier timed out waiting for read index".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Deliver an inbound peer request RPC and await the reply to send back.
    /// Used by [`NodeService`]; rarely called directly.
    ///
    /// # Errors
    /// [`ClientError::Stopped`] if the runtime shut down before replying.
    pub async fn deliver_rpc(
        &self,
        from: NodeId,
        rpc: RaftRpc,
    ) -> Result<RaftRpcReply, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Rpc { from, rpc, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)
    }

    /// Submit a cluster [`JoinRequest`] (join-rpc). On the leader this triggers a
    /// membership change and resolves once it commits; on a follower it returns
    /// [`JoinResponse::Redirect`] (the [`NodeService`] proxies for remote
    /// callers).
    ///
    /// # Errors
    /// [`ClientError::Stopped`] if the runtime shut down before responding.
    pub async fn join(&self, request: JoinRequest) -> Result<JoinResponse, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Join { request, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)
    }

    /// Submit a cluster [`LeaveRequest`]. On the leader this triggers a
    /// membership change and resolves once it commits; on a follower it returns
    /// [`LeaveResponse::Redirect`] (the [`NodeService`] proxies for remote
    /// callers).
    ///
    /// # Errors
    /// [`ClientError::Stopped`] if the runtime shut down before responding.
    pub async fn leave(&self, request: LeaveRequest) -> Result<LeaveResponse, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Leave { request, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)
    }

    /// Propose a joint-consensus membership change to `voters` when this node
    /// is the Raft leader for the group (per-group-raft-membership).
    ///
    /// # Errors
    /// [`ClientError::NotLeader`] or [`ClientError::Driver`] when the core
    /// rejects the change; [`ClientError::Stopped`] if the runtime shut down.
    pub async fn propose_membership(
        &self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> Result<(), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::ProposeMembership {
                voters,
                learners,
                respond,
            })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
    }

    /// Force an immediate election (test/bootstrap helper).
    pub fn campaign(&self) {
        let _ = self.tx.send(Envelope::Campaign);
    }

    /// Snapshot applied state and purge the compacted log prefix durably.
    ///
    /// Returns `Ok(false)` when there is nothing new to compact.
    ///
    /// # Errors
    /// [`ClientError::Driver`] if snapshot capture or persistence fails;
    /// [`ClientError::Stopped`] if the runtime shut down first.
    pub async fn compact(&self) -> Result<bool, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::Compact { respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
    }

    /// Fetch a status snapshot, or `None` if the runtime has stopped.
    pub async fn status(&self) -> Option<NodeStatus> {
        let (respond, rx) = oneshot::channel();
        self.tx.send(Envelope::Status { respond }).ok()?;
        rx.await.ok()
    }

    /// Ask the runtime to stop after draining the current message.
    pub fn shutdown(&self) {
        let _ = self.tx.send(Envelope::Shutdown { done: None });
    }

    /// Stop the runtime and wait until it has exited (storage handles released).
    pub async fn shutdown_and_wait(&self) {
        let (done, rx) = oneshot::channel();
        if self
            .tx
            .send(Envelope::Shutdown { done: Some(done) })
            .is_err()
        {
            return;
        }
        let _ = rx.await;
    }
}

type ReadConfirmSender = oneshot::Sender<Result<(LogIndex, Term), ClientError>>;

/// Owns the driver and mutable correlation state inside the loop task.
struct Runtime<M: StateMachine> {
    driver: RaftDriver<M>,
    transport: Arc<dyn Transport>,
    self_tx: mpsc::UnboundedSender<Envelope<M>>,
    allow_join: bool,
    allow_leave: bool,
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<M::Response, ClientError>>>,
    pending_queries: HashMap<ReadId, oneshot::Sender<Result<M::Response, ClientError>>>,
    /// Leader ReadIndex confirmations awaiting quorum ack (follower-read setup).
    pending_read_confirms: HashMap<ReadId, ReadConfirmSender>,
    /// Join requests awaiting their membership-change entry to commit, keyed by
    /// that entry's log index.
    pending_joins: HashMap<LogIndex, oneshot::Sender<JoinResponse>>,
    /// Leave requests awaiting their membership-change entry to commit.
    pending_leaves: HashMap<LogIndex, oneshot::Sender<LeaveResponse>>,
    next_read_id: u64,
}

impl<M: StateMachine> Runtime<M> {
    /// Dispatch one outbound request RPC; feed its reply back into the mailbox.
    fn dispatch_send(&self, peer: NodeId, rpc: RaftRpc) {
        let transport = Arc::clone(&self.transport);
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            if let Ok(reply) = send_peer_rpc(&*transport, peer, &rpc).await {
                let _ = tx.send(Envelope::Reply { from: peer, reply });
            }
            // On transport error the peer is unreachable for now; the next
            // heartbeat/election round will retry. Nothing to feed back.
        });
    }

    /// Execute a step's effects and route applied/read results to waiting
    /// clients. Returns any reply effects (destined for a peer that made an
    /// inbound request) for the caller to hand back on that request.
    fn settle(&mut self, step: Step<M>) -> Vec<(NodeId, RaftRpcReply)> {
        let mut replies = Vec::new();
        for effect in step.effects {
            match effect {
                NetEffect::Send { peer, rpc } => self.dispatch_send(peer, rpc),
                NetEffect::Reply { peer, reply } => replies.push((peer, reply)),
            }
        }
        for (index, response) in step.applied {
            if let Some(tx) = self.pending_proposals.remove(&index) {
                let _ = tx.send(Ok(response));
            }
        }
        for read in step.reads {
            match read {
                ReadOutcome::Ready { id, response } => {
                    if let Some(tx) = self.pending_queries.remove(&id) {
                        let _ = tx.send(Ok(response));
                    }
                }
                ReadOutcome::Confirmed { id, index } => {
                    if let Some(tx) = self.pending_read_confirms.remove(&id) {
                        let term = self.driver.node().current_term();
                        let _ = tx.send(Ok((index, term)));
                    }
                }
                ReadOutcome::Failed { id } => {
                    if let Some(tx) = self.pending_read_confirms.remove(&id) {
                        let _ = tx.send(Err(ClientError::NotLeader {
                            leader: self.driver.node().leader_id(),
                        }));
                    } else if let Some(tx) = self.pending_queries.remove(&id) {
                        let _ = tx.send(Err(ClientError::NotLeader {
                            leader: self.driver.node().leader_id(),
                        }));
                    }
                }
            }
        }
        // If we are no longer the leader, any still-outstanding client request
        // will never resolve here: an uncommitted proposal in our tail may be
        // overwritten by the new leader, and the core only reports read
        // failures. Fail them all with a `NotLeader` hint so callers stop
        // waiting and retry against the new leader. (A proposal that had
        // already committed applies above before this runs; anything failed
        // here may still commit under the new leader, so proposals must be
        // idempotent — see actor-state-redis.)
        self.resolve_committed_joins();
        self.resolve_committed_leaves();
        if !self.driver.is_leader() {
            self.fail_pending_requests();
        }
        replies
    }

    /// Complete any join whose membership-change entry has now committed.
    fn resolve_committed_joins(&mut self) {
        if self.pending_joins.is_empty() {
            return;
        }
        let commit = self.driver.node().commit_index();
        let ready: Vec<LogIndex> = self
            .pending_joins
            .keys()
            .copied()
            .filter(|index| commit >= *index)
            .collect();
        if ready.is_empty() {
            return;
        }
        let leader = self.driver.node().id();
        let membership = self.driver.node().committed_membership();
        for index in ready {
            if let Some(tx) = self.pending_joins.remove(&index) {
                let _ = tx.send(JoinResponse::Accepted {
                    leader,
                    membership: membership.clone(),
                });
            }
        }
    }

    /// Complete any leave whose membership-change entry has now committed.
    fn resolve_committed_leaves(&mut self) {
        if self.pending_leaves.is_empty() {
            return;
        }
        let commit = self.driver.node().commit_index();
        let ready: Vec<LogIndex> = self
            .pending_leaves
            .keys()
            .copied()
            .filter(|index| commit >= *index)
            .collect();
        if ready.is_empty() {
            return;
        }
        let leader = self.driver.node().id();
        let membership = self.driver.node().committed_membership();
        for index in ready {
            if let Some(tx) = self.pending_leaves.remove(&index) {
                let _ = tx.send(LeaveResponse::Accepted {
                    leader,
                    membership: membership.clone(),
                });
            }
        }
    }

    /// Fail every outstanding client request and join with a leader hint after
    /// losing leadership.
    fn fail_pending_requests(&mut self) {
        let leader = self.driver.node().leader_id();
        for (_, tx) in self.pending_proposals.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_queries.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_read_confirms.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_joins.drain() {
            let _ = tx.send(JoinResponse::Redirect { leader });
        }
        for (_, tx) in self.pending_leaves.drain() {
            let _ = tx.send(LeaveResponse::Redirect { leader });
        }
    }

    /// Process one mailbox message. Returns `Err` on a fatal driver failure
    /// (corrupt log / broken state machine), which stops the node.
    fn on_envelope(&mut self, env: Envelope<M>) -> Result<bool, DriverError> {
        match env {
            Envelope::Shutdown { .. } => return Ok(false),
            Envelope::Rpc { from, rpc, respond } => {
                let step = self.driver.deliver_rpc(from, rpc)?;
                let replies = self.settle(step);
                if let Some(reply) = replies
                    .into_iter()
                    .find_map(|(peer, reply)| (peer == from).then_some(reply))
                {
                    let _ = respond.send(reply);
                }
                // If no reply was produced the responder drops and the caller
                // observes a transport error — expected only for malformed input.
            }
            Envelope::Reply { from, reply } => {
                let step = self.driver.deliver_reply(from, reply)?;
                let _ = self.settle(step);
            }
            Envelope::Propose { command, respond } => match self.driver.propose(&command) {
                Ok((index, step)) => {
                    self.pending_proposals.insert(index, respond);
                    let _ = self.settle(step);
                }
                Err(DriverError::NotLeader { leader }) => {
                    let _ = respond.send(Err(ClientError::NotLeader { leader }));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            },
            Envelope::Query { query, respond } => {
                let id = ReadId(self.next_read_id);
                self.next_read_id += 1;
                match self.driver.query(id, query) {
                    Ok(step) => {
                        self.pending_queries.insert(id, respond);
                        let _ = self.settle(step);
                    }
                    Err(DriverError::NotLeader { leader }) => {
                        let _ = respond.send(Err(ClientError::NotLeader { leader }));
                    }
                    Err(e) => {
                        let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                    }
                }
            }
            Envelope::ConfirmReadIndex { respond } => {
                let id = ReadId(self.next_read_id);
                self.next_read_id += 1;
                match self.driver.confirm_read_index(id) {
                    Ok(step) => {
                        self.pending_read_confirms.insert(id, respond);
                        let _ = self.settle(step);
                    }
                    Err(DriverError::NotLeader { leader }) => {
                        let _ = respond.send(Err(ClientError::NotLeader { leader }));
                    }
                    Err(e) => {
                        let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                    }
                }
            }
            Envelope::LocalQuery { query, respond } => match self.driver.local_query(&query) {
                Ok(response) => {
                    let _ = respond.send(Ok(response));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            },
            Envelope::Join { request, respond } => {
                self.on_join(request, respond)?;
            }
            Envelope::Leave { request, respond } => {
                self.on_leave(request, respond)?;
            }
            Envelope::ProposeMembership {
                voters,
                learners,
                respond,
            } => {
                self.on_propose_membership(voters, learners, respond)?;
            }
            Envelope::Campaign => {
                let step = self.driver.campaign()?;
                let _ = self.settle(step);
            }
            Envelope::Status { respond } => {
                let node = self.driver.node();
                let _ = respond.send(NodeStatus {
                    id: node.id(),
                    role: node.role(),
                    term: node.current_term(),
                    leader: node.leader_id(),
                    commit_index: node.commit_index(),
                    last_applied: node.last_applied(),
                    voters: node.voters(),
                    learners: node.committed_membership().learners,
                    reachable: node.reachable_now(),
                });
            }
            Envelope::ExportMigration { respond } => {
                let result = self
                    .driver
                    .export_migration()
                    .map_err(|e| ClientError::Driver(e.to_string()));
                let _ = respond.send(result);
            }
            Envelope::Compact { respond } => {
                let result = self
                    .driver
                    .compact()
                    .map_err(|e| ClientError::Driver(e.to_string()));
                let _ = respond.send(result);
            }
        }
        Ok(true)
    }

    /// Validate and (on the leader) start a cluster join as a membership change
    /// (join-rpc, join-version-skew). The join resolves to [`JoinResponse::Accepted`] once the
    /// membership entry commits (see [`resolve_committed_joins`]).
    fn on_join(
        &mut self,
        request: JoinRequest,
        respond: oneshot::Sender<JoinResponse>,
    ) -> Result<(), DriverError> {
        // Hard-reject a protocol-version mismatch before anything else (join-version-skew).
        if !protocol_version_compatible(request.protocol_version) {
            let _ = respond.send(JoinResponse::Rejected {
                reason: JoinRejection::VersionSkew {
                    expected: PROTOCOL_VERSION,
                    got: request.protocol_version,
                },
            });
            return Ok(());
        }
        if !self.allow_join {
            let _ = respond.send(JoinResponse::Rejected {
                reason: JoinRejection::JoinsDisabled,
            });
            return Ok(());
        }
        if !self.driver.is_leader() {
            let _ = respond.send(JoinResponse::Redirect {
                leader: self.driver.node().leader_id(),
            });
            return Ok(());
        }
        let mut voters = self.driver.node().voters();
        if voters.contains(&request.node_id) {
            let _ = respond.send(JoinResponse::Rejected {
                reason: JoinRejection::Duplicate,
            });
            return Ok(());
        }
        voters.push(request.node_id);

        match self.driver.propose_membership(voters, Vec::new())? {
            Ok((index, step)) => {
                self.pending_joins.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(MembershipError::NotLeader { leader }) => {
                let _ = respond.send(JoinResponse::Redirect { leader });
            }
            Err(MembershipError::InProgress) => {
                let _ = respond.send(JoinResponse::Rejected {
                    reason: JoinRejection::Other(
                        "a membership change is already in progress".to_string(),
                    ),
                });
            }
            Err(MembershipError::EmptyVoters) => {
                let _ = respond.send(JoinResponse::Rejected {
                    reason: JoinRejection::Other("resulting voter set is empty".to_string()),
                });
            }
        }
        Ok(())
    }

    /// Validate and (on the leader) start a cluster leave as a membership change.
    /// The leave resolves to [`LeaveResponse::Accepted`] once the membership
    /// entry commits (see [`resolve_committed_leaves`]).
    fn on_leave(
        &mut self,
        request: LeaveRequest,
        respond: oneshot::Sender<LeaveResponse>,
    ) -> Result<(), DriverError> {
        if !protocol_version_compatible(request.protocol_version) {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::VersionSkew {
                    expected: PROTOCOL_VERSION,
                    got: request.protocol_version,
                },
            });
            return Ok(());
        }
        if !self.allow_leave {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::LeavesDisabled,
            });
            return Ok(());
        }
        if !self.driver.is_leader() {
            let _ = respond.send(LeaveResponse::Redirect {
                leader: self.driver.node().leader_id(),
            });
            return Ok(());
        }
        let mut voters = self.driver.node().voters();
        if !voters.contains(&request.node_id) {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::NotMember,
            });
            return Ok(());
        }
        if voters.len() <= 1 {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::LastMember,
            });
            return Ok(());
        }
        voters.retain(|id| *id != request.node_id);

        match self.driver.propose_membership(voters, Vec::new())? {
            Ok((index, step)) => {
                self.pending_leaves.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(MembershipError::NotLeader { leader }) => {
                let _ = respond.send(LeaveResponse::Redirect { leader });
            }
            Err(MembershipError::InProgress) => {
                let _ = respond.send(LeaveResponse::Rejected {
                    reason: LeaveRejection::Other(
                        "a membership change is already in progress".to_string(),
                    ),
                });
            }
            Err(MembershipError::EmptyVoters) => {
                let _ = respond.send(LeaveResponse::Rejected {
                    reason: LeaveRejection::LastMember,
                });
            }
        }
        Ok(())
    }

    fn on_propose_membership(
        &mut self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) -> Result<(), DriverError> {
        use craft_core::MembershipError;

        match self.driver.propose_membership(voters, learners)? {
            Ok((_, step)) => {
                let _ = self.settle(step);
                let _ = respond.send(Ok(()));
            }
            Err(MembershipError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
            Err(MembershipError::InProgress) => {
                let _ = respond.send(Err(ClientError::Driver(
                    "a membership change is already in progress".to_string(),
                )));
            }
            Err(MembershipError::EmptyVoters) => {
                let _ = respond.send(Err(ClientError::Driver(
                    "resulting voter set is empty".to_string(),
                )));
            }
        }
        Ok(())
    }
}

/// Spawn a node runtime around `driver`, driving it over `transport`, and
/// return a [`NodeHandle`] for clients and the request handler.
///
/// The returned handle can be cloned freely; the node stops when
/// [`NodeHandle::shutdown`] is called or a fatal driver error occurs.
pub fn spawn<M>(
    driver: RaftDriver<M>,
    transport: Arc<dyn Transport>,
    config: RuntimeConfig,
) -> NodeHandle<M>
where
    M: StateMachine,
{
    let id = driver.node().id();
    let (tx, mut rx) = mpsc::unbounded_channel::<Envelope<M>>();
    let mut runtime = Runtime {
        driver,
        transport,
        self_tx: tx.clone(),
        allow_join: config.allow_join,
        allow_leave: config.allow_leave,
        pending_proposals: HashMap::new(),
        pending_queries: HashMap::new(),
        pending_read_confirms: HashMap::new(),
        pending_joins: HashMap::new(),
        pending_leaves: HashMap::new(),
        next_read_id: 0,
    };

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.tick_period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut shutdown_done = None;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match runtime.driver.tick() {
                        Ok(step) => { let _ = runtime.settle(step); }
                        Err(_) => break,
                    }
                }
                maybe = rx.recv() => {
                    let Some(env) = maybe else { break };
                    if let Envelope::Shutdown { done } = env {
                        shutdown_done = done;
                        break;
                    }
                    match runtime.on_envelope(env) {
                        Ok(true) => {}
                        Ok(false) | Err(_) => break,
                    }
                }
            }
        }
        drop(runtime);
        if let Some(done) = shutdown_done {
            let _ = done.send(());
        }
        // Pending responders drop here, so blocked clients observe `Stopped`.
    });

    NodeHandle { id, tx }
}

/// A [`craft_net`] [`RequestHandler`] that bridges inbound `/peer/wire` and
/// `/client/wire` requests into a running node via its [`NodeHandle`].
///
/// Attach it to a `QuicServer` (or `LocalNetwork`) so remote peers and clients
/// can reach the node. Client requests use **transparent forwarding** (client-routing):
/// a non-leader proxies the request to the current leader over the same
/// `transport` and returns the leader's response, so clients can connect to any
/// node without leader discovery. If no leader is known the request fails with
/// a [`ClientResponse::Error`]; forward attempts are bounded by
/// `forward_timeout` (elections converge quickly, so stale-hint hops are rare
/// and time-bounded rather than looping).
pub struct NodeService<M: StateMachine> {
    handle: NodeHandle<M>,
    transport: Arc<dyn Transport>,
    forward_timeout: Duration,
}

impl<M: StateMachine> NodeService<M> {
    /// Wrap a node handle as a request handler. `transport` is used to forward
    /// client requests to the leader when this node is a follower (client-routing);
    /// pass the same transport the node runtime uses.
    #[must_use]
    pub fn new(handle: NodeHandle<M>, transport: Arc<dyn Transport>) -> Self {
        Self {
            handle,
            transport,
            forward_timeout: Duration::from_secs(5),
        }
    }

    /// Override the per-forward deadline used when proxying to the leader.
    #[must_use]
    pub fn with_forward_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout = timeout;
        self
    }
}

impl<M: StateMachine> RequestHandler for NodeService<M> {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        let handle = self.handle.clone();
        let transport = Arc::clone(&self.transport);
        let forward_timeout = self.forward_timeout;
        Box::pin(async move {
            match route {
                Route::PeerWire => {
                    let rpc: RaftRpc = decode_body(&body)?;
                    let from = rpc_sender(&rpc);
                    let reply = handle
                        .deliver_rpc(from, rpc)
                        .await
                        .map_err(|e| TransportError::Io(e.to_string()))?;
                    Ok(encode_body(&reply)?)
                }
                Route::ClientWire => {
                    let request: ClientRequest = decode_body(&body)?;
                    let response =
                        route_client(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                Route::ClusterJoin => {
                    let request: JoinRequest = decode_body(&body)?;
                    let response = route_join(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                Route::ClusterLeave => {
                    let request: LeaveRequest = decode_body(&body)?;
                    let response = route_leave(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                other => Err(TransportError::Io(format!(
                    "route {other:?} is not served by the node runtime"
                ))),
            }
        })
    }
}

/// Serve a client request, using follower reads for queries (read-consistency) and
/// transparent forwarding for writes (client-routing).
async fn route_client<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: ClientRequest,
) -> ClientResponse {
    match request {
        ClientRequest::Query(bytes) => {
            route_query(handle, transport, forward_timeout, bytes, None).await
        }
        ClientRequest::QueryKeyed { key, query } => {
            route_query(handle, transport, forward_timeout, query, Some(key)).await
        }
        other => route_write_client(handle, transport, forward_timeout, other).await,
    }
}

/// Route a linearizable read: leader serves locally; followers confirm with
/// the leader then answer from local state (etcd-style follower read).
async fn route_query<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    bytes: Vec<u8>,
    route_key: Option<Vec<u8>>,
) -> ClientResponse {
    let query = match <M::Query as craft_core::Query>::from_bytes(&bytes) {
        Ok(q) => q,
        Err(e) => return ClientResponse::Error(format!("decode query: {e}")),
    };
    match handle.query(query).await {
        Ok(response) => encode_client_ok(&response),
        Err(ClientError::NotLeader {
            leader: Some(leader),
        }) if leader != handle.id() => {
            match handle
                .follower_query_bytes(bytes, route_key, leader, transport, forward_timeout)
                .await
            {
                Ok(response) => encode_client_ok(&response),
                Err(e) => ClientResponse::Error(e.to_string()),
            }
        }
        Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
        Err(e) => ClientResponse::Error(e.to_string()),
    }
}

/// Proposals (and keyed writes) still forward to the leader when needed.
async fn route_write_client<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: ClientRequest,
) -> ClientResponse {
    let local = serve_locally(handle, request.clone()).await;
    let ClientResponse::NotLeader { leader } = local else {
        return local;
    };
    match leader {
        Some(leader) if leader != handle.id() => {
            forward_to_leader(transport, forward_timeout, leader, request).await
        }
        _ => ClientResponse::Error("no leader elected".to_string()),
    }
}

/// Proxy a client request to `leader`, bounded by `timeout`.
async fn forward_to_leader(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: ClientRequest,
) -> ClientResponse {
    match tokio::time::timeout(timeout, send_client_request(&**transport, leader, &request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => ClientResponse::Error(format!("forward to leader {leader:?} failed: {e}")),
        Err(_) => ClientResponse::Error(format!("forward to leader {leader:?} timed out")),
    }
}

/// Serve a cluster join, forwarding to the leader if this node is a follower
/// (join-rpc step 2, same transparent pattern as client requests).
async fn route_join<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: JoinRequest,
) -> JoinResponse {
    let local = handle
        .join(request.clone())
        .await
        .unwrap_or_else(|_| JoinResponse::Rejected {
            reason: JoinRejection::Other("node runtime stopped".to_string()),
        });
    // A follower that knows the leader redirects; forward there on the caller's
    // behalf so a joining node only needs one seed address.
    if let JoinResponse::Redirect {
        leader: Some(leader),
    } = local
        && leader != handle.id()
    {
        return forward_join(transport, forward_timeout, leader, request).await;
    }
    local
}

/// Serve a cluster leave, forwarding to the leader if this node is a follower.
async fn route_leave<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: LeaveRequest,
) -> LeaveResponse {
    let local = handle
        .leave(request.clone())
        .await
        .unwrap_or_else(|_| LeaveResponse::Rejected {
            reason: LeaveRejection::Other("node runtime stopped".to_string()),
        });
    if let LeaveResponse::Redirect {
        leader: Some(leader),
    } = local
        && leader != handle.id()
    {
        return forward_leave(transport, forward_timeout, leader, request).await;
    }
    local
}

/// Proxy a join request to `leader`, bounded by `timeout`.
async fn forward_join(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: JoinRequest,
) -> JoinResponse {
    match tokio::time::timeout(timeout, send_join_request(&**transport, leader, &request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => JoinResponse::Redirect {
            leader: Some(leader),
        },
    }
}

/// Proxy a leave request to `leader`, bounded by `timeout`.
async fn forward_leave(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: LeaveRequest,
) -> LeaveResponse {
    match tokio::time::timeout(timeout, send_leave_request(&**transport, leader, &request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => LeaveResponse::Redirect {
            leader: Some(leader),
        },
    }
}

/// Answer a decoded [`ClientRequest`] against the local node, mapping runtime
/// results onto the wire [`ClientResponse`] (no forwarding).
async fn serve_locally<M: StateMachine>(
    handle: &NodeHandle<M>,
    request: ClientRequest,
) -> ClientResponse {
    match request {
        ClientRequest::Propose(bytes) | ClientRequest::ProposeKeyed { command: bytes, .. } => {
            let command = match M::Command::from_bytes(&bytes) {
                Ok(c) => c,
                Err(e) => return ClientResponse::Error(format!("decode command: {e}")),
            };
            match handle.propose(command).await {
                Ok(response) => encode_client_ok(&response),
                Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
                Err(e) => ClientResponse::Error(e.to_string()),
            }
        }
        ClientRequest::Query(bytes) | ClientRequest::QueryKeyed { query: bytes, .. } => {
            let query = match <M::Query as craft_core::Query>::from_bytes(&bytes) {
                Ok(q) => q,
                Err(e) => return ClientResponse::Error(format!("decode query: {e}")),
            };
            match handle.query(query).await {
                Ok(response) => encode_client_ok(&response),
                Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
                Err(e) => ClientResponse::Error(e.to_string()),
            }
        }
        ClientRequest::ReadIndexConfirm { .. } => match handle.confirm_read_index().await {
            Ok((index, term)) => ClientResponse::ReadIndexConfirmed { index, term },
            Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
            Err(e) => ClientResponse::Error(e.to_string()),
        },
    }
}

/// Encode a state-machine response as a successful client response body.
fn encode_client_ok<R: serde::Serialize>(response: &R) -> ClientResponse {
    match craft_proto::encode(response) {
        Ok(bytes) => ClientResponse::Ok(bytes),
        Err(e) => ClientResponse::Error(format!("encode response: {e}")),
    }
}

/// The sending node id carried inside a peer RPC payload. Until per-connection
/// certificate identity is wired (backlog C5), the runtime trusts the id the
/// RPC declares — safe on an mTLS-authenticated cluster where every peer is
/// CA-issued.
fn rpc_sender(rpc: &RaftRpc) -> NodeId {
    match rpc {
        RaftRpc::RequestVote(rv) => rv.candidate_id,
        RaftRpc::AppendEntries(ae) => ae.leader_id,
        RaftRpc::InstallSnapshot(is) => is.leader_id,
    }
}
