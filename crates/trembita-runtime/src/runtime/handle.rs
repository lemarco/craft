use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use trembita_core::{Command as _, Query as _, StateMachine};
use trembita_net::{Transport, send_client_request};
use trembita_proto::{
    CatalogAddRequest, CatalogAddResponse, ClientRequest, ClientResponse, JoinRequest,
    JoinResponse, LeaveRequest, LeaveResponse, LogIndex, NodeId, QueueAutoscalePolicyCommand,
    RaftRpc, RaftRpcReply, SagaJournalCommand, Term, TwoPhaseJournalCommand,
};

use super::types::{ClientError, Envelope, NodeStatus};

/// A cloneable handle to a running node (see [`spawn`](super::spawn)). Dropping every handle
/// does not stop the node; call [`shutdown`](NodeHandle::shutdown) for that.
pub struct NodeHandle<M: StateMachine> {
    pub(super) id: NodeId,
    pub(super) tx: mpsc::UnboundedSender<Envelope<M>>,
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

    /// Run a linearizable query (`ReadIndex`, read-consistency) and await its result.
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
    ///
    /// # Errors
    /// Returns [`ClientError::Stopped`] if the node is shutting down or the
    /// runtime task dropped the response channel.
    pub async fn confirm_read_index(&self) -> Result<(LogIndex, Term), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::ConfirmReadIndex { respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Run a query against local applied state (after a confirmed read index
    /// and apply barrier on a follower).
    ///
    /// # Errors
    /// Returns [`ClientError::Stopped`] if the node is shutting down, or a
    /// driver/query error from the runtime task.
    pub async fn local_query(&self, query: M::Query) -> Result<M::Response, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::LocalQuery { query, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Export durable Raft state for cross-node group migration (write-sharding-multi-raft).
    ///
    /// # Errors
    /// Returns [`ClientError::Stopped`] if the node is shutting down, or a
    /// driver/storage error from the runtime task.
    pub async fn export_migration(
        &self,
    ) -> Result<trembita_proto::GroupMigrationBundle, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::ExportMigration { respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.unwrap_or(Err(ClientError::Stopped))
    }

    /// Etcd-style follower read: confirm with the leader, wait for the apply
    /// barrier, then serve from local state.
    ///
    /// # Errors
    /// Returns [`ClientError`] on decode failure, transport timeout, lost
    /// leadership, or if the node stops before the query completes.
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

    /// Submit a [`CatalogAddRequest`] to grow the multi-Raft catalog (group 0).
    ///
    /// # Errors
    /// [`ClientError::Stopped`] if the runtime shut down before responding.
    pub async fn catalog_add(
        &self,
        request: CatalogAddRequest,
    ) -> Result<CatalogAddResponse, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::CatalogAdd { request, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)
    }

    /// Replicate a saga journal upsert on group 0 (Meta-Raft saga journal).
    ///
    /// # Errors
    /// [`ClientError::NotLeader`] when this node is not the group 0 leader.
    /// [`ClientError::Stopped`] if the runtime shut down before responding.
    pub async fn upsert_saga_journal(
        &self,
        command: SagaJournalCommand,
    ) -> Result<(), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::UpsertSagaJournal { command, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
    }

    /// Replicate a 2PC client journal upsert on Meta-Raft / group 0.
    ///
    /// # Errors
    /// [`ClientError::NotLeader`] when this node is not the metadata leader.
    /// [`ClientError::Stopped`] if the runtime shut down before responding.
    pub async fn upsert_two_phase_journal(
        &self,
        command: TwoPhaseJournalCommand,
    ) -> Result<(), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::UpsertTwoPhaseJournal { command, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
    }

    /// Replicate a queue autoscale policy upsert on Meta-Raft / group 0.
    ///
    /// # Errors
    /// [`ClientError::NotLeader`] when this node is not the metadata leader.
    /// [`ClientError::Stopped`] if the runtime shut down before responding.
    pub async fn upsert_queue_autoscale_policy(
        &self,
        command: QueueAutoscalePolicyCommand,
    ) -> Result<(), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::UpsertQueueAutoscalePolicy { command, respond })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
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

    /// Stage a command for cross-shard 2PC on this group's leader.
    ///
    /// # Errors
    /// Returns [`ClientError::Stopped`] if the node is shutting down, or a
    /// driver error from the runtime task.
    pub async fn two_phase_prepare(
        &self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        command: Vec<u8>,
    ) -> Result<(), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::TwoPhasePrepare {
                tx_id,
                route_key,
                command,
                respond,
            })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
    }

    /// Commit a previously prepared command through the normal Raft log.
    ///
    /// # Errors
    /// Returns [`ClientError::Stopped`] if the node is shutting down, or a
    /// driver/query error from the runtime task.
    pub async fn two_phase_commit(
        &self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
    ) -> Result<M::Response, ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::TwoPhaseCommit {
                tx_id,
                route_key,
                respond,
            })
            .map_err(|_| ClientError::Stopped)?;
        rx.await.map_err(|_| ClientError::Stopped)?
    }

    /// Drop a previously prepared command without committing.
    ///
    /// # Errors
    /// Returns [`ClientError::Stopped`] if the node is shutting down, or a
    /// driver error from the runtime task.
    pub async fn two_phase_abort(
        &self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
    ) -> Result<(), ClientError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(Envelope::TwoPhaseAbort {
                tx_id,
                route_key,
                respond,
            })
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
