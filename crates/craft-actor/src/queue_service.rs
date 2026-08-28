//! Leader-gated queue wire service ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success — so a newly elected
//! leader serves the same backlog.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use craft_net::transport::{Body, BoxFuture, Transport, TransportError};
use craft_net::{
    Route, decode_body, encode_body, send_queue_ack, send_queue_enqueue, send_queue_lease,
    send_queue_metrics, send_queue_nack, send_queue_replicate,
};
use craft_proto::{
    NodeId, QueueAckReply, QueueAckRequest, QueueEnqueueReply, QueueEnqueueRequest,
    QueueLeaseReply, QueueLeaseRequest, QueueLeasedJobWire, QueueMetricsReply, QueueMetricsRequest,
    QueueNackReply, QueueNackRequest, QueueReplicateOp, QueueReplicateReply, QueueReplicateRequest,
};

use crate::supervisor::ClusterState;
use crate::{
    JobId, JobQueue, LeaseId, LeasedJob, NOT_LEADER_REASON, QueueError, QueueMetrics,
    QueueReplicationOps, WorkerId,
};

/// Serves `/raft/v1/queue/*` on the leader; followers transparently forward.
pub struct QueueService {
    node_id: NodeId,
    streams: Mutex<HashMap<String, Arc<dyn JobQueue>>>,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
}

impl QueueService {
    /// Empty service; register streams before accepting traffic.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            streams: Mutex::new(HashMap::new()),
            state,
            transport,
        }
    }

    /// Register a local backing queue for `stream` (opened on every node; kept
    /// in sync via leader replication).
    pub fn register_stream(&self, stream: impl Into<String>, queue: Arc<dyn JobQueue>) {
        self.streams
            .lock()
            .expect("poisoned")
            .insert(stream.into(), queue);
    }

    fn local_stream(&self, stream: &str) -> Result<Arc<dyn JobQueue>, String> {
        self.streams
            .lock()
            .expect("poisoned")
            .get(stream)
            .cloned()
            .ok_or_else(|| format!("unknown queue stream {stream:?}"))
    }

    async fn forward_leader<R>(
        &self,
        send: impl FnOnce(NodeId) -> BoxFuture<'static, Result<R, TransportError>>,
    ) -> Result<R, String> {
        let leader = self
            .state
            .leader_id()
            .ok_or_else(|| "no raft leader elected".to_string())?;
        send(leader)
            .await
            .map_err(|e| format!("forward to leader {leader:?} failed: {e}"))
    }

    /// Push `ops` to every other **reachable** voter; all must ack before the
    /// leader commits success to clients (failover-safe backlog).
    async fn replicate_ops(&self, stream: &str, ops: &QueueReplicationOps) -> Result<(), String> {
        if ops.is_empty() {
            return Ok(());
        }
        let peers: Vec<NodeId> = self
            .state
            .reachable_nodes()
            .into_iter()
            .filter(|id| *id != self.node_id)
            .collect();
        if peers.is_empty() {
            return Ok(());
        }
        let request = QueueReplicateRequest {
            stream: stream.to_string(),
            ops: ops.to_vec(),
        };
        for peer in peers {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            let reply = send_queue_replicate(transport.as_ref(), peer, &request)
                .await
                .map_err(|e| e.to_string())?;
            if let Some(err) = reply.error {
                return Err(err);
            }
        }
        Ok(())
    }

    async fn handle_replicate(&self, request: QueueReplicateRequest) -> QueueReplicateReply {
        match self.local_stream(&request.stream) {
            Err(e) => QueueReplicateReply { error: Some(e) },
            Ok(queue) => {
                for op in &request.ops {
                    if let Err(e) = queue.apply_replicate(op).await {
                        return QueueReplicateReply {
                            error: Some(e.to_string()),
                        };
                    }
                }
                QueueReplicateReply { error: None }
            }
        }
    }

    async fn handle_enqueue(&self, request: QueueEnqueueRequest) -> QueueEnqueueReply {
        if self.state.is_leader() {
            match self.local_stream(&request.stream) {
                Err(e) => QueueEnqueueReply {
                    job_id: None,
                    error: Some(e),
                },
                Ok(queue) => match queue.enqueue_replicated(&request.payload).await {
                    Ok((id, ops)) => {
                        if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                            return QueueEnqueueReply {
                                job_id: None,
                                error: Some(e),
                            };
                        }
                        QueueEnqueueReply {
                            job_id: Some(id.0),
                            error: None,
                        }
                    }
                    Err(e) => QueueEnqueueReply {
                        job_id: None,
                        error: Some(e.to_string()),
                    },
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_enqueue(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueEnqueueReply {
                    job_id: None,
                    error: Some(e),
                },
            }
        }
    }

    async fn handle_lease(&self, request: QueueLeaseRequest) -> QueueLeaseReply {
        if self.state.is_leader() {
            let worker = WorkerId {
                node: NodeId(request.worker_node),
                instance: request.worker_instance,
            };
            match self.local_stream(&request.stream) {
                Err(e) => QueueLeaseReply {
                    jobs: Vec::new(),
                    error: Some(e),
                },
                Ok(queue) => match queue.lease_replicated(worker, request.max).await {
                    Ok((jobs, ops)) => {
                        if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                            return QueueLeaseReply {
                                jobs: Vec::new(),
                                error: Some(e),
                            };
                        }
                        QueueLeaseReply {
                            jobs: jobs
                                .into_iter()
                                .map(|j| QueueLeasedJobWire {
                                    lease_id: j.lease_id.0,
                                    job_id: j.job_id.0,
                                    payload: j.payload,
                                })
                                .collect(),
                            error: None,
                        }
                    }
                    Err(e) => QueueLeaseReply {
                        jobs: Vec::new(),
                        error: Some(e.to_string()),
                    },
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(
                        async move { send_queue_lease(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueLeaseReply {
                    jobs: Vec::new(),
                    error: Some(e),
                },
            }
        }
    }

    async fn handle_ack(&self, request: QueueAckRequest) -> QueueAckReply {
        if self.state.is_leader() {
            let worker = WorkerId {
                node: NodeId(request.worker_node),
                instance: request.worker_instance,
            };
            match self.local_stream(&request.stream) {
                Err(e) => QueueAckReply { error: Some(e) },
                Ok(queue) => match queue
                    .ack_replicated(worker, LeaseId(request.lease_id))
                    .await
                {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                            return QueueAckReply { error: Some(e) };
                        }
                        QueueAckReply { error: None }
                    }
                    Err(e) => QueueAckReply {
                        error: Some(e.to_string()),
                    },
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(
                        async move { send_queue_ack(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueAckReply { error: Some(e) },
            }
        }
    }

    async fn handle_nack(&self, request: QueueNackRequest) -> QueueNackReply {
        if self.state.is_leader() {
            let worker = WorkerId {
                node: NodeId(request.worker_node),
                instance: request.worker_instance,
            };
            match self.local_stream(&request.stream) {
                Err(e) => QueueNackReply { error: Some(e) },
                Ok(queue) => {
                    match queue
                        .nack_replicated(worker, LeaseId(request.lease_id))
                        .await
                    {
                        Ok(ops) => {
                            if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                                return QueueNackReply { error: Some(e) };
                            }
                            QueueNackReply { error: None }
                        }
                        Err(e) => QueueNackReply {
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(
                        async move { send_queue_nack(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueNackReply { error: Some(e) },
            }
        }
    }

    async fn handle_metrics(&self, request: QueueMetricsRequest) -> QueueMetricsReply {
        if self.state.is_leader() {
            match self.local_stream(&request.stream) {
                Err(e) => QueueMetricsReply {
                    pending: 0,
                    leased: 0,
                    oldest_pending_age_ms: 0,
                    error: Some(e),
                },
                Ok(queue) => match queue.metrics().await {
                    Ok(m) => QueueMetricsReply {
                        pending: m.pending,
                        leased: m.leased,
                        oldest_pending_age_ms: m.oldest_pending_age.as_millis() as u64,
                        error: None,
                    },
                    Err(e) => QueueMetricsReply {
                        pending: 0,
                        leased: 0,
                        oldest_pending_age_ms: 0,
                        error: Some(e.to_string()),
                    },
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_metrics(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueMetricsReply {
                    pending: 0,
                    leased: 0,
                    oldest_pending_age_ms: 0,
                    error: Some(e),
                },
            }
        }
    }
}

impl QueueService {
    /// Wire entry point when the service is held in an [`Arc`].
    pub fn handle_request(
        self: &Arc<Self>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        let service = Arc::clone(self);
        match route {
            Route::QueueEnqueue => Box::pin(async move {
                let request: QueueEnqueueRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_enqueue(request).await)?)
            }),
            Route::QueueLease => Box::pin(async move {
                let request: QueueLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_lease(request).await)?)
            }),
            Route::QueueAck => Box::pin(async move {
                let request: QueueAckRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack(request).await)?)
            }),
            Route::QueueNack => Box::pin(async move {
                let request: QueueNackRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_nack(request).await)?)
            }),
            Route::QueueMetrics => Box::pin(async move {
                let request: QueueMetricsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_metrics(request).await)?)
            }),
            Route::QueueReplicate => Box::pin(async move {
                let request: QueueReplicateRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_replicate(request).await)?)
            }),
            other => Box::pin(async move {
                Err(TransportError::Io(format!(
                    "queue handler received unexpected route {other:?}"
                )))
            }),
        }
    }
}

fn replication_unsupported() -> QueueError {
    QueueError::Backend("cluster queue client does not apply replication locally".into())
}

/// Cluster-facing [`JobQueue`] that routes through the leader wire service.
pub struct ClusterJobQueue {
    stream: String,
    node_id: NodeId,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
}

impl ClusterJobQueue {
    /// A queue client for `stream` (leases/acks attribute the worker you pass).
    #[must_use]
    pub fn new(
        stream: impl Into<String>,
        node_id: NodeId,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            stream: stream.into(),
            node_id,
            state,
            transport,
        }
    }

    async fn leader(&self) -> Result<NodeId, QueueError> {
        if self.state.is_leader() {
            return Ok(self.node_id);
        }
        self.state
            .leader_id()
            .ok_or_else(|| QueueError::Backend("no raft leader".into()))
    }
}

impl JobQueue for ClusterJobQueue {
    fn apply_replicate<'a>(
        &'a self,
        _op: &'a QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async { Err(replication_unsupported()) })
    }

    fn enqueue_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let id = self.enqueue(payload).await?;
            Ok((id, Vec::new()))
        })
    }

    fn lease_replicated<'a>(
        &'a self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let jobs = self.lease(worker, max).await?;
            Ok((jobs, Vec::new()))
        })
    }

    fn ack_replicated<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            self.ack(worker, lease_id).await?;
            Ok(Vec::new())
        })
    }

    fn nack_replicated<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            self.nack(worker, lease_id).await?;
            Ok(Vec::new())
        })
    }

    fn enqueue<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move {
            let leader = self.leader().await?;
            let reply = send_queue_enqueue(
                self.transport.as_ref(),
                leader,
                &QueueEnqueueRequest {
                    stream: self.stream.clone(),
                    payload: payload.to_vec(),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                if err == NOT_LEADER_REASON {
                    return Err(QueueError::Backend(err));
                }
                return Err(QueueError::Backend(err));
            }
            reply
                .job_id
                .map(JobId)
                .ok_or_else(|| QueueError::Backend("missing job_id".into()))
        })
    }

    fn lease<'a>(
        &'a self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<Vec<LeasedJob>, QueueError>> {
        Box::pin(async move {
            let leader = self.leader().await?;
            let reply = send_queue_lease(
                self.transport.as_ref(),
                leader,
                &QueueLeaseRequest {
                    stream: self.stream.clone(),
                    worker_node: worker.node.0,
                    worker_instance: worker.instance,
                    max,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok(reply
                .jobs
                .into_iter()
                .map(|j| LeasedJob {
                    lease_id: LeaseId(j.lease_id),
                    job_id: JobId(j.job_id),
                    payload: j.payload,
                })
                .collect())
        })
    }

    fn ack<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader().await?;
            let reply = send_queue_ack(
                self.transport.as_ref(),
                leader,
                &QueueAckRequest {
                    stream: self.stream.clone(),
                    worker_node: worker.node.0,
                    worker_instance: worker.instance,
                    lease_id: lease_id.0,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok(())
        })
    }

    fn nack<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader().await?;
            let reply = send_queue_nack(
                self.transport.as_ref(),
                leader,
                &QueueNackRequest {
                    stream: self.stream.clone(),
                    worker_node: worker.node.0,
                    worker_instance: worker.instance,
                    lease_id: lease_id.0,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok(())
        })
    }

    fn metrics<'a>(&'a self) -> BoxFuture<'a, Result<QueueMetrics, QueueError>> {
        Box::pin(async move {
            let leader = self.leader().await?;
            let reply = send_queue_metrics(
                self.transport.as_ref(),
                leader,
                &QueueMetricsRequest {
                    stream: self.stream.clone(),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok(QueueMetrics {
                pending: reply.pending,
                leased: reply.leased,
                oldest_pending_age: std::time::Duration::from_millis(reply.oldest_pending_age_ms),
            })
        })
    }
}
