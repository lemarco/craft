//! Leader-gated queue wire service ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success — so a newly elected
//! leader serves the same backlog.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::task::JoinSet;

use crafty_net::transport::{Body, BoxFuture, Transport, TransportError};
use crafty_net::{
    Route, decode_body, encode_body, send_queue_ack, send_queue_enqueue, send_queue_job_status,
    send_queue_lease, send_queue_metrics, send_queue_nack, send_queue_replicate,
};
use crafty_proto::{
    NodeId, QueueAckReply, QueueAckRequest, QueueEnqueueReply, QueueEnqueueRequest,
    QueueJobLifecycleWire, QueueJobStatusReply, QueueJobStatusRequest, QueueLeaseReply,
    QueueLeaseRequest, QueueLeasedJobWire, QueueMetricsReply, QueueMetricsRequest, QueueNackReply,
    QueueNackRequest, QueueReplicateOp, QueueReplicateReply, QueueReplicateRequest,
};

use crate::supervisor::ClusterState;
use crate::{
    EnqueueOptions, JobId, JobLifecycle, JobQueue, JobStatus, LeaseId, LeasedJob,
    NOT_LEADER_REASON, QueueError, QueueMetrics, QueueReplicationOps, ShardedJobQueue,
    ShardedReplication, WorkerId,
};

fn job_status_to_reply(job_id: u64, status: Option<JobStatus>) -> QueueJobStatusReply {
    match status {
        None => QueueJobStatusReply {
            found: false,
            job_id,
            lifecycle: None,
            payload_len: 0,
            priority: 0,
            leased_worker_node: None,
            leased_worker_instance: None,
            error: None,
        },
        Some(s) => QueueJobStatusReply {
            found: true,
            job_id,
            lifecycle: Some(match s.lifecycle {
                JobLifecycle::Pending => QueueJobLifecycleWire::Pending,
                JobLifecycle::Leased => QueueJobLifecycleWire::Leased,
                JobLifecycle::Delayed => QueueJobLifecycleWire::Delayed,
            }),
            payload_len: s.payload_len,
            priority: s.priority,
            leased_worker_node: s.leased_by.map(|w| w.node.0),
            leased_worker_instance: s.leased_by.map(|w| w.instance),
            error: None,
        },
    }
}

fn enqueue_options_from_request(request: &QueueEnqueueRequest) -> EnqueueOptions {
    EnqueueOptions {
        priority: request.priority,
        not_before_ms: (request.not_before_ms != 0).then_some(request.not_before_ms),
        shard_key: request.shard_key.clone(),
        dedup_key: request.dedup_key.clone(),
    }
}

fn shard_stream_name(base: &str, shard: usize) -> String {
    format!("{base}~{shard}")
}

const REPLICATE_NOT_LEADER: &str = "queue replicate rejected: caller is not raft leader";
const REPLICATE_UNAUTHENTICATED: &str = "queue replicate rejected: unknown caller";

/// Serves `/raft/v1/queue/*` on the leader; followers transparently forward.
pub struct QueueService {
    node_id: NodeId,
    streams: Mutex<HashMap<String, Arc<dyn JobQueue>>>,
    sharded: Mutex<HashMap<String, Arc<ShardedJobQueue>>>,
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
            sharded: Mutex::new(HashMap::new()),
            state,
            transport,
        }
    }

    /// Register a federated sharded stream (logical name → local [`ShardedJobQueue`]).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_sharded_stream(&self, name: impl Into<String>, queue: Arc<ShardedJobQueue>) {
        self.sharded
            .lock()
            .expect("poisoned")
            .insert(name.into(), queue);
    }

    /// Register a local backing queue for `stream` (opened on every node; kept
    /// in sync via leader replication).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
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

    /// Push `ops` to every other **reachable** voter in parallel; all must ack
    /// before the leader commits success to clients (failover-safe backlog).
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
            ops: ops.clone(),
        };
        let mut set = JoinSet::new();
        for peer in peers {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            set.spawn(async move {
                let reply = send_queue_replicate(transport.as_ref(), peer, &request)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(err) = reply.error {
                    return Err(err);
                }
                Ok(())
            });
        }
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    async fn replicate_sharded(
        &self,
        base: &str,
        reps: &[ShardedReplication],
    ) -> Result<(), String> {
        for rep in reps {
            if rep.ops.is_empty() {
                continue;
            }
            self.replicate_ops(&shard_stream_name(base, rep.shard), &rep.ops)
                .await?;
        }
        Ok(())
    }

    fn authorize_replicate(&self, from: Option<NodeId>) -> Result<(), String> {
        let Some(from) = from else {
            return Err(REPLICATE_UNAUTHENTICATED.to_string());
        };
        let Some(leader) = self.state.leader_id() else {
            return Err("no raft leader elected".to_string());
        };
        if from != leader {
            return Err(REPLICATE_NOT_LEADER.to_string());
        }
        Ok(())
    }

    async fn handle_replicate(
        &self,
        from: Option<NodeId>,
        request: QueueReplicateRequest,
    ) -> QueueReplicateReply {
        if let Err(e) = self.authorize_replicate(from) {
            return QueueReplicateReply { error: Some(e) };
        }
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

    fn sharded_stream(&self, stream: &str) -> Option<Arc<ShardedJobQueue>> {
        self.sharded.lock().expect("poisoned").get(stream).cloned()
    }

    async fn handle_enqueue(&self, request: QueueEnqueueRequest) -> QueueEnqueueReply {
        if self.state.is_leader() {
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                let options = enqueue_options_from_request(&request);
                match sharded
                    .enqueue_opts_replicated_sharded(&request.payload, options)
                    .await
                {
                    Ok((id, rep)) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &[rep]).await {
                            return QueueEnqueueReply {
                                job_id: None,
                                error: Some(e),
                            };
                        }
                        return QueueEnqueueReply {
                            job_id: Some(id.0),
                            error: None,
                        };
                    }
                    Err(e) => {
                        return QueueEnqueueReply {
                            job_id: None,
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
            match self.local_stream(&request.stream) {
                Err(e) => QueueEnqueueReply {
                    job_id: None,
                    error: Some(e),
                },
                Ok(queue) => {
                    let options = enqueue_options_from_request(&request);
                    match queue
                        .enqueue_opts_replicated(&request.payload, options)
                        .await
                    {
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
                    }
                }
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
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded.lease_replicated_sharded(worker, request.max).await {
                    Ok((jobs, reps)) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &reps).await {
                            return QueueLeaseReply {
                                jobs: Vec::new(),
                                error: Some(e),
                            };
                        }
                        return QueueLeaseReply {
                            jobs: jobs
                                .into_iter()
                                .map(|j| QueueLeasedJobWire {
                                    lease_id: j.lease_id.0,
                                    job_id: j.job_id.0,
                                    payload: j.payload,
                                })
                                .collect(),
                            error: None,
                        };
                    }
                    Err(e) => {
                        return QueueLeaseReply {
                            jobs: Vec::new(),
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
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
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded
                    .ack_replicated_sharded(worker, LeaseId(request.lease_id))
                    .await
                {
                    Ok(rep) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &[rep]).await {
                            return QueueAckReply { error: Some(e) };
                        }
                        return QueueAckReply { error: None };
                    }
                    Err(e) => {
                        return QueueAckReply {
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
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
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded
                    .nack_replicated_sharded(worker, LeaseId(request.lease_id))
                    .await
                {
                    Ok(rep) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &[rep]).await {
                            return QueueNackReply { error: Some(e) };
                        }
                        return QueueNackReply { error: None };
                    }
                    Err(e) => {
                        return QueueNackReply {
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
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
            let metrics = if let Some(sharded) = self.sharded_stream(&request.stream) {
                sharded.metrics().await
            } else {
                match self.local_stream(&request.stream) {
                    Err(e) => {
                        return QueueMetricsReply {
                            pending: 0,
                            leased: 0,
                            oldest_pending_age_ms: 0,
                            error: Some(e),
                        };
                    }
                    Ok(queue) => queue.metrics().await,
                }
            };
            match metrics {
                Ok(m) => QueueMetricsReply {
                    pending: m.pending,
                    leased: m.leased,
                    oldest_pending_age_ms: u64::try_from(m.oldest_pending_age.as_millis())
                        .unwrap_or(u64::MAX),
                    error: None,
                },
                Err(e) => QueueMetricsReply {
                    pending: 0,
                    leased: 0,
                    oldest_pending_age_ms: 0,
                    error: Some(e.to_string()),
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

    async fn handle_job_status(&self, request: QueueJobStatusRequest) -> QueueJobStatusReply {
        if self.state.is_leader() {
            let status = if let Some(sharded) = self.sharded_stream(&request.stream) {
                sharded.job_status(JobId(request.job_id)).await
            } else {
                match self.local_stream(&request.stream) {
                    Err(e) => {
                        return QueueJobStatusReply {
                            found: false,
                            job_id: request.job_id,
                            lifecycle: None,
                            payload_len: 0,
                            priority: 0,
                            leased_worker_node: None,
                            leased_worker_instance: None,
                            error: Some(e),
                        };
                    }
                    Ok(queue) => queue.job_status(JobId(request.job_id)).await,
                }
            };
            match status {
                Ok(s) => job_status_to_reply(request.job_id, s),
                Err(e) => QueueJobStatusReply {
                    found: false,
                    job_id: request.job_id,
                    lifecycle: None,
                    payload_len: 0,
                    priority: 0,
                    leased_worker_node: None,
                    leased_worker_instance: None,
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let job_id = request.job_id;
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_job_status(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueJobStatusReply {
                    found: false,
                    job_id,
                    lifecycle: None,
                    payload_len: 0,
                    priority: 0,
                    leased_worker_node: None,
                    leased_worker_instance: None,
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
        self.handle_request_from(None, route, body)
    }

    /// Like [`handle_request`](Self::handle_request) with authenticated caller identity.
    pub fn handle_request_from(
        self: &Arc<Self>,
        from: Option<NodeId>,
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
            Route::QueueJobStatus => Box::pin(async move {
                let request: QueueJobStatusRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_job_status(request).await)?)
            }),
            Route::QueueReplicate => Box::pin(async move {
                let request: QueueReplicateRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_replicate(from, request).await)?)
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

    fn leader(&self) -> Result<NodeId, QueueError> {
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

    fn lease_replicated(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let jobs = self.lease(worker, max).await?;
            Ok((jobs, Vec::new()))
        })
    }

    fn ack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            self.ack(worker, lease_id).await?;
            Ok(Vec::new())
        })
    }

    fn nack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            self.nack(worker, lease_id).await?;
            Ok(Vec::new())
        })
    }

    fn enqueue_opts<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_enqueue(
                self.transport.as_ref(),
                leader,
                &QueueEnqueueRequest {
                    stream: self.stream.clone(),
                    payload: payload.to_vec(),
                    priority: options.priority,
                    not_before_ms: options.not_before_ms.unwrap_or(0),
                    shard_key: options.shard_key.clone(),
                    dedup_key: options.dedup_key.clone(),
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

    fn enqueue<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move { self.enqueue_opts(payload, EnqueueOptions::default()).await })
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

    fn enqueue_opts_replicated<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let id = self.enqueue_opts(payload, options).await?;
            Ok((id, Vec::new()))
        })
    }

    fn lease(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<LeasedJob>, QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
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

    fn ack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
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

    fn nack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
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

    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
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

    fn job_status(&self, job_id: JobId) -> BoxFuture<'_, Result<Option<JobStatus>, QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_job_status(
                self.transport.as_ref(),
                leader,
                &QueueJobStatusRequest {
                    stream: self.stream.clone(),
                    job_id: job_id.0,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            if !reply.found {
                return Ok(None);
            }
            let lifecycle = reply
                .lifecycle
                .ok_or_else(|| QueueError::Backend("job status reply missing lifecycle".into()))?;
            Ok(Some(JobStatus {
                job_id,
                lifecycle: match lifecycle {
                    QueueJobLifecycleWire::Pending => JobLifecycle::Pending,
                    QueueJobLifecycleWire::Leased => JobLifecycle::Leased,
                    QueueJobLifecycleWire::Delayed => JobLifecycle::Delayed,
                },
                payload_len: reply.payload_len,
                priority: reply.priority,
                leased_by: match (reply.leased_worker_node, reply.leased_worker_instance) {
                    (Some(node), Some(instance)) => Some(WorkerId {
                        node: NodeId(node),
                        instance,
                    }),
                    _ => None,
                },
            }))
        })
    }
}
