//! Leader-gated queue wire service ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success — so a newly elected
//! leader serves the same backlog.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use trembita_net::transport::{Body, BoxFuture, Transport, TransportError};
use trembita_net::{
    Route, decode_body, encode_body, send_queue_ack, send_queue_ack_batch, send_queue_enqueue,
    send_queue_enqueue_batch, send_queue_extend_lease, send_queue_job_status, send_queue_lease,
    send_queue_list_jobs, send_queue_metrics, send_queue_nack, send_queue_replicate,
    send_queue_requeue_dead_letter_batch,
};
use trembita_proto::{
    NodeId, QueueAckBatchReply, QueueAckBatchRequest, QueueAckReply, QueueAckRequest,
    QueueEnqueueBatchReply, QueueEnqueueBatchRequest, QueueEnqueueReply, QueueEnqueueRequest,
    QueueExtendLeaseReply, QueueExtendLeaseRequest, QueueJobStatusReply, QueueJobStatusRequest,
    QueueLeaseReply, QueueLeaseRequest, QueueLeasedJobWire, QueueListJobsReply,
    QueueListJobsRequest, QueueMetricsReply, QueueMetricsRequest, QueueNackReply, QueueNackRequest,
    QueueReplicateOp, QueueReplicateReply, QueueReplicateRequest, QueueRequeueDeadLetterBatchReply,
    QueueRequeueDeadLetterBatchRequest, QueueRequeueFailureWire,
};

use crate::backlog_settle_outbox::{BacklogSettleOutbox, push_backlog_settle};
use crate::external_backlog::{
    BacklogSettleEvent, BacklogSettleOutcome, emit_backlog_settle_for_terminal_ops,
};
use crate::queue_lifecycle::QueueLifecycleEvent;
use crate::queue_prefetch::{CachedPendingJob, DEFAULT_QUEUE_BATCH_MAX, QueuePrefetchCache};
use crate::sharded_queue::{decode_global_id, encode_global_id};
use crate::{
    EnqueueOptions, JobId, JobQueue, LeaseId, LeasedJob, QueueError, QueueReplicationOps,
    RecurringJob, RedbJobQueue, ScheduleSource, ShardedJobQueue, ShardedReplication, WorkerId,
};
use trembita_runtime::{
    ClusterState, authorize_replicate_leader, fanout_replicate, replication_peers,
};
use trembita_storage::now_ms;

use crate::queue_service_wire::{
    enqueue_options_from_batch_job, enqueue_options_from_request, filter_from_list_request,
    job_status_to_list_entry, job_status_to_reply, shard_stream_name,
};

use std::time::{Duration, Instant};

const REPLICATE_NOT_LEADER: &str = "queue replicate rejected: caller is not raft leader";

struct ScheduleSourceEntry {
    source: Arc<dyn ScheduleSource>,
    poll: Duration,
    last_good: Option<Vec<RecurringJob>>,
    next_poll_at: Option<Instant>,
}

/// Serves `/raft/v1/queue/*` on the leader; followers transparently forward.
pub struct QueueService {
    node_id: NodeId,
    streams: Mutex<HashMap<String, Arc<dyn JobQueue>>>,
    redb_streams: Mutex<HashMap<String, Arc<RedbJobQueue>>>,
    prefetch: Mutex<HashMap<String, QueuePrefetchCache>>,
    sharded: Mutex<HashMap<String, Arc<ShardedJobQueue>>>,
    schedule_sources: Mutex<HashMap<String, ScheduleSourceEntry>>,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
    lifecycle_hook: Option<Arc<dyn Fn(QueueLifecycleEvent) + Send + Sync>>,
    backlog_settle_outbox: Option<Arc<dyn BacklogSettleOutbox>>,
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
            redb_streams: Mutex::new(HashMap::new()),
            prefetch: Mutex::new(HashMap::new()),
            sharded: Mutex::new(HashMap::new()),
            schedule_sources: Mutex::new(HashMap::new()),
            state,
            transport,
            lifecycle_hook: None,
            backlog_settle_outbox: None,
        }
    }

    /// Persist terminal jobs with dedup keys to the settle outbox ([`crate::run_backlog_settle_drainer`]).
    #[must_use]
    pub fn with_backlog_settle_outbox(mut self, outbox: Arc<dyn BacklogSettleOutbox>) -> Self {
        self.backlog_settle_outbox = Some(outbox);
        self
    }

    /// Emit [`QueueLifecycleEvent`]s to the dashboard / user sinks (observability).
    #[must_use]
    pub fn with_lifecycle_hook(
        mut self,
        hook: Arc<dyn Fn(QueueLifecycleEvent) + Send + Sync>,
    ) -> Self {
        self.lifecycle_hook = Some(hook);
        self
    }

    fn emit_lifecycle(&self, event: QueueLifecycleEvent) {
        if let Some(hook) = &self.lifecycle_hook {
            hook(event);
        }
    }

    fn emit_enqueued(&self, stream: &str, job_id: u64) {
        self.emit_lifecycle(QueueLifecycleEvent::Enqueued {
            stream: stream.to_owned(),
            job_id,
        });
    }

    fn emit_leased(
        &self,
        stream: &str,
        job_id: u64,
        lease_id: u64,
        worker_node: u64,
        worker_instance: u32,
        attempts: u32,
    ) {
        self.emit_lifecycle(QueueLifecycleEvent::Leased {
            stream: stream.to_owned(),
            job_id,
            lease_id,
            worker_node,
            worker_instance,
            attempts,
        });
    }

    fn emit_acked(&self, stream: &str, lease_id: u64, worker_node: u64) {
        self.emit_lifecycle(QueueLifecycleEvent::Acked {
            stream: stream.to_owned(),
            lease_id,
            worker_node,
        });
    }

    fn emit_backlog_settle(
        &self,
        stream: &str,
        dedup_key: Option<Vec<u8>>,
        outcome: BacklogSettleOutcome,
    ) {
        push_backlog_settle(
            self.backlog_settle_outbox.as_deref(),
            BacklogSettleEvent {
                stream: stream.to_owned(),
                dedup_key,
                outcome,
            },
        );
    }

    async fn emit_backlog_settle_for_terminal_ops(
        &self,
        stream: &str,
        queue: &dyn JobQueue,
        ops: &[QueueReplicateOp],
        error: &str,
    ) {
        emit_backlog_settle_for_terminal_ops(
            stream,
            queue,
            self.backlog_settle_outbox.as_deref(),
            ops,
            error,
        )
        .await;
    }

    async fn emit_backlog_settle_for_sharded_reps(
        &self,
        base: &str,
        reps: &[ShardedReplication],
        error: &str,
    ) {
        for rep in reps {
            let stream = shard_stream_name(base, rep.shard);
            if let Ok(queue) = self.local_stream(&stream) {
                self.emit_backlog_settle_for_terminal_ops(&stream, queue.as_ref(), &rep.ops, error)
                    .await;
            }
        }
    }

    async fn peek_lease_meta(&self, stream: &str, lease_id: LeaseId) -> (Option<Vec<u8>>, u32) {
        match self.local_stream(stream) {
            Ok(queue) => queue.peek_lease_meta(lease_id).await.unwrap_or((None, 0)),
            Err(_) => (None, 0),
        }
    }

    /// Register a local redb-backed stream and optional prefetch depth.
    ///
    /// Recurring schedules are loaded via [`Self::register_schedule_source`].
    ///
    /// `prefetch` controls the leader in-memory cache for recently enqueued jobs
    /// (`0` disables prefetch).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_redb_stream(
        &self,
        name: impl Into<String>,
        queue: &Arc<RedbJobQueue>,
        prefetch: usize,
    ) {
        let name = name.into();
        self.streams
            .lock()
            .expect("poisoned")
            .insert(name.clone(), Arc::clone(queue) as Arc<dyn JobQueue>);
        self.redb_streams
            .lock()
            .expect("poisoned")
            .insert(name.clone(), Arc::clone(queue));
        if prefetch > 0 {
            self.prefetch
                .lock()
                .expect("poisoned")
                .insert(name, QueuePrefetchCache::new(prefetch));
        }
    }

    /// Poll a dynamic [`ScheduleSource`] on the leader and reconcile redb + voters.
    ///
    /// Source errors and bootstrap `Ok([])` never clear persisted schedules.
    ///
    /// # Errors
    /// Propagates queue or replication failures as strings.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub async fn poll_schedule_sources(&self) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let now = Instant::now();
        let due: Vec<String> = self
            .schedule_sources
            .lock()
            .expect("poisoned")
            .iter()
            .filter(|(_, entry)| entry.next_poll_at.is_none_or(|at| now >= at))
            .map(|(stream, _)| stream.clone())
            .collect();
        for stream in due {
            self.poll_schedule_source(&stream).await?;
        }
        Ok(())
    }

    async fn poll_schedule_source(&self, stream: &str) -> Result<(), String> {
        let source = {
            self.schedule_sources
                .lock()
                .expect("poisoned")
                .get(stream)
                .map(|entry| Arc::clone(&entry.source))
        };
        let Some(source) = source else {
            return Ok(());
        };

        let desired = match source.schedules().await {
            Err(e) => {
                eprintln!("trembita: schedule source {stream:?}: {e}");
                return Ok(());
            }
            Ok(jobs) => jobs,
        };

        let apply = {
            let mut map = self.schedule_sources.lock().expect("poisoned");
            let Some(entry) = map.get_mut(stream) else {
                return Ok(());
            };
            entry.next_poll_at = Some(Instant::now() + entry.poll);
            if desired.is_empty() && entry.last_good.is_none() {
                false
            } else {
                entry.last_good = Some(desired.clone());
                true
            }
        };

        if apply {
            self.reconcile_schedules(stream, &desired).await?;
        }
        Ok(())
    }

    /// Diff `desired` against live redb schedules and replicate mutations.
    ///
    /// # Errors
    /// Propagates queue or replication failures as strings.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub async fn reconcile_schedules(
        &self,
        stream: &str,
        desired: &[RecurringJob],
    ) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let queue = self
            .redb_streams
            .lock()
            .expect("poisoned")
            .get(stream)
            .cloned()
            .ok_or_else(|| format!("unknown queue stream {stream:?}"))?;
        let ops = queue
            .reconcile_schedules(desired)
            .map_err(|e| e.to_string())?;
        if !ops.is_empty() {
            self.replicate_ops(stream, &ops).await?;
        }
        Ok(())
    }

    /// Register a [`ScheduleSource`] polled on the queue leader.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_schedule_source(
        &self,
        stream: impl Into<String>,
        source: Arc<dyn ScheduleSource>,
        poll: Duration,
    ) {
        let stream = stream.into();
        self.schedule_sources.lock().expect("poisoned").insert(
            stream,
            ScheduleSourceEntry {
                source,
                poll,
                last_good: None,
                next_poll_at: None,
            },
        );
    }

    /// Whether any [`ScheduleSource`] is registered.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn has_schedule_sources(&self) -> bool {
        !self.schedule_sources.lock().expect("poisoned").is_empty()
    }

    /// Shared cluster facts for leader-only background loops.
    #[must_use]
    pub fn cluster_state(&self) -> Arc<dyn ClusterState> {
        Arc::clone(&self.state)
    }

    /// Fire due recurring schedules on the leader and replicate mutations.
    ///
    /// # Errors
    /// Propagates queue or replication failures as strings.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub async fn tick_schedules(&self) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let backends: Vec<(String, Arc<RedbJobQueue>)> = self
            .redb_streams
            .lock()
            .expect("poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (stream, queue) in backends {
            let ops = queue.tick_schedules().await.map_err(|e| e.to_string())?;
            if !ops.is_empty() {
                self.replicate_ops(&stream, &ops).await?;
            }
        }
        Ok(())
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
        let peers = replication_peers(self.state.as_ref(), self.node_id)?;
        let request = QueueReplicateRequest {
            stream: stream.to_string(),
            ops: ops.clone(),
            leader_id: self.node_id.0,
        };
        let transport = Arc::clone(&self.transport);
        fanout_replicate(&peers, move |peer| {
            let transport = Arc::clone(&transport);
            let request = request.clone();
            Box::pin(async move {
                let reply = send_queue_replicate(transport.as_ref(), peer, &request)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(err) = reply.error {
                    return Err(err);
                }
                Ok(())
            })
        })
        .await
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

    fn authorize_replicate(&self, declared_leader: NodeId) -> Result<(), String> {
        authorize_replicate_leader(self.state.as_ref(), declared_leader, REPLICATE_NOT_LEADER)
    }

    async fn handle_replicate(
        &self,
        _from: Option<NodeId>,
        request: QueueReplicateRequest,
    ) -> QueueReplicateReply {
        if let Err(e) = self.authorize_replicate(NodeId(request.leader_id)) {
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

    fn cache_enqueued(
        &self,
        stream: &str,
        job_id: u64,
        payload: Vec<u8>,
        priority: u8,
        not_before_ms: u64,
        dedup_key: Option<Vec<u8>>,
    ) {
        let mut prefetch = self.prefetch.lock().expect("poisoned");
        let Some(cache) = prefetch.get_mut(stream) else {
            return;
        };
        let effective_not_before = if not_before_ms == 0 {
            now_ms()
        } else {
            not_before_ms
        };
        cache.insert_enqueued(CachedPendingJob {
            job_id,
            payload,
            priority,
            not_before_ms: effective_not_before,
            dedup_key,
        });
    }

    fn evict_prefetch(&self, stream: &str, job_ids: impl IntoIterator<Item = u64>) {
        let mut prefetch = self.prefetch.lock().expect("poisoned");
        let Some(cache) = prefetch.get_mut(stream) else {
            return;
        };
        for job_id in job_ids {
            cache.remove_job(job_id);
        }
    }

    fn evict_prefetch_ack_ops(&self, stream: &str, ops: &[QueueReplicateOp]) {
        self.evict_prefetch(
            stream,
            ops.iter().filter_map(|op| match op {
                QueueReplicateOp::Ack { job_id, .. } => Some(*job_id),
                _ => None,
            }),
        );
    }

    fn evict_prefetch_sharded_acks(&self, base: &str, reps: &[ShardedReplication]) {
        for rep in reps {
            self.evict_prefetch_ack_ops(&shard_stream_name(base, rep.shard), &rep.ops);
        }
    }

    fn cache_enqueued_sharded(
        &self,
        base: &str,
        global_id: u64,
        payload: Vec<u8>,
        priority: u8,
        not_before_ms: u64,
        dedup_key: Option<Vec<u8>>,
    ) {
        let (shard, local) = decode_global_id(global_id);
        self.cache_enqueued(
            &shard_stream_name(base, shard),
            local,
            payload,
            priority,
            not_before_ms,
            dedup_key,
        );
    }

    async fn lease_redb_with_prefetch(
        &self,
        stream: &str,
        queue: &RedbJobQueue,
        worker: WorkerId,
        max: usize,
    ) -> Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError> {
        let now = now_ms();
        let prefetched = self
            .prefetch
            .lock()
            .expect("poisoned")
            .get_mut(stream)
            .map(|cache| cache.select_for_lease(max, now))
            .unwrap_or_default();

        let mut jobs = Vec::new();
        let mut ops = Vec::new();
        if !prefetched.is_empty() {
            let (leased, mut step) = queue.lease_prefetched(worker, &prefetched)?;
            jobs.extend(leased);
            ops.append(&mut step);
        }
        if jobs.len() < max {
            let (mut more, mut more_ops) = queue.lease_replicated(worker, max - jobs.len()).await?;
            jobs.append(&mut more);
            ops.append(&mut more_ops);
        }
        Ok((jobs, ops))
    }

    async fn lease_sharded_with_prefetch(
        &self,
        base: &str,
        sharded: &ShardedJobQueue,
        worker: WorkerId,
        max: usize,
    ) -> Result<(Vec<LeasedJob>, Vec<ShardedReplication>), QueueError> {
        let now = now_ms();
        let mut out = Vec::new();
        let mut replications = Vec::new();
        let mut need = max;

        for shard in 0..sharded.shard_count() {
            if need == 0 {
                break;
            }
            let stream = shard_stream_name(base, shard);
            let redb = self
                .redb_streams
                .lock()
                .expect("poisoned")
                .get(&stream)
                .cloned();

            if let Some(redb) = redb {
                let prefetched = self
                    .prefetch
                    .lock()
                    .expect("poisoned")
                    .get_mut(&stream)
                    .map(|cache| cache.select_for_lease(need, now))
                    .unwrap_or_default();

                if !prefetched.is_empty() {
                    let (leased, step) = redb.lease_prefetched(worker, &prefetched)?;
                    need = need.saturating_sub(leased.len());
                    if !step.is_empty() {
                        replications.push(ShardedReplication { shard, ops: step });
                    }
                    out.extend(leased.into_iter().map(|job| LeasedJob {
                        lease_id: LeaseId(encode_global_id(shard, job.lease_id.0)),
                        job_id: JobId(encode_global_id(shard, job.job_id.0)),
                        payload: job.payload,
                        attempts: job.attempts,
                        dedup_key: job.dedup_key,
                    }));
                }
            }

            if need > 0 {
                let (jobs, rep) = sharded.lease_shard_replicated(shard, worker, need).await?;
                need = need.saturating_sub(jobs.len());
                out.extend(jobs);
                if !rep.ops.is_empty() {
                    replications.push(rep);
                }
            }
        }
        Ok((out, replications))
    }

    fn leased_to_wire(jobs: Vec<LeasedJob>) -> Vec<QueueLeasedJobWire> {
        jobs.into_iter()
            .map(|j| QueueLeasedJobWire {
                lease_id: j.lease_id.0,
                job_id: j.job_id.0,
                payload: j.payload,
                attempts: j.attempts,
                dedup_key: j.dedup_key,
            })
            .collect()
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_enqueue(&self, request: QueueEnqueueRequest) -> QueueEnqueueReply {
        if self.state.is_leader() {
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                let options = enqueue_options_from_request(&request);
                match sharded
                    .enqueue_opts_replicated_sharded(&request.payload, options)
                    .await
                {
                    Ok((id, rep)) => {
                        self.emit_backlog_settle_for_sharded_reps(
                            &request.stream,
                            std::slice::from_ref(&rep),
                            "reclaim",
                        )
                        .await;
                        if let Err(e) = self.replicate_sharded(&request.stream, &[rep]).await {
                            return QueueEnqueueReply {
                                job_id: None,
                                error: Some(e),
                            };
                        }
                        self.cache_enqueued_sharded(
                            &request.stream,
                            id.0,
                            request.payload.clone(),
                            request.priority,
                            request.not_before_ms,
                            request.dedup_key.clone(),
                        );
                        self.emit_enqueued(&request.stream, id.0);
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
                            self.emit_backlog_settle_for_terminal_ops(
                                &request.stream,
                                queue.as_ref(),
                                &ops,
                                "reclaim",
                            )
                            .await;
                            self.cache_enqueued(
                                &request.stream,
                                id.0,
                                request.payload.clone(),
                                request.priority,
                                request.not_before_ms,
                                request.dedup_key.clone(),
                            );
                            self.emit_enqueued(&request.stream, id.0);
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

    #[allow(clippy::too_many_lines)] // leader sharded + local + follower forward
    async fn handle_enqueue_batch(
        &self,
        request: QueueEnqueueBatchRequest,
    ) -> QueueEnqueueBatchReply {
        if request.jobs.len() > DEFAULT_QUEUE_BATCH_MAX {
            return QueueEnqueueBatchReply {
                job_ids: Vec::new(),
                error: Some(format!(
                    "batch size {} exceeds max {}",
                    request.jobs.len(),
                    DEFAULT_QUEUE_BATCH_MAX
                )),
            };
        }
        if self.state.is_leader() {
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                let batch: Vec<(Vec<u8>, EnqueueOptions)> = request
                    .jobs
                    .iter()
                    .map(|job| (job.payload.clone(), enqueue_options_from_batch_job(job)))
                    .collect();
                match sharded.enqueue_batch_opts_replicated_sharded(&batch).await {
                    Ok((ids, reps)) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &reps).await {
                            return QueueEnqueueBatchReply {
                                job_ids: Vec::new(),
                                error: Some(e),
                            };
                        }
                        self.emit_backlog_settle_for_sharded_reps(
                            &request.stream,
                            &reps,
                            "reclaim",
                        )
                        .await;
                        for (job, id) in request.jobs.iter().zip(&ids) {
                            self.cache_enqueued_sharded(
                                &request.stream,
                                id.0,
                                job.payload.clone(),
                                job.priority,
                                job.not_before_ms,
                                job.dedup_key.clone(),
                            );
                            self.emit_enqueued(&request.stream, id.0);
                        }
                        return QueueEnqueueBatchReply {
                            job_ids: ids.into_iter().map(|id| id.0).collect(),
                            error: None,
                        };
                    }
                    Err(e) => {
                        return QueueEnqueueBatchReply {
                            job_ids: Vec::new(),
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
            match self.local_stream(&request.stream) {
                Err(e) => QueueEnqueueBatchReply {
                    job_ids: Vec::new(),
                    error: Some(e),
                },
                Ok(queue) => {
                    let batch: Vec<(Vec<u8>, EnqueueOptions)> = request
                        .jobs
                        .iter()
                        .map(|job| (job.payload.clone(), enqueue_options_from_batch_job(job)))
                        .collect();
                    match queue.enqueue_batch_opts_replicated(&batch).await {
                        Ok((ids, ops)) => {
                            if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                                return QueueEnqueueBatchReply {
                                    job_ids: Vec::new(),
                                    error: Some(e),
                                };
                            }
                            self.emit_backlog_settle_for_terminal_ops(
                                &request.stream,
                                queue.as_ref(),
                                &ops,
                                "reclaim",
                            )
                            .await;
                            for (job, id) in request.jobs.iter().zip(&ids) {
                                self.cache_enqueued(
                                    &request.stream,
                                    id.0,
                                    job.payload.clone(),
                                    job.priority,
                                    job.not_before_ms,
                                    job.dedup_key.clone(),
                                );
                                self.emit_enqueued(&request.stream, id.0);
                            }
                            QueueEnqueueBatchReply {
                                job_ids: ids.into_iter().map(|id| id.0).collect(),
                                error: None,
                            }
                        }
                        Err(e) => QueueEnqueueBatchReply {
                            job_ids: Vec::new(),
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
                        send_queue_enqueue_batch(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueEnqueueBatchReply {
                    job_ids: Vec::new(),
                    error: Some(e),
                },
            }
        }
    }

    async fn handle_ack_batch(&self, request: QueueAckBatchRequest) -> QueueAckBatchReply {
        if request.lease_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
            return QueueAckBatchReply {
                error: Some(format!(
                    "batch size {} exceeds max {}",
                    request.lease_ids.len(),
                    DEFAULT_QUEUE_BATCH_MAX
                )),
            };
        }
        if self.state.is_leader() {
            let worker = WorkerId {
                node: NodeId(request.worker_node),
                instance: request.worker_instance,
            };
            let lease_ids: Vec<LeaseId> = request.lease_ids.iter().map(|id| LeaseId(*id)).collect();
            let mut lease_metas = Vec::with_capacity(lease_ids.len());
            for lease_id in &lease_ids {
                lease_metas.push(self.peek_lease_meta(&request.stream, *lease_id).await);
            }
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded
                    .ack_batch_replicated_sharded(worker, &lease_ids)
                    .await
                {
                    Ok(reps) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &reps).await {
                            return QueueAckBatchReply { error: Some(e) };
                        }
                        self.evict_prefetch_sharded_acks(&request.stream, &reps);
                        for (lease_id, (dedup_key, attempts)) in
                            request.lease_ids.iter().zip(lease_metas)
                        {
                            self.emit_acked(&request.stream, *lease_id, request.worker_node);
                            self.emit_backlog_settle(
                                &request.stream,
                                dedup_key,
                                BacklogSettleOutcome::Done { attempts },
                            );
                        }
                        return QueueAckBatchReply { error: None };
                    }
                    Err(e) => {
                        return QueueAckBatchReply {
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
            match self.local_stream(&request.stream) {
                Err(e) => QueueAckBatchReply { error: Some(e) },
                Ok(queue) => match queue.ack_batch_replicated(worker, &lease_ids).await {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                            QueueAckBatchReply { error: Some(e) }
                        } else {
                            self.evict_prefetch_ack_ops(&request.stream, &ops);
                            for (lease_id, (dedup_key, attempts)) in
                                request.lease_ids.iter().zip(lease_metas)
                            {
                                self.emit_acked(&request.stream, *lease_id, request.worker_node);
                                self.emit_backlog_settle(
                                    &request.stream,
                                    dedup_key,
                                    BacklogSettleOutcome::Done { attempts },
                                );
                            }
                            QueueAckBatchReply { error: None }
                        }
                    }
                    Err(e) => QueueAckBatchReply {
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
                        send_queue_ack_batch(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueAckBatchReply { error: Some(e) },
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_lease(&self, request: QueueLeaseRequest) -> QueueLeaseReply {
        if self.state.is_leader() {
            let worker = WorkerId {
                node: NodeId(request.worker_node),
                instance: request.worker_instance,
            };
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match self
                    .lease_sharded_with_prefetch(&request.stream, &sharded, worker, request.max)
                    .await
                {
                    Ok((jobs, reps)) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &reps).await {
                            return QueueLeaseReply {
                                jobs: Vec::new(),
                                error: Some(e),
                            };
                        }
                        self.emit_backlog_settle_for_sharded_reps(
                            &request.stream,
                            &reps,
                            "reclaim",
                        )
                        .await;
                        for j in &jobs {
                            self.emit_leased(
                                &request.stream,
                                j.job_id.0,
                                j.lease_id.0,
                                request.worker_node,
                                request.worker_instance,
                                j.attempts,
                            );
                        }
                        return QueueLeaseReply {
                            jobs: jobs
                                .into_iter()
                                .map(|j| QueueLeasedJobWire {
                                    lease_id: j.lease_id.0,
                                    job_id: j.job_id.0,
                                    payload: j.payload,
                                    attempts: j.attempts,
                                    dedup_key: j.dedup_key,
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
                Ok(queue) => {
                    let redb = self
                        .redb_streams
                        .lock()
                        .expect("poisoned")
                        .get(&request.stream)
                        .cloned();
                    let lease_result = if let Some(redb) = redb {
                        self.lease_redb_with_prefetch(&request.stream, &redb, worker, request.max)
                            .await
                    } else {
                        queue.lease_replicated(worker, request.max).await
                    };
                    match lease_result {
                        Ok((jobs, ops)) => {
                            if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                                return QueueLeaseReply {
                                    jobs: Vec::new(),
                                    error: Some(e),
                                };
                            }
                            self.emit_backlog_settle_for_terminal_ops(
                                &request.stream,
                                queue.as_ref(),
                                &ops,
                                "reclaim",
                            )
                            .await;
                            for j in &jobs {
                                self.emit_leased(
                                    &request.stream,
                                    j.job_id.0,
                                    j.lease_id.0,
                                    request.worker_node,
                                    request.worker_instance,
                                    j.attempts,
                                );
                            }
                            QueueLeaseReply {
                                jobs: Self::leased_to_wire(jobs),
                                error: None,
                            }
                        }
                        Err(e) => QueueLeaseReply {
                            jobs: Vec::new(),
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
            let (dedup_key, attempts) = self
                .peek_lease_meta(&request.stream, LeaseId(request.lease_id))
                .await;
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded
                    .ack_replicated_sharded(worker, LeaseId(request.lease_id))
                    .await
                {
                    Ok(rep) => {
                        if let Err(e) = self
                            .replicate_sharded(&request.stream, std::slice::from_ref(&rep))
                            .await
                        {
                            return QueueAckReply { error: Some(e) };
                        }
                        self.evict_prefetch_sharded_acks(
                            &request.stream,
                            std::slice::from_ref(&rep),
                        );
                        self.emit_acked(&request.stream, request.lease_id, request.worker_node);
                        self.emit_backlog_settle(
                            &request.stream,
                            dedup_key.clone(),
                            BacklogSettleOutcome::Done { attempts },
                        );
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
                        self.evict_prefetch_ack_ops(&request.stream, &ops);
                        self.emit_acked(&request.stream, request.lease_id, request.worker_node);
                        self.emit_backlog_settle(
                            &request.stream,
                            dedup_key,
                            BacklogSettleOutcome::Done { attempts },
                        );
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
                        self.emit_backlog_settle_for_sharded_reps(
                            &request.stream,
                            std::slice::from_ref(&rep),
                            "nack",
                        )
                        .await;
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
                            self.emit_backlog_settle_for_terminal_ops(
                                &request.stream,
                                queue.as_ref(),
                                &ops,
                                "nack",
                            )
                            .await;
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

    async fn handle_extend_lease(&self, request: QueueExtendLeaseRequest) -> QueueExtendLeaseReply {
        if self.state.is_leader() {
            let worker = WorkerId {
                node: NodeId(request.worker_node),
                instance: request.worker_instance,
            };
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded
                    .extend_lease_replicated_sharded(worker, LeaseId(request.lease_id))
                    .await
                {
                    Ok(rep) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &[rep]).await {
                            return QueueExtendLeaseReply { error: Some(e) };
                        }
                        return QueueExtendLeaseReply { error: None };
                    }
                    Err(e) => {
                        return QueueExtendLeaseReply {
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
            match self.local_stream(&request.stream) {
                Err(e) => QueueExtendLeaseReply { error: Some(e) },
                Ok(queue) => {
                    match queue
                        .extend_lease_replicated(worker, LeaseId(request.lease_id))
                        .await
                    {
                        Ok(ops) => {
                            if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                                return QueueExtendLeaseReply { error: Some(e) };
                            }
                            QueueExtendLeaseReply { error: None }
                        }
                        Err(e) => QueueExtendLeaseReply {
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
                        send_queue_extend_lease(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueExtendLeaseReply { error: Some(e) },
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
                            dead_letter: 0,
                            oldest_pending_age_ms: 0,
                            redelivered: 0,
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
                    dead_letter: m.dead_letter,
                    oldest_pending_age_ms: u64::try_from(m.oldest_pending_age.as_millis())
                        .unwrap_or(u64::MAX),
                    redelivered: m.redelivered,
                    error: None,
                },
                Err(e) => QueueMetricsReply {
                    pending: 0,
                    leased: 0,
                    dead_letter: 0,
                    oldest_pending_age_ms: 0,
                    redelivered: 0,
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
                    dead_letter: 0,
                    oldest_pending_age_ms: 0,
                    redelivered: 0,
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
                            attempts: 0,
                            max_attempts: 0,
                            dedup_key: None,
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
                    attempts: 0,
                    max_attempts: 0,
                    dedup_key: None,
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
                    attempts: 0,
                    max_attempts: 0,
                    dedup_key: None,
                    error: Some(e),
                },
            }
        }
    }

    async fn handle_requeue_dead_letter(
        &self,
        request: trembita_proto::QueueRequeueDeadLetterRequest,
    ) -> trembita_proto::QueueRequeueDeadLetterReply {
        if self.state.is_leader() {
            match self.local_stream(&request.stream) {
                Err(e) => trembita_proto::QueueRequeueDeadLetterReply { error: Some(e) },
                Ok(queue) => {
                    match queue
                        .requeue_dead_letter_replicated(JobId(request.job_id))
                        .await
                    {
                        Ok(ops) => {
                            if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                                trembita_proto::QueueRequeueDeadLetterReply { error: Some(e) }
                            } else {
                                trembita_proto::QueueRequeueDeadLetterReply { error: None }
                            }
                        }
                        Err(e) => trembita_proto::QueueRequeueDeadLetterReply {
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
                        trembita_net::send_queue_requeue_dead_letter(
                            transport.as_ref(),
                            leader,
                            &request,
                        )
                        .await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => trembita_proto::QueueRequeueDeadLetterReply { error: Some(e) },
            }
        }
    }

    async fn handle_list_jobs(&self, request: QueueListJobsRequest) -> QueueListJobsReply {
        if self.state.is_leader() {
            let filter = filter_from_list_request(&request);
            let page = if let Some(sharded) = self.sharded_stream(&request.stream) {
                sharded.list_jobs(filter).await
            } else {
                match self.local_stream(&request.stream) {
                    Err(e) => {
                        return QueueListJobsReply {
                            jobs: Vec::new(),
                            has_more: false,
                            error: Some(e),
                        };
                    }
                    Ok(queue) => queue.list_jobs(filter).await,
                }
            };
            match page {
                Ok(page) => QueueListJobsReply {
                    jobs: page
                        .jobs
                        .into_iter()
                        .map(job_status_to_list_entry)
                        .collect(),
                    has_more: page.has_more,
                    error: None,
                },
                Err(e) => QueueListJobsReply {
                    jobs: Vec::new(),
                    has_more: false,
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_list_jobs(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueListJobsReply {
                    jobs: Vec::new(),
                    has_more: false,
                    error: Some(e),
                },
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_requeue_dead_letter_batch(
        &self,
        request: QueueRequeueDeadLetterBatchRequest,
    ) -> QueueRequeueDeadLetterBatchReply {
        if request.job_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
            return QueueRequeueDeadLetterBatchReply {
                requeued: Vec::new(),
                failures: Vec::new(),
                error: Some(format!(
                    "batch size {} exceeds max {DEFAULT_QUEUE_BATCH_MAX}",
                    request.job_ids.len()
                )),
            };
        }
        if self.state.is_leader() {
            let job_ids: Vec<JobId> = request.job_ids.iter().map(|id| JobId(*id)).collect();
            if let Some(sharded) = self.sharded_stream(&request.stream) {
                match sharded
                    .requeue_dead_letter_batch_replicated_sharded(&job_ids)
                    .await
                {
                    Ok((requeued, failures, reps)) => {
                        if let Err(e) = self.replicate_sharded(&request.stream, &reps).await {
                            return QueueRequeueDeadLetterBatchReply {
                                requeued: Vec::new(),
                                failures: Vec::new(),
                                error: Some(e),
                            };
                        }
                        return QueueRequeueDeadLetterBatchReply {
                            requeued: requeued.into_iter().map(|id| id.0).collect(),
                            failures: failures
                                .into_iter()
                                .map(|(id, err)| QueueRequeueFailureWire {
                                    job_id: id.0,
                                    error: err.to_string(),
                                })
                                .collect(),
                            error: None,
                        };
                    }
                    Err(e) => {
                        return QueueRequeueDeadLetterBatchReply {
                            requeued: Vec::new(),
                            failures: Vec::new(),
                            error: Some(e.to_string()),
                        };
                    }
                }
            }
            match self.local_stream(&request.stream) {
                Err(e) => QueueRequeueDeadLetterBatchReply {
                    requeued: Vec::new(),
                    failures: Vec::new(),
                    error: Some(e),
                },
                Ok(queue) => match queue.requeue_dead_letter_batch_replicated(&job_ids).await {
                    Ok((requeued, failures, ops)) => {
                        if let Err(e) = self.replicate_ops(&request.stream, &ops).await {
                            return QueueRequeueDeadLetterBatchReply {
                                requeued: Vec::new(),
                                failures: Vec::new(),
                                error: Some(e),
                            };
                        }
                        QueueRequeueDeadLetterBatchReply {
                            requeued: requeued.into_iter().map(|id| id.0).collect(),
                            failures: failures
                                .into_iter()
                                .map(|(id, err)| QueueRequeueFailureWire {
                                    job_id: id.0,
                                    error: err.to_string(),
                                })
                                .collect(),
                            error: None,
                        }
                    }
                    Err(e) => QueueRequeueDeadLetterBatchReply {
                        requeued: Vec::new(),
                        failures: Vec::new(),
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
                        send_queue_requeue_dead_letter_batch(transport.as_ref(), leader, &request)
                            .await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueRequeueDeadLetterBatchReply {
                    requeued: Vec::new(),
                    failures: Vec::new(),
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
            Route::QueueEnqueueBatch => Box::pin(async move {
                let request: QueueEnqueueBatchRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_enqueue_batch(request).await)?)
            }),
            Route::QueueLease => Box::pin(async move {
                let request: QueueLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_lease(request).await)?)
            }),
            Route::QueueAck => Box::pin(async move {
                let request: QueueAckRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack(request).await)?)
            }),
            Route::QueueAckBatch => Box::pin(async move {
                let request: QueueAckBatchRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack_batch(request).await)?)
            }),
            Route::QueueNack => Box::pin(async move {
                let request: QueueNackRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_nack(request).await)?)
            }),
            Route::QueueExtendLease => Box::pin(async move {
                let request: QueueExtendLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_extend_lease(request).await)?)
            }),
            Route::QueueMetrics => Box::pin(async move {
                let request: QueueMetricsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_metrics(request).await)?)
            }),
            Route::QueueJobStatus => Box::pin(async move {
                let request: QueueJobStatusRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_job_status(request).await)?)
            }),
            Route::QueueListJobs => Box::pin(async move {
                let request: QueueListJobsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_list_jobs(request).await)?)
            }),
            Route::QueueRequeueDeadLetter => Box::pin(async move {
                let request: trembita_proto::QueueRequeueDeadLetterRequest = decode_body(&body)?;
                Ok(encode_body(
                    &service.handle_requeue_dead_letter(request).await,
                )?)
            }),
            Route::QueueRequeueDeadLetterBatch => Box::pin(async move {
                let request: QueueRequeueDeadLetterBatchRequest = decode_body(&body)?;
                Ok(encode_body(
                    &service.handle_requeue_dead_letter_batch(request).await,
                )?)
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
