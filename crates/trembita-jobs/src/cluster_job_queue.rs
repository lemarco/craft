//! Cluster-facing [`JobQueue`] client routing through the leader wire service.

use std::sync::Arc;

use trembita_net::transport::{BoxFuture, Transport};
use trembita_net::{
    send_queue_ack_batch, send_queue_enqueue, send_queue_enqueue_batch, send_queue_extend_lease,
    send_queue_job_status, send_queue_lease, send_queue_list_jobs, send_queue_list_schedules,
    send_queue_metrics, send_queue_nack, send_queue_remove_schedule,
    send_queue_requeue_dead_letter_batch, send_queue_upsert_schedule,
};
use trembita_proto::ProductWireError;
use trembita_proto::{
    DedupKey, MaxAttempts, NodeId, QueueAckBatchRequest, QueueBatchEnqueueJob,
    QueueEnqueueBatchRequest, QueueEnqueueRequest, QueueExtendLeaseRequest, QueueJobLifecycleWire,
    QueueJobStatusRequest, QueueLeaseRequest, QueueListJobsRequest, QueueListSchedulesRequest,
    QueueMetricsRequest, QueueNackRequest, QueueRemoveScheduleRequest, QueueReplicateOp,
    QueueRequeueDeadLetterBatchRequest, QueueUpsertScheduleRequest, RecurringScheduleWire,
    StreamName, UnixMillis,
};
use trembita_runtime::ClusterState;

use crate::{
    EnqueueOptions, JobId, JobLifecycle, JobListFilter, JobQueue, JobStatus, LeaseId, LeasedJob,
    QueueError, QueueMetrics, QueueReplicationOps, RecurringJob, WorkerId,
    recurring_job_to_schedule_wire,
};

fn replication_unsupported() -> QueueError {
    QueueError::Backend("cluster queue client does not apply replication locally".into())
}

fn wire_stream(stream: &str) -> StreamName {
    StreamName::try_new(stream.to_string()).expect("cluster queue stream name is valid")
}

fn wire_dedup_key(key: Option<Vec<u8>>) -> Option<DedupKey> {
    key.and_then(|k| DedupKey::try_new(k).ok())
}

fn resolve_max_attempts(options: &EnqueueOptions, default: u32) -> MaxAttempts {
    options.max_attempts.unwrap_or(MaxAttempts(default))
}

/// Cluster-facing [`JobQueue`] that routes through the leader wire service.
pub struct ClusterJobQueue {
    stream: String,
    node_id: NodeId,
    default_max_attempts: u32,
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
            default_max_attempts: 0,
            state,
            transport,
        }
    }

    /// Attempt ceiling applied when [`EnqueueOptions::max_attempts`] is `None` (`0` = unlimited).
    ///
    /// Resolved here, client-side, so the wire request always carries a concrete
    /// ceiling and the queue protocol stays unchanged.
    #[must_use]
    pub fn default_max_attempts(mut self, max: u32) -> Self {
        self.default_max_attempts = max;
        self
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

    fn extend_lease_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            self.extend_lease(worker, lease_id).await?;
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
                    stream: wire_stream(&self.stream),
                    payload: payload.to_vec(),
                    priority: options.priority,
                    not_before_ms: UnixMillis(options.not_before_ms.unwrap_or(0)),
                    shard_key: options.shard_key.clone(),
                    dedup_key: wire_dedup_key(options.dedup_key.clone()),
                    max_attempts: resolve_max_attempts(&options, self.default_max_attempts),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                if matches!(err, ProductWireError::NotLeader) {
                    return Err(QueueError::Backend(err.to_string()));
                }
                return Err(QueueError::Backend(err.to_string()));
            }
            reply
                .job_id
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

    fn enqueue_batch_opts_replicated<'a>(
        &'a self,
        jobs: &'a [(Vec<u8>, EnqueueOptions)],
    ) -> BoxFuture<'a, Result<(Vec<JobId>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let wire_jobs: Vec<QueueBatchEnqueueJob> = jobs
                .iter()
                .map(|(payload, options)| QueueBatchEnqueueJob {
                    payload: payload.clone(),
                    priority: options.priority,
                    not_before_ms: UnixMillis(options.not_before_ms.unwrap_or(0)),
                    shard_key: options.shard_key.clone(),
                    dedup_key: wire_dedup_key(options.dedup_key.clone()),
                    max_attempts: resolve_max_attempts(options, self.default_max_attempts),
                })
                .collect();
            let reply = send_queue_enqueue_batch(
                self.transport.as_ref(),
                leader,
                &QueueEnqueueBatchRequest {
                    stream: wire_stream(&self.stream),
                    jobs: wire_jobs,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok((reply.job_ids, Vec::new()))
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
                    stream: wire_stream(&self.stream),
                    worker_node: worker.node,
                    worker_instance: worker.instance,
                    max,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(reply
                .jobs
                .into_iter()
                .map(|j| LeasedJob {
                    lease_id: j.lease_id,
                    job_id: j.job_id,
                    payload: j.payload,
                    attempts: j.attempts,
                    dedup_key: j.dedup_key.map(|k| k.as_bytes().to_vec()),
                })
                .collect())
        })
    }

    fn ack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            self.ack_batch_replicated(worker, &[lease_id]).await?;
            Ok(())
        })
    }

    fn ack_batch_replicated<'a>(
        &'a self,
        worker: WorkerId,
        lease_ids: &'a [LeaseId],
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            if lease_ids.is_empty() {
                return Ok(Vec::new());
            }
            let leader = self.leader()?;
            let reply = send_queue_ack_batch(
                self.transport.as_ref(),
                leader,
                &QueueAckBatchRequest {
                    stream: wire_stream(&self.stream),
                    worker_node: worker.node,
                    worker_instance: worker.instance,
                    lease_ids: lease_ids.to_vec(),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(Vec::new())
        })
    }

    fn nack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_nack(
                self.transport.as_ref(),
                leader,
                &QueueNackRequest {
                    stream: wire_stream(&self.stream),
                    worker_node: worker.node,
                    worker_instance: worker.instance,
                    lease_id,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(())
        })
    }

    fn extend_lease(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_extend_lease(
                self.transport.as_ref(),
                leader,
                &QueueExtendLeaseRequest {
                    stream: wire_stream(&self.stream),
                    worker_node: worker.node,
                    worker_instance: worker.instance,
                    lease_id,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
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
                    stream: wire_stream(&self.stream),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(QueueMetrics {
                pending: reply.pending,
                leased: reply.leased,
                dead_letter: reply.dead_letter,
                oldest_pending_age: std::time::Duration::from_millis(reply.oldest_pending_age_ms),
                redelivered: reply.redelivered,
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
                    stream: wire_stream(&self.stream),
                    job_id,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
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
                    QueueJobLifecycleWire::DeadLetter => JobLifecycle::DeadLetter,
                },
                payload_len: reply.payload_len,
                priority: reply.priority,
                leased_by: match (reply.leased_worker_node, reply.leased_worker_instance) {
                    (Some(node), Some(instance)) => Some(WorkerId { node, instance }),
                    _ => None,
                },
                attempts: reply.attempts,
                max_attempts: reply.max_attempts,
                dedup_key: reply.dedup_key.as_ref().map(|k| k.as_bytes().to_vec()),
            }))
        })
    }

    fn list_jobs(
        &self,
        filter: JobListFilter,
    ) -> BoxFuture<'_, Result<crate::JobListPage, QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_list_jobs(
                self.transport.as_ref(),
                leader,
                &QueueListJobsRequest {
                    stream: wire_stream(&self.stream),
                    lifecycle: filter.lifecycle.map(|l| match l {
                        JobLifecycle::Pending => QueueJobLifecycleWire::Pending,
                        JobLifecycle::Leased => QueueJobLifecycleWire::Leased,
                        JobLifecycle::Delayed => QueueJobLifecycleWire::Delayed,
                        JobLifecycle::DeadLetter => QueueJobLifecycleWire::DeadLetter,
                    }),
                    min_attempts: filter.min_attempts,
                    dedup_key: wire_dedup_key(filter.dedup_key.clone()),
                    limit: u32::try_from(filter.effective_limit()).unwrap_or(u32::MAX),
                    after_job_id: filter.after_job_id.unwrap_or(JobId(0)),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(crate::JobListPage {
                jobs: reply
                    .jobs
                    .into_iter()
                    .map(|entry| JobStatus {
                        job_id: entry.job_id,
                        lifecycle: match entry.lifecycle {
                            QueueJobLifecycleWire::Pending => JobLifecycle::Pending,
                            QueueJobLifecycleWire::Leased => JobLifecycle::Leased,
                            QueueJobLifecycleWire::Delayed => JobLifecycle::Delayed,
                            QueueJobLifecycleWire::DeadLetter => JobLifecycle::DeadLetter,
                        },
                        payload_len: entry.payload_len,
                        priority: entry.priority,
                        leased_by: match (entry.leased_worker_node, entry.leased_worker_instance) {
                            (Some(node), Some(instance)) => Some(WorkerId { node, instance }),
                            _ => None,
                        },
                        attempts: entry.attempts,
                        max_attempts: entry.max_attempts,
                        dedup_key: entry.dedup_key.map(|k| k.as_bytes().to_vec()),
                    })
                    .collect(),
                has_more: reply.has_more,
            })
        })
    }

    fn list_schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringScheduleWire>, QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_list_schedules(
                self.transport.as_ref(),
                leader,
                &QueueListSchedulesRequest {
                    stream: wire_stream(&self.stream),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(reply.schedules)
        })
    }

    fn upsert_schedule_replicated<'a>(
        &'a self,
        job: &'a RecurringJob,
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        let wire = recurring_job_to_schedule_wire(job);
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_upsert_schedule(
                self.transport.as_ref(),
                leader,
                &QueueUpsertScheduleRequest {
                    stream: wire_stream(&self.stream),
                    schedule: wire,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(Vec::new())
        })
    }

    fn remove_schedule_replicated<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        let name = name.to_string();
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_remove_schedule(
                self.transport.as_ref(),
                leader,
                &QueueRemoveScheduleRequest {
                    stream: wire_stream(&self.stream),
                    name: name.clone(),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(Vec::new())
        })
    }

    fn requeue_dead_letter_batch_replicated<'a>(
        &'a self,
        job_ids: &'a [JobId],
    ) -> BoxFuture<
        'a,
        Result<(Vec<JobId>, Vec<(JobId, QueueError)>, QueueReplicationOps), QueueError>,
    > {
        let ids: Vec<JobId> = job_ids.to_vec();
        Box::pin(async move {
            let result = self.requeue_dead_letter_batch(&ids).await?;
            Ok((result.requeued, result.failures, Vec::new()))
        })
    }

    fn requeue_dead_letter(&self, job_id: JobId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = trembita_net::send_queue_requeue_dead_letter(
                self.transport.as_ref(),
                leader,
                &trembita_proto::QueueRequeueDeadLetterRequest {
                    stream: wire_stream(&self.stream),
                    job_id,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(())
        })
    }

    fn requeue_dead_letter_batch(
        &self,
        job_ids: &[JobId],
    ) -> BoxFuture<'_, Result<crate::BatchRequeueResult, QueueError>> {
        let ids = job_ids.to_vec();
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_requeue_dead_letter_batch(
                self.transport.as_ref(),
                leader,
                &QueueRequeueDeadLetterBatchRequest {
                    stream: wire_stream(&self.stream),
                    job_ids: ids,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err.to_string()));
            }
            Ok(crate::BatchRequeueResult {
                requeued: reply.requeued,
                failures: reply
                    .failures
                    .into_iter()
                    .map(|f| (f.job_id, QueueError::Backend(f.error)))
                    .collect(),
            })
        })
    }
}
