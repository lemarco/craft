//! Cluster-facing [`JobQueue`] client routing through the leader wire service.

use std::sync::Arc;

use trembita_net::transport::{BoxFuture, Transport};
use trembita_net::{
    send_queue_ack_batch, send_queue_enqueue, send_queue_enqueue_batch, send_queue_extend_lease,
    send_queue_job_status, send_queue_lease, send_queue_list_jobs, send_queue_metrics,
    send_queue_nack, send_queue_requeue_dead_letter_batch,
};
use trembita_proto::{
    NodeId, QueueAckBatchRequest, QueueBatchEnqueueJob, QueueEnqueueBatchRequest,
    QueueEnqueueRequest, QueueExtendLeaseRequest, QueueJobLifecycleWire, QueueJobStatusRequest,
    QueueLeaseRequest, QueueListJobsRequest, QueueMetricsRequest, QueueNackRequest,
    QueueReplicateOp, QueueRequeueDeadLetterBatchRequest,
};
use trembita_runtime::{ClusterState, NOT_LEADER_REASON};

use crate::{
    EnqueueOptions, JobId, JobLifecycle, JobListFilter, JobQueue, JobStatus, LeaseId, LeasedJob,
    QueueError, QueueMetrics, QueueReplicationOps, WorkerId,
};

fn replication_unsupported() -> QueueError {
    QueueError::Backend("cluster queue client does not apply replication locally".into())
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
                    stream: self.stream.clone(),
                    payload: payload.to_vec(),
                    priority: options.priority,
                    not_before_ms: options.not_before_ms.unwrap_or(0),
                    shard_key: options.shard_key.clone(),
                    dedup_key: options.dedup_key.clone(),
                    max_attempts: options.max_attempts.unwrap_or(self.default_max_attempts),
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
                    not_before_ms: options.not_before_ms.unwrap_or(0),
                    shard_key: options.shard_key.clone(),
                    dedup_key: options.dedup_key.clone(),
                    max_attempts: options.max_attempts.unwrap_or(self.default_max_attempts),
                })
                .collect();
            let reply = send_queue_enqueue_batch(
                self.transport.as_ref(),
                leader,
                &QueueEnqueueBatchRequest {
                    stream: self.stream.clone(),
                    jobs: wire_jobs,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok((reply.job_ids.into_iter().map(JobId).collect(), Vec::new()))
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
                    attempts: j.attempts,
                    dedup_key: j.dedup_key,
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
                    stream: self.stream.clone(),
                    worker_node: worker.node.0,
                    worker_instance: worker.instance,
                    lease_ids: lease_ids.iter().map(|id| id.0).collect(),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
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
                    QueueJobLifecycleWire::DeadLetter => JobLifecycle::DeadLetter,
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
                attempts: reply.attempts,
                max_attempts: reply.max_attempts,
                dedup_key: reply.dedup_key.clone(),
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
                    stream: self.stream.clone(),
                    lifecycle: filter.lifecycle.map(|l| match l {
                        JobLifecycle::Pending => QueueJobLifecycleWire::Pending,
                        JobLifecycle::Leased => QueueJobLifecycleWire::Leased,
                        JobLifecycle::Delayed => QueueJobLifecycleWire::Delayed,
                        JobLifecycle::DeadLetter => QueueJobLifecycleWire::DeadLetter,
                    }),
                    min_attempts: filter.min_attempts,
                    dedup_key: filter.dedup_key.clone(),
                    limit: u32::try_from(filter.effective_limit()).unwrap_or(u32::MAX),
                    after_job_id: filter.after_job_id.map_or(0, |id| id.0),
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok(crate::JobListPage {
                jobs: reply
                    .jobs
                    .into_iter()
                    .map(|entry| JobStatus {
                        job_id: JobId(entry.job_id),
                        lifecycle: match entry.lifecycle {
                            QueueJobLifecycleWire::Pending => JobLifecycle::Pending,
                            QueueJobLifecycleWire::Leased => JobLifecycle::Leased,
                            QueueJobLifecycleWire::Delayed => JobLifecycle::Delayed,
                            QueueJobLifecycleWire::DeadLetter => JobLifecycle::DeadLetter,
                        },
                        payload_len: entry.payload_len,
                        priority: entry.priority,
                        leased_by: match (entry.leased_worker_node, entry.leased_worker_instance) {
                            (Some(node), Some(instance)) => Some(WorkerId {
                                node: NodeId(node),
                                instance,
                            }),
                            _ => None,
                        },
                        attempts: entry.attempts,
                        max_attempts: entry.max_attempts,
                        dedup_key: entry.dedup_key,
                    })
                    .collect(),
                has_more: reply.has_more,
            })
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
                    stream: self.stream.clone(),
                    job_id: job_id.0,
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

    fn requeue_dead_letter_batch(
        &self,
        job_ids: &[JobId],
    ) -> BoxFuture<'_, Result<crate::BatchRequeueResult, QueueError>> {
        let ids: Vec<u64> = job_ids.iter().map(|id| id.0).collect();
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_queue_requeue_dead_letter_batch(
                self.transport.as_ref(),
                leader,
                &QueueRequeueDeadLetterBatchRequest {
                    stream: self.stream.clone(),
                    job_ids: ids,
                },
            )
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(QueueError::Backend(err));
            }
            Ok(crate::BatchRequeueResult {
                requeued: reply.requeued.into_iter().map(JobId).collect(),
                failures: reply
                    .failures
                    .into_iter()
                    .map(|f| (JobId(f.job_id), QueueError::Backend(f.error)))
                    .collect(),
            })
        })
    }
}
