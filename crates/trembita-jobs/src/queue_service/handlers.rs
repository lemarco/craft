//! Queue wire route handlers (enqueue, lease, ack, metrics, …).

use std::sync::Arc;

use trembita_net::transport::{BoxFuture, TransportError};
use trembita_net::{
    send_queue_ack, send_queue_ack_batch, send_queue_enqueue, send_queue_enqueue_batch,
    send_queue_extend_lease, send_queue_job_status, send_queue_lease, send_queue_list_jobs,
    send_queue_metrics, send_queue_nack, send_queue_requeue_dead_letter_batch,
};
use trembita_proto::{
    NodeId, QueueAckBatchReply, QueueAckBatchRequest, QueueAckReply, QueueAckRequest,
    QueueEnqueueBatchReply, QueueEnqueueBatchRequest, QueueEnqueueReply, QueueEnqueueRequest,
    QueueExtendLeaseReply, QueueExtendLeaseRequest, QueueJobStatusReply, QueueJobStatusRequest,
    QueueLeaseReply, QueueLeaseRequest, QueueLeasedJobWire, QueueListJobsReply,
    QueueListJobsRequest, QueueMetricsReply, QueueMetricsRequest, QueueNackReply, QueueNackRequest,
    QueueRequeueDeadLetterBatchReply, QueueRequeueDeadLetterBatchRequest, QueueRequeueFailureWire,
};

use super::wire::{
    enqueue_options_from_batch_job, enqueue_options_from_request, filter_from_list_request,
    job_status_to_list_entry, job_status_to_reply,
};
use crate::external_backlog::BacklogSettleOutcome;
use crate::queue_prefetch::DEFAULT_QUEUE_BATCH_MAX;
use crate::sharded_queue::{decode_global_id, encode_global_id};
use crate::{
    EnqueueOptions, JobId, JobLifecycle, JobListFilter, JobQueue, JobStatus, LeaseId, LeasedJob,
    QueueError, WorkerId,
};

use super::QueueService;

impl QueueService {
    pub(super) async fn handle_enqueue(&self, request: QueueEnqueueRequest) -> QueueEnqueueReply {
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
    pub(super) async fn handle_enqueue_batch(
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

    pub(super) async fn handle_ack_batch(
        &self,
        request: QueueAckBatchRequest,
    ) -> QueueAckBatchReply {
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
    pub(super) async fn handle_lease(&self, request: QueueLeaseRequest) -> QueueLeaseReply {
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
                        .registry
                        .lock()
                        .expect("poisoned")
                        .redb_streams
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

    pub(super) async fn handle_ack(&self, request: QueueAckRequest) -> QueueAckReply {
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

    pub(super) async fn handle_nack(&self, request: QueueNackRequest) -> QueueNackReply {
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

    pub(super) async fn handle_extend_lease(
        &self,
        request: QueueExtendLeaseRequest,
    ) -> QueueExtendLeaseReply {
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

    pub(super) async fn handle_metrics(&self, request: QueueMetricsRequest) -> QueueMetricsReply {
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

    pub(super) async fn handle_job_status(
        &self,
        request: QueueJobStatusRequest,
    ) -> QueueJobStatusReply {
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

    pub(super) async fn handle_requeue_dead_letter(
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

    pub(super) async fn handle_list_jobs(
        &self,
        request: QueueListJobsRequest,
    ) -> QueueListJobsReply {
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
    pub(super) async fn handle_requeue_dead_letter_batch(
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
