use std::sync::Arc;

use trembita_net::{send_queue_enqueue, send_queue_enqueue_batch};
use trembita_proto::{
    QueueEnqueueBatchReply, QueueEnqueueBatchRequest, QueueEnqueueReply, QueueEnqueueRequest,
};

use super::super::wire::{enqueue_options_from_batch_job, enqueue_options_from_request};
use crate::EnqueueOptions;
use crate::queue_prefetch::DEFAULT_QUEUE_BATCH_MAX;

use super::super::QueueService;

impl QueueService {
    pub(in crate::queue_service) async fn handle_enqueue(
        &self,
        request: QueueEnqueueRequest,
    ) -> QueueEnqueueReply {
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
    pub(in crate::queue_service) async fn handle_enqueue_batch(
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
}
