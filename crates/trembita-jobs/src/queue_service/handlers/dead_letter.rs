use std::sync::Arc;

use trembita_net::send_queue_requeue_dead_letter_batch;
use trembita_proto::{
    QueueRequeueDeadLetterBatchReply, QueueRequeueDeadLetterBatchRequest, QueueRequeueFailureWire,
};

use crate::queue_prefetch::DEFAULT_QUEUE_BATCH_MAX;

use super::super::QueueService;
use super::super::wire::stream_key;

impl QueueService {
    pub(in crate::queue_service) async fn handle_requeue_dead_letter(
        &self,
        request: trembita_proto::QueueRequeueDeadLetterRequest,
    ) -> trembita_proto::QueueRequeueDeadLetterReply {
        if self.state.is_leader() {
            match self.local_stream(stream_key(&request.stream)) {
                Err(e) => trembita_proto::QueueRequeueDeadLetterReply { error: Some(e) },
                Ok(queue) => match queue.requeue_dead_letter_replicated(request.job_id).await {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(stream_key(&request.stream), &ops).await
                        {
                            trembita_proto::QueueRequeueDeadLetterReply { error: Some(e) }
                        } else {
                            trembita_proto::QueueRequeueDeadLetterReply { error: None }
                        }
                    }
                    Err(e) => trembita_proto::QueueRequeueDeadLetterReply {
                        error: Some(trembita_proto::ProductWireError::backend(e)),
                    },
                },
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

    #[allow(clippy::too_many_lines)]
    pub(in crate::queue_service) async fn handle_requeue_dead_letter_batch(
        &self,
        request: QueueRequeueDeadLetterBatchRequest,
    ) -> QueueRequeueDeadLetterBatchReply {
        if request.job_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
            return QueueRequeueDeadLetterBatchReply {
                requeued: Vec::new(),
                failures: Vec::new(),
                error: Some(trembita_proto::ProductWireError::backend(format!(
                    "batch size {} exceeds max {DEFAULT_QUEUE_BATCH_MAX}",
                    request.job_ids.len()
                ))),
            };
        }
        if self.state.is_leader() {
            let job_ids = &request.job_ids;
            if let Some(sharded) = self.sharded_stream(stream_key(&request.stream)) {
                match sharded
                    .requeue_dead_letter_batch_replicated_sharded(job_ids)
                    .await
                {
                    Ok((requeued, failures, reps)) => {
                        if let Err(e) = self
                            .replicate_sharded(stream_key(&request.stream), &reps)
                            .await
                        {
                            return QueueRequeueDeadLetterBatchReply {
                                requeued: Vec::new(),
                                failures: Vec::new(),
                                error: Some(e),
                            };
                        }
                        return QueueRequeueDeadLetterBatchReply {
                            requeued,
                            failures: failures
                                .into_iter()
                                .map(|(id, err)| QueueRequeueFailureWire {
                                    job_id: id,
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
                            error: Some(trembita_proto::ProductWireError::backend(e)),
                        };
                    }
                }
            }
            match self.local_stream(stream_key(&request.stream)) {
                Err(e) => QueueRequeueDeadLetterBatchReply {
                    requeued: Vec::new(),
                    failures: Vec::new(),
                    error: Some(e),
                },
                Ok(queue) => match queue.requeue_dead_letter_batch_replicated(job_ids).await {
                    Ok((requeued, failures, ops)) => {
                        if let Err(e) = self.replicate_ops(stream_key(&request.stream), &ops).await
                        {
                            return QueueRequeueDeadLetterBatchReply {
                                requeued: Vec::new(),
                                failures: Vec::new(),
                                error: Some(e),
                            };
                        }
                        QueueRequeueDeadLetterBatchReply {
                            requeued,
                            failures: failures
                                .into_iter()
                                .map(|(id, err)| QueueRequeueFailureWire {
                                    job_id: id,
                                    error: err.to_string(),
                                })
                                .collect(),
                            error: None,
                        }
                    }
                    Err(e) => QueueRequeueDeadLetterBatchReply {
                        requeued: Vec::new(),
                        failures: Vec::new(),
                        error: Some(trembita_proto::ProductWireError::backend(e)),
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
