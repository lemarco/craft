use std::sync::Arc;

use trembita_net::{send_queue_ack, send_queue_ack_batch, send_queue_nack};
use trembita_proto::{
    NodeId, QueueAckBatchReply, QueueAckBatchRequest, QueueAckReply, QueueAckRequest,
    QueueNackReply, QueueNackRequest,
};

use crate::external_backlog::BacklogSettleOutcome;
use crate::queue_prefetch::DEFAULT_QUEUE_BATCH_MAX;
use crate::{LeaseId, WorkerId};

use super::super::QueueService;

impl QueueService {
    pub(in crate::queue_service) async fn handle_ack_batch(
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
    pub(in crate::queue_service) async fn handle_ack(
        &self,
        request: QueueAckRequest,
    ) -> QueueAckReply {
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

    pub(in crate::queue_service) async fn handle_nack(
        &self,
        request: QueueNackRequest,
    ) -> QueueNackReply {
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
}
