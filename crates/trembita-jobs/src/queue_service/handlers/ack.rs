use std::sync::Arc;

use trembita_net::{send_queue_ack, send_queue_ack_batch, send_queue_nack};
use trembita_proto::{
    QueueAckBatchReply, QueueAckBatchRequest, QueueAckReply, QueueAckRequest, QueueNackReply,
    QueueNackRequest,
};

use crate::external_backlog::BacklogSettleOutcome;
use crate::queue_prefetch::DEFAULT_QUEUE_BATCH_MAX;

use super::super::QueueService;
use super::super::wire::{stream_key, worker_from_wire};

impl QueueService {
    pub(in crate::queue_service) async fn handle_ack_batch(
        &self,
        request: QueueAckBatchRequest,
    ) -> QueueAckBatchReply {
        if request.lease_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
            return QueueAckBatchReply {
                error: Some(trembita_proto::ProductWireError::backend(format!(
                    "batch size {} exceeds max {}",
                    request.lease_ids.len(),
                    DEFAULT_QUEUE_BATCH_MAX
                ))),
            };
        }
        if self.state.is_leader() {
            let worker = worker_from_wire(request.worker_node, request.worker_instance);
            let lease_ids = &request.lease_ids;
            let mut lease_metas = Vec::with_capacity(lease_ids.len());
            for lease_id in lease_ids {
                lease_metas.push(
                    self.peek_lease_meta(stream_key(&request.stream), *lease_id)
                        .await,
                );
            }
            if let Some(sharded) = self.sharded_stream(stream_key(&request.stream)) {
                match sharded
                    .ack_batch_replicated_sharded(worker, lease_ids)
                    .await
                {
                    Ok(reps) => {
                        if let Err(e) = self
                            .replicate_sharded(stream_key(&request.stream), &reps)
                            .await
                        {
                            return QueueAckBatchReply { error: Some(e) };
                        }
                        self.evict_prefetch_sharded_acks(stream_key(&request.stream), &reps);
                        for (lease_id, (dedup_key, attempts)) in
                            request.lease_ids.iter().zip(lease_metas)
                        {
                            self.emit_acked(
                                stream_key(&request.stream),
                                lease_id.0,
                                request.worker_node.0,
                            );
                            self.emit_backlog_settle(
                                stream_key(&request.stream),
                                dedup_key,
                                BacklogSettleOutcome::Done { attempts },
                            );
                        }
                        return QueueAckBatchReply { error: None };
                    }
                    Err(e) => {
                        return QueueAckBatchReply {
                            error: Some(trembita_proto::ProductWireError::backend(e)),
                        };
                    }
                }
            }
            match self.local_stream(stream_key(&request.stream)) {
                Err(e) => QueueAckBatchReply { error: Some(e) },
                Ok(queue) => match queue.ack_batch_replicated(worker, lease_ids).await {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(stream_key(&request.stream), &ops).await
                        {
                            QueueAckBatchReply { error: Some(e) }
                        } else {
                            self.evict_prefetch_ack_ops(stream_key(&request.stream), &ops);
                            for (lease_id, (dedup_key, attempts)) in
                                request.lease_ids.iter().zip(lease_metas)
                            {
                                self.emit_acked(
                                    stream_key(&request.stream),
                                    lease_id.0,
                                    request.worker_node.0,
                                );
                                self.emit_backlog_settle(
                                    stream_key(&request.stream),
                                    dedup_key,
                                    BacklogSettleOutcome::Done { attempts },
                                );
                            }
                            QueueAckBatchReply { error: None }
                        }
                    }
                    Err(e) => QueueAckBatchReply {
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
            let worker = worker_from_wire(request.worker_node, request.worker_instance);
            let (dedup_key, attempts) = self
                .peek_lease_meta(stream_key(&request.stream), request.lease_id)
                .await;
            if let Some(sharded) = self.sharded_stream(stream_key(&request.stream)) {
                match sharded
                    .ack_replicated_sharded(worker, request.lease_id)
                    .await
                {
                    Ok(rep) => {
                        if let Err(e) = self
                            .replicate_sharded(
                                stream_key(&request.stream),
                                std::slice::from_ref(&rep),
                            )
                            .await
                        {
                            return QueueAckReply { error: Some(e) };
                        }
                        self.evict_prefetch_sharded_acks(
                            stream_key(&request.stream),
                            std::slice::from_ref(&rep),
                        );
                        self.emit_acked(
                            stream_key(&request.stream),
                            request.lease_id.0,
                            request.worker_node.0,
                        );
                        self.emit_backlog_settle(
                            stream_key(&request.stream),
                            dedup_key.clone(),
                            BacklogSettleOutcome::Done { attempts },
                        );
                        return QueueAckReply { error: None };
                    }
                    Err(e) => {
                        return QueueAckReply {
                            error: Some(trembita_proto::ProductWireError::backend(e)),
                        };
                    }
                }
            }
            match self.local_stream(stream_key(&request.stream)) {
                Err(e) => QueueAckReply { error: Some(e) },
                Ok(queue) => match queue.ack_replicated(worker, request.lease_id).await {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(stream_key(&request.stream), &ops).await
                        {
                            return QueueAckReply { error: Some(e) };
                        }
                        self.evict_prefetch_ack_ops(stream_key(&request.stream), &ops);
                        self.emit_acked(
                            stream_key(&request.stream),
                            request.lease_id.0,
                            request.worker_node.0,
                        );
                        self.emit_backlog_settle(
                            stream_key(&request.stream),
                            dedup_key,
                            BacklogSettleOutcome::Done { attempts },
                        );
                        QueueAckReply { error: None }
                    }
                    Err(e) => QueueAckReply {
                        error: Some(trembita_proto::ProductWireError::backend(e)),
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
            let worker = worker_from_wire(request.worker_node, request.worker_instance);
            if let Some(sharded) = self.sharded_stream(stream_key(&request.stream)) {
                match sharded
                    .nack_replicated_sharded(worker, request.lease_id)
                    .await
                {
                    Ok(rep) => {
                        self.emit_backlog_settle_for_sharded_reps(
                            stream_key(&request.stream),
                            std::slice::from_ref(&rep),
                            "nack",
                        )
                        .await;
                        if let Err(e) = self
                            .replicate_sharded(stream_key(&request.stream), &[rep])
                            .await
                        {
                            return QueueNackReply { error: Some(e) };
                        }
                        return QueueNackReply { error: None };
                    }
                    Err(e) => {
                        return QueueNackReply {
                            error: Some(trembita_proto::ProductWireError::backend(e)),
                        };
                    }
                }
            }
            match self.local_stream(stream_key(&request.stream)) {
                Err(e) => QueueNackReply { error: Some(e) },
                Ok(queue) => match queue.nack_replicated(worker, request.lease_id).await {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(stream_key(&request.stream), &ops).await
                        {
                            return QueueNackReply { error: Some(e) };
                        }
                        self.emit_backlog_settle_for_terminal_ops(
                            stream_key(&request.stream),
                            queue.as_ref(),
                            &ops,
                            "nack",
                        )
                        .await;
                        QueueNackReply { error: None }
                    }
                    Err(e) => QueueNackReply {
                        error: Some(trembita_proto::ProductWireError::backend(e)),
                    },
                },
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
