use std::sync::Arc;

use trembita_net::{send_queue_extend_lease, send_queue_lease};
use trembita_proto::{
    QueueExtendLeaseReply, QueueExtendLeaseRequest, QueueLeaseReply, QueueLeaseRequest,
    QueueLeasedJobWire,
};

use super::super::QueueService;
use super::super::replication::rollback_local_ops;
use super::super::wire::{shard_stream_name, stream_key, worker_from_wire};

impl QueueService {
    #[allow(clippy::too_many_lines)]
    pub(in crate::queue_service) async fn handle_lease(
        &self,
        request: QueueLeaseRequest,
    ) -> QueueLeaseReply {
        if self.state.is_leader() {
            let worker = worker_from_wire(request.worker_node, request.worker_instance);
            if let Some(sharded) = self.sharded_stream(stream_key(&request.stream)) {
                match self
                    .lease_sharded_with_prefetch(
                        stream_key(&request.stream),
                        &sharded,
                        worker,
                        request.max,
                    )
                    .await
                {
                    Ok((jobs, reps)) => {
                        if let Err(e) = self
                            .replicate_sharded(stream_key(&request.stream), &reps)
                            .await
                        {
                            for rep in &reps {
                                if let Ok(queue) = self.local_stream(&shard_stream_name(
                                    stream_key(&request.stream),
                                    rep.shard,
                                )) {
                                    rollback_local_ops(queue.as_ref(), &rep.ops).await;
                                }
                            }
                            return QueueLeaseReply {
                                jobs: Vec::new(),
                                error: Some(e),
                            };
                        }
                        self.emit_backlog_settle_for_sharded_reps(
                            stream_key(&request.stream),
                            &reps,
                            "reclaim",
                        )
                        .await;
                        for j in &jobs {
                            self.emit_leased(
                                stream_key(&request.stream),
                                j.job_id.0,
                                j.lease_id.0,
                                request.worker_node.0,
                                request.worker_instance,
                                j.attempts,
                            );
                        }
                        return QueueLeaseReply {
                            jobs: jobs
                                .into_iter()
                                .map(|j| QueueLeasedJobWire {
                                    lease_id: j.lease_id,
                                    job_id: j.job_id,
                                    payload: j.payload,
                                    attempts: j.attempts,
                                    dedup_key: j
                                        .dedup_key
                                        .and_then(|k| trembita_proto::DedupKey::try_new(k).ok()),
                                })
                                .collect(),
                            error: None,
                        };
                    }
                    Err(e) => {
                        return QueueLeaseReply {
                            jobs: Vec::new(),
                            error: Some(trembita_proto::ProductWireError::backend(e)),
                        };
                    }
                }
            }
            match self.local_stream(stream_key(&request.stream)) {
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
                        .get(stream_key(&request.stream))
                        .cloned();
                    let lease_result = if let Some(redb) = redb {
                        self.lease_redb_with_prefetch(
                            stream_key(&request.stream),
                            &redb,
                            worker,
                            request.max,
                        )
                        .await
                    } else {
                        queue.lease_replicated(worker, request.max).await
                    };
                    match lease_result {
                        Ok((jobs, ops)) => {
                            if let Err(e) =
                                self.replicate_ops(stream_key(&request.stream), &ops).await
                            {
                                rollback_local_ops(queue.as_ref(), &ops).await;
                                return QueueLeaseReply {
                                    jobs: Vec::new(),
                                    error: Some(e),
                                };
                            }
                            self.emit_backlog_settle_for_terminal_ops(
                                stream_key(&request.stream),
                                queue.as_ref(),
                                &ops,
                                "reclaim",
                            )
                            .await;
                            for j in &jobs {
                                self.emit_leased(
                                    stream_key(&request.stream),
                                    j.job_id.0,
                                    j.lease_id.0,
                                    request.worker_node.0,
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
                            error: Some(trembita_proto::ProductWireError::backend(e)),
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

    pub(in crate::queue_service) async fn handle_extend_lease(
        &self,
        request: QueueExtendLeaseRequest,
    ) -> QueueExtendLeaseReply {
        if self.state.is_leader() {
            let worker = worker_from_wire(request.worker_node, request.worker_instance);
            if let Some(sharded) = self.sharded_stream(stream_key(&request.stream)) {
                match sharded
                    .extend_lease_replicated_sharded(worker, request.lease_id)
                    .await
                {
                    Ok(rep) => {
                        if let Err(e) = self
                            .replicate_sharded(stream_key(&request.stream), &[rep])
                            .await
                        {
                            return QueueExtendLeaseReply { error: Some(e) };
                        }
                        return QueueExtendLeaseReply { error: None };
                    }
                    Err(e) => {
                        return QueueExtendLeaseReply {
                            error: Some(trembita_proto::ProductWireError::backend(e)),
                        };
                    }
                }
            }
            match self.local_stream(stream_key(&request.stream)) {
                Err(e) => QueueExtendLeaseReply { error: Some(e) },
                Ok(queue) => {
                    match queue
                        .extend_lease_replicated(worker, request.lease_id)
                        .await
                    {
                        Ok(ops) => {
                            if let Err(e) =
                                self.replicate_ops(stream_key(&request.stream), &ops).await
                            {
                                return QueueExtendLeaseReply { error: Some(e) };
                            }
                            QueueExtendLeaseReply { error: None }
                        }
                        Err(e) => QueueExtendLeaseReply {
                            error: Some(trembita_proto::ProductWireError::backend(e)),
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
}
