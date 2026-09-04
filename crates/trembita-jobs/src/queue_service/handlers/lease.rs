use std::sync::Arc;

use trembita_net::{send_queue_extend_lease, send_queue_lease};
use trembita_proto::{
    NodeId, QueueExtendLeaseReply, QueueExtendLeaseRequest, QueueLeaseReply, QueueLeaseRequest,
    QueueLeasedJobWire,
};

use crate::{LeaseId, WorkerId};

use super::super::QueueService;
use super::super::replication::rollback_local_ops;
use super::super::wire::shard_stream_name;

impl QueueService {
    #[allow(clippy::too_many_lines)]
    pub(in crate::queue_service) async fn handle_lease(
        &self,
        request: QueueLeaseRequest,
    ) -> QueueLeaseReply {
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
                            for rep in &reps {
                                if let Ok(queue) = self
                                    .local_stream(&shard_stream_name(&request.stream, rep.shard))
                                {
                                    rollback_local_ops(queue.as_ref(), &rep.ops).await;
                                }
                            }
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
                            error: Some(trembita_proto::ProductWireError::backend(e)),
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
                                rollback_local_ops(queue.as_ref(), &ops).await;
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
                            error: Some(trembita_proto::ProductWireError::backend(e)),
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
