//! Leader forwarding and voter replication for queue mutations.

use std::sync::Arc;

use trembita_net::send_queue_replicate;
use trembita_net::transport::{BoxFuture, TransportError};
use trembita_proto::{
    NodeId, ProductWireError, QueueReplicateOp, QueueReplicateReply, QueueReplicateRequest,
};
use trembita_runtime::{
    authorize_replicate_leader, fanout_product_replicate, forward_to_leader, replicate_reply_err,
};

use super::wire::shard_stream_name;
use crate::{JobQueue, QueueReplicationOps, ShardedReplication};

use super::QueueService;

pub(super) const REPLICATE_NOT_LEADER: &str = "queue replicate rejected: caller is not raft leader";

/// Undo local queue mutations when voter replication fails after the leader applied ops.
pub(super) async fn rollback_local_ops(queue: &dyn JobQueue, ops: &QueueReplicationOps) {
    for op in ops {
        let Some(rollback) = local_rollback_op(op) else {
            continue;
        };
        let _ = queue.apply_replicate(&rollback).await;
    }
}

fn local_rollback_op(op: &QueueReplicateOp) -> Option<QueueReplicateOp> {
    match op {
        QueueReplicateOp::Lease {
            lease_id, job_id, ..
        } => Some(QueueReplicateOp::Reclaim {
            lease_id: *lease_id,
            job_id: *job_id,
            attempts: 0,
            dead_letter: false,
            not_before_ms: 0,
        }),
        _ => None,
    }
}

impl QueueService {
    pub(super) fn local_stream(&self, stream: &str) -> Result<Arc<dyn JobQueue>, ProductWireError> {
        self.registry
            .lock()
            .expect("poisoned")
            .streams
            .get(stream)
            .cloned()
            .ok_or_else(|| ProductWireError::UnknownStream {
                stream: stream.to_string(),
            })
    }

    pub(super) async fn forward_leader<R>(
        &self,
        send: impl FnOnce(NodeId) -> BoxFuture<'static, Result<R, TransportError>>,
    ) -> Result<R, ProductWireError> {
        forward_to_leader(self.state.as_ref(), send).await
    }

    /// Push `ops` to every other **reachable** voter in parallel; all must ack
    /// before the leader commits success to clients (failover-safe backlog).
    pub(super) async fn replicate_ops(
        &self,
        stream: &str,
        ops: &QueueReplicationOps,
    ) -> Result<(), ProductWireError> {
        if ops.is_empty() {
            return Ok(());
        }
        let request = QueueReplicateRequest {
            stream: stream.to_string(),
            ops: ops.clone(),
            leader_id: self.node_id.0,
        };
        let transport = Arc::clone(&self.transport);
        fanout_product_replicate(self.state.as_ref(), self.node_id, move |peer| {
            let transport = Arc::clone(&transport);
            let request = request.clone();
            Box::pin(async move {
                let reply = send_queue_replicate(transport.as_ref(), peer, &request)
                    .await
                    .map_err(|e| e.to_string())?;
                replicate_reply_err(reply.error).map_err(|e| e.to_string())
            })
        })
        .await
    }

    pub(super) async fn replicate_sharded(
        &self,
        base: &str,
        reps: &[ShardedReplication],
    ) -> Result<(), ProductWireError> {
        for rep in reps {
            if rep.ops.is_empty() {
                continue;
            }
            self.replicate_ops(&shard_stream_name(base, rep.shard), &rep.ops)
                .await?;
        }
        Ok(())
    }

    pub(super) fn authorize_replicate(
        &self,
        declared_leader: NodeId,
    ) -> Result<(), ProductWireError> {
        authorize_replicate_leader(self.state.as_ref(), declared_leader, REPLICATE_NOT_LEADER)
    }

    pub(super) async fn handle_replicate(
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
                            error: Some(ProductWireError::ReplicateApply(e.to_string())),
                        };
                    }
                }
                QueueReplicateReply { error: None }
            }
        }
    }
}
