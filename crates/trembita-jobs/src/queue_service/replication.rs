//! Leader forwarding and voter replication for queue mutations.

use std::sync::Arc;

use trembita_net::send_queue_replicate;
use trembita_net::transport::{BoxFuture, TransportError};
use trembita_proto::{NodeId, QueueReplicateReply, QueueReplicateRequest};
use trembita_runtime::{authorize_replicate_leader, fanout_replicate, replication_peers};

use super::wire::shard_stream_name;
use crate::{JobQueue, QueueReplicationOps, ShardedReplication};

use super::QueueService;

pub(super) const REPLICATE_NOT_LEADER: &str = "queue replicate rejected: caller is not raft leader";

impl QueueService {
    pub(super) fn local_stream(&self, stream: &str) -> Result<Arc<dyn JobQueue>, String> {
        self.streams
            .lock()
            .expect("poisoned")
            .get(stream)
            .cloned()
            .ok_or_else(|| format!("unknown queue stream {stream:?}"))
    }

    pub(super) async fn forward_leader<R>(
        &self,
        send: impl FnOnce(NodeId) -> BoxFuture<'static, Result<R, TransportError>>,
    ) -> Result<R, String> {
        let leader = self
            .state
            .leader_id()
            .ok_or_else(|| "no raft leader elected".to_string())?;
        send(leader)
            .await
            .map_err(|e| format!("forward to leader {leader:?} failed: {e}"))
    }

    /// Push `ops` to every other **reachable** voter in parallel; all must ack
    /// before the leader commits success to clients (failover-safe backlog).
    pub(super) async fn replicate_ops(
        &self,
        stream: &str,
        ops: &QueueReplicationOps,
    ) -> Result<(), String> {
        if ops.is_empty() {
            return Ok(());
        }
        let peers = replication_peers(self.state.as_ref(), self.node_id)?;
        let request = QueueReplicateRequest {
            stream: stream.to_string(),
            ops: ops.clone(),
            leader_id: self.node_id.0,
        };
        let transport = Arc::clone(&self.transport);
        fanout_replicate(&peers, move |peer| {
            let transport = Arc::clone(&transport);
            let request = request.clone();
            Box::pin(async move {
                let reply = send_queue_replicate(transport.as_ref(), peer, &request)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(err) = reply.error {
                    return Err(err);
                }
                Ok(())
            })
        })
        .await
    }

    pub(super) async fn replicate_sharded(
        &self,
        base: &str,
        reps: &[ShardedReplication],
    ) -> Result<(), String> {
        for rep in reps {
            if rep.ops.is_empty() {
                continue;
            }
            self.replicate_ops(&shard_stream_name(base, rep.shard), &rep.ops)
                .await?;
        }
        Ok(())
    }

    pub(super) fn authorize_replicate(&self, declared_leader: NodeId) -> Result<(), String> {
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
                            error: Some(e.to_string()),
                        };
                    }
                }
                QueueReplicateReply { error: None }
            }
        }
    }
}
