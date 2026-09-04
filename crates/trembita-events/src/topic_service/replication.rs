//! Leader forwarding and voter replication for topic mutations.

use std::sync::Arc;

use trembita_net::send_topic_replicate;
use trembita_net::transport::{BoxFuture, TransportError};
use trembita_proto::{NodeId, ProductWireError, TopicReplicateReply, TopicReplicateRequest};
use trembita_runtime::{
    authorize_replicate_leader, fanout_product_replicate, forward_to_leader, replicate_reply_err,
};

use crate::{EventTopic, TopicReplicationOps};

use super::TopicService;

pub(super) const REPLICATE_NOT_LEADER: &str = "topic replicate rejected: caller is not raft leader";

impl TopicService {
    pub(super) fn local_topic(&self, name: &str) -> Result<Arc<dyn EventTopic>, ProductWireError> {
        self.topics
            .lock()
            .expect("poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| ProductWireError::UnknownTopic {
                topic: name.to_string(),
            })
    }

    pub(super) async fn forward_leader<R>(
        &self,
        send: impl FnOnce(NodeId) -> BoxFuture<'static, Result<R, TransportError>>,
    ) -> Result<R, ProductWireError> {
        forward_to_leader(self.state.as_ref(), send).await
    }

    pub(super) async fn replicate_ops(
        &self,
        topic: &str,
        ops: &TopicReplicationOps,
    ) -> Result<(), ProductWireError> {
        if ops.is_empty() {
            return Ok(());
        }
        let request = TopicReplicateRequest {
            topic: topic.to_string(),
            ops: ops.clone(),
            leader_id: self.node_id.0,
        };
        let transport = Arc::clone(&self.transport);
        fanout_product_replicate(self.state.as_ref(), self.node_id, move |peer| {
            let transport = Arc::clone(&transport);
            let request = request.clone();
            Box::pin(async move {
                let reply = send_topic_replicate(transport.as_ref(), peer, &request)
                    .await
                    .map_err(|e| e.to_string())?;
                replicate_reply_err(reply.error).map_err(|e| e.to_string())
            })
        })
        .await
    }

    pub(super) fn authorize_replicate(
        &self,
        declared_leader: NodeId,
    ) -> Result<(), ProductWireError> {
        authorize_replicate_leader(self.state.as_ref(), declared_leader, REPLICATE_NOT_LEADER)
    }

    pub(in crate::topic_service) async fn handle_replicate(
        &self,
        _from: Option<NodeId>,
        request: TopicReplicateRequest,
    ) -> TopicReplicateReply {
        if let Err(e) = self.authorize_replicate(NodeId(request.leader_id)) {
            return TopicReplicateReply { error: Some(e) };
        }
        match self.local_topic(&request.topic) {
            Err(e) => TopicReplicateReply { error: Some(e) },
            Ok(topic) => {
                for op in &request.ops {
                    if let Err(e) = topic.apply_replicate(op).await {
                        return TopicReplicateReply {
                            error: Some(ProductWireError::ReplicateApply(e.to_string())),
                        };
                    }
                }
                TopicReplicateReply { error: None }
            }
        }
    }
}
