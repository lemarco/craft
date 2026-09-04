use std::sync::Arc;
use std::time::Duration;

use trembita_net::transport::Transport;
use trembita_net::{
    send_topic_ack, send_topic_lease, send_topic_metrics, send_topic_nack, send_topic_publish,
};
use trembita_proto::{
    BoxFuture, NodeId, TopicAckRequest, TopicLeaseRequest, TopicMetricsRequest, TopicNackRequest,
    TopicPublishRequest, TopicReplicateOp, WorkerId,
};
use trembita_runtime::ClusterState;

use crate::{
    EventId, EventTopic, LeasedEvent, TopicError, TopicLeaseId, TopicMetrics, TopicReplicationOps,
    TopicSubscriptionDef,
};

fn replication_unsupported() -> TopicError {
    TopicError::Backend("cluster topic client does not apply replication locally".into())
}

/// Cluster-facing [`EventTopic`] that routes through the leader wire service.
pub struct ClusterEventTopic {
    topic: String,
    _node_id: NodeId,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
}

impl ClusterEventTopic {
    /// A topic client for `topic` (leases/acks attribute the worker you pass).
    #[must_use]
    pub fn new(
        topic: impl Into<String>,
        node_id: NodeId,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            topic: topic.into(),
            _node_id: node_id,
            state,
            transport,
        }
    }

    fn leader(&self) -> Result<NodeId, TopicError> {
        self.state.leader_id().ok_or(TopicError::NotLeader)
    }

    fn worker_ids(worker: WorkerId) -> (u64, u32) {
        (worker.node.0, worker.instance)
    }
}

impl EventTopic for ClusterEventTopic {
    fn publish_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(EventId, TopicReplicationOps), TopicError>> {
        let payload = payload.to_vec();
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_topic_publish(
                self.transport.as_ref(),
                leader,
                &TopicPublishRequest {
                    topic: self.topic.clone(),
                    payload,
                },
            )
            .await
            .map_err(|e| TopicError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(TopicError::Backend(err.to_string()));
            }
            Ok((EventId(reply.event_id), Vec::new()))
        })
    }

    fn apply_replicate<'a>(
        &'a self,
        _op: &'a TopicReplicateOp,
    ) -> BoxFuture<'a, Result<(), TopicError>> {
        Box::pin(async move { Err(replication_unsupported()) })
    }

    fn register_subscriptions<'a>(
        &'a self,
        _subs: &'a [TopicSubscriptionDef],
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move { Err(replication_unsupported()) })
    }

    fn lease_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<(Vec<LeasedEvent>, TopicReplicationOps), TopicError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let (worker_node, worker_instance) = Self::worker_ids(worker);
            let reply = send_topic_lease(
                self.transport.as_ref(),
                leader,
                &TopicLeaseRequest {
                    topic: self.topic.clone(),
                    subscription: subscription.to_string(),
                    worker_node,
                    worker_instance,
                    max: u32::try_from(max).unwrap_or(u32::MAX),
                },
            )
            .await
            .map_err(|e| TopicError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(TopicError::Backend(err.to_string()));
            }
            Ok((
                reply
                    .events
                    .into_iter()
                    .map(|e| LeasedEvent {
                        lease_id: TopicLeaseId(e.lease_id),
                        event_id: EventId(e.event_id),
                        payload: e.payload,
                        attempts: e.attempts,
                    })
                    .collect(),
                Vec::new(),
            ))
        })
    }

    fn ack_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let (worker_node, worker_instance) = Self::worker_ids(worker);
            let reply = send_topic_ack(
                self.transport.as_ref(),
                leader,
                &TopicAckRequest {
                    topic: self.topic.clone(),
                    subscription: subscription.to_string(),
                    worker_node,
                    worker_instance,
                    lease_id: lease_id.0,
                },
            )
            .await
            .map_err(|e| TopicError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(TopicError::Backend(err.to_string()));
            }
            Ok(Vec::new())
        })
    }

    fn nack_replicated<'a>(
        &'a self,
        subscription: &'a str,
        worker: WorkerId,
        lease_id: TopicLeaseId,
    ) -> BoxFuture<'a, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let (worker_node, worker_instance) = Self::worker_ids(worker);
            let reply = send_topic_nack(
                self.transport.as_ref(),
                leader,
                &TopicNackRequest {
                    topic: self.topic.clone(),
                    subscription: subscription.to_string(),
                    worker_node,
                    worker_instance,
                    lease_id: lease_id.0,
                },
            )
            .await
            .map_err(|e| TopicError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(TopicError::Backend(err.to_string()));
            }
            Ok(Vec::new())
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<TopicMetrics, TopicError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_topic_metrics(
                self.transport.as_ref(),
                leader,
                &TopicMetricsRequest {
                    topic: self.topic.clone(),
                },
            )
            .await
            .map_err(|e| TopicError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(TopicError::Backend(err.to_string()));
            }
            Ok(TopicMetrics {
                event_count: reply.event_count,
                head: reply.head,
                compact_head: reply.compact_head,
                oldest_event_age: Duration::from_millis(reply.oldest_event_age_ms),
                subscriptions: reply
                    .subscriptions
                    .into_iter()
                    .map(|s| crate::TopicSubscriptionMetrics {
                        subscription: s.subscription,
                        cursor: s.cursor,
                        lag: s.lag,
                        pending: s.pending,
                        leased: s.leased,
                        retention_discards: s.retention_discards,
                    })
                    .collect(),
            })
        })
    }

    fn enforce_retention_replicated(
        &self,
    ) -> BoxFuture<'_, Result<TopicReplicationOps, TopicError>> {
        Box::pin(async move { Err(replication_unsupported()) })
    }
}
