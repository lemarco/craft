use std::sync::Arc;

use trembita_net::{
    send_topic_ack, send_topic_lease, send_topic_metrics, send_topic_nack, send_topic_publish,
};
use trembita_proto::{
    NodeId, TopicAckReply, TopicAckRequest, TopicLeaseReply, TopicLeaseRequest, TopicMetricsReply,
    TopicMetricsRequest, TopicNackReply, TopicNackRequest, TopicPublishReply, TopicPublishRequest,
    WorkerId,
};

use crate::TopicLeaseId;

use super::TopicService;

impl TopicService {
    pub(in crate::topic_service) async fn handle_publish(
        &self,
        request: TopicPublishRequest,
    ) -> TopicPublishReply {
        if self.state.is_leader() {
            match self.local_topic(&request.topic) {
                Err(e) => TopicPublishReply {
                    event_id: 0,
                    error: Some(e),
                },
                Ok(topic) => match topic.publish_replicated(&request.payload).await {
                    Ok((id, ops)) => {
                        if let Err(e) = self.replicate_ops(&request.topic, &ops).await {
                            return TopicPublishReply {
                                event_id: 0,
                                error: Some(e),
                            };
                        }
                        TopicPublishReply {
                            event_id: id.0,
                            error: None,
                        }
                    }
                    Err(e) => TopicPublishReply {
                        event_id: 0,
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
                        send_topic_publish(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => TopicPublishReply {
                    event_id: 0,
                    error: Some(e),
                },
            }
        }
    }

    pub(in crate::topic_service) async fn handle_lease(
        &self,
        request: TopicLeaseRequest,
    ) -> TopicLeaseReply {
        let worker = WorkerId {
            node: NodeId(request.worker_node),
            instance: request.worker_instance,
        };
        if self.state.is_leader() {
            match self.local_topic(&request.topic) {
                Err(e) => TopicLeaseReply {
                    events: Vec::new(),
                    error: Some(e),
                },
                Ok(topic) => {
                    match topic
                        .lease_replicated(&request.subscription, worker, request.max as usize)
                        .await
                    {
                        Ok((events, ops)) => {
                            if let Err(e) = self.replicate_ops(&request.topic, &ops).await {
                                return TopicLeaseReply {
                                    events: Vec::new(),
                                    error: Some(e),
                                };
                            }
                            TopicLeaseReply {
                                events: events
                                    .into_iter()
                                    .map(|e| trembita_proto::TopicLeasedEventWire {
                                        lease_id: e.lease_id.0,
                                        event_id: e.event_id.0,
                                        payload: e.payload,
                                        attempts: e.attempts,
                                    })
                                    .collect(),
                                error: None,
                            }
                        }
                        Err(e) => TopicLeaseReply {
                            events: Vec::new(),
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
                        async move { send_topic_lease(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => TopicLeaseReply {
                    events: Vec::new(),
                    error: Some(e),
                },
            }
        }
    }

    pub(in crate::topic_service) async fn handle_ack(
        &self,
        request: TopicAckRequest,
    ) -> TopicAckReply {
        let worker = WorkerId {
            node: NodeId(request.worker_node),
            instance: request.worker_instance,
        };
        if self.state.is_leader() {
            match self.local_topic(&request.topic) {
                Err(e) => TopicAckReply { error: Some(e) },
                Ok(topic) => {
                    match topic
                        .ack_replicated(
                            &request.subscription,
                            worker,
                            TopicLeaseId(request.lease_id),
                        )
                        .await
                    {
                        Ok(ops) => {
                            if let Err(e) = self.replicate_ops(&request.topic, &ops).await {
                                return TopicAckReply { error: Some(e) };
                            }
                            TopicAckReply { error: None }
                        }
                        Err(e) => TopicAckReply {
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
                        async move { send_topic_ack(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => TopicAckReply { error: Some(e) },
            }
        }
    }

    pub(in crate::topic_service) async fn handle_nack(
        &self,
        request: TopicNackRequest,
    ) -> TopicNackReply {
        let worker = WorkerId {
            node: NodeId(request.worker_node),
            instance: request.worker_instance,
        };
        if self.state.is_leader() {
            match self.local_topic(&request.topic) {
                Err(e) => TopicNackReply { error: Some(e) },
                Ok(topic) => {
                    match topic
                        .nack_replicated(
                            &request.subscription,
                            worker,
                            TopicLeaseId(request.lease_id),
                        )
                        .await
                    {
                        Ok(ops) => {
                            if let Err(e) = self.replicate_ops(&request.topic, &ops).await {
                                return TopicNackReply { error: Some(e) };
                            }
                            TopicNackReply { error: None }
                        }
                        Err(e) => TopicNackReply {
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
                        async move { send_topic_nack(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => TopicNackReply { error: Some(e) },
            }
        }
    }

    pub(in crate::topic_service) async fn handle_metrics(
        &self,
        request: TopicMetricsRequest,
    ) -> TopicMetricsReply {
        if self.state.is_leader() {
            match self.local_topic(&request.topic) {
                Err(e) => TopicMetricsReply {
                    event_count: 0,
                    head: 0,
                    compact_head: 0,
                    oldest_event_age_ms: 0,
                    subscriptions: Vec::new(),
                    error: Some(e),
                },
                Ok(topic) => match topic.metrics().await {
                    Ok(m) => TopicMetricsReply {
                        event_count: m.event_count,
                        head: m.head,
                        compact_head: m.compact_head,
                        oldest_event_age_ms: u64::try_from(m.oldest_event_age.as_millis())
                            .unwrap_or(u64::MAX),
                        subscriptions: m
                            .subscriptions
                            .into_iter()
                            .map(|s| trembita_proto::TopicSubscriptionMetricsWire {
                                subscription: s.subscription,
                                cursor: s.cursor,
                                lag: s.lag,
                                pending: s.pending,
                                leased: s.leased,
                                retention_discards: s.retention_discards,
                            })
                            .collect(),
                        error: None,
                    },
                    Err(e) => TopicMetricsReply {
                        event_count: 0,
                        head: 0,
                        compact_head: 0,
                        oldest_event_age_ms: 0,
                        subscriptions: Vec::new(),
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
                        send_topic_metrics(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => TopicMetricsReply {
                    event_count: 0,
                    head: 0,
                    compact_head: 0,
                    oldest_event_age_ms: 0,
                    subscriptions: Vec::new(),
                    error: Some(e),
                },
            }
        }
    }
}
