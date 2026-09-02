//! Leader-gated topic wire service ([event-topics](../../../docs/decisions/event-topics.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::task::JoinSet;

use trembita_net::transport::{Body, BoxFuture, Transport, TransportError};
use trembita_net::{
    Route, decode_body, encode_body, send_topic_ack, send_topic_lease, send_topic_metrics,
    send_topic_nack, send_topic_publish, send_topic_replicate,
};
use trembita_proto::{
    NodeId, TopicAckReply, TopicAckRequest, TopicLeaseReply, TopicLeaseRequest, TopicMetricsReply,
    TopicMetricsRequest, TopicNackReply, TopicNackRequest, TopicPublishReply, TopicPublishRequest,
    TopicReplicateOp, TopicReplicateReply, TopicReplicateRequest,
};

use trembita_jobs::WorkerId;
use trembita_runtime::{ClusterState, NOT_LEADER_REASON};

use crate::{
    EventId, EventTopic, LeasedEvent, RedbEventTopic, TopicError, TopicLeaseId, TopicMetrics,
    TopicReplicationOps, TopicSubscriptionDef,
};

const REPLICATE_NOT_LEADER: &str = "topic replicate rejected: caller is not raft leader";

/// Serves `/raft/v1/topic/*` on the leader; followers transparently forward.
pub struct TopicService {
    node_id: NodeId,
    topics: Mutex<HashMap<String, Arc<dyn EventTopic>>>,
    redb_topics: Mutex<HashMap<String, Arc<RedbEventTopic>>>,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
}

impl TopicService {
    /// Empty service; register topics before accepting traffic.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            topics: Mutex::new(HashMap::new()),
            redb_topics: Mutex::new(HashMap::new()),
            state,
            transport,
        }
    }

    /// Register a local redb-backed topic.
    ///
    /// Call [`Self::bootstrap_subscriptions`] after all topics are registered.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_redb_topic(&self, name: impl Into<String>, topic: &Arc<RedbEventTopic>) {
        let name = name.into();
        self.topics
            .lock()
            .expect("poisoned")
            .insert(name.clone(), Arc::clone(topic) as Arc<dyn EventTopic>);
        self.redb_topics
            .lock()
            .expect("poisoned")
            .insert(name, Arc::clone(topic));
    }

    /// Register subscriptions on an already-open topic (leader boot path).
    ///
    /// # Errors
    /// Propagates topic failures as strings.
    pub async fn bootstrap_subscriptions(
        &self,
        name: &str,
        subscriptions: &[TopicSubscriptionDef],
    ) -> Result<(), String> {
        if subscriptions.is_empty() {
            return Ok(());
        }
        let topic = self.local_topic(name)?;
        let ops = topic
            .register_subscriptions(subscriptions)
            .await
            .map_err(|e| e.to_string())?;
        self.replicate_ops(name, &ops).await?;
        Ok(())
    }

    fn local_topic(&self, name: &str) -> Result<Arc<dyn EventTopic>, String> {
        self.topics
            .lock()
            .expect("poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown topic {name:?}"))
    }

    async fn forward_leader<R>(
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

    async fn replicate_ops(&self, topic: &str, ops: &TopicReplicationOps) -> Result<(), String> {
        if ops.is_empty() {
            return Ok(());
        }
        let peers: Vec<NodeId> = self
            .state
            .reachable_nodes()
            .into_iter()
            .filter(|id| *id != self.node_id)
            .collect();
        if peers.is_empty() {
            return Ok(());
        }
        let request = TopicReplicateRequest {
            topic: topic.to_string(),
            ops: ops.clone(),
            leader_id: self.node_id.0,
        };
        let mut set = JoinSet::new();
        for peer in peers {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            set.spawn(async move {
                let reply = send_topic_replicate(transport.as_ref(), peer, &request)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(err) = reply.error {
                    return Err(err);
                }
                Ok(())
            });
        }
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    fn authorize_replicate(&self, declared_leader: NodeId) -> Result<(), String> {
        let Some(leader) = self.state.leader_id() else {
            return Err("no raft leader elected".to_string());
        };
        if declared_leader != leader {
            return Err(REPLICATE_NOT_LEADER.to_string());
        }
        Ok(())
    }

    async fn handle_replicate(
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
                            error: Some(e.to_string()),
                        };
                    }
                }
                TopicReplicateReply { error: None }
            }
        }
    }

    async fn handle_publish(&self, request: TopicPublishRequest) -> TopicPublishReply {
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

    async fn handle_lease(&self, request: TopicLeaseRequest) -> TopicLeaseReply {
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

    async fn handle_ack(&self, request: TopicAckRequest) -> TopicAckReply {
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

    async fn handle_nack(&self, request: TopicNackRequest) -> TopicNackReply {
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

    async fn handle_metrics(&self, request: TopicMetricsRequest) -> TopicMetricsReply {
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

    /// Enforce retention on every registered redb topic (leader-only ticker).
    ///
    /// # Errors
    ///
    /// Returns an error string when topic lookup, retention, or replication fails.
    ///
    /// # Panics
    ///
    /// Panics if the redb topic registry mutex is poisoned.
    pub async fn enforce_retention_all(&self) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let names: Vec<String> = self
            .redb_topics
            .lock()
            .expect("poisoned")
            .keys()
            .cloned()
            .collect();
        for name in names {
            let topic = self.local_topic(&name)?;
            let ops = topic
                .enforce_retention_replicated()
                .await
                .map_err(|e| e.to_string())?;
            self.replicate_ops(&name, &ops).await?;
        }
        Ok(())
    }
}

impl TopicService {
    /// Wire entry point when the service is held in an [`Arc`].
    pub fn handle_request(
        self: &Arc<Self>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        self.handle_request_from(None, route, body)
    }

    /// Like [`handle_request`](Self::handle_request) with authenticated caller identity.
    pub fn handle_request_from(
        self: &Arc<Self>,
        from: Option<NodeId>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        let service = Arc::clone(self);
        match route {
            Route::TopicPublish => Box::pin(async move {
                let request: TopicPublishRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_publish(request).await)?)
            }),
            Route::TopicLease => Box::pin(async move {
                let request: TopicLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_lease(request).await)?)
            }),
            Route::TopicAck => Box::pin(async move {
                let request: TopicAckRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack(request).await)?)
            }),
            Route::TopicNack => Box::pin(async move {
                let request: TopicNackRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_nack(request).await)?)
            }),
            Route::TopicMetrics => Box::pin(async move {
                let request: TopicMetricsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_metrics(request).await)?)
            }),
            Route::TopicReplicate => Box::pin(async move {
                let request: TopicReplicateRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_replicate(from, request).await)?)
            }),
            other => Box::pin(async move {
                Err(TransportError::Io(format!(
                    "topic handler received unexpected route {other:?}"
                )))
            }),
        }
    }
}

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
                return Err(TopicError::Backend(err));
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
                return Err(TopicError::Backend(err));
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
                return Err(TopicError::Backend(err));
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
                return Err(TopicError::Backend(err));
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
                return Err(TopicError::Backend(err));
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

use std::time::Duration;

// Silence unused import when NOT_LEADER_REASON is only for symmetry with queue service.
#[allow(unused_imports)]
use NOT_LEADER_REASON as _;
