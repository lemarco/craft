//! Leader-gated actor workflow store wire service ([actor-state-store](../../../docs/decisions/actor-state-store.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;

use crafty_net::transport::{Body, BoxFuture, Transport, TransportError};
use crafty_net::{
    Route, decode_body, encode_body, send_store_compare_and_set, send_store_delete,
    send_store_replicate, send_store_set,
};
use crafty_proto::{
    NodeId, StoreCompareAndSetReply, StoreCompareAndSetRequest, StoreDeleteReply,
    StoreDeleteRequest, StoreReplicateReply, StoreReplicateRequest, StoreSetReply, StoreSetRequest,
};

use crate::store::{ActorStateStore, BoxFuture as StoreBoxFuture, StoreError};
use crate::supervisor::ClusterState;
use crate::{NOT_LEADER_REASON, RedbActorStateStore, StoreReplicationOps};

const REPLICATE_NOT_LEADER: &str = "actor store replicate rejected: caller is not raft leader";
const REPLICATE_UNAUTHENTICATED: &str = "actor store replicate rejected: unknown caller";

fn ttl_from_secs(secs: u64) -> Option<Duration> {
    (secs != 0).then(|| Duration::from_secs(secs))
}

/// Serves `/raft/v1/actor-store/*` on the leader; followers apply replication ops.
pub struct StoreService {
    node_id: NodeId,
    local: Arc<RedbActorStateStore>,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
}

impl StoreService {
    /// Wire service over `local` redb backend.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        local: Arc<RedbActorStateStore>,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            local,
            state,
            transport,
        }
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

    async fn replicate_ops(&self, ops: &StoreReplicationOps) -> Result<(), String> {
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
        let request = StoreReplicateRequest { ops: ops.clone() };
        let mut set = JoinSet::new();
        for peer in peers {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            set.spawn(async move {
                let reply = send_store_replicate(transport.as_ref(), peer, &request)
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

    fn authorize_replicate(&self, from: Option<NodeId>) -> Result<(), String> {
        let Some(from) = from else {
            return Err(REPLICATE_UNAUTHENTICATED.to_string());
        };
        let Some(leader) = self.state.leader_id() else {
            return Err("no raft leader elected".to_string());
        };
        if from != leader {
            return Err(REPLICATE_NOT_LEADER.to_string());
        }
        Ok(())
    }

    async fn handle_set(&self, request: StoreSetRequest) -> StoreSetReply {
        if self.state.is_leader() {
            match self.local.set_replicated(
                &request.key,
                &request.value,
                ttl_from_secs(request.ttl_secs),
            ) {
                Ok(ops) => {
                    if let Err(e) = self.replicate_ops(&ops).await {
                        return StoreSetReply { error: Some(e) };
                    }
                    StoreSetReply { error: None }
                }
                Err(e) => StoreSetReply {
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(
                        async move { send_store_set(transport.as_ref(), leader, &request).await },
                    )
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => StoreSetReply { error: Some(e) },
            }
        }
    }

    async fn handle_delete(&self, request: StoreDeleteRequest) -> StoreDeleteReply {
        if self.state.is_leader() {
            match self.local.delete_replicated(&request.key) {
                Ok(ops) => {
                    if let Err(e) = self.replicate_ops(&ops).await {
                        return StoreDeleteReply { error: Some(e) };
                    }
                    StoreDeleteReply { error: None }
                }
                Err(e) => StoreDeleteReply {
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_store_delete(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => StoreDeleteReply { error: Some(e) },
            }
        }
    }

    async fn handle_compare_and_set(
        &self,
        request: StoreCompareAndSetRequest,
    ) -> StoreCompareAndSetReply {
        if self.state.is_leader() {
            let expected = request.expected.as_deref();
            match self.local.compare_and_set_replicated(
                &request.key,
                expected,
                &request.value,
                ttl_from_secs(request.ttl_secs),
            ) {
                Ok((applied, ops)) => {
                    if applied && let Err(e) = self.replicate_ops(&ops).await {
                        return StoreCompareAndSetReply {
                            applied: false,
                            error: Some(e),
                        };
                    }
                    StoreCompareAndSetReply {
                        applied,
                        error: None,
                    }
                }
                Err(e) => StoreCompareAndSetReply {
                    applied: false,
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_store_compare_and_set(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => StoreCompareAndSetReply {
                    applied: false,
                    error: Some(e),
                },
            }
        }
    }

    fn handle_replicate(
        &self,
        from: Option<NodeId>,
        request: &StoreReplicateRequest,
    ) -> StoreReplicateReply {
        if let Err(e) = self.authorize_replicate(from) {
            return StoreReplicateReply { error: Some(e) };
        }
        for op in &request.ops {
            if let Err(e) = self.local.apply_replicate(op) {
                return StoreReplicateReply {
                    error: Some(e.to_string()),
                };
            }
        }
        StoreReplicateReply { error: None }
    }

    /// Leader-only sweep of expired TTL keys; replicates deletes to voters.
    ///
    /// # Errors
    /// Returns an error when the local scan/delete or replication fails.
    pub async fn gc_expired_ttl(&self, max_keys: usize) -> Result<usize, String> {
        if !self.state.is_leader() {
            return Ok(0);
        }
        let (removed, ops) = self.local.gc_expired(max_keys).map_err(|e| e.to_string())?;
        if removed > 0 {
            self.replicate_ops(&ops).await?;
        }
        Ok(removed)
    }

    /// Wire entry point when the service is held in an [`Arc`].
    #[must_use]
    pub fn handle_request(
        self: &Arc<Self>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        self.handle_request_from(None, route, body)
    }

    /// Like [`handle_request`](Self::handle_request) with authenticated caller identity.
    #[must_use]
    pub fn handle_request_from(
        self: &Arc<Self>,
        from: Option<NodeId>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        let service = Arc::clone(self);
        match route {
            Route::ActorStoreSet => Box::pin(async move {
                let request: StoreSetRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_set(request).await)?)
            }),
            Route::ActorStoreDelete => Box::pin(async move {
                let request: StoreDeleteRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_delete(request).await)?)
            }),
            Route::ActorStoreCompareAndSet => Box::pin(async move {
                let request: StoreCompareAndSetRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_compare_and_set(request).await)?)
            }),
            Route::ActorStoreReplicate => Box::pin(async move {
                let request: StoreReplicateRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_replicate(from, &request))?)
            }),
            other => Box::pin(async move {
                Err(TransportError::Io(format!(
                    "store handler received unexpected route {other:?}"
                )))
            }),
        }
    }
}

/// Cluster-facing [`ActorStateStore`] — local reads, leader-routed writes.
pub struct ClusterActorStateStore {
    local: Arc<RedbActorStateStore>,
    state: Arc<dyn ClusterState>,
    transport: Arc<dyn Transport>,
}

impl ClusterActorStateStore {
    /// Route writes through the leader wire service; read from `local`.
    #[must_use]
    pub fn new(
        local: Arc<RedbActorStateStore>,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            local,
            state,
            transport,
        }
    }

    fn leader(&self) -> Result<NodeId, StoreError> {
        self.state
            .leader_id()
            .ok_or_else(|| StoreError::Backend("no raft leader elected".into()))
    }
}

impl ActorStateStore for ClusterActorStateStore {
    fn get<'a>(&'a self, key: &'a str) -> StoreBoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move { self.local.get(key).await })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> StoreBoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let ttl_secs = ttl.map_or(0, |d| d.as_secs());
            let reply = send_store_set(
                self.transport.as_ref(),
                leader,
                &StoreSetRequest {
                    key: key.to_string(),
                    value: value.to_vec(),
                    ttl_secs,
                },
            )
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                if err == NOT_LEADER_REASON {
                    return Err(StoreError::Backend(err));
                }
                return Err(StoreError::Backend(err));
            }
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreBoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let reply = send_store_delete(
                self.transport.as_ref(),
                leader,
                &StoreDeleteRequest {
                    key: key.to_string(),
                },
            )
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(StoreError::Backend(err));
            }
            Ok(())
        })
    }

    fn compare_and_set<'a>(
        &'a self,
        key: &'a str,
        expected: Option<&'a [u8]>,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> StoreBoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let leader = self.leader()?;
            let ttl_secs = ttl.map_or(0, |d| d.as_secs());
            let reply = send_store_compare_and_set(
                self.transport.as_ref(),
                leader,
                &StoreCompareAndSetRequest {
                    key: key.to_string(),
                    expected: expected.map(<[u8]>::to_vec),
                    value: value.to_vec(),
                    ttl_secs,
                },
            )
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
            if let Some(err) = reply.error {
                return Err(StoreError::Backend(err));
            }
            Ok(reply.applied)
        })
    }
}

/// Leader-only loop: purge expired actor-store TTL keys with voter replication.
pub async fn run_actor_store_gc_ticker(
    service: Arc<StoreService>,
    poll_interval: Duration,
    max_keys: usize,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            break;
        }
        let _ = service.gc_expired_ttl(max_keys).await;
        tokio::select! {
            () = tokio::time::sleep(poll_interval) => {}
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
        }
    }
}
