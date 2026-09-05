//! Route table for cluster introspection snapshots ([`Observer`](trembita_dashboard::Observer)).

use std::sync::Arc;

use http::StatusCode;

use crate::IntrospectApiState;
use crate::introspect_types::IntrospectApiError;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};

fn json_ok<T: serde::Serialize>(value: T) -> Result<Response, IntrospectApiError> {
    let json = serde_json::to_value(value)
        .map_err(|e| IntrospectApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::OK, json))
}

/// Route table for read-only introspection routes.
#[must_use]
pub fn route_table(state: Arc<IntrospectApiState>) -> RouteTable {
    let s1 = Arc::clone(&state);
    let s2 = Arc::clone(&state);
    let s3 = Arc::clone(&state);
    let s4 = Arc::clone(&state);
    let s5 = Arc::clone(&state);
    let s6 = Arc::clone(&state);
    let s7 = state;
    RouteTable::new()
        .get("/introspect/cluster", move |ctx| {
            let state = Arc::clone(&s1);
            async move { get_cluster(state, ctx).await }
        })
        .get("/introspect/raft-groups", move |ctx| {
            let state = Arc::clone(&s2);
            async move { get_raft_groups(state, ctx).await }
        })
        .get("/introspect/actors", move |ctx| {
            let state = Arc::clone(&s3);
            async move { get_actors(state, ctx).await }
        })
        .get("/introspect/actors/{id}", move |ctx| {
            let state = Arc::clone(&s4);
            async move { get_actor(state, ctx).await }
        })
        .get("/introspect/node/{id}", move |ctx| {
            let state = Arc::clone(&s5);
            async move { get_node(state, ctx).await }
        })
        .get("/introspect/queues", move |ctx| {
            let state = Arc::clone(&s6);
            async move { get_queues(state, ctx).await }
        })
        .get("/introspect/sagas", move |ctx| {
            let state = Arc::clone(&s7);
            async move { get_sagas(state, ctx).await }
        })
}

async fn get_cluster(
    state: Arc<IntrospectApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match get_cluster_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_cluster_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    json_ok(state.observer.cluster().await)
}

async fn get_raft_groups(
    state: Arc<IntrospectApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match get_raft_groups_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_raft_groups_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    json_ok(state.observer.raft_groups().await)
}

async fn get_actors(
    state: Arc<IntrospectApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match get_actors_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_actors_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    json_ok(state.observer.actors().await)
}

async fn get_actor(state: Arc<IntrospectApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_actor_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_actor_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    let id = ctx
        .params()
        .get("id")
        .ok_or_else(|| IntrospectApiError::BadRequest("missing id".into()))?
        .to_string();
    state
        .observer
        .actor(&id)
        .await
        .map(json_ok)
        .ok_or_else(|| IntrospectApiError::NotFound("no such actor".into()))?
}

async fn get_node(state: Arc<IntrospectApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_node_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_node_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    let id = ctx
        .params()
        .get("id")
        .ok_or_else(|| IntrospectApiError::BadRequest("missing id".into()))?;
    let node_id = id
        .parse::<u64>()
        .map_err(|_| IntrospectApiError::BadRequest("invalid node id".into()))?;
    state
        .observer
        .node(node_id)
        .await
        .map(json_ok)
        .ok_or_else(|| IntrospectApiError::NotFound("no such node".into()))?
}

async fn get_queues(
    state: Arc<IntrospectApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match get_queues_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_queues_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    json_ok(state.observer.queues().await)
}

async fn get_sagas(state: Arc<IntrospectApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_sagas_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_sagas_inner(
    state: &IntrospectApiState,
    ctx: RequestCtx,
) -> Result<Response, IntrospectApiError> {
    json_ok(state.observer.sagas().await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Method;
    use std::collections::HashMap;
    use std::future;
    use std::sync::Arc;
    use trembita_dashboard::{
        ActorView, BoxFuture, ClusterView, NodeSummary, NodeView, Observer, QueuesView,
        RaftGroupsView, Readiness, SagaRecordView,
    };

    struct FakeObserver {
        actor_id: String,
    }

    impl Observer for FakeObserver {
        fn readiness(&self) -> BoxFuture<'_, Readiness> {
            Box::pin(async move {
                Readiness {
                    node_id: 1,
                    role: "leader".into(),
                    member: true,
                    draining: false,
                    workers: vec![],
                    reason: None,
                }
            })
        }

        fn cluster(&self) -> BoxFuture<'_, ClusterView> {
            Box::pin(async move {
                ClusterView {
                    leader: Some(1),
                    term: 1,
                    commit_index: 0,
                    nodes: vec![NodeSummary {
                        id: 1,
                        role: "leader".into(),
                        member: true,
                    }],
                }
            })
        }

        fn raft_groups(&self) -> BoxFuture<'_, RaftGroupsView> {
            Box::pin(async move {
                RaftGroupsView {
                    shard_count: 1,
                    shard_routing: "modulus".into(),
                    catalog_size: 1,
                    catalog_version: 1,
                    replication_factor: 3,
                    learner_factor: 0,
                    hosted_groups: vec![0],
                    groups: vec![],
                }
            })
        }

        fn actors(&self) -> BoxFuture<'_, Vec<ActorView>> {
            let id = self.actor_id.clone();
            Box::pin(async move {
                vec![ActorView {
                    id,
                    node: 1,
                    actor_type: "Worker".into(),
                    mailbox_depth: 0,
                    uptime_secs: 1,
                    generation: 1,
                    messages_per_sec: 0.0,
                }]
            })
        }

        fn actor(&self, id: &str) -> BoxFuture<'_, Option<ActorView>> {
            let want = self.actor_id.clone();
            let id = id.to_owned();
            Box::pin(async move {
                (id == want).then(|| ActorView {
                    id,
                    node: 1,
                    actor_type: "Worker".into(),
                    mailbox_depth: 0,
                    uptime_secs: 1,
                    generation: 1,
                    messages_per_sec: 0.0,
                })
            })
        }

        fn node(&self, id: u64) -> BoxFuture<'_, Option<NodeView>> {
            Box::pin(async move {
                (id == 1).then(|| NodeView {
                    id,
                    workers: vec!["orders".into()],
                    cpus: 4,
                    store_healthy: true,
                })
            })
        }

        fn queues(&self) -> BoxFuture<'_, QueuesView> {
            Box::pin(async move { QueuesView { streams: vec![] } })
        }

        fn sagas(&self) -> BoxFuture<'_, Vec<SagaRecordView>> {
            Box::pin(async move { vec![] })
        }
    }

    #[tokio::test]
    async fn get_cluster_returns_json() {
        let observer: Arc<dyn Observer> = Arc::new(FakeObserver {
            actor_id: "orders/0".into(),
        });
        let table = route_table(Arc::new(IntrospectApiState {
            observer,
            auth: None,
        }));
        let resp = table
            .dispatch_open(
                &Method::GET,
                "/introspect/cluster",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
    }
}
