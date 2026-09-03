//! Axum routes for cluster introspection snapshots ([`Observer`](trembita_dashboard::Observer)).

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, Uri};
use axum::routing::get;

use crate::IntrospectApiState;
use crate::introspect_types::IntrospectApiError;
use crate::types::JobsApiError;

/// Axum sub-router for read-only introspection routes.
pub fn introspect_router() -> Router<Arc<IntrospectApiState>> {
    Router::new()
        .route("/introspect/cluster", get(get_cluster))
        .route("/introspect/raft-groups", get(get_raft_groups))
        .route("/introspect/actors", get(get_actors))
        .route("/introspect/actors/{id}", get(get_actor))
        .route("/introspect/node/{id}", get(get_node))
        .route("/introspect/queues", get(get_queues))
        .route("/introspect/sagas", get(get_sagas))
}

async fn authorize(
    state: &IntrospectApiState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<(), IntrospectApiError> {
    if let Some(auth) = &state.auth {
        auth(method.clone(), uri.clone(), headers.clone())
            .await
            .map_err(|e| match e {
                JobsApiError::Unauthorized(m) => IntrospectApiError::Unauthorized(m),
                other => IntrospectApiError::BadRequest(other.to_string()),
            })?;
    }
    Ok(())
}

async fn get_cluster(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<trembita_dashboard::ClusterView>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    Ok(Json(state.observer.cluster().await))
}

async fn get_raft_groups(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<trembita_dashboard::RaftGroupsView>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    Ok(Json(state.observer.raft_groups().await))
}

async fn get_actors(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<Vec<trembita_dashboard::ActorView>>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    Ok(Json(state.observer.actors().await))
}

async fn get_actor(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<trembita_dashboard::ActorView>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    state
        .observer
        .actor(&id)
        .await
        .map(Json)
        .ok_or_else(|| IntrospectApiError::NotFound("no such actor".into()))
}

async fn get_node(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<trembita_dashboard::NodeView>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let node_id = id
        .parse::<u64>()
        .map_err(|_| IntrospectApiError::BadRequest("invalid node id".into()))?;
    state
        .observer
        .node(node_id)
        .await
        .map(Json)
        .ok_or_else(|| IntrospectApiError::NotFound("no such node".into()))
}

async fn get_queues(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<trembita_dashboard::QueuesView>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    Ok(Json(state.observer.queues().await))
}

async fn get_sagas(
    State(state): State<Arc<IntrospectApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<Vec<trembita_dashboard::SagaRecordView>>, IntrospectApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    Ok(Json(state.observer.sagas().await))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use std::future;
    use std::sync::Arc;
    use tower::ServiceExt;
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

    fn test_state(
        observer: Arc<dyn Observer>,
        auth: Option<crate::AuthFn>,
    ) -> Arc<IntrospectApiState> {
        Arc::new(IntrospectApiState { observer, auth })
    }

    #[tokio::test]
    async fn get_cluster_returns_json() {
        let observer: Arc<dyn Observer> = Arc::new(FakeObserver {
            actor_id: "orders/0".into(),
        });
        let app = introspect_router().with_state(test_state(observer, None));
        let req = Request::builder()
            .method("GET")
            .uri("/introspect/cluster")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_actor_returns_not_found() {
        let observer: Arc<dyn Observer> = Arc::new(FakeObserver {
            actor_id: "orders/0".into(),
        });
        let app = introspect_router().with_state(test_state(observer, None));
        let req = Request::builder()
            .method("GET")
            .uri("/introspect/actors/missing")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn invalid_node_id_returns_bad_request() {
        let observer: Arc<dyn Observer> = Arc::new(FakeObserver {
            actor_id: "orders/0".into(),
        });
        let app = introspect_router().with_state(test_state(observer, None));
        let req = Request::builder()
            .method("GET")
            .uri("/introspect/node/notanumber")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn auth_rejects_unauthorized_requests() {
        let observer: Arc<dyn Observer> = Arc::new(FakeObserver {
            actor_id: "orders/0".into(),
        });
        let auth: crate::AuthFn = Arc::new(|_, _, _| {
            Box::pin(future::ready(Err(JobsApiError::Unauthorized(
                "nope".into(),
            ))))
        });
        let app = introspect_router().with_state(test_state(observer, Some(auth)));
        let req = Request::builder()
            .method("GET")
            .uri("/introspect/cluster")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
