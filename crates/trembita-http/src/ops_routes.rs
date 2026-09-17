//! Operational routes — health, readiness, metrics, dashboard, introspection.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::StatusCode;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use hyper::body::{Bytes, Frame};
use tokio_stream::wrappers::ReceiverStream;
use trembita_dashboard::{DASHBOARD_HTML, EventBus, Metrics, Observer, Readiness};

use crate::introspect_routes;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;
type HttpResponse = http::Response<BoxBody>;

/// Shared state for ops routes.
pub struct OpsApiState {
    pub(crate) observer: Arc<dyn Observer>,
    pub(crate) metrics: Metrics,
    pub(crate) events: EventBus,
}

/// Operational HTTP API (health, metrics, dashboard, introspection).
#[derive(Clone)]
pub struct OpsApi {
    observer: Arc<dyn Observer>,
    metrics: Metrics,
    events: EventBus,
}

impl OpsApi {
    /// Build from observability sources (same inputs as the former admin server).
    #[must_use]
    pub fn new(observer: Arc<dyn Observer>, metrics: Metrics, events: EventBus) -> Self {
        Self {
            observer,
            metrics,
            events,
        }
    }

    /// Full ops route table including `/introspect/*`.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        let state = Arc::new(OpsApiState {
            observer: Arc::clone(&self.observer),
            metrics: self.metrics.clone(),
            events: self.events.clone(),
        });
        ops_route_table(state)
    }
}

/// Route table for health, readiness, metrics, dashboard, and introspection.
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn ops_route_table(state: Arc<OpsApiState>) -> RouteTable {
    let s1 = Arc::clone(&state);
    let s2 = Arc::clone(&state);
    let events = state.events.clone();

    let introspect = crate::IntrospectApiState {
        observer: Arc::clone(&state.observer),
    };

    RouteTable::new()
        .get("/health", move |_ctx: RequestCtx| async move {
            Ok(Response::json(
                StatusCode::OK,
                serde_json::json!({ "status": "ok" }),
            ))
        })
        .get("/ready", move |_ctx: RequestCtx| {
            let state = Arc::clone(&s1);
            async move { ready_response(&state).await }
        })
        .get("/metrics", move |_ctx: RequestCtx| {
            let state = Arc::clone(&s2);
            async move { metrics_response(&state).await }
        })
        .get("/dashboard", move |_ctx: RequestCtx| async move {
            let mut resp = Response::text(StatusCode::OK, DASHBOARD_HTML.to_string());
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("text/html; charset=utf-8"),
            );
            Ok(resp)
        })
        .sse("/dashboard/events", move || {
            dashboard_events_sse(events.clone())
        })
        .merge(introspect_routes::route_table(Arc::new(introspect)))
}

async fn ready_response(state: &OpsApiState) -> Result<Response, HttpError> {
    let readiness: Readiness = state.observer.readiness().await;
    let status = if readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Ok(Response::json(
        status,
        serde_json::to_value(readiness).unwrap_or_default(),
    ))
}

async fn metrics_response(state: &OpsApiState) -> Result<Response, HttpError> {
    let _ = state.observer.queues().await;
    let _ = state.observer.topics().await;
    let _ = state.observer.sagas().await;
    let body = state.metrics.render();
    let mut resp = Response::text(StatusCode::OK, body);
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    Ok(resp)
}

fn dashboard_events_sse(events: EventBus) -> Pin<Box<dyn Future<Output = HttpResponse> + Send>> {
    Box::pin(async move {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);
        let mut sub = events.subscribe();
        tokio::spawn(async move {
            if tx
                .send(Ok(Frame::data(Bytes::from_static(b": connected\n\n"))))
                .await
                .is_err()
            {
                return;
            }
            while let Some(event) = sub.recv().await {
                let json = serde_json::to_string(&event).unwrap_or_default();
                let msg = format!("data: {json}\n\n");
                if tx.send(Ok(Frame::data(Bytes::from(msg)))).await.is_err() {
                    break;
                }
            }
        });
        let body = StreamBody::new(ReceiverStream::new(rx))
            .map_err(|never| match never {})
            .boxed();
        http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .body(body)
            .expect("valid sse response")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use http::HeaderMap;
    use trembita_dashboard::{BoxFuture, JoinPhase};

    use crate::ResponseBody;

    fn readiness_fixture(
        node_id: u64,
        role: &str,
        member: bool,
        draining: bool,
        workers: Vec<String>,
        phase: JoinPhase,
        reason: Option<String>,
    ) -> Readiness {
        Readiness {
            node_id,
            role: role.into(),
            member,
            draining,
            workers: workers.clone(),
            reason,
            join_phase: phase,
            committed_learner: !member && phase != JoinPhase::AwaitingMembership,
            log_caught_up: !matches!(phase, JoinPhase::AwaitingMembership | JoinPhase::CatchingUp),
            hosts_wired: member || !workers.is_empty(),
        }
    }

    #[derive(Clone)]
    struct FakeObserver(Readiness);

    impl FakeObserver {
        fn ready_leader() -> Self {
            Self(readiness_fixture(
                1,
                "leader",
                true,
                false,
                Vec::new(),
                JoinPhase::PoolReady,
                None,
            ))
        }
    }

    impl Observer for FakeObserver {
        fn readiness(&self) -> BoxFuture<'_, Readiness> {
            let snapshot = self.0.clone();
            Box::pin(async move { snapshot })
        }

        fn cluster(&self) -> BoxFuture<'_, trembita_dashboard::ClusterView> {
            Box::pin(async {
                trembita_dashboard::ClusterView {
                    leader: Some(1),
                    term: 1,
                    commit_index: 0,
                    nodes: Vec::new(),
                }
            })
        }

        fn raft_groups(&self) -> BoxFuture<'_, trembita_dashboard::RaftGroupsView> {
            Box::pin(async {
                trembita_dashboard::RaftGroupsView {
                    shard_count: 1,
                    shard_routing: "modulus".into(),
                    catalog_size: 1,
                    catalog_version: 1,
                    replication_factor: 1,
                    learner_factor: 0,
                    hosted_groups: Vec::new(),
                    groups: Vec::new(),
                }
            })
        }

        fn actors(&self) -> BoxFuture<'_, Vec<trembita_dashboard::ActorView>> {
            Box::pin(async { Vec::new() })
        }

        fn actor(&self, _id: &str) -> BoxFuture<'_, Option<trembita_dashboard::ActorView>> {
            Box::pin(async { None })
        }

        fn node(&self, _id: u64) -> BoxFuture<'_, Option<trembita_dashboard::NodeView>> {
            Box::pin(async { None })
        }

        fn queues(&self) -> BoxFuture<'_, trembita_dashboard::QueuesView> {
            Box::pin(async {
                trembita_dashboard::QueuesView {
                    streams: Vec::new(),
                }
            })
        }

        fn sagas(&self) -> BoxFuture<'_, Vec<trembita_dashboard::SagaRecordView>> {
            Box::pin(async { Vec::new() })
        }

        fn topics(&self) -> BoxFuture<'_, trembita_dashboard::TopicsView> {
            Box::pin(async { trembita_dashboard::TopicsView { topics: Vec::new() } })
        }
    }

    async fn dispatch_get(table: &RouteTable, path: &str) -> StatusCode {
        table
            .dispatch_open(
                &http::Method::GET,
                path,
                HashMap::default(),
                HeaderMap::default(),
                Bytes::new(),
            )
            .await
            .expect("dispatch")
            .status_code()
    }

    fn ops_table(readiness: Readiness) -> RouteTable {
        OpsApi::new(
            Arc::new(FakeObserver(readiness)),
            Metrics::new(),
            EventBus::new(16),
        )
        .route_table()
    }

    #[tokio::test]
    async fn ops_health_and_ready() {
        let table = ops_table(FakeObserver::ready_leader().0);
        assert_eq!(dispatch_get(&table, "/health").await, StatusCode::OK);
        assert_eq!(dispatch_get(&table, "/ready").await, StatusCode::OK);
    }

    /// B-30 — table-driven LB pool contract on `/ready` and liveness on `/health`.
    #[tokio::test]
    async fn ingress_lb_readiness_pool_contract_scenarios() {
        let scenarios: &[(&str, Readiness, StatusCode)] = &[
            (
                "member leader in pool",
                readiness_fixture(
                    1,
                    "leader",
                    true,
                    false,
                    vec!["w#1".into()],
                    JoinPhase::PoolReady,
                    None,
                ),
                StatusCode::OK,
            ),
            (
                "member follower in pool",
                readiness_fixture(
                    2,
                    "follower",
                    true,
                    false,
                    Vec::new(),
                    JoinPhase::PoolReady,
                    None,
                ),
                StatusCode::OK,
            ),
            (
                "joining not in pool",
                readiness_fixture(
                    3,
                    "follower",
                    false,
                    false,
                    Vec::new(),
                    JoinPhase::AwaitingMembership,
                    Some("joining".into()),
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "draining removed from pool",
                readiness_fixture(
                    1,
                    "leader",
                    true,
                    true,
                    Vec::new(),
                    JoinPhase::PoolReady,
                    Some("drain".into()),
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "non-member without reason",
                readiness_fixture(
                    4,
                    "learner",
                    false,
                    false,
                    Vec::new(),
                    JoinPhase::CatchingUp,
                    None,
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "member draining even with workers",
                readiness_fixture(
                    2,
                    "follower",
                    true,
                    true,
                    vec!["w#9".into()],
                    JoinPhase::PoolReady,
                    Some("upgrade".into()),
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "candidate still member",
                readiness_fixture(
                    1,
                    "candidate",
                    true,
                    false,
                    Vec::new(),
                    JoinPhase::PoolReady,
                    None,
                ),
                StatusCode::OK,
            ),
            (
                "learner awaiting hosts",
                readiness_fixture(
                    5,
                    "learner",
                    false,
                    false,
                    Vec::new(),
                    JoinPhase::AwaitingHosts,
                    Some("learner".into()),
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "elastic learner in pool after hosts",
                readiness_fixture(
                    6,
                    "learner",
                    false,
                    false,
                    vec!["workers".into()],
                    JoinPhase::PoolReady,
                    None,
                ),
                StatusCode::OK,
            ),
            (
                "rolling upgrade drain flag",
                readiness_fixture(
                    2,
                    "follower",
                    true,
                    true,
                    vec!["w#2".into()],
                    JoinPhase::PoolReady,
                    Some("rolling-upgrade".into()),
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];

        for (name, readiness, want_ready) in scenarios {
            let table = ops_table(readiness.clone());
            let health = dispatch_get(&table, "/health").await;
            assert_eq!(health, StatusCode::OK, "{name}: liveness must stay 200");
            let ready = dispatch_get(&table, "/ready").await;
            assert_eq!(ready, *want_ready, "{name}: readiness status");
            let body = table
                .dispatch_open(
                    &http::Method::GET,
                    "/ready",
                    HashMap::default(),
                    HeaderMap::default(),
                    Bytes::new(),
                )
                .await
                .expect("ready body");
            let json = match body.body() {
                ResponseBody::Json(v) => v.clone(),
                ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("ready json"),
                other => panic!("{name}: expected json, got {other:?}"),
            };
            assert_eq!(
                json["member"].as_bool(),
                Some(readiness.member),
                "{name}: member field"
            );
            assert_eq!(
                json["draining"].as_bool(),
                Some(readiness.draining),
                "{name}: draining field"
            );
            assert_eq!(
                json["join_phase"],
                serde_json::to_value(readiness.join_phase).expect("join_phase json"),
                "{name}: join_phase field"
            );
        }
    }

    #[test]
    fn b35_join_phase_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(JoinPhase::AwaitingMembership).unwrap(),
            "awaiting_membership"
        );
        assert_eq!(
            serde_json::to_value(JoinPhase::PoolReady).unwrap(),
            "pool_ready"
        );
    }

    async fn dispatch_open_status(
        table: &RouteTable,
        method: &http::Method,
        path: &str,
    ) -> Result<StatusCode, HttpError> {
        table
            .dispatch_open(
                method,
                path,
                HashMap::default(),
                HeaderMap::default(),
                Bytes::new(),
            )
            .await
            .map(|r| r.status_code())
    }

    #[tokio::test]
    async fn ingress_lb_health_check_http_methods() {
        let table = ops_table(FakeObserver::ready_leader().0);
        assert_eq!(dispatch_get(&table, "/health").await, StatusCode::OK);
        assert_eq!(dispatch_get(&table, "/ready").await, StatusCode::OK);
        assert!(matches!(
            dispatch_open_status(&table, &http::Method::POST, "/health").await,
            Err(HttpError::NotFound)
        ));
        assert!(matches!(
            dispatch_open_status(&table, &http::Method::POST, "/ready").await,
            Err(HttpError::NotFound)
        ));
        assert!(matches!(
            dispatch_open_status(&table, &http::Method::GET, "/healthz").await,
            Err(HttpError::NotFound)
        ));
        assert!(matches!(
            dispatch_open_status(&table, &http::Method::GET, "/Ready").await,
            Err(HttpError::NotFound)
        ));
    }
}
