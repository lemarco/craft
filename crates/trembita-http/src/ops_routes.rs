//! Operational routes — health, readiness, metrics, dashboard, introspection.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::StatusCode;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use hyper::body::{Bytes, Frame, Incoming};
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
pub fn ops_route_table(state: Arc<OpsApiState>) -> RouteTable {
    let s1 = Arc::clone(&state);
    let s2 = Arc::clone(&state);
    let s3 = Arc::clone(&state);
    let s4 = Arc::clone(&state);
    let events = state.events.clone();

    let introspect = crate::IntrospectApiState {
        observer: Arc::clone(&state.observer),
        auth: None,
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
    use trembita_dashboard::BoxFuture;

    struct FakeObserver;

    impl Observer for FakeObserver {
        fn readiness(&self) -> BoxFuture<'_, Readiness> {
            Box::pin(async {
                Readiness {
                    node_id: 1,
                    role: "leader".into(),
                    member: true,
                    draining: false,
                    workers: Vec::new(),
                    reason: None,
                }
            })
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
    }

    #[tokio::test]
    async fn ops_health_and_ready() {
        let api = OpsApi::new(Arc::new(FakeObserver), Metrics::new(), EventBus::new(16));
        let table = api.route_table();
        let health = table
            .dispatch_open(
                &http::Method::GET,
                "/health",
                Default::default(),
                Default::default(),
                Bytes::new(),
            )
            .await
            .expect("health");
        assert_eq!(health.status_code(), StatusCode::OK);
        let ready = table
            .dispatch_open(
                &http::Method::GET,
                "/ready",
                Default::default(),
                Default::default(),
                Bytes::new(),
            )
            .await
            .expect("ready");
        assert_eq!(ready.status_code(), StatusCode::OK);
    }
}
