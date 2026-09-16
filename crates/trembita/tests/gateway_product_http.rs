//! Product gateway E2E: topics + workflows on default/composite surfaces with identity.

#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use trembita::{
    AppManifest, DefaultGatewayApis, Gateway, GatewayIdentity, GatewayOpts, GatewayRequest,
    IdentityError, TopicOpts, TrembitaApp, TrembitaConfigure, WorkflowBuilder, WorkflowOpts,
    journal_workflow,
};
use trembita_http::{ResponseBody, RouteTable};
use trembita_test_support::{
    advance, boot_local_app, spawn_test_gateway, wait_for_trembita_app_leader,
};

struct BearerSecret;

impl GatewayIdentity for BearerSecret {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        match req.bearer_token() {
            Some("secret") => Ok("alice".into()),
            _ => Err(IdentityError::Unauthorized),
        }
    }
}

fn noop_plan(saga_id: &str) -> trembita_client::SagaPlan {
    let key = b"workflow".to_vec();
    WorkflowBuilder::new(saga_id)
        .step("checkpoint", &key, trembita::proto::encode(&()).unwrap())
        .compensate("checkpoint", trembita::proto::encode(&()).unwrap())
        .build()
        .unwrap()
}

fn composite_product_gateway_config() -> trembita::GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .identity(BearerSecret)
        .surfaces(|state| {
            Gateway::new(false)
                .dev_fallback(RouteTable::new())
                .merge_routes(TrembitaApp::default_product_routes(
                    &state,
                    DefaultGatewayApis {
                        ops: false,
                        jobs: false,
                        schedules: false,
                        actors: false,
                        workflows: true,
                        topics: true,
                    },
                ))
        })
        .build_config()
}

#[tokio::test(start_paused = true)]
async fn gateway_topics_and_workflows_require_identity() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-product-http-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(&base),
                )
                .manifest(AppManifest::new().topics([TopicOpts::topic("orders.events")]))
                .manifest(
                    AppManifest::new().workflows([WorkflowOpts::new(noop_plan, journal_workflow)]),
                )
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
                        .identity(BearerSecret)
                        .surfaces(|_| Gateway::new(false).dev_fallback(RouteTable::new())),
                )
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let addr = spawn_test_gateway(&app, composite_product_gateway_config()).await;
    let client = reqwest::Client::new();
    let auth = |req: reqwest::RequestBuilder| {
        req.header("Host", "127.0.0.1")
            .header("authorization", "Bearer secret")
            .header("x-trembita-user", "alice")
    };

    let unauth = client
        .post(format!("http://{addr}/topics/orders.events/publish"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .body(r#"{"payload":"evt"}"#)
        .send()
        .await
        .expect("request");
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let publish = auth(
        client
            .post(format!("http://{addr}/topics/orders.events/publish"))
            .header("content-type", "application/json"),
    )
    .body(r#"{"payload":"evt"}"#)
    .send()
    .await
    .expect("publish");
    assert_eq!(publish.status(), StatusCode::ACCEPTED);

    let metrics = auth(client.get(format!("http://{addr}/topics/orders.events")))
        .send()
        .await
        .expect("metrics");
    assert_eq!(metrics.status(), StatusCode::OK);

    let run = auth(
        client
            .post(format!("http://{addr}/workflows/run"))
            .header("content-type", "application/json"),
    )
    .body(r#"{"saga_id":"gateway-e2e"}"#)
    .send()
    .await
    .expect("workflow");
    assert_eq!(run.status(), StatusCode::OK);
}

#[tokio::test(start_paused = true)]
async fn ops_introspect_topics_snapshot() {
    let base = std::env::temp_dir().join(format!(
        "trembita-introspect-topics-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(&base),
                )
                .manifest(AppManifest::new().topics([TopicOpts::topic("orders.events")]))
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;
    app.publish("orders.events", b"evt").await.expect("publish");
    advance(Duration::from_millis(50)).await;

    let table = app.ops_api().route_table();
    let resp = table
        .dispatch_open(
            &Method::GET,
            "/introspect/topics",
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        )
        .await
        .expect("dispatch");
    assert_eq!(resp.status_code(), StatusCode::OK);
    let json = match resp.body() {
        ResponseBody::Json(v) => v.clone(),
        ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("json bytes"),
        other => panic!("expected json body, got {other:?}"),
    };
    let names: Vec<_> = json["topics"]
        .as_array()
        .expect("topics array")
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(names.contains(&"orders.events"));
}
