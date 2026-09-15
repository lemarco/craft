//! Registration-driven product HTTP on the default gateway (workflows, topics).

#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use trembita::{
    GatewayOpts, TopicOpts, TrembitaApp, TrembitaConfigure, WorkflowBuilder, WorkflowOpts,
    journal_workflow,
};
use trembita_http::RouteTable;
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

async fn dispatch(table: &RouteTable, method: Method, path: &str, body: Bytes) -> StatusCode {
    table
        .dispatch_open(&method, path, HashMap::new(), http::HeaderMap::new(), body)
        .await
        .expect("dispatch")
        .status_code()
}

fn noop_plan(saga_id: &str) -> trembita_client::SagaPlan {
    let key = b"workflow".to_vec();
    WorkflowBuilder::new(saga_id)
        .step("checkpoint", &key, trembita::proto::encode(&()).unwrap())
        .compensate("checkpoint", trembita::proto::encode(&()).unwrap())
        .build()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn default_product_routes_include_workflows_and_topics() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-symmetry-{}",
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
                .data_dir(&base)
                .topics([TopicOpts::topic("orders.events")])
                .workflows([WorkflowOpts::new(noop_plan, journal_workflow)])
                .gateway(GatewayOpts::new("127.0.0.1:0".parse().expect("addr")))
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

    let table = TrembitaApp::workflows_api(Arc::clone(&app))
        .route_table()
        .merge(TrembitaApp::topics_api(Arc::clone(&app)).route_table());

    assert_eq!(
        dispatch(
            &table,
            Method::POST,
            "/workflows/run",
            Bytes::from(r#"{"saga_id":"symmetry-test"}"#),
        )
        .await,
        StatusCode::OK
    );

    assert_eq!(
        dispatch(
            &table,
            Method::POST,
            "/topics/orders.events/publish",
            Bytes::from(r#"{"payload":"created"}"#),
        )
        .await,
        StatusCode::ACCEPTED
    );
}

#[tokio::test(start_paused = true)]
async fn without_topics_api_excludes_topic_routes() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-no-topics-{}",
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
                .data_dir(&base)
                .topics([TopicOpts::topic("orders.events")])
                .without_topics_api()
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    let state = TrembitaGatewayState::new(Arc::clone(&app));
    let table = TrembitaApp::default_product_routes(
        &state,
        DefaultGatewayApis {
            ops: false,
            jobs: false,
            actors: false,
            workflows: false,
            topics: false,
        },
    );
    assert!(
        table
            .dispatch_open(
                &Method::POST,
                "/topics/orders.events/publish",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::from("evt"),
            )
            .await
            .is_err()
    );
}
