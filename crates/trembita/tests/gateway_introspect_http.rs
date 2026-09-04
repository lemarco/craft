//! B-19: introspection snapshots through the product gateway router.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;
use trembita::cluster::build_gateway_router;
use trembita::{
    GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError, QueueOpts, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

struct BearerSecret;

impl GatewayIdentity for BearerSecret {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        match req.bearer_token() {
            Some("secret") => Ok("operator".into()),
            _ => Err(IdentityError::Unauthorized),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn gateway_introspect_requires_auth_and_returns_cluster_json() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-introspect-{}",
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
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                        .with_introspect_api(true)
                        .identity(BearerSecret)
                        .protect_product_apis(true),
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

    let router = build_gateway_router(
        &app,
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .with_introspect_api(true)
            .identity(BearerSecret)
            .protect_product_apis(true)
            .build_config(),
    )
    .expect("gateway config");

    let unauth = Request::builder()
        .method("GET")
        .uri("/introspect/cluster")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(unauth).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let authed = Request::builder()
        .method("GET")
        .uri("/introspect/cluster")
        .header(header::AUTHORIZATION, "Bearer secret")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(authed).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("term").is_some());
    assert!(json.get("nodes").and_then(|v| v.as_array()).is_some());

    let queues = Request::builder()
        .method("GET")
        .uri("/introspect/queues")
        .header(header::AUTHORIZATION, "Bearer secret")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(queues).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
