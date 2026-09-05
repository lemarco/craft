//! B-19: introspection snapshots through the product gateway router.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use http::StatusCode;
use trembita::{
    GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError, QueueOpts, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_support::{
    advance, boot_local_app, gateway_introspect_config_identity,
    gateway_introspect_surfaces_identity, spawn_test_gateway, wait_for_trembita_app_leader,
};

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
                        .identity(BearerSecret)
                        .surfaces(gateway_introspect_surfaces_identity),
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

    let addr = spawn_test_gateway(&app, gateway_introspect_config_identity(BearerSecret)).await;
    let client = reqwest::Client::new();

    let unauth = client
        .get(format!("http://{addr}/introspect/cluster"))
        .header("Host", "127.0.0.1")
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let authed = client
        .get(format!("http://{addr}/introspect/cluster"))
        .header("Host", "127.0.0.1")
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(authed.status(), StatusCode::OK);

    let json: serde_json::Value = authed.json().await.unwrap();
    assert!(json.get("term").is_some());
    assert!(json.get("nodes").and_then(|v| v.as_array()).is_some());

    let queues = client
        .get(format!("http://{addr}/introspect/queues"))
        .header("Host", "127.0.0.1")
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(queues.status(), StatusCode::OK);
}
