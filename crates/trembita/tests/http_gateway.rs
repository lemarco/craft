//! [`TrembitaApp`] gateway integration (HTTP product surface).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use http::StatusCode;
use trembita::{
    GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError, QueueOpts, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_support::{
    advance, boot_local_app, gateway_jobs_surfaces, spawn_test_gateway,
    wait_for_trembita_app_leader,
};

struct TestGatewayIdentity;

impl GatewayIdentity for TestGatewayIdentity {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, _: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        Ok("test".into())
    }
}

#[tokio::test(start_paused = true)]
async fn gateway_serves_jobs_api_on_configured_addr() {
    let base = std::env::temp_dir().join(format!(
        "trembita-http-gateway-{}",
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
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                        .identity(TestGatewayIdentity)
                        .surfaces(gateway_jobs_surfaces),
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let addr = spawn_test_gateway(
        &app,
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .identity(TestGatewayIdentity)
            .surfaces(gateway_jobs_surfaces)
            .build_config(),
    )
    .await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/jobs/jobs"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .body(r#"{"payload":"via-gateway"}"#)
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
