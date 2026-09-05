//! Gateway dispatch: SessionGate + CorsPolicy + identity hook together.

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use trembita::{GatewayOpts, TrembitaApp, TrembitaConfigure};
use trembita_http::{
    CorsPolicy, Gateway, HttpError, RequestCtx, Response, RouteTable, SessionGate,
};
use trembita_test_support::{boot_local_app, spawn_test_gateway};

struct BearerSecret;

impl trembita::GatewayIdentity for BearerSecret {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(
        &self,
        req: &trembita::GatewayRequest<'_>,
    ) -> Result<String, trembita::IdentityError> {
        match req.bearer_token() {
            Some("secret") => Ok("operator".into()),
            _ => Err(trembita::IdentityError::Unauthorized),
        }
    }
}

fn gateway_config() -> trembita::GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .identity(BearerSecret)
        .surfaces(|_state| {
            Gateway::new(false).surface(|s| {
                s.hosts(["api.example.com"])
                    .cors(CorsPolicy::credentials(["https://app.example.com"]))
                    .session(SessionGate::validate("sess", |token| async move {
                        if token == "valid" {
                            Ok(())
                        } else {
                            Err(HttpError::Unauthorized("bad session".into()))
                        }
                    }))
                    .routes(
                        RouteTable::new()
                            .get_identity("/me", |_: RequestCtx| async move {
                                Ok(Response::text(StatusCode::OK, "me"))
                            })
                            .post_session("/orders", |_: RequestCtx| async move {
                                Ok(Response::status(StatusCode::ACCEPTED))
                            }),
                    )
            })
        })
        .build_config()
}

#[tokio::test]
async fn cors_session_and_identity_gates_on_one_surface() {
    let app = boot_local_app(
        || {
            TrembitaApp::builder().configure(TrembitaConfigure {
                tick_period: Duration::from_millis(5),
                ..TrembitaConfigure::default()
            })
        },
        None,
    )
    .await;

    let app = Arc::new(app);
    let addr = spawn_test_gateway(&app, gateway_config()).await;
    let client = reqwest::Client::new();
    let origin = "https://app.example.com";

    let preflight = client
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/orders"))
        .header("Host", "api.example.com")
        .header("Origin", origin)
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), StatusCode::OK);
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some(origin)
    );

    let unauth_me = client
        .get(format!("http://{addr}/me"))
        .header("Host", "api.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_me.status(), StatusCode::UNAUTHORIZED);

    let authed_me = client
        .get(format!("http://{addr}/me"))
        .header("Host", "api.example.com")
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(authed_me.status(), StatusCode::OK);

    let no_cookie = client
        .post(format!("http://{addr}/orders"))
        .header("Host", "api.example.com")
        .header("Origin", origin)
        .send()
        .await
        .unwrap();
    assert_eq!(no_cookie.status(), StatusCode::UNAUTHORIZED);

    let with_cookie = client
        .post(format!("http://{addr}/orders"))
        .header("Host", "api.example.com")
        .header("Origin", origin)
        .header("Cookie", "sess=valid")
        .send()
        .await
        .unwrap();
    assert_eq!(with_cookie.status(), StatusCode::ACCEPTED);
    assert_eq!(
        with_cookie
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some(origin)
    );
}
