//! HTTP gateway end-to-end: identity → session → cast (POST + GET).

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use crafty::actor::{UserActor, actor};
use crafty::cluster::build_gateway_router;
use crafty::{
    ActorGroupOpts, CraftyApp, CraftyConfigure, CraftyGatewayState, GatewayOpts,
    OpenActorSessionError,
};
use crafty_test_support::{
    advance, boot_local_app, eventually_default, wait_for_crafty_app_leader,
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

struct FixedToken;

impl crafty::GatewayIdentity for FixedToken {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(
        &self,
        req: &crafty::GatewayRequest<'_>,
    ) -> Result<String, crafty::IdentityError> {
        if let Some(bearer) = req.bearer_token() {
            if bearer != "secret" {
                return Err(crafty::IdentityError::Unauthorized);
            }
            return req
                .headers
                .get("x-crafty-user")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .ok_or(crafty::IdentityError::Unauthorized);
        }
        let user = req
            .query("user")
            .ok_or(crafty::IdentityError::Unauthorized)?;
        let token = req
            .query("token")
            .ok_or(crafty::IdentityError::Unauthorized)?;
        if token == "secret" {
            Ok(user)
        } else {
            Err(crafty::IdentityError::Unauthorized)
        }
    }
}

#[derive(Debug)]
struct EchoErr;
impl std::fmt::Display for EchoErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("echo")
    }
}
impl std::error::Error for EchoErr {}

struct EchoWorker;

#[actor]
impl UserActor for EchoWorker {
    type Config = u32;
    type Message = String;
    type Error = EchoErr;

    fn start(_seed: Self::Config) -> Result<Self, EchoErr> {
        Ok(Self)
    }

    fn handle(
        &mut self,
        _msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), EchoErr>> + Send {
        std::future::ready(Ok(()))
    }
}

#[derive(Deserialize)]
struct ChatPost {
    message: String,
}

#[derive(Serialize)]
struct ChatAck {
    ok: bool,
    user: String,
}

#[derive(Serialize)]
struct MeResponse {
    user: String,
}

async fn post_chat(
    axum::extract::State(state): axum::extract::State<CraftyGatewayState>,
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    Json(body): Json<ChatPost>,
) -> Result<Json<ChatAck>, OpenActorSessionError> {
    let mut handle = state
        .open_actor_session_parts(
            "echo",
            &method,
            &uri,
            &headers,
            Some(Duration::from_secs(60)),
        )
        .await?;
    let user = handle.session_key().to_string();
    let payload = crafty::proto::encode(&body.message).expect("encode");
    handle.cast(payload).await.expect("cast");
    Ok(Json(ChatAck { ok: true, user }))
}

async fn get_me(
    axum::extract::State(state): axum::extract::State<CraftyGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    match state.extract_session_parts(&method, &uri, &headers).await {
        Ok(extracted) => Json(MeResponse {
            user: extracted.session_key().to_string(),
        })
        .into_response(),
        Err(err) => err.into_response(),
    }
}

fn gateway_routes(state: CraftyGatewayState) -> Router {
    Router::new()
        .route("/chat", post(post_chat))
        .route("/me", get(get_me))
        .with_state(state)
}

async fn boot_with_workers(base: &std::path::Path) -> Arc<CraftyApp> {
    let app = boot_local_app(
        CraftyApp::builder()
            .data_dir(base)
            .actors::<EchoWorker>("echo", ActorGroupOpts::new(0))
            .configure(CraftyConfigure {
                tick_period: Duration::from_millis(5),
                reconcile_period: Duration::from_millis(20),
                directory_publish_period: Duration::from_millis(20),
                ..CraftyConfigure::default()
            }),
        None,
    )
    .await;
    wait_for_crafty_app_leader(&app).await;
    advance(Duration::from_millis(500)).await;
    eventually_default("echo worker in directory", || {
        !app.cluster_ref("echo").is_empty()
    })
    .await;
    app
}

#[tokio::test(start_paused = true)]
async fn http_post_chat_with_query_auth() {
    let base = std::env::temp_dir().join(format!(
        "crafty-gateway-http-post-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let router = build_gateway_router(
        Arc::clone(&app),
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .identity(FixedToken)
            .routes(gateway_routes)
            .build_config(),
    );

    let req = Request::builder()
        .method("POST")
        .uri("/chat?user=alice&token=secret")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"message":"hello"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::OK);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn extract_session_from_on_http_request() {
    let base = std::env::temp_dir().join(format!(
        "crafty-gateway-http-from-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let state = CraftyGatewayState::with_identity(Arc::clone(&app), FixedToken);

    let req = Request::builder()
        .method("GET")
        .uri("/me?user=bob&token=secret")
        .body(Body::empty())
        .unwrap();
    let extracted = state.extract_session_from(&req).await.expect("auth");
    assert_eq!(extracted.session_key(), "bob");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn http_get_me_route() {
    let base = std::env::temp_dir().join(format!(
        "crafty-gateway-http-get-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let router = build_gateway_router(
        Arc::clone(&app),
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .identity(FixedToken)
            .routes(gateway_routes)
            .build_config(),
    );

    let req = Request::builder()
        .method("GET")
        .uri("/me?user=bob&token=secret")
        .body(Body::empty())
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::OK);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn http_post_chat_with_bearer_auth() {
    let base = std::env::temp_dir().join(format!(
        "crafty-gateway-http-bearer-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let router = build_gateway_router(
        Arc::clone(&app),
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .identity(FixedToken)
            .routes(gateway_routes)
            .build_config(),
    );

    let req = Request::builder()
        .method("POST")
        .uri("/chat")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .header(axum::http::header::AUTHORIZATION, "Bearer secret")
        .header("x-crafty-user", "carol")
        .body(Body::from(r#"{"message":"hi"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::OK);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn http_post_without_auth_returns_401() {
    let base = std::env::temp_dir().join(format!(
        "crafty-gateway-http-401-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let router = build_gateway_router(
        Arc::clone(&app),
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .identity(FixedToken)
            .routes(gateway_routes)
            .build_config(),
    );

    let req = Request::builder()
        .method("POST")
        .uri("/chat")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"message":"nope"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
