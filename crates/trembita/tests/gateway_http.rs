//! HTTP gateway end-to-end: identity → session → cast (POST + GET).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita::{
    AppManifest, GatewayOpts, OpenActorSessionError, TrembitaApp, TrembitaConfigure,
    TrembitaGatewayState, WorkerOpts, WorkerScale, workers,
};
use trembita_http::{Gateway, HttpError, RequestCtx, Response, RouteTable};
use trembita_runtime::{UserActor, actor};
use trembita_test_support::{
    advance, boot_local_app, eventually_default, spawn_test_gateway, wait_for_trembita_app_leader,
};

struct FixedToken;

impl trembita::GatewayIdentity for FixedToken {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(
        &self,
        req: &trembita::GatewayRequest<'_>,
    ) -> Result<String, trembita::IdentityError> {
        if let Some(bearer) = req.bearer_token() {
            if bearer != "secret" {
                return Err(trembita::IdentityError::Unauthorized);
            }
            return req
                .headers
                .get("x-trembita-user")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .ok_or(trembita::IdentityError::Unauthorized);
        }
        let user = req
            .query("user")
            .ok_or(trembita::IdentityError::Unauthorized)?;
        let token = req
            .query("token")
            .ok_or(trembita::IdentityError::Unauthorized)?;
        if token == "secret" {
            Ok(user)
        } else {
            Err(trembita::IdentityError::Unauthorized)
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

fn ctx_uri(ctx: &RequestCtx) -> http::Uri {
    let query = ctx.query();
    if query.is_empty() {
        ctx.path().parse().expect("path uri")
    } else {
        let qs = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{qs}", ctx.path())
            .parse()
            .expect("path+query uri")
    }
}

async fn post_chat(state: TrembitaGatewayState, ctx: RequestCtx) -> Result<Response, HttpError> {
    let uri = ctx_uri(&ctx);
    let mut handle = state
        .open_actor_session_parts(
            "echo",
            ctx.method(),
            &uri,
            ctx.headers(),
            Some(Duration::from_secs(60)),
        )
        .await
        .map_err(session_err)?;
    let body: ChatPost = ctx.json()?;
    let user = handle.session_key().to_string();
    let payload = trembita::proto::encode(&body.message).expect("encode");
    handle.cast(payload).await.expect("cast");
    Ok(Response::json(
        StatusCode::OK,
        serde_json::to_value(ChatAck { ok: true, user }).expect("json"),
    ))
}

async fn get_me(state: TrembitaGatewayState, ctx: RequestCtx) -> Result<Response, HttpError> {
    let uri = ctx_uri(&ctx);
    match state
        .extract_session_parts(ctx.method(), &uri, ctx.headers())
        .await
    {
        Ok(extracted) => Ok(Response::json(
            StatusCode::OK,
            serde_json::to_value(MeResponse {
                user: extracted.session_key().to_string(),
            })
            .expect("json"),
        )),
        Err(err) => Ok(err.into_http_response()),
    }
}

fn session_err(err: OpenActorSessionError) -> HttpError {
    match err {
        OpenActorSessionError::Identity(e) => HttpError::Unauthorized(e.to_string()),
        OpenActorSessionError::NoWorker(e) => HttpError::Internal(e.to_string()),
    }
}

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    let chat_state = state.clone();
    let me_state = state;
    Gateway::new(false).dev_fallback(
        RouteTable::new()
            .post("/chat", move |ctx: RequestCtx| {
                let st = chat_state.clone();
                async move { post_chat(st, ctx).await }
            })
            .get("/me", move |ctx: RequestCtx| {
                let st = me_state.clone();
                async move { get_me(st, ctx).await }
            }),
    )
}

async fn boot_with_workers(base: &std::path::Path) -> Arc<TrembitaApp> {
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(base),
                )
                .manifest(AppManifest::new().workers(workers!(
                    WorkerOpts::<EchoWorker>::new("echo")
                        .config(0)
                        .scale(WorkerScale::Fixed(1)),
                )))
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    reconcile_period: Duration::from_millis(20),
                    directory_publish_period: Duration::from_millis(20),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;
    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(500)).await;
    eventually_default("echo worker in directory", || {
        !app.cluster_ref("echo").is_empty()
    })
    .await;
    app
}

fn gateway_config() -> trembita::GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .identity(FixedToken)
        .surfaces(gateway_surfaces)
        .build_config()
}

#[tokio::test(start_paused = true)]
async fn http_post_chat_with_query_auth() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-http-post-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let addr = spawn_test_gateway(&app, gateway_config()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/chat?user=alice&token=secret"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .body(r#"{"message":"hello"}"#)
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), StatusCode::OK);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn extract_session_from_on_http_request() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-http-from-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let state = TrembitaGatewayState::with_identity(Arc::clone(&app), FixedToken);

    let req = http::Request::builder()
        .method("GET")
        .uri("/me?user=bob&token=secret")
        .body(())
        .unwrap();
    let extracted = state.extract_session_from(&req).await.expect("auth");
    assert_eq!(extracted.session_key(), "bob");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn http_get_me_route() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-http-get-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let addr = spawn_test_gateway(&app, gateway_config()).await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/me?user=bob&token=secret"))
        .header("Host", "127.0.0.1")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), StatusCode::OK);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn http_post_chat_with_bearer_auth() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-http-bearer-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let addr = spawn_test_gateway(&app, gateway_config()).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/chat"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .header("authorization", "Bearer secret")
        .header("x-trembita-user", "carol")
        .body(r#"{"message":"hi"}"#)
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), StatusCode::OK);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn http_post_without_auth_returns_401() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-http-401-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_with_workers(&base).await;
    let addr = spawn_test_gateway(&app, gateway_config()).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/chat"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .body(r#"{"message":"nope"}"#)
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
