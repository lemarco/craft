//! B-29 — cluster-signed session cookies validated on any gateway node.

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use trembita::{
    ClusterSessionSecret, GatewayOpts, SignedCookieSessionIssuer, SignedCookieSessionVerifier,
    TrembitaApp, TrembitaConfigure, capstore_session_gate, cluster_session_gate,
    register_capstore_session, revoke_capstore_session, rotating_cluster_session_gate,
    session_user_from_cookie, session_user_from_verifier, verify_capstore_session,
};
use trembita_http::SessionIssuer;
use trembita_http::{CookieConfig, Gateway, RequestCtx, Response, RouteTable};

const SECRET_A: &str = "0123456789abcdef";
const SECRET_B: &str = "fedcba9876543210";

struct LoginIdentity;

impl trembita::GatewayIdentity for LoginIdentity {
    type Identity = String;

    async fn extract(
        &self,
        req: &trembita::GatewayRequest<'_>,
    ) -> Result<String, trembita::IdentityError> {
        Ok(req.query("user").unwrap_or_else(|| "anonymous".into()))
    }
}

fn cookie_config() -> CookieConfig {
    CookieConfig::from_env("TEST", "sess")
}

fn me_table(secret: ClusterSessionSecret) -> RouteTable {
    RouteTable::new().get_session("/me", move |ctx: RequestCtx| {
        let secret = secret.clone();
        async move {
            let user = session_user_from_cookie("sess", ctx.headers(), &secret)?;
            Ok(Response::text(StatusCode::OK, user))
        }
    })
}

fn gateway_with_session_routes(secret: ClusterSessionSecret) -> trembita::GatewayConfig {
    let cookie = cookie_config();
    let gate = cluster_session_gate(secret.clone(), cookie);
    let routes = me_table(secret);
    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .identity(LoginIdentity)
        .surfaces(move |_state| {
            Gateway::new(false)
                .dev_fallback_session(gate.clone())
                .dev_fallback(routes.clone())
        })
        .build_config()
}

async fn boot_app() -> Arc<TrembitaApp> {
    trembita_test_facade::boot_local_app(
        || {
            TrembitaApp::builder().configure(TrembitaConfigure {
                tick_period: Duration::from_millis(5),
                ..TrembitaConfigure::default()
            })
        },
        None,
    )
    .await
}

async fn get_me(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    cookie: &str,
) -> reqwest::Response {
    client
        .get(format!("http://{addr}/me"))
        .header("Cookie", cookie)
        .send()
        .await
        .expect("GET /me")
}

#[tokio::test]
async fn cluster_session_gate_accepts_signed_cookie_on_session_route() {
    let secret = ClusterSessionSecret::from_bytes(SECRET_A).unwrap();
    let token = secret.issue("alice", Duration::from_secs(3600)).unwrap();
    let config = gateway_with_session_routes(secret.clone());
    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();

    let denied = client
        .get(format!("http://{addr}/me"))
        .send()
        .await
        .expect("request");
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let ok = get_me(&client, addr, &format!("sess={token}")).await;
    assert_eq!(ok.status(), StatusCode::OK);
    assert_eq!(ok.text().await.expect("body"), "alice");
}

#[tokio::test]
async fn login_issues_set_cookie_then_session_route_reads_verified_user() {
    let secret = ClusterSessionSecret::from_bytes(SECRET_A).unwrap();
    let cookie = cookie_config();
    let gate = cluster_session_gate(secret.clone(), cookie.clone());

    let config = GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .identity(LoginIdentity)
        .surfaces({
            let gate = gate.clone();
            move |_state| {
                let routes = RouteTable::new()
                    .get_identity("/login", {
                        let gate = gate.clone();
                        let secret = secret.clone();
                        move |ctx: RequestCtx| {
                            let gate = gate.clone();
                            let secret = secret.clone();
                            async move {
                                let user =
                                    ctx.query_param("user").unwrap_or("anonymous").to_string();
                                let token =
                                    secret.issue(&user, Duration::from_secs(3600)).map_err(
                                        |e| trembita_http::HttpError::Internal(e.to_string()),
                                    )?;
                                let mut resp = Response::text(StatusCode::OK, user.clone());
                                gate.set_session_cookie(&mut resp, &token)?;
                                Ok(resp)
                            }
                        }
                    })
                    .get_session("/me", {
                        let secret = secret.clone();
                        move |ctx: RequestCtx| {
                            let secret = secret.clone();
                            async move {
                                let user =
                                    session_user_from_cookie("sess", ctx.headers(), &secret)?;
                                Ok(Response::text(StatusCode::OK, user))
                            }
                        }
                    });
                Gateway::new(false)
                    .dev_fallback_session(gate.clone())
                    .dev_fallback(routes)
            }
        })
        .build_config();

    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();

    let login = client
        .get(format!("http://{addr}/login?user=bob"))
        .send()
        .await
        .expect("login");
    assert_eq!(login.status(), StatusCode::OK);
    let set_cookie = login
        .headers()
        .get_all("set-cookie")
        .iter()
        .next()
        .and_then(|v| v.to_str().ok())
        .expect("Set-Cookie");
    assert!(set_cookie.starts_with("sess="));

    let me = client
        .get(format!("http://{addr}/me"))
        .header("Cookie", set_cookie.split(';').next().expect("cookie pair"))
        .send()
        .await
        .expect("me");
    assert_eq!(me.status(), StatusCode::OK);
    assert_eq!(me.text().await.expect("body"), "bob");
}

#[tokio::test]
async fn second_gateway_instance_accepts_cookie_signed_with_same_cluster_secret() {
    let secret = ClusterSessionSecret::from_bytes(SECRET_A).unwrap();
    let token = secret.issue("carol", Duration::from_secs(3600)).unwrap();
    let cookie = format!("sess={token}");

    let config_a = gateway_with_session_routes(secret.clone());
    let config_b = gateway_with_session_routes(secret.clone());

    let app_a = boot_app().await;
    let app_b = boot_app().await;
    let addr_a = trembita_test_facade::spawn_test_gateway(&app_a, config_a).await;
    let addr_b = trembita_test_facade::spawn_test_gateway(&app_b, config_b).await;
    let client = reqwest::Client::new();

    let on_a = get_me(&client, addr_a, &cookie).await;
    assert_eq!(on_a.status(), StatusCode::OK);
    assert_eq!(on_a.text().await.expect("body"), "carol");

    let on_b = get_me(&client, addr_b, &cookie).await;
    assert_eq!(on_b.status(), StatusCode::OK);
    assert_eq!(on_b.text().await.expect("body"), "carol");
}

#[tokio::test]
async fn b40_rotating_gate_accepts_token_signed_with_previous_secret() {
    let current = ClusterSessionSecret::from_bytes(SECRET_A).unwrap();
    let previous = ClusterSessionSecret::from_bytes(SECRET_B).unwrap();
    let token = previous
        .issue("rotating", Duration::from_secs(3600))
        .unwrap();
    let verifier = SignedCookieSessionVerifier::with_previous(current, Some(previous));
    let config = GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .surfaces({
            let gate = rotating_cluster_session_gate(verifier.clone(), cookie_config());
            move |_state| {
                let routes = RouteTable::new().get_session("/me", {
                    let verifier = verifier.clone();
                    move |ctx: RequestCtx| {
                        let verifier = verifier.clone();
                        async move {
                            let user = session_user_from_verifier("sess", ctx.headers(), &verifier)
                                .await?;
                            Ok(Response::text(StatusCode::OK, user))
                        }
                    }
                });
                Gateway::new(false)
                    .dev_fallback_session(gate)
                    .dev_fallback(routes)
            }
        })
        .build_config();
    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();
    let ok = get_me(&client, addr, &format!("sess={token}")).await;
    assert_eq!(ok.status(), StatusCode::OK);
    assert_eq!(ok.text().await.expect("body"), "rotating");
}

#[tokio::test]
async fn gateway_rejects_cookie_signed_with_different_cluster_secret() {
    let issuer = ClusterSessionSecret::from_bytes(SECRET_A).unwrap();
    let verifier = ClusterSessionSecret::from_bytes(SECRET_B).unwrap();
    let token = issuer.issue("mallory", Duration::from_secs(3600)).unwrap();

    let config = gateway_with_session_routes(verifier);
    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();

    let resp = get_me(&client, addr, &format!("sess={token}")).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn b40_signed_cookie_issuer_login_then_verifier_me_route() {
    let secret = ClusterSessionSecret::from_bytes(SECRET_A).unwrap();
    let issuer = SignedCookieSessionIssuer::new(secret.clone());
    let verifier = SignedCookieSessionVerifier::new(secret.clone());
    let cookie = cookie_config();
    let gate = rotating_cluster_session_gate(verifier.clone(), cookie.clone());

    let config = GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .identity(LoginIdentity)
        .surfaces({
            let gate = gate.clone();
            move |_state| {
                let routes = RouteTable::new()
                    .get_identity("/login", {
                        let gate = gate.clone();
                        let issuer = issuer.clone();
                        move |ctx: RequestCtx| {
                            let gate = gate.clone();
                            let issuer = issuer.clone();
                            async move {
                                let user =
                                    ctx.query_param("user").unwrap_or("anonymous").to_string();
                                let token = issuer
                                    .issue_boxed(user.clone(), Duration::from_secs(3600))
                                    .await?;
                                let mut resp = Response::text(StatusCode::OK, user.clone());
                                gate.set_session_cookie(&mut resp, &token)?;
                                Ok(resp)
                            }
                        }
                    })
                    .get_session("/me", {
                        let verifier = verifier.clone();
                        move |ctx: RequestCtx| {
                            let verifier = verifier.clone();
                            async move {
                                let user =
                                    session_user_from_verifier("sess", ctx.headers(), &verifier)
                                        .await?;
                                Ok(Response::text(StatusCode::OK, user))
                            }
                        }
                    });
                Gateway::new(false)
                    .dev_fallback_session(gate.clone())
                    .dev_fallback(routes)
            }
        })
        .build_config();

    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();

    let login = client
        .get(format!("http://{addr}/login?user=issuer-bob"))
        .send()
        .await
        .expect("login");
    assert_eq!(login.status(), StatusCode::OK);
    let set_cookie = login
        .headers()
        .get_all("set-cookie")
        .iter()
        .next()
        .and_then(|v| v.to_str().ok())
        .expect("Set-Cookie");

    let me = client
        .get(format!("http://{addr}/me"))
        .header("Cookie", set_cookie.split(';').next().expect("pair"))
        .send()
        .await
        .expect("me");
    assert_eq!(me.status(), StatusCode::OK);
    assert_eq!(me.text().await.expect("body"), "issuer-bob");
}

#[tokio::test]
async fn b40_capstore_revoked_session_returns_unauthorized_on_me() {
    let store: Arc<dyn trembita_capstore::CapStateStore> = Arc::new(trembita::InMemoryStore::new());
    let token = register_capstore_session(store.as_ref(), "revoked", Duration::from_secs(3600))
        .await
        .expect("register");
    revoke_capstore_session(store.as_ref(), &token)
        .await
        .expect("revoke");
    let cookie = cookie_config();
    let gate = capstore_session_gate(Arc::clone(&store), cookie);

    let config = GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .surfaces(move |_state| {
            let routes = RouteTable::new().get_session("/me", |_ctx: RequestCtx| async {
                Ok(Response::text(StatusCode::OK, "never"))
            });
            Gateway::new(false)
                .dev_fallback_session(gate.clone())
                .dev_fallback(routes)
        })
        .build_config();

    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();
    let resp = get_me(&client, addr, &format!("sess={token}")).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn capstore_session_gate_accepts_registered_token_on_http_route() {
    let store: Arc<dyn trembita_capstore::CapStateStore> = Arc::new(trembita::InMemoryStore::new());
    let token = register_capstore_session(store.as_ref(), "eve", Duration::from_secs(3600))
        .await
        .expect("register");
    let cookie = cookie_config();
    let gate = capstore_session_gate(Arc::clone(&store), cookie.clone());

    let config = GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .surfaces({
            let store = Arc::clone(&store);
            move |_state| {
                let store = Arc::clone(&store);
                let routes = RouteTable::new().get_session("/me", move |ctx: RequestCtx| {
                    let store = Arc::clone(&store);
                    async move {
                        let token = ctx.cookie("sess").ok_or_else(|| {
                            trembita_http::HttpError::Unauthorized("missing session cookie".into())
                        })?;
                        let user = verify_capstore_session(store.as_ref(), token)
                            .await
                            .map_err(|e| trembita_http::HttpError::Unauthorized(e.to_string()))?
                            .user;
                        Ok(Response::text(StatusCode::OK, user))
                    }
                });
                Gateway::new(false)
                    .dev_fallback_session(gate.clone())
                    .dev_fallback(routes)
            }
        })
        .build_config();

    let app = boot_app().await;
    let addr = trembita_test_facade::spawn_test_gateway(&app, config).await;
    let client = reqwest::Client::new();

    let ok = get_me(&client, addr, &format!("sess={token}")).await;
    assert_eq!(ok.status(), StatusCode::OK);
    assert_eq!(ok.text().await.expect("body"), "eve");
}
