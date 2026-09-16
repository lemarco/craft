//! [`TrembitaApp`] gateway identity extraction and [`SessionHandle`].

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::sync::Arc;
use std::time::Duration;

use http::{HeaderMap, Method, StatusCode, Uri};
use trembita::{
    GatewayBearerIdentity, GatewayIdentity, GatewayOpts, GatewayRequest, GatewayTokenIdentity,
    IdentityError, IdentityTypeError, SessionHandle, SessionKey, TrembitaConfigure,
    TrembitaGatewayState,
};
use trembita_test_facade::{boot_local_app, gateway_jobs_surfaces, wait_for_trembita_app_leader};
use trembita_test_support::advance;

struct FixedToken;

impl GatewayIdentity for FixedToken {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        let user = req.query("user").ok_or(IdentityError::Unauthorized)?;
        let token = req.query("token").ok_or(IdentityError::Unauthorized)?;
        if token == "secret" {
            Ok(user)
        } else {
            Err(IdentityError::Unauthorized)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserIdentity {
    user_id: String,
    room: String,
}

impl SessionKey for UserIdentity {
    fn session_key(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(&self.user_id)
    }
}

struct RoomIdentity;

impl GatewayIdentity for RoomIdentity {
    type Identity = UserIdentity;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<UserIdentity, IdentityError> {
        let user = req.query("user").ok_or(IdentityError::Unauthorized)?;
        Ok(UserIdentity {
            user_id: user,
            room: "lobby".into(),
        })
    }
}

#[tokio::test(start_paused = true)]
async fn gateway_state_extracts_identity_session_key() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-identity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            trembita::TrembitaApp::builder().configure(
                TrembitaConfigure::default()
                    .with_local_gateway_apis()
                    .with_data_dir(&base),
            )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(50)).await;

    let state = TrembitaGatewayState::with_identity(Arc::clone(&app), FixedToken);

    let uri: Uri = "/ws?user=alice&token=secret".parse().unwrap();
    let headers = HeaderMap::new();
    let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
    let extracted = state.extract_session(&req).await.expect("identity");
    assert_eq!(extracted.session_key(), "alice");
    assert_eq!(extracted.require::<String>().unwrap(), "alice");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn identity_mapped_uses_custom_session_key() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-map-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            trembita::TrembitaApp::builder().configure(
                TrembitaConfigure::default()
                    .with_local_gateway_apis()
                    .with_data_dir(&base),
            )
        },
        None,
    )
    .await;
    let state = TrembitaGatewayState::with_identity_mapped(
        Arc::clone(&app),
        RoomIdentity,
        |u: &UserIdentity| u.room.clone(),
    );

    let uri: Uri = "/ws?user=alice".parse().unwrap();
    let headers = HeaderMap::new();
    let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
    let extracted = state.extract_session(&req).await.expect("identity");
    assert_eq!(extracted.session_key(), "lobby");
    let user = extracted.require::<UserIdentity>().expect("type");
    assert_eq!(user.user_id, "alice");
    assert_eq!(extracted.require::<String>(), Err(IdentityTypeError));

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn session_handle_none_without_workers() {
    let base = std::env::temp_dir().join(format!(
        "trembita-session-handle-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = Arc::new(
        boot_local_app(
            || {
                trembita::TrembitaApp::builder().configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(&base),
                )
            },
            None,
        )
        .await,
    );
    wait_for_trembita_app_leader(&app).await;

    assert!(SessionHandle::open(&app, "missing", "user-1", None).is_none());

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn identity_error_status_codes() {
    assert_eq!(
        IdentityError::Unauthorized.status_code(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        IdentityError::Forbidden.status_code(),
        StatusCode::FORBIDDEN
    );
}

#[test]
fn gateway_opts_build_config_includes_drain_timeout() {
    let config = GatewayOpts::new("127.0.0.1:8090".parse().unwrap())
        .surfaces(gateway_jobs_surfaces)
        .drain_timeout(Duration::from_secs(5))
        .build_config();
    assert_eq!(config.drain_timeout, Duration::from_secs(5));
}

#[test]
fn gateway_token_identity_from_env_type() {
    let _ = GatewayTokenIdentity::from_env();
}

#[tokio::test]
async fn bearer_identity_requires_user_header() {
    let identity = GatewayBearerIdentity::with_static_token("secret");

    let uri: Uri = "/api?user=alice".parse().expect("uri");
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer secret"),
    );
    let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
    assert_eq!(
        identity.extract(&req).await,
        Err(IdentityError::Unauthorized)
    );

    headers.insert("x-trembita-user", http::HeaderValue::from_static("alice"));
    let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
    assert_eq!(identity.extract(&req).await, Ok("alice".into()));
}

#[tokio::test]
async fn bearer_identity_rejects_query_token() {
    let identity = GatewayBearerIdentity::with_static_token("secret");

    let uri: Uri = "/ws?user=alice&token=secret".parse().expect("uri");
    let mut headers = HeaderMap::new();
    headers.insert("x-trembita-user", http::HeaderValue::from_static("alice"));
    let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
    assert_eq!(
        identity.extract(&req).await,
        Err(IdentityError::Unauthorized)
    );
}
