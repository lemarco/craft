//! [`CraftyApp`] gateway identity extraction and [`SessionHandle`].

use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, Method, Uri};
use crafty::{
    CraftyGatewayState, GatewayIdentity, GatewayOpts, GatewayRequest, GatewayTokenIdentity,
    IdentityError, IdentityTypeError, SessionHandle, SessionKey,
};
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_leader};

struct FixedToken;

impl GatewayIdentity for FixedToken {
    type Identity = String;

    async fn extract(
        &self,
        req: &GatewayRequest<'_>,
    ) -> Result<String, IdentityError> {
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

    async fn extract(
        &self,
        req: &GatewayRequest<'_>,
    ) -> Result<UserIdentity, IdentityError> {
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
        "crafty-gateway-identity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(crafty::CraftyApp::builder().data_dir(&base), None).await;

    wait_for_crafty_leader(app.cluster()).await;
    advance(Duration::from_millis(50)).await;

    let state = CraftyGatewayState::with_identity(Arc::clone(&app), FixedToken);

    let uri: Uri = "/ws?user=alice&token=secret".parse().unwrap();
    let headers = HeaderMap::new();
    let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
    let extracted = state.extract_session(&req).await.expect("identity");
    assert_eq!(extracted.session_key(), "alice");
    assert_eq!(extracted.require::<String>().unwrap(), "alice");

    app.cluster().shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn identity_mapped_uses_custom_session_key() {
    let base = std::env::temp_dir().join(format!(
        "crafty-gateway-map-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(crafty::CraftyApp::builder().data_dir(&base), None).await;
    let state = CraftyGatewayState::with_identity_mapped(
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

    app.cluster().shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn session_handle_none_without_workers() {
    let base = std::env::temp_dir().join(format!(
        "crafty-session-handle-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = Arc::new(boot_local_app(crafty::CraftyApp::builder().data_dir(&base), None).await);
    wait_for_crafty_leader(app.cluster()).await;

    assert!(SessionHandle::open(&app, "missing", "user-1", None).is_none());

    app.cluster().shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn identity_error_status_codes() {
    assert_eq!(
        IdentityError::Unauthorized.status_code(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        IdentityError::Forbidden.status_code(),
        axum::http::StatusCode::FORBIDDEN
    );
}

#[test]
fn gateway_opts_build_config_includes_drain_timeout() {
    let config = GatewayOpts::new("127.0.0.1:8090".parse().unwrap())
        .with_jobs_api(true)
        .drain_timeout(Duration::from_secs(5))
        .build_config();
    assert!(config.jobs_api);
    assert_eq!(config.drain_timeout, Duration::from_secs(5));
}

#[test]
fn gateway_token_identity_from_env_type() {
    let _ = GatewayTokenIdentity::from_env();
}
