//! Cluster-verifiable gateway session cookies (B-29) — any node validates without local RAM.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use trembita_capstore::CapStateStore;
use trembita_http::{
    CookieConfig, HttpError, SessionGate, SessionIssuer, SessionVerifier, VerifiedSession,
    session_verifier,
};

use crate::capstore::{CapStoreError as StoreError, store_get, store_set};

use super::identity::constant_time_eq;

const TOKEN_VERSION: &str = "v1";

/// Shared secret for signed session cookies (`TREMBITA_GATEWAY_SESSION_SECRET`).
#[derive(Clone)]
pub struct ClusterSessionSecret(Arc<[u8]>);

impl ClusterSessionSecret {
    /// Load from `TREMBITA_GATEWAY_SESSION_SECRET` (alias `GATEWAY_SESSION_SECRET`).
    ///
    /// # Errors
    /// Missing or empty env.
    pub fn from_env() -> Result<Self, ClusterSessionError> {
        let raw = std::env::var("TREMBITA_GATEWAY_SESSION_SECRET")
            .or_else(|_| std::env::var("GATEWAY_SESSION_SECRET"))
            .map_err(|_| ClusterSessionError::MissingSecret)?;
        Self::from_bytes(raw.trim())
    }

    /// Dev / single-node fallback — **not** safe for multi-node clusters.
    #[must_use]
    pub fn showcase_dev() -> Self {
        Self(Arc::from(
            b"trembita-realtime-showcase-dev-secret".as_slice(),
        ))
    }

    /// Build from explicit bytes (minimum 16 bytes).
    ///
    /// # Errors
    /// Empty or too short secret.
    pub fn from_bytes(secret: impl AsRef<[u8]>) -> Result<Self, ClusterSessionError> {
        let bytes = secret.as_ref();
        if bytes.len() < 16 {
            return Err(ClusterSessionError::WeakSecret);
        }
        Ok(Self(Arc::from(bytes)))
    }

    /// Issue a tamper-evident session token for `user` (session key for sticky routing).
    pub fn issue(&self, user: &str, ttl: Duration) -> Result<String, ClusterSessionError> {
        validate_gateway_session_user(user)?;
        let exp = unix_now().saturating_add(ttl.as_secs());
        let body = format!("{TOKEN_VERSION}|{exp}|{user}");
        let sig = mac_hex(self.0.as_ref(), body.as_bytes());
        Ok(format!("{body}|{sig}"))
    }

    /// Verify token and return the embedded session key.
    ///
    /// # Errors
    /// Malformed token, bad signature, or expiry.
    pub fn verify(&self, token: &str) -> Result<VerifiedClusterSession, ClusterSessionError> {
        let (body, sig) = token
            .rsplit_once('|')
            .ok_or(ClusterSessionError::Malformed)?;
        let expected = mac_hex(self.0.as_ref(), body.as_bytes());
        if !constant_time_eq(sig, &expected) {
            return Err(ClusterSessionError::BadSignature);
        }
        let mut parts = body.split('|');
        let ver = parts.next().ok_or(ClusterSessionError::Malformed)?;
        if ver != TOKEN_VERSION {
            return Err(ClusterSessionError::Malformed);
        }
        let exp: u64 = parts
            .next()
            .ok_or(ClusterSessionError::Malformed)?
            .parse()
            .map_err(|_| ClusterSessionError::Malformed)?;
        let user = parts.next().ok_or(ClusterSessionError::Malformed)?;
        if parts.next().is_some() {
            return Err(ClusterSessionError::Malformed);
        }
        validate_gateway_session_user(user)?;
        if exp <= unix_now() {
            return Err(ClusterSessionError::Expired);
        }
        Ok(VerifiedClusterSession {
            user: user.to_string(),
            expires_at: exp,
        })
    }
}

/// Parsed session after signature verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClusterSession {
    /// Sticky routing key (user id, room id, …).
    pub user: String,
    /// Unix seconds when the ticket expires.
    pub expires_at: u64,
}

/// Session validation failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterSessionError {
    /// `TREMBITA_GATEWAY_SESSION_SECRET` unset.
    #[error("TREMBITA_GATEWAY_SESSION_SECRET is not set")]
    MissingSecret,
    /// Secret too short.
    #[error("gateway session secret must be at least 16 bytes")]
    WeakSecret,
    /// Token structure invalid.
    #[error("malformed session token")]
    Malformed,
    /// HMAC mismatch.
    #[error("invalid session signature")]
    BadSignature,
    /// Past `exp`.
    #[error("session expired")]
    Expired,
    /// Cap store miss.
    #[error("session not found in cluster store")]
    NotRegistered,
}

impl ClusterSessionError {
    /// Map to HTTP 401 for gateway gates.
    pub fn unauthorized(self) -> HttpError {
        HttpError::Unauthorized(self.to_string())
    }
}

/// Marker row for cap-store session registry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClusterSessionRecord {
    /// Session key for sticky workers.
    pub user: String,
}

/// Session key constraints shared by signed cookies and opaque registry rows (B-46).
pub fn validate_gateway_session_user(user: &str) -> Result<(), ClusterSessionError> {
    if user.is_empty() || user.len() > 256 || user.contains('|') {
        return Err(ClusterSessionError::Malformed);
    }
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn mac_hex(secret: &[u8], body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret);
    hasher.update(body);
    hex::encode(hasher.finalize())
}

/// [`SessionGate`] that verifies cluster-signed cookies on every node.
#[must_use]
pub fn cluster_session_gate(secret: ClusterSessionSecret, cookie: CookieConfig) -> SessionGate {
    rotating_cluster_session_gate(SignedCookieSessionVerifier::new(secret), cookie)
}

/// [`SessionGate`] with optional previous secret for rotation (B-40).
#[must_use]
pub fn rotating_cluster_session_gate(
    verifier: SignedCookieSessionVerifier,
    cookie: CookieConfig,
) -> SessionGate {
    SessionGate::from_verifier(session_verifier(verifier), cookie)
}

/// Register an opaque token in [`CapStateStore`] (cluster-visible when store is shared/durable).
///
/// # Errors
/// Store write failure.
pub async fn register_capstore_session(
    store: &dyn CapStateStore,
    user: &str,
    ttl: Duration,
) -> Result<String, StoreError> {
    validate_gateway_session_user(user)
        .map_err(|_| StoreError::Backend("invalid session user".into()))?;
    let token = opaque_gateway_session_token(user);
    let key = capstore_session_key(&token);
    store_set(
        store,
        &key,
        &ClusterSessionRecord {
            user: user.to_string(),
        },
        Some(ttl),
    )
    .await?;
    Ok(token)
}

/// Remove opaque session token from cap store (logout).
///
/// # Errors
/// Store backend error.
pub async fn revoke_capstore_session(
    store: &dyn CapStateStore,
    token: &str,
) -> Result<(), StoreError> {
    store.delete(&capstore_session_key(token)).await
}

/// Resolve session key from a cap-store token.
///
/// # Errors
/// Missing row or store error mapped to unauthorized.
pub async fn verify_capstore_session(
    store: &dyn CapStateStore,
    token: &str,
) -> Result<VerifiedClusterSession, ClusterSessionError> {
    let key = capstore_session_key(token);
    let row = store_get::<ClusterSessionRecord>(store, &key)
        .await
        .map_err(|_| ClusterSessionError::NotRegistered)?;
    let Some(row) = row else {
        return Err(ClusterSessionError::NotRegistered);
    };
    Ok(VerifiedClusterSession {
        user: row.user,
        expires_at: 0,
    })
}

/// [`SessionGate`] backed by cap-store session rows (opaque cookie value).
#[must_use]
pub fn capstore_session_gate(store: Arc<dyn CapStateStore>, cookie: CookieConfig) -> SessionGate {
    let registry: Arc<dyn GatewaySessionStore> =
        Arc::new(CapStoreGatewaySessionStore::new(Arc::clone(&store)));
    SessionGate::from_verifier(
        session_verifier(CapStoreSessionVerifier::new(Arc::clone(&registry))),
        cookie,
    )
}

fn capstore_session_key(token: &str) -> String {
    format!("gw:sess:{token}")
}

/// Session key from a request cookie using cluster-signed validation.
///
/// # Errors
/// Missing cookie or invalid token.
pub fn session_user_from_cookie(
    cookie_name: &str,
    headers: &http::HeaderMap,
    secret: &ClusterSessionSecret,
) -> Result<String, HttpError> {
    let token = cookie_value(headers, cookie_name)
        .ok_or_else(|| HttpError::Unauthorized("missing session cookie".into()))?;
    secret
        .verify(token)
        .map(|v| v.user)
        .map_err(ClusterSessionError::unauthorized)
}

/// Session key from cookie via [`SignedCookieSessionVerifier`] (supports rotation).
///
/// # Errors
/// Missing cookie or failed verification.
pub async fn session_user_from_verifier(
    cookie_name: &str,
    headers: &http::HeaderMap,
    verifier: &SignedCookieSessionVerifier,
) -> Result<String, HttpError> {
    let token = cookie_value(headers, cookie_name)
        .ok_or_else(|| HttpError::Unauthorized("missing session cookie".into()))?
        .to_string();
    verifier.verify_boxed(token).await.map(|v| v.user)
}

fn cookie_value<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    let header = headers.get(http::header::COOKIE)?.to_str().ok()?;
    parse_cookie_header(header, name)
}

fn parse_cookie_header<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    header.split(';').find_map(|part| {
        let part = part.trim();
        part.strip_prefix(&prefix)
    })
}

/// Opaque cookie value for cap-store / Postgres session registries (B-40/B-46).
pub fn opaque_gateway_session_token(user: &str) -> String {
    let n = unix_now();
    let body = format!("opaque|{n}|{user}");
    format!("cs_{}", hex::encode(Sha256::digest(body.as_bytes())))
}

// --- B-40 session ports (logic vs storage) ------------------------------------

use std::future::Future;
use std::pin::Pin;

/// Cluster-visible session registry (opaque cookie tokens).
pub trait GatewaySessionStore: Send + Sync {
    /// Register `user`, return opaque cookie value.
    fn register_boxed<'a>(
        &'a self,
        user: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String, StoreError>> + Send + 'a>>;

    /// Resolve session key from opaque token.
    fn verify_boxed<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<VerifiedClusterSession, ClusterSessionError>> + Send + 'a>,
    >;

    /// Revoke opaque token (logout).
    fn revoke_boxed<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + 'a>> {
        let _ = token;
        Box::pin(async { Ok(()) })
    }
}

/// [`CapStateStore`] adapter for gateway session rows.
#[derive(Clone)]
pub struct CapStoreGatewaySessionStore {
    store: Arc<dyn CapStateStore>,
}

impl CapStoreGatewaySessionStore {
    /// Wrap a shared cap store (redb / Redis / Postgres when cluster-visible).
    #[must_use]
    pub fn new(store: Arc<dyn CapStateStore>) -> Self {
        Self { store }
    }
}

impl GatewaySessionStore for CapStoreGatewaySessionStore {
    fn register_boxed<'a>(
        &'a self,
        user: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String, StoreError>> + Send + 'a>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move { register_capstore_session(store.as_ref(), user, ttl).await })
    }

    fn verify_boxed<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<VerifiedClusterSession, ClusterSessionError>> + Send + 'a>,
    > {
        let store = Arc::clone(&self.store);
        let token = token.to_string();
        Box::pin(async move { verify_capstore_session(store.as_ref(), &token).await })
    }

    fn revoke_boxed<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + 'a>> {
        let store = Arc::clone(&self.store);
        let token = token.to_string();
        Box::pin(async move { revoke_capstore_session(store.as_ref(), &token).await })
    }
}

/// Signed cluster cookie verifier ([`ClusterSessionSecret`]).
#[derive(Clone)]
pub struct SignedCookieSessionVerifier {
    secret: ClusterSessionSecret,
    previous: Option<ClusterSessionSecret>,
}

impl SignedCookieSessionVerifier {
    /// Verify with a single secret.
    #[must_use]
    pub fn new(secret: ClusterSessionSecret) -> Self {
        Self {
            secret,
            previous: None,
        }
    }

    /// Current + optional previous secret for rotation.
    #[must_use]
    pub fn with_previous(
        current: ClusterSessionSecret,
        previous: Option<ClusterSessionSecret>,
    ) -> Self {
        Self {
            secret: current,
            previous,
        }
    }

    /// Load current (and optional `TREMBITA_GATEWAY_SESSION_SECRET_PREVIOUS`) from env.
    ///
    /// # Errors
    /// Missing or weak primary secret.
    pub fn from_env() -> Result<Self, ClusterSessionError> {
        let current = ClusterSessionSecret::from_env()?;
        let previous = std::env::var("TREMBITA_GATEWAY_SESSION_SECRET_PREVIOUS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| ClusterSessionSecret::from_bytes(s.trim()))
            .transpose()?;
        Ok(Self::with_previous(current, previous))
    }
}

impl SessionVerifier for SignedCookieSessionVerifier {
    fn verify_boxed<'a>(
        &'a self,
        token: String,
    ) -> Pin<Box<dyn Future<Output = Result<VerifiedSession, HttpError>> + Send + 'a>> {
        Box::pin(async move {
            match self.secret.verify(&token) {
                Ok(v) => Ok(VerifiedSession { user: v.user }),
                Err(primary) => {
                    if let Some(prev) = &self.previous {
                        prev.verify(&token)
                            .map(|v| VerifiedSession { user: v.user })
                            .map_err(|_| primary.clone().unauthorized())
                    } else {
                        Err(primary.unauthorized())
                    }
                }
            }
        })
    }
}

/// Signed cluster cookie issuer.
#[derive(Clone)]
pub struct SignedCookieSessionIssuer {
    secret: ClusterSessionSecret,
}

impl SignedCookieSessionIssuer {
    /// Issue tokens with `secret`.
    #[must_use]
    pub fn new(secret: ClusterSessionSecret) -> Self {
        Self { secret }
    }
}

impl SessionIssuer for SignedCookieSessionIssuer {
    fn issue_boxed<'a>(
        &'a self,
        user: String,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String, HttpError>> + Send + 'a>> {
        let secret = self.secret.clone();
        Box::pin(async move {
            secret
                .issue(&user, ttl)
                .map_err(|e| HttpError::Internal(e.to_string()))
        })
    }
}

/// Cap-store opaque token verifier.
#[derive(Clone)]
pub struct CapStoreSessionVerifier {
    store: Arc<dyn GatewaySessionStore>,
}

impl CapStoreSessionVerifier {
    /// Verify against `store`.
    #[must_use]
    pub fn new(store: Arc<dyn GatewaySessionStore>) -> Self {
        Self { store }
    }
}

impl SessionVerifier for CapStoreSessionVerifier {
    fn verify_boxed<'a>(
        &'a self,
        token: String,
    ) -> Pin<Box<dyn Future<Output = Result<VerifiedSession, HttpError>> + Send + 'a>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            store
                .verify_boxed(&token)
                .await
                .map(|v| VerifiedSession { user: v.user })
                .map_err(ClusterSessionError::unauthorized)
        })
    }
}

/// Cap-store opaque token issuer.
#[derive(Clone)]
pub struct CapStoreSessionIssuer {
    store: Arc<dyn GatewaySessionStore>,
}

impl CapStoreSessionIssuer {
    /// Issue via [`GatewaySessionStore::register_boxed`].
    #[must_use]
    pub fn new(store: Arc<dyn GatewaySessionStore>) -> Self {
        Self { store }
    }
}

impl SessionIssuer for CapStoreSessionIssuer {
    fn issue_boxed<'a>(
        &'a self,
        user: String,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String, HttpError>> + Send + 'a>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            store
                .register_boxed(&user, ttl)
                .await
                .map_err(|e| HttpError::Internal(e.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[test]
    fn issue_and_verify_round_trip() {
        let secret = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let token = secret
            .issue("alice", Duration::from_secs(3600))
            .expect("issue");
        let v = secret.verify(&token).expect("verify");
        assert_eq!(v.user, "alice");
    }

    #[test]
    fn verify_rejects_tampered_token() {
        let secret = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let mut token = secret.issue("bob", Duration::from_secs(60)).unwrap();
        token.push('x');
        assert_eq!(
            secret.verify(&token),
            Err(ClusterSessionError::BadSignature)
        );
    }

    #[test]
    fn verify_rejects_expired_ticket() {
        let secret = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let exp = unix_now().saturating_sub(120);
        let body = format!("{TOKEN_VERSION}|{exp}|alice");
        let sig = mac_hex(secret.0.as_ref(), body.as_bytes());
        let token = format!("{body}|{sig}");
        assert_eq!(secret.verify(&token), Err(ClusterSessionError::Expired));
    }

    #[test]
    fn verify_rejects_signature_from_different_secret() {
        let a = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let b = ClusterSessionSecret::from_bytes("fedcba9876543210").unwrap();
        let token = a.issue("alice", Duration::from_secs(3600)).unwrap();
        assert_eq!(b.verify(&token), Err(ClusterSessionError::BadSignature));
    }

    #[tokio::test]
    async fn capstore_gate_rejects_unknown_token() {
        let store = std::sync::Arc::new(crate::capstore::InMemoryStore::new());
        let cookie = CookieConfig::from_env("TEST", "sess");
        let gate = capstore_session_gate(store, cookie);
        let err = gate
            .authorize(&http::HeaderMap::new())
            .await
            .expect_err("missing cookie");
        assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::COOKIE,
            http::HeaderValue::from_static("sess=not-registered"),
        );
        let err = gate.authorize(&headers).await.expect_err("unknown token");
        assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn session_user_from_cookie_reads_verified_user() {
        let secret = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let token = secret.issue("carol", Duration::from_secs(60)).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::COOKIE,
            http::HeaderValue::from_str(&format!("sess={token}")).unwrap(),
        );
        let user = session_user_from_cookie("sess", &headers, &secret).expect("user");
        assert_eq!(user, "carol");
    }

    #[tokio::test]
    async fn capstore_register_and_verify() {
        let store = crate::capstore::InMemoryStore::new();
        let token = register_capstore_session(&store, "dave", Duration::from_secs(60))
            .await
            .expect("register");
        let v = verify_capstore_session(&store, &token)
            .await
            .expect("verify");
        assert_eq!(v.user, "dave");
    }

    #[test]
    fn b40_rotating_verifier_accepts_previous_secret_token() {
        let current = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let previous = ClusterSessionSecret::from_bytes("fedcba9876543210").unwrap();
        let token = previous
            .issue("rot-user", Duration::from_secs(3600))
            .expect("issue");
        let verifier = SignedCookieSessionVerifier::with_previous(current, Some(previous.clone()));
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let ok = rt.block_on(verifier.verify_boxed(token)).expect("verify");
        assert_eq!(ok.user, "rot-user");
    }

    #[tokio::test]
    async fn b40_revoke_capstore_session() {
        let store = crate::capstore::InMemoryStore::new();
        let token = register_capstore_session(&store, "frank", Duration::from_secs(60))
            .await
            .expect("register");
        revoke_capstore_session(&store, &token)
            .await
            .expect("revoke");
        assert_eq!(
            verify_capstore_session(&store, &token).await,
            Err(ClusterSessionError::NotRegistered)
        );
    }

    /// B-40 — [`SignedCookieSessionIssuer`] / [`SignedCookieSessionVerifier`] trait ports.
    #[tokio::test]
    async fn b40_signed_cookie_issuer_verifier_port_roundtrip() {
        let secret = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let issuer = SignedCookieSessionIssuer::new(secret.clone());
        let verifier = SignedCookieSessionVerifier::new(secret);
        let token = issuer
            .issue_boxed("port-user".into(), Duration::from_secs(3600))
            .await
            .expect("issue");
        let v = verifier.verify_boxed(token).await.expect("verify");
        assert_eq!(v.user, "port-user");
    }

    /// B-40 — cap-store [`SessionIssuer`] / [`SessionVerifier`] adapters.
    #[tokio::test]
    async fn b40_capstore_issuer_verifier_ports_roundtrip() {
        let inner: Arc<dyn CapStateStore> = Arc::new(crate::capstore::InMemoryStore::new());
        let store: Arc<dyn GatewaySessionStore> = Arc::new(CapStoreGatewaySessionStore::new(inner));
        let issuer = CapStoreSessionIssuer::new(Arc::clone(&store));
        let verifier = CapStoreSessionVerifier::new(store);
        let token = issuer
            .issue_boxed("store-user".into(), Duration::from_secs(120))
            .await
            .expect("issue");
        assert!(token.starts_with("cs_"));
        let v = verifier.verify_boxed(token).await.expect("verify");
        assert_eq!(v.user, "store-user");
    }

    #[test]
    fn b40_rotating_verifier_rejects_previous_when_not_configured() {
        let current = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let other = ClusterSessionSecret::from_bytes("fedcba9876543210").unwrap();
        let token = other.issue("ghost", Duration::from_secs(3600)).unwrap();
        let verifier = SignedCookieSessionVerifier::new(current);
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let err = rt
            .block_on(verifier.verify_boxed(token))
            .expect_err("must reject");
        assert!(matches!(err, HttpError::Unauthorized(_)));
    }

    #[test]
    fn b40_session_user_from_cookie_among_multiple_cookies() {
        let secret = ClusterSessionSecret::from_bytes("0123456789abcdef").unwrap();
        let token = secret.issue("multi", Duration::from_secs(60)).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::COOKIE,
            http::HeaderValue::from_str(&format!("foo=1; sess={token}; bar=2")).unwrap(),
        );
        let user = session_user_from_cookie("sess", &headers, &secret).expect("user");
        assert_eq!(user, "multi");
    }

    #[test]
    fn b46_validate_gateway_session_user_scenarios_table() {
        struct Row {
            user: &'static str,
            ok: bool,
        }
        let rows = [
            Row {
                user: "alice",
                ok: true,
            },
            Row {
                user: "",
                ok: false,
            },
            Row {
                user: "a|b",
                ok: false,
            },
        ];
        for row in rows {
            assert_eq!(
                validate_gateway_session_user(row.user).is_ok(),
                row.ok,
                "user={:?}",
                row.user
            );
        }
    }
}
