//! Cluster-verifiable gateway session cookies (B-29) — any node validates without local RAM.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use trembita_capstore::CapStateStore;
use trembita_http::{CookieConfig, HttpError, SessionGate};

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
        validate_session_user(user)?;
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
        validate_session_user(user)?;
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
    fn unauthorized(self) -> HttpError {
        HttpError::Unauthorized(self.to_string())
    }
}

/// Marker row for cap-store session registry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClusterSessionRecord {
    /// Session key for sticky workers.
    pub user: String,
}

fn validate_session_user(user: &str) -> Result<(), ClusterSessionError> {
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
    let secret = Arc::new(secret);
    SessionGate::validate(cookie.name.clone(), {
        let secret = Arc::clone(&secret);
        move |token| {
            let secret = Arc::clone(&secret);
            async move {
                secret
                    .verify(&token)
                    .map(|_| ())
                    .map_err(ClusterSessionError::unauthorized)
            }
        }
    })
    .with_cookie_config(cookie)
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
    validate_session_user(user).map_err(|_| StoreError::Backend("invalid session user".into()))?;
    let token = opaque_session_token(user);
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
    SessionGate::validate(cookie.name.clone(), move |token| {
        let store = Arc::clone(&store);
        async move {
            verify_capstore_session(store.as_ref(), &token)
                .await
                .map(|_| ())
                .map_err(ClusterSessionError::unauthorized)
        }
    })
    .with_cookie_config(cookie)
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

fn opaque_session_token(user: &str) -> String {
    let n = unix_now();
    let body = format!("opaque|{n}|{user}");
    format!("cs_{}", hex::encode(Sha256::digest(body.as_bytes())))
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
}
