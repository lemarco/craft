//! [`GatewaySessionStore`](super::cluster_session::GatewaySessionStore) adapter for Postgres (B-46).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use trembita_gateway_session_postgres::PgGatewaySessionStore;

use super::cluster_session::{ClusterSessionError, GatewaySessionStore, VerifiedClusterSession};
use crate::capstore::CapStoreError as StoreError;

impl GatewaySessionStore for PgGatewaySessionStore {
    fn register_boxed<'a>(
        &'a self,
        user: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String, StoreError>> + Send + 'a>> {
        Box::pin(async move { self.register(user, ttl).await })
    }

    fn verify_boxed<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<VerifiedClusterSession, ClusterSessionError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match self.lookup(token).await {
                Ok(trembita_gateway_session_postgres::PgSessionLookup::Live(row)) => {
                    let expires_at = u64::try_from(row.expires_at_ms).unwrap_or(u64::MAX);
                    Ok(VerifiedClusterSession {
                        user: row.user,
                        expires_at,
                    })
                }
                Ok(trembita_gateway_session_postgres::PgSessionLookup::Expired) => {
                    Err(ClusterSessionError::Expired)
                }
                Ok(trembita_gateway_session_postgres::PgSessionLookup::NotFound) => {
                    Err(ClusterSessionError::NotRegistered)
                }
                Err(_) => Err(ClusterSessionError::NotRegistered),
            }
        })
    }

    fn revoke_boxed<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + 'a>> {
        Box::pin(async move { self.revoke(token).await })
    }
}

/// [`SessionGate`](trembita_http::SessionGate) backed by Postgres opaque sessions.
#[must_use]
pub fn pg_gateway_session_gate(
    store: Arc<PgGatewaySessionStore>,
    cookie: trembita_http::CookieConfig,
) -> trembita_http::SessionGate {
    let registry: Arc<dyn GatewaySessionStore> = store;
    use trembita_http::{SessionGate, session_verifier};

    use super::cluster_session::CapStoreSessionVerifier;

    SessionGate::from_verifier(
        session_verifier(CapStoreSessionVerifier::new(registry)),
        cookie,
    )
}
