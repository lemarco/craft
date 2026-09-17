//! PostgreSQL opaque gateway session registry (B-46).
//!
//! Wire through `GatewaySessionStore` on the `trembita` facade via the
//! `gateway-session-postgres` feature (avoids a crate dependency cycle).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};
use trembita_capstore::StoreError;

/// Column mapping for a Postgres gateway session table.
#[derive(Debug, Clone)]
pub struct PgGatewaySessionSchema {
    /// Table name (validated identifier).
    pub table: String,
}

impl Default for PgGatewaySessionSchema {
    fn default() -> Self {
        Self {
            table: "trembita_gateway_sessions".into(),
        }
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn ttl_to_expires_at_ms(ttl: Duration) -> i64 {
    now_ms().saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX))
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Opaque cookie token (same algorithm as `trembita::opaque_gateway_session_token`).
#[must_use]
pub fn opaque_gateway_session_token(user: &str) -> String {
    let n = unix_now_secs();
    let body = format!("opaque|{n}|{user}");
    format!("cs_{}", hex::encode(Sha256::digest(body.as_bytes())))
}

fn validate_session_user(user: &str) -> Result<(), StoreError> {
    if user.is_empty() || user.len() > 256 || user.contains('|') {
        return Err(StoreError::Backend("invalid session user".into()));
    }
    Ok(())
}

/// Result of [`PgGatewaySessionStore::lookup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgSessionLookup {
    /// Active session row.
    Live(PgVerifiedSession),
    /// No row for token.
    NotFound,
    /// Row existed but `expires_at_ms` is in the past (row deleted).
    Expired,
}

/// Verified row returned from [`PgGatewaySessionStore::verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgVerifiedSession {
    /// Sticky routing key.
    pub user: String,
    /// Expiry as unix millis.
    pub expires_at_ms: i64,
}

/// Postgres-backed opaque gateway session registry.
pub struct PgGatewaySessionStore {
    pool: PgPool,
    schema: PgGatewaySessionSchema,
}

impl PgGatewaySessionStore {
    /// `CREATE TABLE IF NOT EXISTS` for `schema`.
    #[must_use]
    pub fn ddl(schema: &PgGatewaySessionSchema) -> String {
        format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
    token TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL
);"#,
            schema.table
        )
    }

    fn ident(name: &str) -> Result<String, StoreError> {
        if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !name.is_empty() {
            Ok(name.to_string())
        } else {
            Err(StoreError::Backend(format!(
                "invalid sql identifier: {name:?}"
            )))
        }
    }

    /// Connect and ensure the table exists.
    ///
    /// # Errors
    /// Pool or DDL failure.
    pub async fn connect(database_url: &str, table: impl Into<String>) -> Result<Self, StoreError> {
        let schema = PgGatewaySessionSchema {
            table: table.into(),
        };
        Self::ident(&schema.table)?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let ddl = Self::ddl(&schema);
        sqlx::query(AssertSqlSafe(ddl))
            .execute(&pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self { pool, schema })
    }

    /// Use an existing pool and schema (caller runs DDL if needed).
    #[must_use]
    pub fn with_pool(pool: PgPool, schema: PgGatewaySessionSchema) -> Self {
        Self { pool, schema }
    }

    fn table(&self) -> &str {
        &self.schema.table
    }

    /// Register `user` and return opaque cookie token.
    ///
    /// # Errors
    /// Invalid user or SQL failure.
    pub async fn register(&self, user: &str, ttl: Duration) -> Result<String, StoreError> {
        validate_session_user(user)?;
        let token = opaque_gateway_session_token(user);
        let expires_at_ms = ttl_to_expires_at_ms(ttl);
        let table = Self::ident(self.table())?;
        let sql =
            format!("INSERT INTO {table} (token, user_name, expires_at_ms) VALUES ($1, $2, $3)");
        sqlx::query(AssertSqlSafe(sql))
            .bind(&token)
            .bind(user)
            .bind(expires_at_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(token)
    }

    /// Resolve session key from opaque token.
    ///
    /// # Errors
    /// SQL backend errors only.
    pub async fn lookup(&self, token: &str) -> Result<PgSessionLookup, StoreError> {
        let table = Self::ident(self.table())?;
        let sql = format!("SELECT user_name, expires_at_ms FROM {table} WHERE token = $1");
        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(token)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let Some(row) = row else {
            return Ok(PgSessionLookup::NotFound);
        };
        let user: String = row
            .try_get("user_name")
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let expires_at_ms: i64 = row
            .try_get("expires_at_ms")
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        if expires_at_ms <= now_ms() {
            let del = format!("DELETE FROM {table} WHERE token = $1");
            sqlx::query(AssertSqlSafe(del))
                .bind(token)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            return Ok(PgSessionLookup::Expired);
        }
        Ok(PgSessionLookup::Live(PgVerifiedSession {
            user,
            expires_at_ms,
        }))
    }

    /// `lookup` → `Some` only for live sessions (ergonomic helper).
    ///
    /// # Errors
    /// SQL backend errors only.
    pub async fn verify(&self, token: &str) -> Result<Option<PgVerifiedSession>, StoreError> {
        match self.lookup(token).await? {
            PgSessionLookup::Live(row) => Ok(Some(row)),
            PgSessionLookup::NotFound | PgSessionLookup::Expired => Ok(None),
        }
    }

    /// Remove opaque token (logout).
    ///
    /// # Errors
    /// SQL failure.
    pub async fn revoke(&self, token: &str) -> Result<(), StoreError> {
        let table = Self::ident(self.table())?;
        let sql = format!("DELETE FROM {table} WHERE token = $1");
        sqlx::query(AssertSqlSafe(sql))
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b46_ddl_default_table_name() {
        let ddl = PgGatewaySessionStore::ddl(&PgGatewaySessionSchema::default());
        assert!(ddl.contains("trembita_gateway_sessions"));
        assert!(ddl.contains("token TEXT PRIMARY KEY"));
    }

    #[test]
    fn b46_ident_rejects_invalid_table() {
        assert!(PgGatewaySessionStore::ident("bad-name").is_err());
        assert!(PgGatewaySessionStore::ident("ok_table").is_ok());
    }

    #[test]
    fn b46_opaque_token_prefix_and_validate_user() {
        let t = opaque_gateway_session_token("alice");
        assert!(t.starts_with("cs_"));
        assert!(validate_session_user("").is_err());
        assert!(validate_session_user("a|b").is_err());
        assert!(validate_session_user("ok").is_ok());
    }
}
