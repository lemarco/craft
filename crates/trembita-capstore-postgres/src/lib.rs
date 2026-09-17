//! PostgreSQL [`CapStateStore`](trembita_capstore::CapStateStore) adapter.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};
use trembita_capstore::{BoxFuture, CapStateStore, StoreError};

/// Column mapping for a Postgres KV table.
#[derive(Debug, Clone)]
pub struct PgCapStoreSchema {
    /// Table name (validated identifier).
    pub table: String,
}

impl Default for PgCapStoreSchema {
    fn default() -> Self {
        Self {
            table: "trembita_capstore_kv".into(),
        }
    }
}

/// Postgres-backed capability workflow store.
pub struct PgCapStore {
    pool: PgPool,
    schema: PgCapStoreSchema,
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

impl PgCapStore {
    /// `CREATE TABLE IF NOT EXISTS` for `schema`.
    #[must_use]
    pub fn ddl(schema: &PgCapStoreSchema) -> String {
        format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL,
    expires_at_ms BIGINT
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
    /// Returns [`StoreError`] when the pool or DDL fails.
    pub async fn connect(database_url: &str, table: impl Into<String>) -> Result<Self, StoreError> {
        let schema = PgCapStoreSchema {
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

    fn table(&self) -> &str {
        &self.schema.table
    }

    async fn read_live(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let table = Self::ident(self.table())?;
        let sql = format!("SELECT value, expires_at_ms FROM {table} WHERE key = $1");
        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let expires_at_ms: Option<i64> = row.try_get("expires_at_ms").ok();
        if expires_at_ms.is_some_and(|ms| ms <= now_ms()) {
            let del = format!("DELETE FROM {table} WHERE key = $1");
            sqlx::query(AssertSqlSafe(del))
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            return Ok(None);
        }
        row.try_get("value")
            .map_err(|e| StoreError::Backend(e.to_string()))
    }
}

impl CapStateStore for PgCapStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move { self.read_live(key).await })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let table = Self::ident(self.table())?;
            let expires_at_ms = ttl
                .map(|d| now_ms().saturating_add(i64::try_from(d.as_millis()).unwrap_or(i64::MAX)));
            let sql = format!(
                "INSERT INTO {table} (key, value, expires_at_ms) VALUES ($1, $2, $3)
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at_ms = EXCLUDED.expires_at_ms"
            );
            sqlx::query(AssertSqlSafe(sql))
                .bind(key)
                .bind(value)
                .bind(expires_at_ms)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let table = Self::ident(self.table())?;
            let sql = format!("DELETE FROM {table} WHERE key = $1");
            sqlx::query(AssertSqlSafe(sql))
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            Ok(())
        })
    }

    fn compare_and_set<'a>(
        &'a self,
        key: &'a str,
        expected: Option<&'a [u8]>,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let current = self.read_live(key).await?;
            let matches = match (current.as_deref(), expected) {
                (None, None) => true,
                (Some(cur), Some(exp)) => cur == exp,
                _ => false,
            };
            if !matches {
                return Ok(false);
            }
            self.set(key, value, ttl).await?;
            Ok(true)
        })
    }
}
