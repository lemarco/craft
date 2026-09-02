//! PostgreSQL [`ExternalBacklog`] using `FOR UPDATE SKIP LOCKED`.
//!
//! Default table layout is documented in the crate README.

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use trembita_actor_store::BoxFuture;
use trembita_jobs::{BacklogError, BacklogItem, ExternalBacklog, Settlement};

/// Column mapping for a Postgres work table.
#[derive(Debug, Clone)]
pub struct PgBacklogSchema {
    /// Table name (validated identifier).
    pub table: String,
    /// Primary key / idempotency column.
    pub id_column: String,
    /// Payload column (`BYTEA`).
    pub payload_column: String,
    /// Priority column (`SMALLINT`).
    pub priority_column: String,
    /// Status column (`TEXT`).
    pub status_column: String,
    /// Error text column (`TEXT`, optional on settle).
    pub error_column: String,
    /// Attempt counter column (`INT`).
    pub attempts_column: String,
    /// Status value for claimable rows.
    pub pending_status: String,
    /// Status after successful job queue ack.
    pub done_status: String,
    /// Status after terminal failure / dead letter.
    pub failed_status: String,
    /// Status while claimed by trembita (in-flight window).
    pub claimed_status: String,
}

impl Default for PgBacklogSchema {
    fn default() -> Self {
        Self {
            table: "trembita_jobs".into(),
            id_column: "id".into(),
            payload_column: "payload".into(),
            priority_column: "priority".into(),
            status_column: "status".into(),
            error_column: "error".into(),
            attempts_column: "attempts".into(),
            pending_status: "pending".into(),
            done_status: "done".into(),
            failed_status: "failed".into(),
            claimed_status: "claimed".into(),
        }
    }
}

/// Postgres-backed external backlog.
pub struct PgBacklog {
    pool: PgPool,
    schema: PgBacklogSchema,
}

impl PgBacklog {
    /// Connect and use the default [`PgBacklogSchema`] for `table`.
    ///
    /// # Errors
    /// Returns [`BacklogError`] when the pool cannot be created.
    pub async fn connect(
        database_url: &str,
        table: impl Into<String>,
    ) -> Result<Self, BacklogError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| BacklogError::Backend(e.to_string()))?;
        Ok(Self {
            pool,
            schema: PgBacklogSchema {
                table: table.into(),
                ..PgBacklogSchema::default()
            },
        })
    }

    /// Connect with an explicit column mapping.
    ///
    /// # Errors
    /// Returns [`BacklogError`] when the pool cannot be created.
    pub async fn connect_with_schema(
        database_url: &str,
        schema: PgBacklogSchema,
    ) -> Result<Self, BacklogError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| BacklogError::Backend(e.to_string()))?;
        Ok(Self { pool, schema })
    }

    fn ident(name: &str) -> Result<String, BacklogError> {
        if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !name.is_empty() {
            Ok(name.to_string())
        } else {
            Err(BacklogError::Backend(format!(
                "invalid sql identifier: {name:?}"
            )))
        }
    }
}

impl ExternalBacklog for PgBacklog {
    fn depth(&self) -> BoxFuture<'_, Result<u64, BacklogError>> {
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        Box::pin(async move {
            let table = Self::ident(&schema.table)?;
            let status = Self::ident(&schema.status_column)?;
            let pending = schema.pending_status.replace('\'', "''");
            let claimed = schema.claimed_status.replace('\'', "''");
            let sql = format!(
                "SELECT COUNT(*)::BIGINT AS n FROM {table} WHERE {status} IN ('{pending}', '{claimed}')"
            );
            let row = sqlx::query(&sql)
                .fetch_one(&pool)
                .await
                .map_err(|e| BacklogError::Backend(e.to_string()))?;
            let n: i64 = row.get("n");
            Ok(u64::try_from(n).unwrap_or(u64::MAX))
        })
    }

    fn claim(&self, max: usize) -> BoxFuture<'_, Result<Vec<BacklogItem>, BacklogError>> {
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        Box::pin(async move {
            if max == 0 {
                return Ok(Vec::new());
            }
            let table = Self::ident(&schema.table)?;
            let id = Self::ident(&schema.id_column)?;
            let payload = Self::ident(&schema.payload_column)?;
            let priority = Self::ident(&schema.priority_column)?;
            let status = Self::ident(&schema.status_column)?;
            let pending = schema.pending_status.replace('\'', "''");
            let claimed = schema.claimed_status.replace('\'', "''");
            let sql = format!(
                "UPDATE {table} AS t SET {status} = '{claimed}' \
                 FROM ( \
                   SELECT {id} FROM {table} \
                   WHERE {status} = '{pending}' \
                   ORDER BY {priority} DESC, {id} \
                   FOR UPDATE SKIP LOCKED \
                   LIMIT $1 \
                 ) AS picked \
                 WHERE t.{id} = picked.{id} \
                 RETURNING t.{id}, t.{payload}, t.{priority}"
            );
            let rows = sqlx::query(&sql)
                .bind(i64::try_from(max).unwrap_or(i64::MAX))
                .fetch_all(&pool)
                .await
                .map_err(|e| BacklogError::Backend(e.to_string()))?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let key: String = row.get(id.as_str());
                let payload: Vec<u8> = row.get(payload.as_str());
                let priority: i16 = row.get(priority.as_str());
                out.push(BacklogItem {
                    key: key.into_bytes(),
                    payload,
                    priority: u8::try_from(priority.max(0)).unwrap_or(0),
                });
            }
            Ok(out)
        })
    }

    fn settle(&self, key: &[u8], outcome: Settlement) -> BoxFuture<'_, Result<(), BacklogError>> {
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let key = key.to_vec();
        Box::pin(async move {
            let table = Self::ident(&schema.table)?;
            let id = Self::ident(&schema.id_column)?;
            let status = Self::ident(&schema.status_column)?;
            let error = Self::ident(&schema.error_column)?;
            let attempts_col = Self::ident(&schema.attempts_column)?;
            let id_str = String::from_utf8_lossy(&key).into_owned();
            match outcome {
                Settlement::Done => {
                    let next = schema.done_status.replace('\'', "''");
                    let sql = format!("UPDATE {table} SET {status} = '{next}' WHERE {id} = $1");
                    sqlx::query(&sql)
                        .bind(id_str)
                        .execute(&pool)
                        .await
                        .map_err(|e| BacklogError::Backend(e.to_string()))?;
                }
                Settlement::Failed {
                    attempts,
                    error: msg,
                } => {
                    let next = schema.pending_status.replace('\'', "''");
                    let sql = format!(
                        "UPDATE {table} SET {status} = '{next}', {error} = $2, {attempts_col} = $3 \
                         WHERE {id} = $1"
                    );
                    sqlx::query(&sql)
                        .bind(id_str)
                        .bind(msg)
                        .bind(i32::try_from(attempts).unwrap_or(i32::MAX))
                        .execute(&pool)
                        .await
                        .map_err(|e| BacklogError::Backend(e.to_string()))?;
                }
                Settlement::DeadLettered {
                    attempts,
                    error: msg,
                } => {
                    let next = schema.failed_status.replace('\'', "''");
                    let sql = format!(
                        "UPDATE {table} SET {status} = '{next}', {error} = $2, {attempts_col} = $3 \
                         WHERE {id} = $1"
                    );
                    sqlx::query(&sql)
                        .bind(id_str)
                        .bind(msg)
                        .bind(i32::try_from(attempts).unwrap_or(i32::MAX))
                        .execute(&pool)
                        .await
                        .map_err(|e| BacklogError::Backend(e.to_string()))?;
                }
            }
            Ok(())
        })
    }
}

/// Shared handle for [`JobOpts::backlog`](https://docs.rs/trembita/latest/trembita/struct.JobOpts.html#method.backlog).
pub type SharedPgBacklog = Arc<PgBacklog>;
