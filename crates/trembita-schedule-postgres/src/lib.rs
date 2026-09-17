//! PostgreSQL [`ScheduleSource`] adapter (B-41).

use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};
use trembita_jobs::{RecurringJob, ScheduleError, ScheduleSource};
use trembita_proto::BoxFuture;

/// Column mapping for a Postgres schedules table.
#[derive(Debug, Clone)]
pub struct PgScheduleSchema {
    /// Table name (validated identifier).
    pub table: String,
    /// Unique schedule name column.
    pub name_column: String,
    /// Cron expression column.
    pub cron_column: String,
    /// Payload column (`BYTEA`).
    pub payload_column: String,
    /// Priority column.
    pub priority_column: String,
    /// Max attempts column.
    pub max_attempts_column: String,
    /// Enabled flag column.
    pub enabled_column: String,
    /// Calendar interval days column (`0` = cron mode).
    pub every_days_column: String,
    /// Anchor unix ms column.
    pub anchor_ms_column: String,
}

impl Default for PgScheduleSchema {
    fn default() -> Self {
        Self {
            table: "trembita_schedules".into(),
            name_column: "name".into(),
            cron_column: "cron".into(),
            payload_column: "payload".into(),
            priority_column: "priority".into(),
            max_attempts_column: "max_attempts".into(),
            enabled_column: "enabled".into(),
            every_days_column: "every_days".into(),
            anchor_ms_column: "anchor_ms".into(),
        }
    }
}

/// Postgres-backed schedule source.
pub struct PgScheduleSource {
    pool: PgPool,
    schema: PgScheduleSchema,
}

impl PgScheduleSource {
    /// Connect and use the default [`PgScheduleSchema`] for `table`.
    ///
    /// # Errors
    /// Pool or identifier validation failure.
    pub async fn connect(
        database_url: &str,
        table: impl Into<String>,
    ) -> Result<Self, ScheduleError> {
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .connect(database_url)
            .await
            .map_err(|e| ScheduleError::Backend(e.to_string()))?;
        Ok(Self {
            pool,
            schema: PgScheduleSchema {
                table: table.into(),
                ..PgScheduleSchema::default()
            },
        })
    }

    /// Use a custom column layout.
    #[must_use]
    pub fn with_schema(pool: PgPool, schema: PgScheduleSchema) -> Self {
        Self { pool, schema }
    }

    fn ident(name: &str) -> Result<String, ScheduleError> {
        if name.is_empty()
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || name.as_bytes()[0].is_ascii_digit()
        {
            return Err(ScheduleError::Backend(format!(
                "invalid sql identifier: {name:?}"
            )));
        }
        Ok(name.to_string())
    }

    async fn load(&self) -> Result<Vec<RecurringJob>, ScheduleError> {
        let schema = &self.schema;
        let table = Self::ident(&schema.table)?;
        let name = Self::ident(&schema.name_column)?;
        let cron = Self::ident(&schema.cron_column)?;
        let payload = Self::ident(&schema.payload_column)?;
        let priority = Self::ident(&schema.priority_column)?;
        let max_attempts = Self::ident(&schema.max_attempts_column)?;
        let enabled = Self::ident(&schema.enabled_column)?;
        let every_days = Self::ident(&schema.every_days_column)?;
        let anchor_ms = Self::ident(&schema.anchor_ms_column)?;
        let sql = format!(
            "SELECT {name}, {cron}, {payload}, {priority}, {max_attempts}, {enabled}, {every_days}, {anchor_ms} \
             FROM {table} WHERE {enabled} = TRUE"
        );
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ScheduleError::Backend(e.to_string()))?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row
                .try_get(0)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let cron: String = row
                .try_get(1)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let payload: Vec<u8> = row
                .try_get(2)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let priority: i16 = row
                .try_get(3)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let max_attempts: i32 = row
                .try_get(4)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let every_days: i32 = row
                .try_get(6)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let anchor_ms: i64 = row
                .try_get(7)
                .map_err(|e| ScheduleError::Backend(e.to_string()))?;
            let mut job = if every_days > 0 {
                RecurringJob::every_calendar_days(
                    name,
                    u32::try_from(every_days).unwrap_or(0),
                    u64::try_from(anchor_ms).unwrap_or(0),
                    payload,
                )
            } else {
                RecurringJob::new(name, cron, payload)
            };
            job.priority = u8::try_from(i32::from(priority).clamp(0, 255)).unwrap_or(0);
            job.max_attempts = u32::try_from(max_attempts.max(0)).unwrap_or(0);
            jobs.push(job);
        }
        Ok(jobs)
    }
}

impl ScheduleSource for PgScheduleSource {
    fn schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringJob>, ScheduleError>> {
        Box::pin(async move { self.load().await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b41_rejects_invalid_table_ident() {
        let err = PgScheduleSource::ident("bad-name").expect_err("ident");
        assert!(err.to_string().contains("invalid sql identifier"));
    }

    /// B-41 — SQL identifier validation for dynamic `SELECT` ([`PgScheduleSource::ident`]).
    #[test]
    fn b41_sql_ident_validation_scenarios_table() {
        struct Row {
            name: &'static str,
            ok: bool,
        }
        let rows = [
            Row {
                name: "trembita_schedules",
                ok: true,
            },
            Row {
                name: "valid_col_2",
                ok: true,
            },
            Row {
                name: "bad-name",
                ok: false,
            },
            Row {
                name: "9starts_with_digit",
                ok: false,
            },
            Row {
                name: "",
                ok: false,
            },
        ];
        for row in rows {
            let got = PgScheduleSource::ident(row.name);
            assert_eq!(got.is_ok(), row.ok, "ident={:?}", row.name);
        }
    }

    #[test]
    fn b41_default_schema_matches_documented_columns() {
        let s = PgScheduleSchema::default();
        assert_eq!(s.table, "trembita_schedules");
        assert_eq!(s.name_column, "name");
        assert_eq!(s.cron_column, "cron");
        assert_eq!(s.enabled_column, "enabled");
    }
}
