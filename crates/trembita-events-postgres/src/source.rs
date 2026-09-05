//! [`PgEventOutboxSource`] — Postgres `EventOutboxSource` implementation.

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use trembita_actor_store::BoxFuture;
use trembita_events::{EventOutboxError, EventOutboxSource, OutboxEvent};

use crate::schema::PgEventOutboxSchema;

fn backend(e: impl std::fmt::Display) -> EventOutboxError {
    EventOutboxError::Backend(e.to_string())
}

/// Postgres-backed transactional outbox source.
pub struct PgEventOutboxSource {
    pool: PgPool,
    schema: PgEventOutboxSchema,
}

impl PgEventOutboxSource {
    /// Connect with the default [`PgEventOutboxSchema`].
    ///
    /// # Errors
    /// Returns [`EventOutboxError::Backend`] when the pool cannot be created.
    pub async fn connect(database_url: &str) -> Result<Arc<Self>, EventOutboxError> {
        Self::connect_with_schema(database_url, PgEventOutboxSchema::default()).await
    }

    /// Connect with an explicit column mapping.
    ///
    /// # Errors
    /// Returns [`EventOutboxError::Backend`] when the pool or schema validation fails.
    pub async fn connect_with_schema(
        database_url: &str,
        schema: PgEventOutboxSchema,
    ) -> Result<Arc<Self>, EventOutboxError> {
        schema.validate().map_err(backend)?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(backend)?;
        Ok(Arc::new(Self { pool, schema }))
    }

    fn encode_payload(value: sqlx::types::Json<serde_json::Value>) -> Vec<u8> {
        serde_json::to_vec(&value.0).unwrap_or_default()
    }
}

impl EventOutboxSource for PgEventOutboxSource {
    fn poll(
        &self,
        after: Option<&[u8]>,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<OutboxEvent>, EventOutboxError>> {
        let after = after.map(<[u8]>::to_vec);
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        Box::pin(async move {
            schema.validate().map_err(backend)?;
            let table = &schema.table;
            let id = &schema.id_column;
            let payload = &schema.payload_column;
            let published = &schema.published_at_column;
            let order = &schema.order_column;
            let after_str = after
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned());

            let sql = format!(
                "SELECT {id}::text AS id, {payload} AS payload \
                 FROM {table} \
                 WHERE {published} IS NULL \
                   AND ($1::text IS NULL OR {id}::text > $1) \
                 ORDER BY {order} \
                 LIMIT $2"
            );

            let rows = sqlx::query(&sql)
                .bind(after_str)
                .bind(i64::try_from(max.max(1)).unwrap_or(i64::MAX))
                .fetch_all(&pool)
                .await
                .map_err(backend)?;

            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let id: String = row.try_get("id").map_err(backend)?;
                let payload_bytes = match row.try_get::<Vec<u8>, _>("payload") {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        let json: sqlx::types::Json<serde_json::Value> =
                            row.try_get("payload").map_err(backend)?;
                        PgEventOutboxSource::encode_payload(json)
                    }
                };
                out.push(OutboxEvent {
                    id: id.into_bytes(),
                    payload: payload_bytes,
                });
            }
            Ok(out)
        })
    }

    fn mark_published(&self, ids: &[Vec<u8>]) -> BoxFuture<'_, Result<(), EventOutboxError>> {
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let ids: Vec<String> = ids
            .iter()
            .filter_map(|id| String::from_utf8(id.clone()).ok())
            .collect();
        Box::pin(async move {
            if ids.is_empty() {
                return Ok(());
            }
            schema.validate().map_err(backend)?;
            let table = &schema.table;
            let id_col = &schema.id_column;
            let published = &schema.published_at_column;
            let sql = format!(
                "UPDATE {table} SET {published} = NOW() \
                 WHERE {id_col}::text = ANY($1::text[])"
            );
            sqlx::query(&sql)
                .bind(&ids)
                .execute(&pool)
                .await
                .map_err(backend)?;
            Ok(())
        })
    }
}
