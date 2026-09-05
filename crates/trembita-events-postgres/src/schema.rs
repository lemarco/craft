//! Column mapping for a Postgres outbox table.

/// Column mapping for [`super::PgEventOutboxSource`].
#[derive(Debug, Clone)]
pub struct PgEventOutboxSchema {
    /// Table name (validated identifier).
    pub table: String,
    /// Primary key column (`UUID` or `TEXT` — exposed as UTF-8 bytes to the drainer cursor).
    pub id_column: String,
    /// Payload column (`JSONB`, `BYTEA`, or `TEXT`).
    pub payload_column: String,
    /// Nullable timestamp/text column set when published (`NULL` = unpublished).
    pub published_at_column: String,
    /// Stable ordering column for `poll` (`created_at`, serial id, …).
    pub order_column: String,
}

impl Default for PgEventOutboxSchema {
    fn default() -> Self {
        Self {
            table: "outbox_events".into(),
            id_column: "id".into(),
            payload_column: "payload".into(),
            published_at_column: "published_at".into(),
            order_column: "created_at".into(),
        }
    }
}

impl PgEventOutboxSchema {
    /// Validate SQL identifiers.
    ///
    /// # Errors
    /// Returns an error string when any identifier is invalid.
    pub fn validate(&self) -> Result<(), String> {
        for name in [
            &self.table,
            &self.id_column,
            &self.payload_column,
            &self.published_at_column,
            &self.order_column,
        ] {
            if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') || name.is_empty() {
                return Err(format!("invalid sql identifier: {name:?}"));
            }
        }
        Ok(())
    }
}
