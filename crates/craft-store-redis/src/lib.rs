//! `craft-store-redis` — Redis implementation of
//! [`ActorStateStore`](craft_actor::ActorStateStore) ([ADR 021](../decisions/021-actor-state-redis.md)).
//!
//! Optional crate for externalizing stateful-actor data (backlog Track G) so it
//! survives a VPS crash: when the leader respawns a worker on another node it
//! reloads its keys from Redis (ADR 013, ADR 018). Consensus data stays in the
//! Raft [`StateMachine`](craft_actor::craft_core::StateMachine) — Redis holds
//! only workflow/session/idempotency state.
//!
//! ```no_run
//! use std::sync::Arc;
//! use craft_actor::ActorStateStore;
//! use craft_store_redis::RedisStore;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = RedisStore::connect("redis://127.0.0.1:6379").await?.with_prefix("orders:");
//! let store: Arc<dyn ActorStateStore> = Arc::new(store);
//! store.set("order:42", b"processing", None).await?;
//! assert_eq!(store.get("order:42").await?, Some(b"processing".to_vec()));
//! # Ok(())
//! # }
//! ```
//!
//! Connections use a [`ConnectionManager`](redis::aio::ConnectionManager),
//! which multiplexes over one connection and transparently reconnects. Real
//! Redis behavior is covered by a `testcontainers`-backed integration test
//! (ADR 029), gated `#[ignore]` so it runs in the heavy CI lane rather than on
//! every push.

pub use craft_actor;

use std::time::Duration;

use craft_actor::{ActorStateStore, BoxFuture, StoreError};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

/// Atomic compare-and-set. `KEYS[1]` = key; `ARGV[1]` = `"1"`/`"0"` whether an
/// expected value was supplied; `ARGV[2]` = expected bytes; `ARGV[3]` = new
/// value; `ARGV[4]` = TTL in ms (empty for none). Returns `1` on swap, `0`
/// otherwise. Binary-safe: all comparisons are on raw byte strings.
const CAS_LUA: &str = r"
local cur = redis.call('GET', KEYS[1])
local matches
if ARGV[1] == '1' then
  matches = (cur == ARGV[2])
else
  matches = (cur == false)
end
if not matches then return 0 end
if ARGV[4] == '' then
  redis.call('SET', KEYS[1], ARGV[3])
else
  redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
end
return 1
";

/// A Redis-backed [`ActorStateStore`].
#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
    prefix: String,
}

impl RedisStore {
    /// Connect to Redis at `url` (e.g. `redis://127.0.0.1:6379` or
    /// `rediss://…` for TLS) and build a multiplexed, auto-reconnecting store.
    ///
    /// # Errors
    /// [`StoreError::Backend`] if the URL is invalid or the initial connection
    /// cannot be established.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let client = redis::Client::open(url).map_err(backend)?;
        let conn = ConnectionManager::new(client).await.map_err(backend)?;
        Ok(Self {
            conn,
            prefix: String::new(),
        })
    }

    /// Namespace every key with `prefix` (e.g. `"orders:"`), isolating this
    /// store's keys from other tenants sharing the same Redis.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}{key}", self.prefix)
        }
    }
}

fn backend<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Backend(err.to_string())
}

impl ActorStateStore for RedisStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let value: Option<Vec<u8>> = conn.get(self.full_key(key)).await.map_err(backend)?;
            Ok(value)
        })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let full = self.full_key(key);
            match ttl {
                Some(ttl) => {
                    let ms = ttl.as_millis().max(1) as u64;
                    let _: () = conn.pset_ex(full, value, ms).await.map_err(backend)?;
                }
                None => {
                    let _: () = conn.set(full, value).await.map_err(backend)?;
                }
            }
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let _: () = conn.del(self.full_key(key)).await.map_err(backend)?;
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
            let mut conn = self.conn.clone();
            let ttl_arg =
                ttl.map_or_else(String::new, |d| (d.as_millis().max(1) as u64).to_string());
            let script = redis::Script::new(CAS_LUA);
            let mut invocation = script.key(self.full_key(key));
            invocation
                .arg(i32::from(expected.is_some()))
                .arg(expected.unwrap_or(&[]))
                .arg(value)
                .arg(ttl_arg);
            let swapped: i64 = invocation.invoke_async(&mut conn).await.map_err(backend)?;
            Ok(swapped == 1)
        })
    }
}
