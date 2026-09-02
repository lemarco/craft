//! `trembita-store-redis` — Redis implementation of
//! [`ActorStateStore`] ([actor-state-redis](../decisions/actor-state-redis.md)).
//!
//! Optional crate for externalizing stateful-actor data (backlog Track G) so it
//! survives a VPS crash: when the leader respawns a worker on another node it
//! reloads its keys from Redis (cross-node-actors, supervisor-leader). Consensus data stays in the
//! Raft [`StateMachine`](trembita_runtime::trembita_core::StateMachine) — Redis holds
//! only workflow/session/idempotency state.
//!
//! ```no_run
//! use std::sync::Arc;
//! use trembita_actor_store::ActorStateStore;
//! use trembita_store_redis::RedisStore;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = RedisStore::connect("redis://127.0.0.1:6379").await?.with_prefix("orders:");
//! // Private CA:
//! // let ca = std::fs::read("/etc/redis/ca.pem")?;
//! // let store = RedisStore::connect_with_tls(
//! //     "rediss://redis.internal:6379",
//! //     &RedisTlsConfig::with_root_ca_pem(ca),
//! // )
//! // .await?
//! // .with_prefix("orders:");
//! let store: Arc<dyn ActorStateStore> = Arc::new(store);
//! store.set("order:42", b"processing", None).await?;
//! assert_eq!(store.get("order:42").await?, Some(b"processing".to_vec()));
//! # Ok(())
//! # }
//! ```
//!
//! Connections use a [`ConnectionManager`],
//! which multiplexes over one connection and transparently reconnects. Plain
//! `redis://` and TLS `rediss://` URLs are supported; pass a custom Redis CA
//! (and optional client cert) via [`RedisTlsConfig`] and
//! [`RedisStore::connect_with_tls`]. Real Redis behavior is covered by
//! `testcontainers`-backed integration tests (testing-strategy), gated `#[ignore]` so
//! they run in the heavy CI lane rather than on every push.

mod tls;

pub use tls::RedisTlsConfig;

use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use trembita_actor_store::{ActorStateStore, BoxFuture, StoreError};

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
    /// Connect to Redis at `url` (`redis://…` or `rediss://…` with a public /
    /// webpki-trusted CA) and build a multiplexed, auto-reconnecting store.
    ///
    /// For `rediss://` with a **private CA** or Redis **mTLS**, use
    /// [`connect_with_tls`](Self::connect_with_tls).
    ///
    /// # Errors
    /// [`StoreError::Backend`] if the URL is invalid or the initial connection
    /// cannot be established.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        Self::connect_client(redis::Client::open(url).map_err(backend)?).await
    }

    /// Connect to Redis over TLS at `url` (`rediss://…`) with explicit trust /
    /// client material. Use this when the Redis CA is not in the OS store.
    ///
    /// # Errors
    /// [`StoreError::Backend`] if TLS material is inconsistent, the URL is not
    /// `rediss://`, or the initial connection cannot be established.
    pub async fn connect_with_tls(url: &str, tls: &RedisTlsConfig) -> Result<Self, StoreError> {
        if !url.starts_with("rediss://") {
            return Err(StoreError::Backend(
                "connect_with_tls requires a rediss:// URL".into(),
            ));
        }
        let certs = tls::redis_tls_certificates(tls)?;
        let client = redis::Client::build_with_tls(url, certs).map_err(backend)?;
        Self::connect_client(client).await
    }

    async fn connect_client(client: redis::Client) -> Result<Self, StoreError> {
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
                    let ms = u64::try_from(ttl.as_millis().max(1)).expect("ttl fits redis px ms");
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
            let ttl_arg = ttl.map_or_else(String::new, |d| {
                u64::try_from(d.as_millis().max(1))
                    .expect("ttl fits redis px ms")
                    .to_string()
            });
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
