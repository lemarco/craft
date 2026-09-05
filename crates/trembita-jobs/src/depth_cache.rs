//! TTL cache for expensive [`ExternalBacklog::depth`](crate::ExternalBacklog::depth) queries.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use trembita_proto::BoxFuture;

use crate::{BacklogError, BacklogItem, ExternalBacklog, Settlement};

/// In-memory TTL cache for a single `depth()` value.
#[derive(Debug)]
pub struct DepthCache {
    ttl: Duration,
    inner: Mutex<Option<(Instant, u64)>>,
}

impl DepthCache {
    /// Cache hits expire after `ttl`.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new(None),
        }
    }

    /// Return a cached depth when still fresh.
    #[must_use]
    pub fn get(&self) -> Option<u64> {
        let guard = self.inner.lock().ok()?;
        let (at, value) = (*guard)?;
        (at.elapsed() < self.ttl).then_some(value)
    }

    /// Store a freshly computed depth.
    pub fn set(&self, value: u64) {
        if let Ok(mut guard) = self.inner.lock() {
            *guard = Some((Instant::now(), value));
        }
    }

    /// Drop any cached value (e.g. after `claim` / `settle`).
    pub fn invalidate(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            *guard = None;
        }
    }
}

/// Wraps an [`ExternalBacklog`] with a TTL-cached [`depth`](ExternalBacklog::depth).
pub struct CachedDepth<B> {
    inner: B,
    cache: Arc<DepthCache>,
}

impl<B> CachedDepth<B> {
    /// Wrap `inner`, caching `depth()` for `ttl`.
    #[must_use]
    pub fn new(inner: B, ttl: Duration) -> Self {
        Self {
            inner,
            cache: Arc::new(DepthCache::new(ttl)),
        }
    }

    /// Shared cache handle — invalidate from claim/settle hooks if needed.
    #[must_use]
    pub fn cache(&self) -> Arc<DepthCache> {
        Arc::clone(&self.cache)
    }
}

impl<B: ExternalBacklog> ExternalBacklog for CachedDepth<B> {
    fn depth(&self) -> BoxFuture<'_, Result<u64, BacklogError>> {
        if let Some(value) = self.cache.get() {
            return Box::pin(async move { Ok(value) });
        }
        let cache = Arc::clone(&self.cache);
        let fut = self.inner.depth();
        Box::pin(async move {
            let value = fut.await?;
            cache.set(value);
            Ok(value)
        })
    }

    fn claim(&self, max: usize) -> BoxFuture<'_, Result<Vec<BacklogItem>, BacklogError>> {
        self.cache.invalidate();
        self.inner.claim(max)
    }

    fn settle(&self, key: &[u8], outcome: Settlement) -> BoxFuture<'_, Result<(), BacklogError>> {
        self.cache.invalidate();
        self.inner.settle(key, outcome)
    }

    fn reclaim_abandoned_claims(&self) -> BoxFuture<'_, Result<u64, BacklogError>> {
        self.cache.invalidate();
        self.inner.reclaim_abandoned_claims()
    }

    fn release_claim(&self, key: &[u8]) -> BoxFuture<'_, Result<(), BacklogError>> {
        self.cache.invalidate();
        self.inner.release_claim(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BacklogItem, InMemoryExternalBacklog};

    #[test]
    fn cache_expires_after_ttl() {
        let cache = DepthCache::new(Duration::from_millis(1));
        cache.set(42);
        assert_eq!(cache.get(), Some(42));
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(cache.get(), None);
    }

    #[tokio::test]
    async fn cached_depth_wraps_external_backlog() {
        let inner = InMemoryExternalBacklog::new();
        inner.push(BacklogItem {
            key: b"k".to_vec(),
            payload: b"p".to_vec(),
            priority: 0,
        });
        let wrapped = CachedDepth::new(inner, Duration::from_secs(30));
        let d1 = wrapped.depth().await.unwrap();
        let d2 = wrapped.depth().await.unwrap();
        assert_eq!(d1, d2);
        assert_eq!(d1, 1);
    }
}
