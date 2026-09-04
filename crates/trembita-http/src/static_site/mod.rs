//! Static asset serving for product SPAs ([`StaticSite`]).
//!
//! Three backends share one Axum router shape:
//!
//! - [`StaticSource::Embedded`] — compile-time bytes (`include_dir!`, release artifact)
//! - [`StaticSource::Filesystem`] — serve from a directory (dev / staging)
//! - [`StaticSource::ObjectStore`] — fetch from S3-compatible storage (feature `static-s3`)
//!
//! Wire into [`HostRouter`](crate::HostRouter) or merge the router directly:
//!
//! ```rust
//! use trembita_http::{HostRouter, StaticSite, StaticSource};
//!
//! let site = StaticSite::new(StaticSource::filesystem("/var/www/client/dist"))
//!     .spa_fallback(true);
//! let app = HostRouter::new()
//!     .host("app.example.com", site.router())
//!     .build();
//! ```

mod embedded;
mod env;
mod filesystem;
mod mime;
mod serve;

#[cfg(feature = "static-s3")]
mod object_store;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, Response};
use thiserror::Error;

pub use embedded::{EmbeddedAssets, EmbeddedFile, embedded_from_dir};
pub use env::StaticSiteEnvError;
#[cfg(feature = "static-s3")]
pub use object_store::{ObjectStoreConfig, S3Delivery};
pub use serve::StaticResponse;

/// Where static bytes come from.
#[derive(Clone, Debug)]
pub enum StaticSource {
    /// Compile-time directory (typically `include_dir!("../fe/dist")`).
    Embedded(EmbeddedAssets),
    /// Read from a directory on disk (`vite build` output, staging mounts).
    Filesystem {
        /// Root directory containing built assets.
        root: PathBuf,
    },
    /// S3-compatible object storage (requires feature `static-s3`).
    #[cfg(feature = "static-s3")]
    ObjectStore(object_store::ObjectStoreConfig),
}

impl StaticSource {
    /// Serve from `root` on the local filesystem.
    #[must_use]
    pub fn filesystem(root: impl Into<PathBuf>) -> Self {
        Self::Filesystem { root: root.into() }
    }

    /// Serve compile-time embedded assets.
    #[must_use]
    pub fn embedded(assets: EmbeddedAssets) -> Self {
        Self::Embedded(assets)
    }

    /// Build from env vars prefixed with `prefix` (e.g. `TREMBITA_STATIC_CLIENT`).
    ///
    /// Reads `{prefix}_SOURCE` (`embedded` | `filesystem` | `s3`), plus backend-specific
    /// keys documented on [`StaticSite::from_env`].
    ///
    /// # Errors
    /// Returns [`StaticSiteEnvError`] when required variables are missing or invalid.
    pub fn from_env(prefix: &str) -> Result<Self, StaticSiteEnvError> {
        env::source_from_env(prefix)
    }
}

/// How precompressed sibling files (`.gz` / `.br`) are handled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Precompressed {
    /// Serve `.gz` / `.br` when `Accept-Encoding` matches and the file exists.
    #[default]
    Auto,
    /// Never serve precompressed variants.
    Off,
}

/// SPA + cache policy for a static site mounted on a hostname.
#[derive(Clone, Debug)]
pub struct StaticSite {
    source: StaticSource,
    spa_fallback: bool,
    index_cache_control: String,
    asset_cache_control: String,
    precompressed: Precompressed,
}

impl StaticSite {
    /// Create a site with defaults: SPA fallback on, sensible cache headers.
    #[must_use]
    pub fn new(source: StaticSource) -> Self {
        Self {
            source,
            spa_fallback: true,
            index_cache_control: "no-cache".to_string(),
            asset_cache_control: "public, max-age=31536000, immutable".to_string(),
            precompressed: Precompressed::default(),
        }
    }

    /// Parse `{prefix}_*` env vars into a configured site.
    ///
    /// | Variable | Values |
    /// |----------|--------|
    /// | `{prefix}_SOURCE` | `embedded` (default), `filesystem`, `s3` |
    /// | `{prefix}_ROOT` | required for `filesystem` |
    /// | `{prefix}_EMBED_DIR` | path to dist for compile-time embed (build script) |
    /// | `{prefix}_BUCKET` | required for `s3` |
    /// | `{prefix}_PREFIX` | optional object key prefix |
    /// | `{prefix}_ENDPOINT` | optional S3 endpoint URL |
    /// | `{prefix}_REGION` | optional region (default `us-east-1`) |
    ///
    /// Embedded source without `{prefix}_ROOT` requires assets passed to [`StaticSource::embedded`]
    /// at compile time — env alone cannot load embedded bytes at runtime.
    ///
    /// # Errors
    /// Returns [`StaticSiteEnvError`] when configuration is incomplete.
    pub fn from_env(prefix: &str) -> Result<Self, StaticSiteEnvError> {
        Ok(Self::new(StaticSource::from_env(prefix)?))
    }

    /// Unknown paths return `index.html` (except under `/assets/`).
    #[must_use]
    pub fn spa_fallback(mut self, enabled: bool) -> Self {
        self.spa_fallback = enabled;
        self
    }

    /// `Cache-Control` for HTML entrypoints (`index.html`, SPA fallback).
    #[must_use]
    pub fn index_cache_control(mut self, value: impl Into<String>) -> Self {
        self.index_cache_control = value.into();
        self
    }

    /// `Cache-Control` for fingerprinted assets (paths containing `.` under `/assets/`).
    #[must_use]
    pub fn asset_cache_control(mut self, value: impl Into<String>) -> Self {
        self.asset_cache_control = value.into();
        self
    }

    /// Whether to serve `.gz` / `.br` siblings when the client accepts them.
    #[must_use]
    pub fn precompressed(mut self, mode: Precompressed) -> Self {
        self.precompressed = mode;
        self
    }

    /// Axum router that serves all methods/paths from this site (use as host router or fallback).
    pub fn router(self) -> Router {
        let state = Arc::new(StaticSiteState::from(self));
        Router::new().fallback(move |req: Request<Body>| {
            let state = Arc::clone(&state);
            async move { state.serve(req).await }
        })
    }
}

#[derive(Clone)]
struct StaticSiteState {
    backend: StaticBackend,
    spa_fallback: bool,
    index_cache_control: String,
    asset_cache_control: String,
    precompressed: Precompressed,
}

impl From<StaticSite> for StaticSiteState {
    fn from(site: StaticSite) -> Self {
        Self {
            backend: StaticBackend::from(site.source),
            spa_fallback: site.spa_fallback,
            index_cache_control: site.index_cache_control,
            asset_cache_control: site.asset_cache_control,
            precompressed: site.precompressed,
        }
    }
}

#[derive(Clone)]
enum StaticBackend {
    Embedded(EmbeddedAssets),
    Filesystem {
        /// Site root on disk.
        root: PathBuf,
    },
    #[cfg(feature = "static-s3")]
    ObjectStore(Arc<object_store::ObjectStoreBackend>),
}

impl StaticBackend {
    fn from(source: StaticSource) -> Self {
        match source {
            StaticSource::Embedded(assets) => Self::Embedded(assets),
            StaticSource::Filesystem { root } => Self::Filesystem { root },
            #[cfg(feature = "static-s3")]
            StaticSource::ObjectStore(config) => {
                Self::ObjectStore(Arc::new(object_store::ObjectStoreBackend::new(config)))
            }
        }
    }
}

impl StaticSiteState {
    async fn serve(&self, req: Request<Body>) -> Response<Body> {
        let path = req.uri().path();
        match self.backend.resolve(path, self.precompressed).await {
            Ok(Some(response)) => response.into_response(
                path,
                &self.index_cache_control,
                &self.asset_cache_control,
                self.spa_fallback,
            ),
            Ok(None) if self.spa_fallback && !path.starts_with("/assets/") => {
                match self
                    .backend
                    .resolve("/index.html", self.precompressed)
                    .await
                {
                    Ok(Some(response)) => response.into_response(
                        "/index.html",
                        &self.index_cache_control,
                        &self.asset_cache_control,
                        false,
                    ),
                    Ok(None) => serve::not_found(),
                    Err(err) => serve::internal_error(&err),
                }
            }
            Ok(None) => serve::not_found(),
            Err(err) => serve::internal_error(&err),
        }
    }
}

/// Static site configuration or IO error.
#[derive(Debug, Error)]
pub enum StaticSiteError {
    /// Filesystem read failed.
    #[error("filesystem: {0}")]
    Filesystem(#[from] std::io::Error),
    /// Object store operation failed.
    #[cfg(feature = "static-s3")]
    #[error("object store: {0}")]
    ObjectStore(#[from] Box<object_store::ObjectStoreError>),
    /// Embedded asset missing.
    #[error("embedded asset not found: {path}")]
    NotFound {
        /// Request path.
        path: String,
    },
}

impl StaticBackend {
    async fn resolve(
        &self,
        path: &str,
        precompressed: Precompressed,
    ) -> Result<Option<StaticResponse>, StaticSiteError> {
        match self {
            Self::Embedded(assets) => Ok(embedded::resolve(assets, path, precompressed)),
            Self::Filesystem { root } => filesystem::resolve(root, path, precompressed).await,
            #[cfg(feature = "static-s3")]
            Self::ObjectStore(store) => store.resolve(path, precompressed).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    use super::*;
    use crate::HostRouter;

    fn sample_assets() -> EmbeddedAssets {
        EmbeddedAssets {
            files: HashMap::from([
                (
                    "index.html".to_string(),
                    EmbeddedFile {
                        bytes: b"<!doctype html><html></html>",
                        content_type: "text/html".to_string(),
                        encoding: None,
                    },
                ),
                (
                    "assets/app.js".to_string(),
                    EmbeddedFile {
                        bytes: b"console.log('hi')",
                        content_type: "application/javascript".to_string(),
                        encoding: None,
                    },
                ),
            ]),
        }
    }

    #[tokio::test]
    async fn embedded_serves_asset_and_spa_fallback() {
        let app = StaticSite::new(StaticSource::embedded(sample_assets())).router();

        let asset = Request::builder()
            .uri("/assets/app.js")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(asset).await.unwrap().status(),
            StatusCode::OK
        );

        let deep = Request::builder()
            .uri("/brands/123")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(deep).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn host_router_static_site_integration() {
        let site = StaticSite::new(StaticSource::embedded(sample_assets())).router();
        let app = HostRouter::new().host("app.example.com", site).build();

        let req = Request::builder()
            .uri("/assets/app.js")
            .header(header::HOST, "app.example.com")
            .body(Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }
}
