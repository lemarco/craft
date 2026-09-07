//! S3-compatible object storage backend (feature `static-s3`).

use opendal::Operator;
use opendal::services::S3;
use thiserror::Error;

use super::{Precompressed, StaticResponse, StaticSiteError, mime};

/// How objects are delivered to the client.
#[derive(Clone, Debug)]
pub enum S3Delivery {
    /// Node fetches bytes and serves them (default).
    Proxy,
    /// Redirect to a CDN base URL + object key.
    Redirect {
        /// Public CDN origin (e.g. `https://cdn.example.com`).
        cdn_base: String,
    },
}

/// S3 / `MinIO` / R2 configuration.
#[derive(Clone, Debug)]
pub struct ObjectStoreConfig {
    /// Bucket name.
    pub bucket: String,
    /// Optional key prefix (`releases/v1/`).
    pub prefix: Option<String>,
    /// Custom endpoint (`MinIO`, R2, …).
    pub endpoint: Option<String>,
    /// AWS region.
    pub region: String,
    /// Proxy through the node or redirect to CDN.
    pub delivery: S3Delivery,
}

/// Object store operation failed.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// opendal error.
    #[error(transparent)]
    OpenDal(#[from] opendal::Error),
    /// Redirect response could not be built.
    #[error("redirect: {0}")]
    Redirect(String),
}

/// Live S3 operator handle.
pub struct ObjectStoreBackend {
    op: Operator,
    prefix: String,
    delivery: S3Delivery,
}

impl ObjectStoreBackend {
    /// Open the configured bucket using standard AWS env credentials (`AWS_*`).
    pub fn new(config: ObjectStoreConfig) -> Self {
        let mut builder = S3::default().bucket(&config.bucket).region(&config.region);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint(endpoint);
        }
        let op = Operator::new(builder).expect("s3 operator").finish();
        let prefix = config.prefix.unwrap_or_default();
        Self {
            op,
            prefix,
            delivery: config.delivery,
        }
    }

    fn object_key(&self, path: &str) -> String {
        let rel = path.trim_start_matches('/');
        let rel = if rel.is_empty() { "index.html" } else { rel };
        if self.prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), rel)
        }
    }

    /// Fetch object bytes or build a redirect response marker.
    pub async fn resolve(
        &self,
        path: &str,
        precompressed: Precompressed,
    ) -> Result<Option<StaticResponse>, StaticSiteError> {
        if let S3Delivery::Redirect { cdn_base } = &self.delivery {
            let key = self.object_key(path);
            let url = format!("{}/{}", cdn_base.trim_end_matches('/'), key);
            return Ok(Some(StaticResponse {
                body: Vec::new(),
                content_type: "text/plain".to_string(),
                content_encoding: None,
                redirect_to: Some(url),
            }));
        }

        self.fetch_object(path, precompressed).await
    }

    async fn fetch_object(
        &self,
        path: &str,
        precompressed: Precompressed,
    ) -> Result<Option<StaticResponse>, StaticSiteError> {
        let key = self.object_key(path);
        match self.op.read(&key).await {
            Ok(body) => Ok(Some(StaticResponse {
                body: body.to_vec(),
                content_type: mime::from_path(path),
                content_encoding: None,
                redirect_to: None,
            })),
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => {
                if precompressed == Precompressed::Auto {
                    for suffix in [".gz", ".br"] {
                        let alt = format!("{key}{suffix}");
                        if let Ok(body) = self.op.read(&alt).await {
                            let encoding = if suffix == ".gz" { "gzip" } else { "br" };
                            return Ok(Some(StaticResponse {
                                body: body.to_vec(),
                                content_type: mime::from_path(path),
                                content_encoding: Some(encoding.to_string()),
                                redirect_to: None,
                            }));
                        }
                    }
                }
                Ok(None)
            }
            Err(err) => Err(StaticSiteError::ObjectStore(Box::new(
                ObjectStoreError::OpenDal(err),
            ))),
        }
    }
}
