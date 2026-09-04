//! Environment-based static site configuration.

use std::env;
use std::path::PathBuf;

use thiserror::Error;

use super::StaticSource;

#[cfg(feature = "static-s3")]
use super::object_store::{ObjectStoreConfig, S3Delivery};

/// Env parsing failed.
#[derive(Debug, Error)]
pub enum StaticSiteEnvError {
    /// Required variable missing.
    #[error("{prefix}_{key} is required for source={mode}")]
    Missing {
        /// Env prefix (e.g. `TREMBITA_STATIC_CLIENT`).
        prefix: String,
        /// Variable suffix (`ROOT`, `BUCKET`, …).
        key: String,
        /// Selected source mode.
        mode: String,
    },
    /// Unknown source value.
    #[error("{prefix}_SOURCE={value}: expected embedded, filesystem, or s3")]
    UnknownSource {
        /// Env prefix.
        prefix: String,
        /// Invalid value.
        value: String,
    },
    /// Embedded source cannot be selected purely from runtime env.
    #[error("{prefix}_SOURCE=embedded requires compile-time EmbeddedAssets; use filesystem or s3 in env, or StaticSite::new(StaticSource::embedded(...))")]
    EmbeddedRequiresCompileTime {
        /// Env prefix.
        prefix: String,
    },
}

/// Parse `{prefix}_SOURCE` and backend-specific variables.
pub fn source_from_env(prefix: &str) -> Result<StaticSource, StaticSiteEnvError> {
    let source_key = format!("{prefix}_SOURCE");
    let source = env::var(&source_key).unwrap_or_else(|_| "filesystem".to_string());
    let source = source.to_ascii_lowercase();

    match source.as_str() {
        "embedded" => Err(StaticSiteEnvError::EmbeddedRequiresCompileTime {
            prefix: prefix.to_string(),
        }),
        "filesystem" | "fs" | "dir" => {
            let root_key = format!("{prefix}_ROOT");
            let root = env::var(&root_key).map_err(|_| StaticSiteEnvError::Missing {
                prefix: prefix.to_string(),
                key: "ROOT".to_string(),
                mode: source.clone(),
            })?;
            Ok(StaticSource::Filesystem {
                root: PathBuf::from(root),
            })
        }
        "s3" | "object-store" | "objectstore" => {
            #[cfg(feature = "static-s3")]
            {
                Ok(StaticSource::ObjectStore(object_store_from_env(prefix)?))
            }
            #[cfg(not(feature = "static-s3"))]
            {
                let _ = prefix;
                Err(StaticSiteEnvError::UnknownSource {
                    prefix: prefix.to_string(),
                    value: "s3 (feature static-s3 disabled)".to_string(),
                })
            }
        }
        other => Err(StaticSiteEnvError::UnknownSource {
            prefix: prefix.to_string(),
            value: other.to_string(),
        }),
    }
}

#[cfg(feature = "static-s3")]
fn object_store_from_env(prefix: &str) -> Result<ObjectStoreConfig, StaticSiteEnvError> {
    let bucket = env_var(prefix, "BUCKET", "s3")?;
    let prefix_key = env::var(format!("{prefix}_PREFIX")).ok();
    let endpoint = env::var(format!("{prefix}_ENDPOINT")).ok();
    let region = env::var(format!("{prefix}_REGION")).unwrap_or_else(|_| "us-east-1".to_string());
    let delivery = match env::var(format!("{prefix}_DELIVERY"))
        .unwrap_or_else(|_| "proxy".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "redirect" => S3Delivery::Redirect {
            cdn_base: env_var(prefix, "CDN_BASE", "s3")?,
        },
        _ => S3Delivery::Proxy,
    };

    Ok(ObjectStoreConfig {
        bucket,
        prefix: prefix_key,
        endpoint,
        region,
        delivery,
    })
}

fn env_var(prefix: &str, key: &str, source: &str) -> Result<String, StaticSiteEnvError> {
    env::var(format!("{prefix}_{key}")).map_err(|_| StaticSiteEnvError::Missing {
        prefix: prefix.to_string(),
        key: key.to_string(),
        mode: source.to_string(),
    })
}
