//! Artifact download (`file://`, `http://`, `https://`).

use thiserror::Error;

/// Download errors.
#[derive(Debug, Error)]
pub enum UpgradeFetchError {
    /// Unsupported URL scheme.
    #[error("unsupported artifact url: {0}")]
    UnsupportedUrl(String),
    /// Local file read failed.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// HTTP client failure.
    #[error("http fetch failed: {0}")]
    Http(String),
}

/// Fetch artifact bytes from `url`.
///
/// Supports `file://` paths and remote HTTP(S) via `reqwest`.
///
/// # Errors
/// Returns [`UpgradeFetchError`] when the URL is unsupported or the fetch fails.
pub async fn fetch_artifact(url: &str) -> Result<Vec<u8>, UpgradeFetchError> {
    if let Some(path) = url.strip_prefix("file://") {
        return tokio::fs::read(path).await.map_err(Into::into);
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| UpgradeFetchError::Http(e.to_string()))?;
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| UpgradeFetchError::Http(e.to_string()))?;
        if !response.status().is_success() {
            return Err(UpgradeFetchError::Http(format!(
                "status {}",
                response.status()
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|e| UpgradeFetchError::Http(e.to_string()))?;
        return Ok(bytes.to_vec());
    }
    Err(UpgradeFetchError::UnsupportedUrl(url.to_string()))
}
