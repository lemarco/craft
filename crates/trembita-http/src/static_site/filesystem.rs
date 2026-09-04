//! Serve static files from a directory on disk.

use std::path::{Component, Path, PathBuf};

use tokio::fs;

use super::{Precompressed, StaticResponse, StaticSiteError, mime};

/// Resolve `path` under `root`, with optional precompressed siblings.
pub async fn resolve(
    root: &Path,
    path: &str,
    precompressed: Precompressed,
) -> Result<Option<StaticResponse>, StaticSiteError> {
    let rel = safe_relative_path(path)?;
    let full = root.join(&rel);
    if !full.starts_with(root) {
        return Ok(None);
    }

    if let Some(response) = read_file(&full).await? {
        return Ok(Some(response));
    }

    if precompressed == Precompressed::Auto {
        let gz = PathBuf::from(format!("{}.gz", full.display()));
        if let Some(response) = read_precompressed(&gz, "gzip", &rel).await? {
            return Ok(Some(response));
        }

        let br_path = PathBuf::from(format!("{}.br", full.display()));
        if let Some(response) = read_precompressed(&br_path, "br", &rel).await? {
            return Ok(Some(response));
        }
    }

    Ok(None)
}

async fn read_file(path: &Path) -> Result<Option<StaticResponse>, StaticSiteError> {
    match fs::read(path).await {
        Ok(body) => Ok(Some(StaticResponse {
            body,
            content_type: mime::from_path(path.to_string_lossy().as_ref()),
            content_encoding: None,
            redirect_to: None,
        })),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StaticSiteError::Filesystem(err)),
    }
}

async fn read_precompressed(
    path: &Path,
    encoding: &str,
    rel: &str,
) -> Result<Option<StaticResponse>, StaticSiteError> {
    match fs::read(path).await {
        Ok(body) => Ok(Some(StaticResponse {
            body,
            content_type: mime::from_path(rel),
            content_encoding: Some(encoding.to_string()),
            redirect_to: None,
        })),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StaticSiteError::Filesystem(err)),
    }
}

fn safe_relative_path(path: &str) -> Result<String, StaticSiteError> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return Ok("index.html".to_string());
    }

    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(StaticSiteError::NotFound {
                    path: path.to_string(),
                });
            }
        }
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_path_maps_to_index_html() {
        assert_eq!(safe_relative_path("/").unwrap(), "index.html");
    }

    #[test]
    fn rejects_parent_traversal() {
        assert!(safe_relative_path("/../etc/passwd").is_err());
    }

    #[tokio::test]
    async fn serves_file_from_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("hello.txt"), b"hi").unwrap();
        let resp = resolve(dir.path(), "/hello.txt", Precompressed::Off)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resp.body, b"hi");
    }
}
