//! Snapshot backup and restore for multi-group `data_dir` layouts.

use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use opendal::Operator;
use opendal::services::{Fs, Gcs, S3};
use tar::{Archive, Builder};
use thiserror::Error;
use walkdir::WalkDir;

/// Backup/restore errors.
#[derive(Debug, Error)]
pub enum OpsError {
    /// Local filesystem I/O failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Object storage backend failure.
    #[error("object store: {0}")]
    ObjectStore(Box<opendal::Error>),
    /// Invalid paths, layout, or other operational error.
    #[error("{0}")]
    Other(String),
}

impl From<opendal::Error> for OpsError {
    fn from(value: opendal::Error) -> Self {
        Self::ObjectStore(Box::new(value))
    }
}

/// Pack every file under `data_dir` into `archive` (gzip tar).
///
/// # Errors
///
/// Returns [`OpsError::Other`] when `data_dir` is missing or not a directory, or when a
/// walked path cannot be stripped relative to `data_dir`. Propagates I/O and tar errors.
pub fn export_local(data_dir: &Path, archive: &Path) -> Result<(), OpsError> {
    if !data_dir.is_dir() {
        return Err(OpsError::Other(format!(
            "data_dir {} is not a directory",
            data_dir.display()
        )));
    }
    let file = std::fs::File::create(archive)?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = Builder::new(enc);
    for entry in WalkDir::new(data_dir).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path == data_dir {
            continue;
        }
        let rel = path
            .strip_prefix(data_dir)
            .map_err(|e| OpsError::Other(e.to_string()))?;
        if entry.file_type().is_dir() {
            tar.append_dir(rel, path)?;
        } else if entry.file_type().is_file() {
            tar.append_path_with_name(path, rel)?;
        }
    }
    tar.finish()?;
    Ok(())
}

/// Extract `archive` into `data_dir` (creates the directory if needed).
///
/// # Errors
///
/// Propagates I/O, tar read/unpack, and path errors while extracting entries.
pub fn import_local(data_dir: &Path, archive: &Path) -> Result<(), OpsError> {
    std::fs::create_dir_all(data_dir)?;
    let file = std::fs::File::open(archive)?;
    let dec = GzDecoder::new(file);
    let mut archive = Archive::new(dec);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let out = data_dir.join(path);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?;
        }
        entry.unpack(out)?;
    }
    Ok(())
}

/// Upload a local file to object storage (`s3://bucket/key`, `gs://bucket/key`, `file:///dir/key`).
///
/// # Errors
///
/// Returns [`OpsError::Other`] for unsupported URIs. Propagates read/write and object-store errors.
pub async fn push_object(local: &Path, dest_uri: &str) -> Result<(), OpsError> {
    let ObjectTarget { op, key } = operator_for_uri(dest_uri)?;
    let bytes = tokio::fs::read(local).await?;
    op.write(&key, bytes).await?;
    Ok(())
}

/// Download from object storage into `local`.
///
/// # Errors
///
/// Returns [`OpsError::Other`] for unsupported URIs. Propagates object-store read and local write errors.
pub async fn pull_object(src_uri: &str, local: &Path) -> Result<(), OpsError> {
    let ObjectTarget { op, key } = operator_for_uri(src_uri)?;
    let bytes = op.read(&key).await?.to_vec();
    tokio::fs::write(local, bytes).await?;
    Ok(())
}

struct ObjectTarget {
    op: Operator,
    key: String,
}

fn operator_for_uri(uri: &str) -> Result<ObjectTarget, OpsError> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, key) = split_bucket_key(rest)?;
        let op = Operator::new(S3::default().bucket(&bucket))?.finish();
        return Ok(ObjectTarget { op, key });
    }
    if let Some(rest) = uri.strip_prefix("gs://") {
        let (bucket, key) = split_bucket_key(rest)?;
        let op = Operator::new(Gcs::default().bucket(&bucket))?.finish();
        return Ok(ObjectTarget { op, key });
    }
    if let Some(rest) = uri.strip_prefix("file://") {
        let (root, key) = split_file_root_key(rest)?;
        let root_str = root.to_string_lossy().into_owned();
        let op = Operator::new(Fs::default().root(&root_str))?.finish();
        return Ok(ObjectTarget {
            op,
            key: key.clone(),
        });
    }
    Err(OpsError::Other(format!(
        "unsupported URI {uri:?} (want s3://, gs://, or file://)"
    )))
}

fn split_bucket_key(rest: &str) -> Result<(String, String), OpsError> {
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| OpsError::Other(format!("URI missing object key after bucket: {rest:?}")))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(OpsError::Other(format!("invalid object URI: {rest:?}")));
    }
    Ok((bucket.to_string(), key.to_string()))
}

fn split_file_root_key(rest: &str) -> Result<(PathBuf, String), OpsError> {
    let path = Path::new(rest);
    let key = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| OpsError::Other(format!("file URI missing object name: {rest:?}")))?;
    let root = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("/"));
    Ok((root.to_path_buf(), key.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn export_import_roundtrip() {
        let data = tempdir().unwrap();
        let group = data.path().join("group-0.redb");
        std::fs::write(&group, b"snapshot").unwrap();
        let out = tempdir().unwrap();
        let archive = out.path().join("backup.tar.gz");
        export_local(data.path(), &archive).unwrap();
        let restore = tempdir().unwrap();
        import_local(restore.path(), &archive).unwrap();
        assert_eq!(
            std::fs::read(restore.path().join("group-0.redb")).unwrap(),
            b"snapshot"
        );
    }

    #[tokio::test]
    async fn file_uri_push_pull_roundtrip() {
        let dir = tempdir().unwrap();
        let local = dir.path().join("payload.bin");
        std::fs::write(&local, b"backup-bytes").unwrap();
        let uri = format!("file://{}/remote.bin", dir.path().display());
        push_object(&local, &uri).await.unwrap();
        let pulled = dir.path().join("out.bin");
        pull_object(&uri, &pulled).await.unwrap();
        assert_eq!(std::fs::read(pulled).unwrap(), b"backup-bytes");
    }
}
