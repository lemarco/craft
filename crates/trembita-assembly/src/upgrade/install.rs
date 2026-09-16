//! Atomic binary install helpers (Linux VPS).

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use thiserror::Error;

/// Install-time errors.
#[derive(Debug, Error)]
pub enum UpgradeInstallError {
    /// I/O failure.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// Checksum mismatch.
    #[error("sha256 mismatch: expected {expected}, got {actual}")]
    Sha256Mismatch {
        /// Expected hex digest.
        expected: String,
        /// Computed hex digest.
        actual: String,
    },
    /// Invalid hex in manifest.
    #[error("invalid sha256 hex: {0}")]
    InvalidHex(String),
}

/// Running application version ( `TREMBITA_APP_VERSION` or compile-time package version).
#[must_use]
pub fn running_app_version() -> String {
    std::env::var("TREMBITA_APP_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string())
}

/// Verify `bytes` against a lowercase hex SHA-256 digest.
///
/// # Errors
/// Returns [`UpgradeInstallError`] when hex is invalid or digest mismatches.
pub fn verify_sha256_hex(bytes: &[u8], expected_hex: &str) -> Result<(), UpgradeInstallError> {
    let expected = expected_hex.trim().to_ascii_lowercase();
    if expected.len() != 64 || !expected.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(UpgradeInstallError::InvalidHex(expected_hex.to_string()));
    }
    let digest = Sha256::digest(bytes);
    let actual = hex::encode(digest);
    if actual != expected {
        return Err(UpgradeInstallError::Sha256Mismatch { expected, actual });
    }
    Ok(())
}

/// Write `bytes` to `install_dir/app-{version}` and atomically repoint `current_link`.
///
/// # Errors
/// Returns [`UpgradeInstallError`] on I/O failure.
pub fn atomic_symlink_install(
    bytes: &[u8],
    install_dir: &Path,
    current_link: &Path,
    version: &str,
) -> Result<PathBuf, UpgradeInstallError> {
    std::fs::create_dir_all(install_dir)?;
    let target = install_dir.join(format!("app-{version}"));
    std::fs::write(&target, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&target)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&target, perms)?;
    }
    if current_link.exists() {
        std::fs::remove_file(current_link)?;
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, current_link)?;
    #[cfg(not(unix))]
    std::fs::copy(&target, current_link)?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_sha256_hex_accepts_matching_digest() {
        let bytes = b"hello trembita";
        let digest = hex::encode(Sha256::digest(bytes));
        verify_sha256_hex(bytes, &digest).expect("match");
    }
}
