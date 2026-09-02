//! Server-only TLS for the admin HTTP port (admin TLS).
//!
//! Distinct from the mTLS crafty wire: admin TLS terminates HTTPS for probes and
//! dashboards without requiring client certificates.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;

/// PEM paths for the admin listener certificate chain and private key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminTlsPaths {
    /// Server certificate chain (`CRAFTY_ADMIN_TLS_CERT`).
    pub cert: PathBuf,
    /// Private key (`CRAFTY_ADMIN_TLS_KEY`).
    pub key: PathBuf,
}

/// Errors loading admin TLS material.
#[derive(Debug, Error)]
pub enum AdminTlsError {
    /// PEM file read failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid certificate chain or private key.
    #[error("tls config: {0}")]
    Config(String),
}

/// Load a rustls [`ServerConfig`] (no client auth) from PEM files.
///
/// # Errors
/// Returns [`AdminTlsError`] when files are missing or PEM parsing fails.
pub fn server_config(paths: &AdminTlsPaths) -> Result<Arc<ServerConfig>, AdminTlsError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = read_certs(&paths.cert)?;
    let key = read_private_key(&paths.key)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| AdminTlsError::Config(e.to_string()))?;
    Ok(Arc::new(config))
}

fn read_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, AdminTlsError> {
    let file = File::open(path)?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| AdminTlsError::Config(format!("parse certs in {}: {e}", path.display())))
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, AdminTlsError> {
    let file = File::open(path)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| AdminTlsError::Config(format!("parse key in {}: {e}", path.display())))?
        .ok_or_else(|| AdminTlsError::Config(format!("no private key in {}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_cert_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let paths = AdminTlsPaths {
            cert: dir.path().join("missing.pem"),
            key: dir.path().join("missing.key"),
        };
        assert!(server_config(&paths).is_err());
    }

    #[test]
    fn loads_self_signed_pem() {
        let dir = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        let paths = AdminTlsPaths {
            cert: cert_path,
            key: key_path,
        };
        assert!(server_config(&paths).is_ok());
    }
}
