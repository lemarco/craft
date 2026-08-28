//! Load mTLS material from PEM files on disk (cert-provisioning, cert-automation).
//!
//! Production deployments and cert-manager/step-ca renewers write to fixed paths;
//! [`load_pem_material`] re-reads them when certs rotate.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use crafty_proto::NodeId;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::tls::{NodeIdentity, TlsError, root_store};

/// Paths to the three PEM files a node reads at startup (cert-provisioning env vars).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertPaths {
    /// Leaf certificate chain (`CRAFTY_NODE_CERT`).
    pub node_cert: PathBuf,
    /// Private key (`CRAFTY_NODE_KEY`).
    pub node_key: PathBuf,
    /// Cluster trust anchor(s) (`CRAFTY_CA_CERT`).
    pub ca_cert: PathBuf,
}

impl CertPaths {
    /// Build paths from explicit locations.
    #[must_use]
    pub fn new(
        node_cert: impl Into<PathBuf>,
        node_key: impl Into<PathBuf>,
        ca_cert: impl Into<PathBuf>,
    ) -> Self {
        Self {
            node_cert: node_cert.into(),
            node_key: node_key.into(),
            ca_cert: ca_cert.into(),
        }
    }
}

/// Loaded identity + trust root, ready to build QUIC TLS configs.
#[derive(Clone)]
pub struct PemMaterial {
    /// This node's mTLS identity.
    pub identity: NodeIdentity,
    /// CA bundle used to verify peers and clients.
    pub roots: RootCertStore,
}

/// Read `paths` from disk and construct [`PemMaterial`] for `node_id`.
///
/// # Errors
/// Returns [`TlsError::Config`] if a file is missing or PEM parsing fails.
pub fn load_pem_material(node_id: NodeId, paths: &CertPaths) -> Result<PemMaterial, TlsError> {
    let cert_chain = read_certs(&paths.node_cert)?;
    let key = read_private_key(&paths.node_key)?;
    let ca_certs = read_certs(&paths.ca_cert)?;
    let identity = NodeIdentity::from_der(node_id, cert_chain, key);
    let roots = root_store(ca_certs.iter())?;
    Ok(PemMaterial { identity, roots })
}

/// A cheap fingerprint of on-disk cert files for rotation detection.
///
/// Compare successive values; a change means the operator or a renewer rewrote
/// at least one PEM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertFingerprint {
    node_cert: FileMeta,
    node_key: FileMeta,
    ca_cert: FileMeta,
}

impl CertFingerprint {
    /// Snapshot the current on-disk metadata for `paths`.
    ///
    /// # Errors
    /// Returns [`TlsError::Config`] if a path cannot be stat'd.
    pub fn read(paths: &CertPaths) -> Result<Self, TlsError> {
        Ok(Self {
            node_cert: file_meta(&paths.node_cert)?,
            node_key: file_meta(&paths.node_key)?,
            ca_cert: file_meta(&paths.ca_cert)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileMeta {
    len: u64,
    modified: Option<std::time::SystemTime>,
}

fn file_meta(path: &Path) -> Result<FileMeta, TlsError> {
    let meta = std::fs::metadata(path)
        .map_err(|e| TlsError::Config(format!("stat {}: {e}", path.display())))?;
    Ok(FileMeta {
        len: meta.len(),
        modified: meta.modified().ok(),
    })
}

fn read_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file =
        File::open(path).map_err(|e| TlsError::Config(format!("open {}: {e}", path.display())))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Config(format!("parse certs in {}: {e}", path.display())))
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file =
        File::open(path).map_err(|e| TlsError::Config(format!("open {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| TlsError::Config(format!("parse key in {}: {e}", path.display())))?
        .ok_or_else(|| TlsError::Config(format!("no private key in {}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use crate::tls::ClusterCa;

    #[test]
    fn fingerprint_changes_when_a_pem_is_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let ca = ClusterCa::generate().unwrap();
        let id = ca.issue_node(NodeId(1)).unwrap();
        let cert_path = dir.path().join("node.pem");
        let key_path = dir.path().join("node.key");
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&cert_path, id.cert_chain()[0].as_ref()).unwrap();
        std::fs::write(&key_path, b"dummy-key").unwrap();
        std::fs::write(&ca_path, ca.ca_cert().as_ref()).unwrap();

        let paths = CertPaths::new(&cert_path, &key_path, &ca_path);
        let before = CertFingerprint::read(&paths).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        std::fs::OpenOptions::new()
            .append(true)
            .open(&ca_path)
            .unwrap()
            .write_all(b"\n")
            .unwrap();
        let after = CertFingerprint::read(&paths).unwrap();
        assert_ne!(before, after);
    }
}
