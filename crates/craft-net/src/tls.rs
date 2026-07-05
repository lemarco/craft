//! mTLS configuration for the QUIC transport (ADR 006, `docs/protocol.md`).
//!
//! Every production network path is mutually authenticated: peers and clients
//! present a certificate issued by the cluster CA, and both ends verify the
//! other against that CA. This module builds the `quinn` server/client configs
//! from a [`NodeIdentity`] (this node's cert chain + private key) and a trust
//! root, and — under the `dev-certs` feature — can mint a self-signed
//! [`ClusterCa`] and per-node identities for the dev profile and tests.
//!
//! The `ring` crypto provider is selected explicitly rather than via a
//! process-wide default, so embedding craft never fights the host app over
//! `CryptoProvider::install_default`.

use std::sync::Arc;

use craft_proto::NodeId;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::RootCertStore;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;

/// ALPN protocol identifier negotiated on every craft QUIC connection.
pub const ALPN: &[u8] = b"craft/1";

/// The Common Name prefix used to bind a certificate to a [`NodeId`]
/// (`craft-node-<id>`).
pub const NODE_CN_PREFIX: &str = "craft-node-";

/// An error building a TLS configuration or (under `dev-certs`) a certificate.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A rustls/quinn configuration step failed (bad key, verifier, etc.).
    #[error("tls config: {0}")]
    Config(String),

    /// Certificate generation failed (`dev-certs`).
    #[cfg(feature = "dev-certs")]
    #[error("certificate generation: {0}")]
    Rcgen(#[from] rcgen::Error),
}

/// This node's TLS material: its certificate chain (leaf first, up to but not
/// including the trust root) and the matching private key, tagged with the
/// [`NodeId`] the certificate attests to.
#[derive(Debug)]
pub struct NodeIdentity {
    node_id: NodeId,
    cert_chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Clone for NodeIdentity {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            cert_chain: self.cert_chain.clone(),
            key: self.key.clone_key(),
        }
    }
}

impl NodeIdentity {
    /// Build an identity from an already-loaded DER cert chain and key (e.g.
    /// operator-provisioned production certs).
    #[must_use]
    pub fn from_der(
        node_id: NodeId,
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        Self {
            node_id,
            cert_chain,
            key,
        }
    }

    /// The node this identity attests to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The certificate chain (leaf first).
    #[must_use]
    pub fn cert_chain(&self) -> &[CertificateDer<'static>] {
        &self.cert_chain
    }
}

/// Build a [`RootCertStore`] trusting the given CA certificate(s).
///
/// # Errors
/// Returns [`TlsError::Config`] if a certificate cannot be added.
pub fn root_store<'a, I>(cas: I) -> Result<RootCertStore, TlsError>
where
    I: IntoIterator<Item = &'a CertificateDer<'a>>,
{
    let mut roots = RootCertStore::empty();
    for ca in cas {
        roots
            .add(ca.clone().into_owned())
            .map_err(|e| TlsError::Config(e.to_string()))?;
    }
    Ok(roots)
}

/// Build the `quinn` **server** config for `identity`, requiring every incoming
/// peer/client to present a certificate that chains to `roots` (mTLS).
///
/// # Errors
/// Returns [`TlsError::Config`] if the verifier, certificate, or QUIC crypto
/// cannot be constructed.
pub fn server_config(
    identity: &NodeIdentity,
    roots: RootCertStore,
) -> Result<quinn::ServerConfig, TlsError> {
    let provider = Arc::new(default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| TlsError::Config(e.to_string()))?;

    let mut crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Config(e.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.cert_chain.clone(), identity.key.clone_key())
        .map_err(|e| TlsError::Config(e.to_string()))?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicServerConfig::try_from(crypto).map_err(|e| TlsError::Config(e.to_string()))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(quic)))
}

/// Build the `quinn` **client** config for `identity`, presenting its
/// certificate for mutual auth and trusting servers that chain to `roots`.
///
/// # Errors
/// Returns [`TlsError::Config`] if the certificate or QUIC crypto cannot be
/// constructed.
pub fn client_config(
    identity: &NodeIdentity,
    roots: RootCertStore,
) -> Result<quinn::ClientConfig, TlsError> {
    let provider = Arc::new(default_provider());
    let mut crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Config(e.to_string()))?
        .with_root_certificates(roots)
        .with_client_auth_cert(identity.cert_chain.clone(), identity.key.clone_key())
        .map_err(|e| TlsError::Config(e.to_string()))?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicClientConfig::try_from(crypto).map_err(|e| TlsError::Config(e.to_string()))?;
    Ok(quinn::ClientConfig::new(Arc::new(quic)))
}

/// A self-signed cluster certificate authority for the dev profile and tests
/// (ADR 006). Issues per-node [`NodeIdentity`]s whose Common Name binds the
/// certificate to a [`NodeId`]. **Not** for production — real deployments feed
/// operator-provisioned certs to [`NodeIdentity::from_der`].
#[cfg(feature = "dev-certs")]
pub struct ClusterCa {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    ca_der: CertificateDer<'static>,
}

#[cfg(feature = "dev-certs")]
impl ClusterCa {
    /// Generate a brand-new self-signed cluster CA.
    ///
    /// # Errors
    /// Returns [`TlsError::Rcgen`] if key or certificate generation fails.
    pub fn generate() -> Result<Self, TlsError> {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};

        let mut params = CertificateParams::new(Vec::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "craft cluster CA");

        let key = KeyPair::generate()?;
        let ca_cert = params.self_signed(&key)?;
        let ca_der = ca_cert.der().clone();
        let issuer = Issuer::new(params, key);
        Ok(Self { issuer, ca_der })
    }

    /// Issue a fresh [`NodeIdentity`] for `node`, with `craft-node-<id>` as both
    /// the certificate Common Name and a DNS SAN (so it can also serve as the
    /// server name a peer dials).
    ///
    /// # Errors
    /// Returns [`TlsError::Rcgen`] if key or certificate generation fails.
    pub fn issue_node(&self, node: NodeId) -> Result<NodeIdentity, TlsError> {
        use rcgen::{CertificateParams, DnType, KeyPair};
        use rustls::pki_types::PrivatePkcs8KeyDer;

        let cn = node_server_name(node);
        let mut params = CertificateParams::new(vec![cn.clone()])?;
        params.distinguished_name.push(DnType::CommonName, cn);

        let key = KeyPair::generate()?;
        let cert = params.signed_by(&key, &self.issuer)?;
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));

        Ok(NodeIdentity {
            node_id: node,
            cert_chain: vec![cert_der],
            key: key_der,
        })
    }

    /// The CA certificate (DER), to be distributed as the cluster trust anchor.
    #[must_use]
    pub fn ca_cert(&self) -> &CertificateDer<'static> {
        &self.ca_der
    }

    /// A [`RootCertStore`] trusting only this CA.
    ///
    /// # Errors
    /// Returns [`TlsError::Config`] if the CA certificate cannot be added.
    pub fn root_store(&self) -> Result<RootCertStore, TlsError> {
        root_store([&self.ca_der])
    }
}

/// The DNS server-name (and certificate CN) for a node: `craft-node-<id>`.
#[must_use]
pub fn node_server_name(node: NodeId) -> String {
    format!("{NODE_CN_PREFIX}{}", node.0)
}
