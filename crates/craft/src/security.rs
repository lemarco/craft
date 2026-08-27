//! mTLS material for the QUIC transport (ADR 006): this node's certificate
//! identity plus the trust root every peer/client is verified against.
//!
//! Production deployments build a [`Security`] from operator-provisioned certs
//! ([`Security::new`] / [`Security::from_ca_certs`] / [`PemSecurity::load`](crate::certs::PemSecurity));
//! the dev profile and tests can mint a throwaway cluster CA with
//! [`Security::dev`] (requires the `dev-certs` feature).

use craft_net::tls::root_store;
use craft_net::{NodeIdentity, PemMaterial, TlsError};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

/// TLS configuration for one node: its [`NodeIdentity`] (cert chain + key) and
/// the [`RootCertStore`] used to verify peers and clients (mutual TLS).
pub struct Security {
    pub(crate) identity: NodeIdentity,
    pub(crate) roots: RootCertStore,
}

impl Security {
    /// Build from a fully-formed identity and trust root (maximum control).
    #[must_use]
    pub fn new(identity: NodeIdentity, roots: RootCertStore) -> Self {
        Self { identity, roots }
    }

    /// Build from an identity and the cluster CA certificate(s) in DER form,
    /// constructing the trust root for you.
    ///
    /// # Errors
    /// Returns [`TlsError`] if a CA certificate cannot be added to the store.
    pub fn from_ca_certs(
        identity: NodeIdentity,
        ca_certs: &[CertificateDer<'_>],
    ) -> Result<Self, TlsError> {
        let roots = root_store(ca_certs.iter())?;
        Ok(Self { identity, roots })
    }

    /// Build from material loaded off disk ([`craft_net::load_pem_material`]).
    #[must_use]
    pub fn from_material(material: PemMaterial) -> Self {
        Self {
            identity: material.identity,
            roots: material.roots,
        }
    }

    /// Mint a dev identity for `node_id` issued by `ca`, trusting only `ca`.
    /// **Not for production** — see [`craft_net::tls::ClusterCa`].
    ///
    /// # Errors
    /// Returns [`TlsError`] if certificate generation or the trust store fails.
    #[cfg(feature = "dev-certs")]
    pub fn dev(
        ca: &craft_net::tls::ClusterCa,
        node_id: craft_proto::NodeId,
    ) -> Result<Self, TlsError> {
        let identity = ca.issue_node(node_id)?;
        let roots = ca.root_store()?;
        Ok(Self { identity, roots })
    }
}
