//! TLS options for [`super::RedisStore::connect_with_tls`] (`rediss://` URLs).

/// PEM material for a TLS Redis connection (`rediss://…`).
///
/// Redis TLS is **independent** of the craft cluster mTLS CA (ADR 021, ADR 024):
/// operators supply the Redis server's trust anchor (and optional client cert for
/// Redis mTLS) here.
#[derive(Clone, Default, Debug)]
pub struct RedisTlsConfig {
    /// Trust anchor in PEM form when the Redis CA is not in the OS / webpki store.
    /// Required for most self-hosted Redis TLS deployments.
    pub root_ca_pem: Option<Vec<u8>>,
    /// Client certificate PEM for Redis mTLS (must be paired with [`Self::client_key_pem`]).
    pub client_cert_pem: Option<Vec<u8>>,
    /// Client private key PEM for Redis mTLS (must be paired with [`Self::client_cert_pem`]).
    pub client_key_pem: Option<Vec<u8>>,
}

impl RedisTlsConfig {
    /// Trust anchor only (typical `rediss://` with a private CA).
    #[must_use]
    pub fn with_root_ca_pem(root_ca_pem: Vec<u8>) -> Self {
        Self {
            root_ca_pem: Some(root_ca_pem),
            ..Self::default()
        }
    }
}

pub(crate) fn redis_tls_certificates(
    tls: &RedisTlsConfig,
) -> Result<redis::TlsCertificates, super::StoreError> {
    let client_tls = match (&tls.client_cert_pem, &tls.client_key_pem) {
        (None, None) => None,
        (Some(client_cert), Some(client_key)) => Some(redis::ClientTlsConfig {
            client_cert: client_cert.clone(),
            client_key: client_key.clone(),
        }),
        _ => {
            return Err(super::StoreError::Backend(
                "Redis TLS client certificate and private key must both be set".into(),
            ));
        }
    };
    Ok(redis::TlsCertificates {
        client_tls,
        root_cert: tls.root_ca_pem.clone(),
    })
}
