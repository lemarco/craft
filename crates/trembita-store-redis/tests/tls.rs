//! Fast TLS config validation tests (no Docker).

use trembita_store_redis::{RedisStore, RedisTlsConfig};

#[tokio::test]
async fn connect_with_tls_rejects_plain_redis_url() {
    let Err(err) =
        RedisStore::connect_with_tls("redis://127.0.0.1:6379", &RedisTlsConfig::default()).await
    else {
        panic!("plain URL must be rejected");
    };
    assert!(
        err.to_string().contains("rediss://"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn tls_config_requires_matching_client_cert_and_key() {
    let Err(err) = RedisStore::connect_with_tls(
        "rediss://127.0.0.1:6379",
        &RedisTlsConfig {
            client_cert_pem: Some(b"cert".to_vec()),
            ..RedisTlsConfig::default()
        },
    )
    .await
    else {
        panic!("partial client tls material must fail");
    };
    assert!(
        err.to_string().contains("certificate and private key"),
        "unexpected error: {err}"
    );
}
