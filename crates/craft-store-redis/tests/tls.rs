//! Fast TLS config validation tests (no Docker).

use craft_actor::ActorStateStore;
use craft_store_redis::{RedisStore, RedisTlsConfig};

#[tokio::test]
async fn connect_with_tls_rejects_plain_redis_url() {
    let err =
        match RedisStore::connect_with_tls("redis://127.0.0.1:6379", &RedisTlsConfig::default())
            .await
        {
            Ok(_) => panic!("plain URL must be rejected"),
            Err(err) => err,
        };
    assert!(
        err.to_string().contains("rediss://"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn tls_config_requires_matching_client_cert_and_key() {
    let err = match RedisStore::connect_with_tls(
        "rediss://127.0.0.1:6379",
        &RedisTlsConfig {
            client_cert_pem: Some(b"cert".to_vec()),
            ..RedisTlsConfig::default()
        },
    )
    .await
    {
        Ok(_) => panic!("partial client tls material must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("certificate and private key"),
        "unexpected error: {err}"
    );
}
