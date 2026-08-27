//! TLS Redis integration tests (`rediss://`) via testcontainers (ADR 029).
//!
//! Run locally with:
//!
//! ```text
//! cargo test -p craft-store-redis --test tls -- --ignored
//! ```

use craft_actor::ActorStateStore;
use craft_store_redis::{RedisStore, RedisTlsConfig};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use testcontainers_modules::testcontainers::{
    GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};

const REDIS_TLS_PORT: u16 = 6379;

struct TlsPemBundle {
    ca_pem: Vec<u8>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

fn generate_test_server_tls() -> TlsPemBundle {
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "craft-redis-test-ca");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().expect("server key");
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_cert = server_params
        .signed_by(&server_key, &issuer)
        .expect("server cert");

    TlsPemBundle {
        ca_pem: ca_cert.pem().into_bytes(),
        cert_pem: server_cert.pem().into_bytes(),
        key_pem: server_key.serialize_pem().into_bytes(),
    }
}

async fn redis_tls_endpoint(
    bundle: &TlsPemBundle,
) -> (
    testcontainers_modules::testcontainers::ContainerAsync<GenericImage>,
    String,
) {
    let container = GenericImage::new("redis", "7.2-alpine")
        .with_copy_to("/tls/ca.crt", bundle.ca_pem.clone())
        .with_copy_to("/tls/redis.crt", bundle.cert_pem.clone())
        .with_copy_to("/tls/redis.key", bundle.key_pem.clone())
        .with_cmd([
            "redis-server",
            "--tls-port",
            "6379",
            "--port",
            "0",
            "--tls-cert-file",
            "/tls/redis.crt",
            "--tls-key-file",
            "/tls/redis.key",
            "--tls-ca-cert-file",
            "/tls/ca.crt",
            "--tls-auth-clients",
            "no",
        ])
        .with_ready_conditions(vec![WaitFor::message_on_stdout(
            "Ready to accept connections",
        )])
        .start()
        .await
        .expect("start tls redis");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(REDIS_TLS_PORT)
        .await
        .expect("mapped tls port");
    let url = format!("rediss://{host}:{port}");
    (container, url)
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn rediss_connect_with_private_ca_round_trips() {
    let bundle = generate_test_server_tls();
    let (_c, url) = redis_tls_endpoint(&bundle).await;
    let tls = RedisTlsConfig::with_root_ca_pem(bundle.ca_pem);
    let store = RedisStore::connect_with_tls(&url, &tls)
        .await
        .expect("tls connect");

    store.set("k", b"v", None).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
}

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
