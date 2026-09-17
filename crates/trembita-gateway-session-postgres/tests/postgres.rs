//! Integration tests for [`PgGatewaySessionStore`] — Docker / testcontainers only.
//!
//! ```text
//! cargo test -p trembita-gateway-session-postgres --features docker-tests -- --ignored
//! ```

use std::time::Duration;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use trembita_gateway_session_postgres::{PgGatewaySessionStore, PgSessionLookup};

async fn pg_url() -> (
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    String,
) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (container, url)
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn b46_register_lookup_revoke_roundtrip() {
    let (_c, url) = pg_url().await;
    let store = PgGatewaySessionStore::connect(&url, "trembita_gw_sess_test")
        .await
        .expect("connect");

    let token = store
        .register("alice", Duration::from_secs(3600))
        .await
        .expect("register");
    assert!(token.starts_with("cs_"));

    match store.lookup(&token).await.expect("lookup") {
        PgSessionLookup::Live(row) => assert_eq!(row.user, "alice"),
        other => panic!("expected live session, got {other:?}"),
    }

    store.revoke(&token).await.expect("revoke");
    assert!(matches!(
        store.lookup(&token).await.expect("lookup after revoke"),
        PgSessionLookup::NotFound
    ));
}
