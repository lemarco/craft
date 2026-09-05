//! Spawn a gateway for integration tests (hyper + reqwest clients).

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use trembita::TrembitaApp;
use trembita::cluster::build_gateway_service;
use trembita::gateway::GatewayConfig;

/// Bind an ephemeral port and serve `config` until the runtime shuts down.
///
/// # Panics
/// When gateway wiring is invalid or the listen socket cannot be bound.
pub async fn spawn_test_gateway(app: &Arc<TrembitaApp>, config: GatewayConfig) -> SocketAddr {
    let service = build_gateway_service(app, config).expect("gateway service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway test listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let service = service.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let hyper_service = TowerToHyperService::new(service);
                let _ = http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    addr
}
