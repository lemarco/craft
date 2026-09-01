//! HTTP trigger for QUIC cluster migration demo (`POST /demo/migrate/run`).

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use crafty::proto::ActorId;
use crafty::{CraftyApp, NodeId};

use crate::migrate_counter::{CounterMsg, StatefulCounter};

fn migrate_target() -> NodeId {
    std::env::var("CRAFTY_MIGRATE_TARGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(NodeId)
        .unwrap_or(NodeId(2))
}

pub fn migrate_routes(state: crafty::CraftyGatewayState) -> Router {
    Router::new()
        .route("/demo/migrate/run", post(run_demo))
        .with_state(state.app)
}

async fn run_demo(State(app): State<Arc<CraftyApp>>) -> impl IntoResponse {
    match run_demo_inner(&app).await {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn run_demo_inner(app: &CraftyApp) -> Result<String, String> {
    let local = app.node_id();
    let target = migrate_target();
    tracing::debug!(target: "showcase", local = local.0, to = target.0, "migration demo start");

    app.control()
        .spawn_remote::<StatefulCounter>(local, "counter", 0)
        .await
        .map_err(|e| format!("spawn: {e}"))?;

    let counter = app
        .registry()
        .get::<StatefulCounter>("counter")
        .ok_or_else(|| "counter not registered".to_string())?;

    for _ in 0..3 {
        counter.send(CounterMsg::Inc).map_err(|e| format!("inc: {e}"))?;
    }

    let source = ActorId {
        node: local,
        name: "counter".into(),
        instance: 0,
        generation: 0,
    };

    let migrated = app
        .control()
        .migrate::<StatefulCounter>(source, target, 0, Duration::from_secs(10))
        .await
        .map_err(|e| format!("migrate: {e}"))?;

    app.registry()
        .get::<StatefulCounter>("counter")
        .ok_or_else(|| "counter missing after migrate".to_string())?
        .send(CounterMsg::Inc)
        .map_err(|e| format!("post-migrate inc: {e}"))?;

    Ok(format!(
        "migration OK: node {} → {} (generation {}) — expect [counter] → 4 in logs",
        local.0, migrated.node.0, migrated.generation
    ))
}
