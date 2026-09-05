//! HTTP trigger for QUIC cluster migration demo (`POST /demo/migrate/run`).

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use trembita::proto::ActorId;
use trembita::{Gateway, NodeId, RequestCtx, Response, RouteTable, TrembitaApp, TrembitaGatewayState};

use crate::migrate_counter::{CounterMsg, StatefulCounter};

fn migrate_target() -> NodeId {
    std::env::var("TREMBITA_MIGRATE_TARGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(NodeId)
        .unwrap_or(NodeId(2))
}

pub fn surfaces(state: TrembitaGatewayState) -> Gateway {
    let app = state.app;
    Gateway::new(false).dev_fallback(RouteTable::new().post("/demo/migrate/run", move |_: RequestCtx| {
        let app = Arc::clone(&app);
        async move {
            match run_demo_inner(&app).await {
                Ok(msg) => Ok(Response::text(StatusCode::OK, msg)),
                Err(e) => Ok(Response::text(StatusCode::INTERNAL_SERVER_ERROR, e)),
            }
        }
    }))
}

async fn run_demo_inner(app: &TrembitaApp) -> Result<String, String> {
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
