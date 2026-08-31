//! QUIC cluster server for the workflows showcase (uses public [`WorkflowsApi`]).
//!
//! Every node runs `CRAFTY_TRIGGER` with `/workflows/run` and `/workflows/resume`.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use crafty::client::{RemoteClient, SagaOutcome};
use crafty::kv::KvMachine;
use crafty::net::{
    QuicTransport, Transport, client_config, client_endpoint, load_pem_material,
};
use crafty::{CraftyCluster, NodeId, ReadyOpts, app_config_from_env};
use crafty_http::{WorkflowAccepted, WorkflowsApi, WorkflowsApiError, spawn_workflows_server};
use crafty_showcase_common::{cluster_mode, data_dir, display_addr};
use tokio::sync::Mutex;

use crate::{build_plan, debug, print_outcome};

const DATA_DIR_NAME: &str = "crafty-showcase-workflows";

#[derive(Clone)]
struct WorkflowHooks {
    cluster: Arc<CraftyCluster<KvMachine>>,
    client: Arc<RemoteClient>,
    lock: Arc<Mutex<()>>,
}

fn member_ids(peers: &crafty::PeerDirectory) -> Vec<NodeId> {
    peers.node_ids()
}

fn outcome_label(outcome: &SagaOutcome) -> &'static str {
    match outcome {
        SagaOutcome::Completed(_) => "completed",
        SagaOutcome::Compensated { .. } => "compensated",
    }
}

async fn quic_client(
    cfg: &crafty::AppConfig,
) -> Result<Arc<dyn Transport>, Box<dyn std::error::Error>> {
    let paths = cfg.pem_paths.clone().ok_or("cert env required")?;
    let material = load_pem_material(cfg.node_id, &paths)?;
    let client_cfg = client_config(&material.identity, material.roots)?;
    let endpoint = client_endpoint("0.0.0.0:0".parse()?)?;
    Ok(Arc::new(QuicTransport::new(
        endpoint,
        client_cfg,
        cfg.peers.clone(),
    )))
}

async fn wait_cluster_leader(cluster: &CraftyCluster<KvMachine>) {
    if cluster.wait_until_ready(ReadyOpts::default()).await {
        debug::cluster_ready();
    } else {
        tracing::warn!(target: "showcase", showcase = debug::NAME, "cluster not ready after 60s");
        eprintln!("warn: no leader yet — start nodes 2+3");
    }
}

async fn run_saga(hooks: &WorkflowHooks, saga_id: &str) -> Result<WorkflowAccepted, WorkflowsApiError> {
    debug::http_trigger("/workflows/run", saga_id);
    debug::saga_run(saga_id, false);
    let _guard = hooks.lock.lock().await;
    let plan = build_plan(saga_id);
    let journal = hooks.cluster.saga_journal();
    let outcome = hooks
        .cluster
        .run_keyed_saga(hooks.client.as_ref(), &plan, journal.as_ref())
        .await
        .map_err(|e| WorkflowsApiError::Failed(e.to_string()))?;
    let label = outcome_label(&outcome).to_string();
    print_outcome(saga_id, outcome);
    Ok(WorkflowAccepted {
        saga_id: saga_id.to_string(),
        outcome: label,
    })
}

async fn resume_saga(
    hooks: &WorkflowHooks,
    saga_id: &str,
) -> Result<WorkflowAccepted, WorkflowsApiError> {
    debug::http_trigger("/workflows/resume", saga_id);
    debug::saga_resume(saga_id, false);
    let _guard = hooks.lock.lock().await;
    let plan = build_plan(saga_id);
    let journal = hooks.cluster.saga_journal();
    let outcome = hooks
        .cluster
        .resume_keyed_saga(hooks.client.as_ref(), &plan, journal.as_ref())
        .await
        .map_err(|e| WorkflowsApiError::Failed(e.to_string()))?;
    let label = outcome_label(&outcome).to_string();
    print_outcome(saga_id, outcome);
    Ok(WorkflowAccepted {
        saga_id: saga_id.to_string(),
        outcome: label,
    })
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    assert!(cluster_mode());
    let cfg = app_config_from_env().map_err(|e| format!("config: {e}"))?;
    let mut builder = CraftyCluster::builder(cfg.node_id, KvMachine::default())
        .members(cfg.members.clone())
        .tick_period(Duration::from_millis(10));
    if let Some(dir) = cfg.data_dir.clone() {
        builder = builder.data_dir(dir);
    }
    if let Some(admin) = cfg.admin {
        builder = builder.admin_addr(admin);
    }
    let transport = quic_client(&cfg).await?;
    let targets = member_ids(&cfg.peers);
    let cluster = Arc::new(
        builder
            .start_quic(cfg.security, cfg.listen, cfg.peers)
            .await
            .map_err(|e| format!("start: {e}"))?,
    );
    wait_cluster_leader(cluster.as_ref()).await;

    let client = Arc::new(RemoteClient::new(transport, targets));
    let hooks = WorkflowHooks {
        cluster: Arc::clone(&cluster),
        client,
        lock: Arc::new(Mutex::new(())),
    };

    let node_id = cluster.node_id().0;
    debug::startup("quic", node_id, &data_dir(DATA_DIR_NAME));
    println!("crafty showcase · workflows (tier A)");
    println!("  mode     QUIC cluster (node {node_id})");
    println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_default());
    if let Ok(admin) = env::var("CRAFTY_ADMIN") {
        if admin != "-" {
            println!("  admin    http://{}/dashboard", display_addr(&admin));
        }
    }
    if let Ok(trigger) = env::var("CRAFTY_TRIGGER") {
        if trigger != "-" {
            let addr: std::net::SocketAddr = trigger.parse()?;
            let run_hooks = hooks.clone();
            let resume_hooks = hooks.clone();
            let api = WorkflowsApi::new(
                Arc::new(move |saga_id| {
                    let h = run_hooks.clone();
                    Box::pin(async move { run_saga(&h, &saga_id).await })
                }),
                Arc::new(move |saga_id| {
                    let h = resume_hooks.clone();
                    Box::pin(async move { resume_saga(&h, &saga_id).await })
                }),
            );
            spawn_workflows_server(api, addr).await?;
            println!("  trigger  http://{}/workflows/run", display_addr(&trigger));
        }
    } else {
        println!("  trigger  (set CRAFTY_TRIGGER to enable HTTP)");
    }
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("  debug    RUST_LOG=showcase=debug");
    println!("press Ctrl-C to stop");

    tokio::signal::ctrl_c().await?;
    debug::shutdown();
    cluster.shutdown();
    Ok(())
}
