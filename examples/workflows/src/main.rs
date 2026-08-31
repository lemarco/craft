//! # Workflows showcase (messaging **tier A** — saga over Raft)
//!
//! Multi-step **onboarding** with compensators. Progress is journaled in Meta-Raft
//! (`group-meta.redb`), not Redis.
//!
//! Uses the public [`crafty::KvMachine`] state machine — no test-support dependency.
//!
//! ## Local vs cluster
//!
//! | Mode | Entry | Saga trigger |
//! |------|-------|--------------|
//! | Local | `cargo run` | In-process `run_keyed_saga` |
//! | Cluster | `./cluster.sh up` | `./trigger.sh` → HTTP on any node |

mod debug;
mod server;

use std::env;
use std::sync::Arc;
use std::time::Duration;

use crafty::client::{RemoteClient, SagaOutcome, SagaPlan};
use crafty::kv::{KvCommand, KvMachine};
use crafty::net::LocalNetwork;
use crafty::proto::encode;
use crafty::{CraftyCluster, NodeId, ReadyOpts, WorkflowBuilder};
use crafty_showcase_common::{cluster_mode, data_dir};

const DATA_DIR_NAME: &str = "crafty-showcase-workflows";
const TICK: Duration = Duration::from_millis(10);

pub(crate) fn build_plan(saga_id: &str) -> SagaPlan {
    let key = b"user:42".to_vec();
    WorkflowBuilder::new(saga_id)
        .step(
            "create_account",
            &key,
            encode(&KvCommand::Set {
                key: "user:42".into(),
                value: "active".into(),
            })
            .expect("encode"),
        )
        .compensate(
            "create_account",
            encode(&KvCommand::Delete {
                key: "user:42".into(),
            })
            .expect("encode"),
        )
        .step(
            "send_welcome",
            &key,
            encode(&KvCommand::Set {
                key: "welcome:42".into(),
                value: "sent".into(),
            })
            .expect("encode"),
        )
        .compensate(
            "send_welcome",
            encode(&KvCommand::Delete {
                key: "welcome:42".into(),
            })
            .expect("encode"),
        )
        .build()
        .expect("valid workflow")
}

pub(crate) fn print_outcome(saga_id: &str, outcome: SagaOutcome) {
    let label = match &outcome {
        SagaOutcome::Completed(_) => "completed",
        SagaOutcome::Compensated { .. } => "compensated",
    };
    crate::debug::saga_outcome(saga_id, label);
    match outcome {
        SagaOutcome::Completed(responses) => {
            println!(
                "workflow `{saga_id}` completed ({} steps)",
                responses.len()
            );
            for (i, r) in responses.iter().enumerate() {
                println!("  step {i}: {} bytes response", r.len());
            }
        }
        SagaOutcome::Compensated {
            failed_step,
            compensated_steps,
            ..
        } => {
            println!(
                "workflow `{saga_id}` compensated at step {failed_step} ({compensated_steps} compensators ran)"
            );
        }
    }
}

async fn start_local_cluster() -> CraftyCluster<KvMachine> {
    let dir = data_dir(DATA_DIR_NAME);
    std::fs::create_dir_all(&dir).expect("data_dir");
    let net = LocalNetwork::new();
    let cluster = CraftyCluster::builder(NodeId(1), KvMachine::default())
        .members([NodeId(1), NodeId(2), NodeId(3)])
        .tick_period(TICK)
        .data_dir(&dir)
        .start_local(&net)
        .await;
    if !cluster.wait_until_ready(ReadyOpts::default()).await {
        eprintln!("warn: local cluster not ready after 60s");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    cluster
}

async fn run_local_saga(saga_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    debug::saga_run(saga_id, true);
    let net = LocalNetwork::new();
    let cluster = start_local_cluster().await;
    let plan = build_plan(saga_id);
    println!("running keyed saga `{saga_id}` (local 3-member cluster)");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    let client = RemoteClient::new(Arc::new(net), [cluster.node_id()]);
    let journal = cluster.saga_journal();
    let outcome = cluster
        .run_keyed_saga(&client, &plan, journal.as_ref())
        .await?;
    print_outcome(saga_id, outcome);
    cluster.shutdown();
    Ok(())
}

async fn resume_local_saga(saga_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    debug::saga_resume(saga_id, true);
    let net = LocalNetwork::new();
    let cluster = start_local_cluster().await;
    let plan = build_plan(saga_id);
    println!("resuming saga `{saga_id}` from journal (local)");
    let client = RemoteClient::new(Arc::new(net), [cluster.node_id()]);
    let journal = cluster.saga_journal();
    let outcome = cluster
        .resume_keyed_saga(&client, &plan, journal.as_ref())
        .await?;
    print_outcome(saga_id, outcome);
    cluster.shutdown();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    let mode = env::args().nth(1).unwrap_or_default();
    let saga_id = env::args()
        .nth(2)
        .unwrap_or_else(|| "onboard-42".into());

    match mode.as_str() {
        "resume" if cluster_mode() => {
            return Err("cluster resume: use ./trigger.sh resume (HTTP trigger on any node)".into());
        }
        "resume" => return resume_local_saga(&saga_id).await,
        "run" if cluster_mode() => {
            return Err("cluster run: use ./trigger.sh (HTTP trigger on any node)".into());
        }
        "run" => return run_local_saga(&saga_id).await,
        "" if cluster_mode() => return server::run().await,
        "" => return run_local_saga(&saga_id).await,
        other => return Err(format!("unknown mode {other:?}").into()),
    }
}
