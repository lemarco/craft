//! Resume a keyed workflow from the durable journal (B-05e CLI stub).
//!
//! Run via `./scripts/crafty-workflow.sh resume <saga_id>` or:
//! `cargo run -p crafty --example workflow_resume_cli -- onboard-42`

use std::sync::Arc;
use std::time::Duration;

use crafty::client::{RemoteClient, SagaOutcome};
use crafty::net::LocalNetwork;
use crafty::proto::encode;
use crafty::{CraftyCluster, NodeId, WorkflowBuilder};
use crafty_test_support::{KvCommand, KvMachine, TICK_PERIOD, advance, wait_for_crafty_leader};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let saga_id = args.next().unwrap_or_else(|| {
        eprintln!("usage: workflow_resume_cli <saga_id> [--data-dir PATH]");
        std::process::exit(1);
    });
    let mut data_dir = None;
    let rest: Vec<_> = args.collect();
    let mut i = 0;
    while i < rest.len() {
        if rest[i] == "--data-dir" {
            i += 1;
            data_dir = rest.get(i).cloned();
        }
        i += 1;
    }

    let net = LocalNetwork::new();
    let mut builder =
        CraftyCluster::builder(NodeId(1), KvMachine::default()).members([NodeId(1), NodeId(2), NodeId(3)]);
    builder = builder.tick_period(TICK_PERIOD);
    if let Some(dir) = data_dir {
        builder = builder.data_dir(dir);
    }
    let cluster = builder.start_local(&net).await;

    wait_for_crafty_leader(&cluster).await;
    advance(Duration::from_millis(200)).await;

    let key = b"user:42".to_vec();
    let plan = WorkflowBuilder::new(&saga_id)
        .step(
            "create_account",
            &key,
            encode(&KvCommand::Set {
                key: "user:42".into(),
                value: "active".into(),
            })
            .unwrap(),
        )
        .compensate(
            "create_account",
            encode(&KvCommand::Delete {
                key: "user:42".into(),
            })
            .unwrap(),
        )
        .step(
            "send_welcome",
            &key,
            encode(&KvCommand::Set {
                key: "welcome:42".into(),
                value: "sent".into(),
            })
            .unwrap(),
        )
        .compensate(
            "send_welcome",
            encode(&KvCommand::Delete {
                key: "welcome:42".into(),
            })
            .unwrap(),
        )
        .build()
        .expect("valid workflow");

    let client = RemoteClient::new(Arc::new(net.clone()), [cluster.node_id()]);
    let journal = cluster.saga_journal();
    let outcome = cluster
        .resume_keyed_saga(&client, &plan, journal.as_ref())
        .await
        .expect("resume workflow");

    match outcome {
        SagaOutcome::Completed(steps) => {
            println!("workflow {saga_id} completed ({} steps)", steps.len());
        }
        SagaOutcome::Compensated {
            failed_step,
            compensated_steps,
            ..
        } => {
            println!(
                "workflow {saga_id} compensated at step {failed_step} ({compensated_steps} reversed)"
            );
        }
    }

    cluster.shutdown();
}
