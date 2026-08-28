//! Onboarding workflow — [`WorkflowBuilder`] + Meta-Raft journal (B-05).
//!
//! Run: `cargo run -p crafty --example onboarding_workflow`

use std::sync::Arc;
use std::time::Duration;

use crafty::client::{RemoteClient, SagaOutcome};
use crafty::net::LocalNetwork;
use crafty::proto::encode;
use crafty::{CraftyCluster, NodeId, WorkflowBuilder};
use crafty_test_support::{KvCommand, KvMachine, TICK_PERIOD, advance, wait_for_crafty_leader};

#[tokio::main]
async fn main() {
    let net = LocalNetwork::new();
    let cluster = CraftyCluster::builder(NodeId(1), KvMachine::default())
        .members([NodeId(1), NodeId(2), NodeId(3)])
        .tick_period(TICK_PERIOD)
        .start_local(&net)
        .await;

    wait_for_crafty_leader(&cluster).await;
    advance(Duration::from_millis(200)).await;

    let key = b"user:42".to_vec();
    let plan = WorkflowBuilder::new("onboard-42")
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
        .run_keyed_saga(&client, &plan, journal.as_ref())
        .await
        .expect("workflow");

    match outcome {
        SagaOutcome::Completed(responses) => {
            println!("onboarding complete ({} steps)", responses.len());
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
                "workflow compensated at step {failed_step} ({compensated_steps} compensators ran)"
            );
        }
    }

    cluster.shutdown();
}
