//! [`dispatch_work_trigger`] integration with [`TrembitaApp`].

#![allow(clippy::large_futures)]

use std::time::Duration;

use trembita::client::SagaOutcome;
use trembita::{
    AppManifest, DispatchOutcome, GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError,
    QueueOpts, ReadyOpts, TrembitaApp, TrembitaConfigure, WorkflowBuilder, WorkflowOpts,
    dispatch_work_trigger, journal_workflow,
};
use trembita_jobs::{RecurringJob, WorkTrigger};
use trembita_test_support::{
    TICK_PERIOD, advance, boot_local_app, fast_raft_config_with_seed, gateway_workflows_surfaces,
    wait_for_trembita_app_leader,
};

struct TestGatewayIdentity;

impl GatewayIdentity for TestGatewayIdentity {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, _: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        Ok("test".into())
    }
}

fn noop_plan(saga_id: &str) -> trembita::client::SagaPlan {
    let key = b"workflow".to_vec();
    WorkflowBuilder::new(saga_id)
        .step("checkpoint", &key, trembita::proto::encode(&()).unwrap())
        .compensate("checkpoint", trembita::proto::encode(&()).unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn dispatch_runs_registered_workflow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure {
                    raft_config: fast_raft_config_with_seed(11),
                    tick_period: TICK_PERIOD,
                    ..TrembitaConfigure::default()
                })
                .manifest(
                    AppManifest::new().workflows([WorkflowOpts::new(noop_plan, journal_workflow)]),
                )
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
                        .identity(TestGatewayIdentity)
                        .surfaces(gateway_workflows_surfaces),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let payload = WorkTrigger::Workflow {
        saga_id: "weekly-report".into(),
    }
    .to_payload();
    let outcome = dispatch_work_trigger(&app, &payload)
        .await
        .expect("dispatch");
    assert!(matches!(
        outcome,
        DispatchOutcome::Workflow(SagaOutcome::Completed(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn dispatch_enqueues_follow_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(dir.path()),
                )
                .manifest(AppManifest::new().queue([
                    QueueOpts::new("orchestration", Duration::from_secs(60)),
                    QueueOpts::new("seo-parse", Duration::from_secs(60)),
                ]))
                .configure(TrembitaConfigure {
                    raft_config: fast_raft_config_with_seed(12),
                    tick_period: TICK_PERIOD,
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;
    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let payload = WorkTrigger::Enqueue {
        stream: "seo-parse".into(),
        payload: b"unit".to_vec(),
    }
    .to_payload();
    let outcome = dispatch_work_trigger(&app, &payload)
        .await
        .expect("dispatch");
    assert!(matches!(outcome, DispatchOutcome::Enqueued(_)));
}

#[test]
fn recurring_job_trigger_workflow_wire() {
    let job = RecurringJob::trigger_workflow("weekly", "0 3 * * 1", "weekly-report");
    let decoded = WorkTrigger::decode(&job.payload)
        .expect("decode")
        .expect("trigger");
    assert_eq!(
        decoded,
        WorkTrigger::Workflow {
            saga_id: "weekly-report".into()
        }
    );
}
