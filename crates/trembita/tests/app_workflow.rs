//! [`TrembitaApp`] workflow integration (coordination machinery, not Raft state machine).

use trembita::client::SagaOutcome;
use trembita::{
    GatewayOpts, ReadyOpts, TrembitaApp, TrembitaConfigure, WorkflowBuilder, WorkflowOpts,
    journal_workflow,
};
use trembita_test_support::{TICK_PERIOD, boot_local_app, fast_raft_config_with_seed};

fn noop_plan(saga_id: &str) -> trembita_client::SagaPlan {
    let key = b"workflow".to_vec();
    WorkflowBuilder::new(saga_id)
        .step("checkpoint", &key, trembita::proto::encode(&()).unwrap())
        .compensate("checkpoint", trembita::proto::encode(&()).unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn trembita_app_runs_workflow_locally() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(dir.path())
                .configure(TrembitaConfigure {
                    raft_config: fast_raft_config_with_seed(7),
                    tick_period: TICK_PERIOD,
                    ..TrembitaConfigure::default()
                })
                .workflows([WorkflowOpts::new(noop_plan, journal_workflow)])
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().expect("addr")).with_workflows_api(true),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let outcome = app.run_workflow_id("onboard-test").await.expect("run");
    assert!(matches!(outcome, SagaOutcome::Completed(_)));
}

#[tokio::test]
async fn trembita_app_workflows_api_on_gateway() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(dir.path())
                .configure(TrembitaConfigure {
                    raft_config: fast_raft_config_with_seed(8),
                    tick_period: TICK_PERIOD,
                    ..TrembitaConfigure::default()
                })
                .workflows([WorkflowOpts::new(noop_plan, journal_workflow)])
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().expect("addr")).with_workflows_api(true),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let outcome = app.run_workflow_id("api-test").await.expect("run");
    assert!(matches!(outcome, SagaOutcome::Completed(_)));
}

#[cfg(feature = "http-jobs")]
mod gateway_merge {
    use trembita::cluster::build_gateway_router;
    use trembita::{
        GatewayOpts, ReadyOpts, TrembitaApp, TrembitaConfigure, WorkflowOpts, journal_workflow,
    };
    use trembita_test_support::{TICK_PERIOD, boot_local_app, fast_raft_config_with_seed};

    use super::noop_plan;

    #[tokio::test]
    async fn gateway_router_merges_workflows_api() {
        let dir = tempfile::tempdir().expect("tempdir");
        let app = boot_local_app(
            || {
                TrembitaApp::builder()
                    .data_dir(dir.path())
                    .configure(TrembitaConfigure {
                        raft_config: fast_raft_config_with_seed(9),
                        tick_period: TICK_PERIOD,
                        ..TrembitaConfigure::default()
                    })
                    .workflows([WorkflowOpts::new(noop_plan, journal_workflow)])
                    .gateway(
                        GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
                            .with_workflows_api(true),
                    )
            },
            Some(ReadyOpts::default()),
        )
        .await;

        let router = build_gateway_router(
            app,
            GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
                .with_workflows_api(true)
                .build_config(),
        );
        let _ = router;
    }
}
