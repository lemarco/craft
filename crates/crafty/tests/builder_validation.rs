//! [`CraftyAppBuilder`] boot-time validation (queue / cron / consumer / workflows).

use std::time::Duration;

use crafty::advanced::RecurringJob;
use crafty::{
    ConsumerOpts, CraftyApp, CraftyConfigure, CronOpts, GatewayOpts, QueueOpts, RunOpts,
    StartError, WorkflowBuilder, WorkflowOpts, consumer, journal_workflow,
};
use crafty_test_support::{TICK_PERIOD, fast_raft_config_with_seed};

#[consumer("orphan")]
#[allow(clippy::unused_async)]
async fn orphan(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

#[consumer("emails")]
#[allow(clippy::unused_async)]
async fn email_worker(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

fn noop_plan(saga_id: &str) -> crafty_client::SagaPlan {
    let key = b"workflow".to_vec();
    WorkflowBuilder::new(saga_id)
        .step("checkpoint", &key, crafty::proto::encode(&()).unwrap())
        .compensate("checkpoint", crafty::proto::encode(&()).unwrap())
        .build()
        .unwrap()
}

fn local_builder(dir: &std::path::Path) -> crafty::CraftyAppBuilder {
    CraftyApp::builder()
        .data_dir(dir)
        .configure(CraftyConfigure {
            raft_config: fast_raft_config_with_seed(42),
            tick_period: TICK_PERIOD,
            ..CraftyConfigure::default()
        })
}

fn assert_config_err(err: StartError, needle: &str) {
    match err {
        StartError::Config(msg) => assert!(
            msg.contains(needle),
            "expected config error containing {needle:?}, got {msg:?}"
        ),
        other => panic!("expected StartError::Config, got {other:?}"),
    }
}

#[tokio::test]
async fn cron_without_matching_queue_fails_at_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = local_builder(dir.path())
        .cron([CronOpts::new(
            "emails",
            RecurringJob::new("daily", "0 9 * * *", b"tick"),
        )])
        .boot_for_test(RunOpts::local())
        .await;
    match result {
        Ok(_) => panic!("expected boot failure for cron without queue"),
        Err(err) => assert_config_err(err, "`.cron()` stream \"emails\""),
    }
}

#[tokio::test]
async fn consumer_without_matching_queue_fails_at_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = local_builder(dir.path())
        .consumer(OrphanConsumer, ConsumerOpts::default())
        .boot_for_test(RunOpts::local())
        .await;
    match result {
        Ok(_) => panic!("expected boot failure for consumer without queue"),
        Err(err) => assert_config_err(err, "`.consumer()` stream \"orphan\""),
    }
}

#[tokio::test]
async fn cron_and_consumer_succeed_when_queue_matches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = CraftyApp::builder()
        .data_dir(dir.path())
        .queue([QueueOpts::new("emails", Duration::from_secs(60))])
        .cron([CronOpts::new(
            "emails",
            RecurringJob::new("daily", "0 9 * * *", b"tick"),
        )])
        .consumer(EmailWorkerConsumer, ConsumerOpts::default())
        .configure(CraftyConfigure {
            raft_config: fast_raft_config_with_seed(43),
            tick_period: TICK_PERIOD,
            ..CraftyConfigure::default()
        })
        .boot_for_test(RunOpts::local())
        .await
        .expect("valid builder");
    app.cluster().shutdown();
}

#[cfg(feature = "http-jobs")]
mod workflows {
    use super::*;

    #[tokio::test]
    async fn workflows_without_gateway_fails_at_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = local_builder(dir.path())
            .workflows([WorkflowOpts::new(noop_plan, journal_workflow)])
            .boot_for_test(RunOpts::local())
            .await;
        match result {
            Ok(_) => panic!("expected boot failure for workflows without gateway"),
            Err(err) => assert_config_err(err, "`.workflows([…])` requires"),
        }
    }

    #[tokio::test]
    async fn workflows_without_workflows_api_flag_fails_at_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = local_builder(dir.path())
            .workflows([WorkflowOpts::new(noop_plan, journal_workflow)])
            .gateway(GatewayOpts::new("127.0.0.1:0".parse().expect("addr")))
            .boot_for_test(RunOpts::local())
            .await;
        match result {
            Ok(_) => panic!("expected boot failure for gateway without workflows_api"),
            Err(err) => assert_config_err(err, "`.workflows([…])` requires"),
        }
    }
}
