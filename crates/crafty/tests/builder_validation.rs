//! [`CraftyAppBuilder`] boot-time validation (queue / cron / consumer / workflows).

use std::time::Duration;

use crafty::cluster::RecurringJob;
use crafty::{
    ConsumerOpts, CraftyApp, CraftyConfigure, CronOpts, GatewayOpts, JobOpts, QueueOpts, RunOpts,
    StartError, WorkerOpts, WorkerScale, WorkflowBuilder, WorkflowOpts, consumer, journal_workflow,
    workers,
};
use crafty_test_support::{TICK_PERIOD, advance, eventually_default, fast_raft_config_with_seed};

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
    app.shutdown();
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

#[tokio::test]
async fn jobs_consumer_stream_mismatch_fails_at_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = local_builder(dir.path())
        .jobs([JobOpts::new("other").consumer(EmailWorkerConsumer)])
        .boot_for_test(RunOpts::local())
        .await;
    match result {
        Ok(_) => panic!("expected boot failure for stream mismatch"),
        Err(err) => assert_config_err(err, "stream mismatch"),
    }
}

#[tokio::test(start_paused = true)]
async fn jobs_registers_queue_handler_and_jobs_api() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = local_builder(dir.path())
        .jobs([JobOpts::new("emails")
            .consumer(EmailWorkerConsumer)
            .http_enqueue(true)])
        .gateway(GatewayOpts::new("127.0.0.1:0".parse().unwrap()))
        .boot_for_test(RunOpts::local())
        .await
        .expect("boot");
    crafty_test_support::wait_for_crafty_app_leader(&app).await;
    assert!(app.job_queue("emails").is_some());
    app.shutdown();
}

#[derive(Debug)]
struct FixedWorker;

impl crafty::actor::UserActor for FixedWorker {
    type Config = u32;
    type Message = ();
    type Error = std::convert::Infallible;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(FixedWorker)
    }

    fn handle(
        &mut self,
        _msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, crafty::actor::ConfigCodecError> {
        crafty::proto::encode(config)
            .map_err(|e| crafty::actor::ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, crafty::actor::ConfigCodecError> {
        crafty::proto::decode(bytes)
            .map_err(|e| crafty::actor::ConfigCodecError::Codec(e.to_string()))
    }
}

#[tokio::test]
async fn workers_without_config_fails_at_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = local_builder(dir.path())
        .workers(workers![
            WorkerOpts::<FixedWorker>::new("w").scale(WorkerScale::Fixed(1))
        ])
        .boot_for_test(RunOpts::local())
        .await;
    match result {
        Ok(_) => panic!("expected boot failure for missing config"),
        Err(err) => assert_config_err(err, "call .config(...)"),
    }
}

#[tokio::test]
async fn workers_autoscale_without_queue_fails_at_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = local_builder(dir.path())
        .workers(workers![
            WorkerOpts::<FixedWorker>::new("w")
                .config(0)
                .scale(WorkerScale::Auto { min: 1, max: 2 })
        ])
        .boot_for_test(RunOpts::local())
        .await;
    match result {
        Ok(_) => panic!("expected boot failure for autoscale without queue"),
        Err(err) => assert_config_err(err, "autoscale stream"),
    }
}

#[tokio::test(start_paused = true)]
async fn workers_fixed_registers_actor_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = local_builder(dir.path())
        .workers(workers![
            WorkerOpts::<FixedWorker>::new("w")
                .config(7)
                .scale(WorkerScale::Fixed(1))
        ])
        .configure(CraftyConfigure {
            raft_config: fast_raft_config_with_seed(44),
            tick_period: TICK_PERIOD,
            reconcile_period: Duration::from_millis(20),
            directory_publish_period: Duration::from_millis(20),
            ..CraftyConfigure::default()
        })
        .boot_for_test(RunOpts::local())
        .await
        .expect("boot");
    crafty_test_support::wait_for_crafty_app_leader(&app).await;
    advance(Duration::from_millis(500)).await;
    eventually_default("worker in directory", || !app.workers("w").is_empty()).await;
    assert_eq!(app.workers("w").len(), 1);
    app.shutdown();
}
