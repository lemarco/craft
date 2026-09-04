//! Cluster builder, runtime handle, queues, journals, and related types.
//!
//! Product apps typically use [`crate::TrembitaApp`] and [`crate::prelude`]; import from here
//! for custom [`StateMachine`](crate::core::StateMachine) wiring or direct cluster control.

pub use crate::app::{EmptyStateMachine, WorkerInfo};
pub use crate::builder::{StartError, TrembitaClusterBuilder};
pub use crate::certs::{
    CertReloadError, CertReloadHandle, PemSecurity, ReloadOpts, cert_paths_for_node,
    cert_paths_from_env,
};
pub use crate::cluster_handle::{
    AddRaftGroupsError, ClusterFacts, LeaveError, ScaleClusterError, TrembitaCluster,
};
pub use crate::gateway::{
    GATEWAY_MAX_BODY_BYTES, GatewayConfig, GatewayConfigError, GatewayHandle, GatewaySpawnError,
    build_gateway_router, gateway_has_product_apis, gateway_token_from_env, spawn_gateway,
    validate_gateway_config,
};
pub use crate::saga::{
    CompositeSagaJournal, Group0SagaJournal, MetaRaftSagaJournal, SagaRegistry, StoreSagaJournal,
    record_saga_metrics, saga_metrics_callback,
};
pub use crate::security::Security;
pub use crate::two_phase::{
    CompositeTwoPhaseJournal, MetaRaftTwoPhaseJournal, StoreTwoPhaseJournal, TwoPhaseRegistry,
    record_two_phase_event, record_two_phase_gc_aborted, record_two_phase_metrics,
    two_phase_metrics_callback,
};
pub use crate::upgrade::{
    UpgradeFetchError, UpgradeInstallError, UpgradeOpts, UpgradeRunError, atomic_symlink_install,
    fetch_artifact, report_upgrade_boot, running_app_version, spawn_upgrade_coordinator,
    spawn_upgrade_runtime, verify_sha256_hex,
};
pub use crate::workflow::{WorkflowBuildError, WorkflowBuilder};
pub use trembita_actor_store::{
    ClusterActorStateStore, RedbActorStateStore, StoreService, run_actor_store_gc_ticker,
};
pub use trembita_core::ReachabilityConfig;
pub use trembita_core::kv;
pub use trembita_core::kv::{Kv, KvCommand, KvError, KvMachine, KvQuery, KvResponse};
pub use trembita_core::upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradePhase, UpgradeQuery,
    UpgradeResponse, UpgradeState, UpgradeStateMachine, UpgradeView, plan_next_grant, upgrade_view,
};
pub use trembita_core::{CompactionPolicy, DEFAULT_COMPACT_BYTES, DEFAULT_COMPACT_ENTRIES};
#[cfg(feature = "http-jobs")]
pub use trembita_http::{
    ActorView, ClusterView, HostRouter, IntrospectApi, IntrospectApiError, NodeSummary, NodeView,
    Observer, QueueStreamView, QueuesView, RaftGroupSummary, RaftGroupsView, SagaBody,
    SagaRecordView, WorkflowAccepted, WorkflowsApi, WorkflowsApiError, is_local_dev_host,
    normalize_host, spawn_workflows_server,
};
pub use trembita_jobs::{
    AutoscalePolicy, BacklogError, BacklogFeedOpts, BacklogItem, BacklogRegistry, ClusterJobQueue,
    ConsumerCount, DEFAULT_QUEUE_BATCH_MAX, DEFAULT_QUEUE_PREFETCH, EnqueueOptions,
    ExternalBacklog, InMemoryExternalBacklog, InMemoryJobQueue, JobId, JobQueue, LeaseId,
    LeasedJob, MembershipAutoscalePolicy, QueueConsumerWorkload, QueueError, QueueMetrics,
    QueueService, RecurringJob, RedbJobQueue, Settlement, ShardedJobQueue, WorkerId, WorkloadOpts,
    effective_queue_depth, run_backlog_feeder, run_queue_autoscaler, run_queue_consumer,
    run_queue_membership_autoscaler, run_queue_schedule_ticker, run_workload_governor,
};
pub use trembita_net::PeerDirectory;
pub use trembita_net::{CertPaths, load_pem_material};
pub use trembita_runtime::{
    ActorSession, ComputeTokenPool, DEFAULT_DRAIN_TIMEOUT, DirectoryPolicy, DirectoryRetry,
    ExternalLoad, InMemoryMailboxSpool, LeaderGate, LeaderLoopOpts, LeaderSession, MailboxSpool,
    ManualExternalLoad, RedbMailboxSpool, ResourceProfile, VpsResources, run_leader_loop,
};
