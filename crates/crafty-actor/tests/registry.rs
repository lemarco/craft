//! Tests for the local [`crafty_actor::ActorRegistry`] (backlog E6): singleton
//! spawn + cast/ask, pool round-robin and keyed routing, `scale_local`, `stop`,
//! and the production one-worker-per-name guard (one-worker-per-vps).

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crafty_actor::{
    ActorRegistry, AskError, PlacementMode, RpcReplyPort, ScaleError, SpawnError, UserActor,
};

// ---------------------------------------------------------------------------
// Test actors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("worker error: {0}")]
struct WorkerError(String);

/// Shared observability: per-instance hit counts keyed by the sequence number
/// each instance grabs at start, so tests can see how messages were routed.
type Hits = Arc<Mutex<HashMap<u32, u32>>>;

#[derive(Clone)]
struct WorkerCfg {
    next_seq: Arc<AtomicU32>,
    hits: Hits,
    fail_start: bool,
}

impl WorkerCfg {
    fn new() -> Self {
        Self {
            next_seq: Arc::new(AtomicU32::new(0)),
            hits: Arc::new(Mutex::new(HashMap::new())),
            fail_start: false,
        }
    }
}

enum WorkerMsg {
    Inc,
    WhoAmI(RpcReplyPort<u32>),
}

struct Worker {
    seq: u32,
    hits: Hits,
}

impl UserActor for Worker {
    type Config = WorkerCfg;
    type Message = WorkerMsg;
    type Error = WorkerError;

    fn start(config: Self::Config) -> Result<Self, Self::Error> {
        if config.fail_start {
            return Err(WorkerError("refused to start".into()));
        }
        let seq = config.next_seq.fetch_add(1, Ordering::SeqCst);
        Ok(Worker {
            seq,
            hits: config.hits,
        })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            WorkerMsg::Inc => {
                *self.hits.lock().unwrap().entry(self.seq).or_insert(0) += 1;
            }
            WorkerMsg::WhoAmI(port) => {
                let _ = port.reply(self.seq);
            }
        }
        Ok(())
    }
}

/// A second actor type, to exercise type-mismatch lookups.
#[derive(Debug, thiserror::Error)]
#[error("ping error")]
struct PingError;

struct Ping;

impl UserActor for Ping {
    type Config = ();
    type Message = RpcReplyPort<&'static str>;
    type Error = PingError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Ping)
    }

    async fn handle(&mut self, port: Self::Message) -> Result<(), Self::Error> {
        let _ = port.reply("pong");
        Ok(())
    }
}

/// An actor that never answers an `ask`: it parks the reply port in its own
/// state (so the port is *not* dropped, which would surface as `NoReply`),
/// leaving the caller to hit the ask deadline.
struct Mute {
    held: Vec<RpcReplyPort<u32>>,
}

impl UserActor for Mute {
    type Config = ();
    type Message = RpcReplyPort<u32>;
    type Error = PingError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Mute { held: Vec::new() })
    }

    async fn handle(&mut self, port: Self::Message) -> Result<(), Self::Error> {
        self.held.push(port);
        Ok(())
    }
}

/// Poll until the recorded hits sum to `expected` (or panic after a timeout).
async fn wait_total(hits: &Hits, expected: u32) {
    for _ in 0..200 {
        if hits.lock().unwrap().values().copied().sum::<u32>() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!(
        "timed out waiting for {expected} messages; saw {:?}",
        hits.lock().unwrap()
    );
}

// ---------------------------------------------------------------------------
// Singleton
// ---------------------------------------------------------------------------

#[tokio::test]
async fn singleton_handles_casts_and_asks() {
    let registry = ActorRegistry::new();
    let cfg = WorkerCfg::new();
    let actor = registry.spawn::<Worker>("w", cfg.clone()).unwrap();

    actor.send(WorkerMsg::Inc).unwrap();
    actor.send(WorkerMsg::Inc).unwrap();
    actor.send(WorkerMsg::Inc).unwrap();

    // An ask flushes the serial mailbox: all three casts run before this reply.
    let who = actor.ask(WorkerMsg::WhoAmI).await.unwrap();
    assert_eq!(who, 0, "the single instance grabbed sequence 0");
    assert_eq!(cfg.hits.lock().unwrap().get(&0).copied(), Some(3));
    assert!(actor.is_alive());
    assert_eq!(actor.name(), "w");
}

#[tokio::test]
async fn duplicate_name_is_rejected() {
    let registry = ActorRegistry::new();
    registry.spawn::<Worker>("w", WorkerCfg::new()).unwrap();
    let err = registry.spawn::<Worker>("w", WorkerCfg::new()).unwrap_err();
    assert!(matches!(err, SpawnError::NameExists(n) if n == "w"));
}

#[tokio::test]
async fn start_failure_surfaces_as_spawn_error() {
    let registry = ActorRegistry::new();
    let mut cfg = WorkerCfg::new();
    cfg.fail_start = true;
    let err = registry.spawn::<Worker>("w", cfg).unwrap_err();
    assert!(matches!(err, SpawnError::Start(_)));
    assert!(!registry.contains("w"), "a failed spawn registers nothing");
}

#[tokio::test]
async fn lookup_with_wrong_type_returns_none() {
    let registry = ActorRegistry::new();
    registry.spawn::<Worker>("w", WorkerCfg::new()).unwrap();
    assert!(registry.get::<Worker>("w").is_some());
    assert!(
        registry.get::<Ping>("w").is_none(),
        "same name, different type must not resolve"
    );
    assert!(registry.get::<Worker>("missing").is_none());
}

// ---------------------------------------------------------------------------
// Pools (development mode)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_round_robins_across_instances() {
    let registry = ActorRegistry::new_dev();
    let cfg = WorkerCfg::new();
    let pool = registry.spawn_pool::<Worker>("p", 3, cfg.clone()).unwrap();
    assert_eq!(pool.len(), 3);
    assert_eq!(pool.instance_ids(), vec![0, 1, 2]);

    for _ in 0..30 {
        pool.send(WorkerMsg::Inc).unwrap();
    }
    wait_total(&cfg.hits, 30).await;

    let hits = cfg.hits.lock().unwrap();
    assert_eq!(hits.get(&0), Some(&10));
    assert_eq!(hits.get(&1), Some(&10));
    assert_eq!(hits.get(&2), Some(&10));
}

#[tokio::test]
async fn keyed_routing_pins_a_key_to_one_instance() {
    let registry = ActorRegistry::new_dev();
    let cfg = WorkerCfg::new();
    let pool = registry.spawn_pool::<Worker>("p", 4, cfg.clone()).unwrap();

    for _ in 0..20 {
        pool.send_keyed(&"tenant-42", WorkerMsg::Inc).unwrap();
    }
    wait_total(&cfg.hits, 20).await;

    let hits = cfg.hits.lock().unwrap();
    let nonzero: Vec<u32> = hits.values().copied().filter(|&c| c > 0).collect();
    assert_eq!(
        nonzero.len(),
        1,
        "all messages for one key hit exactly one instance: {hits:?}"
    );
    assert_eq!(nonzero[0], 20);
}

#[tokio::test]
async fn scale_local_grows_and_shrinks() {
    let registry = ActorRegistry::new_dev();
    let cfg = WorkerCfg::new();
    registry.spawn_pool::<Worker>("p", 2, cfg.clone()).unwrap();

    let grown = registry
        .scale_local::<Worker>("p", 4, cfg.clone())
        .await
        .unwrap();
    assert_eq!(grown.len(), 4);

    let shrunk = registry
        .scale_local::<Worker>("p", 1, cfg.clone())
        .await
        .unwrap();
    assert_eq!(shrunk.len(), 1);
    assert_eq!(registry.instance_count("p"), 1);
}

#[tokio::test]
async fn scale_local_type_mismatch_is_reported() {
    let registry = ActorRegistry::new_dev();
    registry.spawn::<Worker>("w", WorkerCfg::new()).unwrap();
    let err = registry.scale_local::<Ping>("w", 1, ()).await.unwrap_err();
    assert!(matches!(err, ScaleError::TypeMismatch { .. }));

    let missing = registry
        .scale_local::<Worker>("nope", 1, WorkerCfg::new())
        .await
        .unwrap_err();
    assert!(matches!(missing, ScaleError::NotFound(n) if n == "nope"));
}

// ---------------------------------------------------------------------------
// Production one-worker-per-name guard (one-worker-per-vps)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn production_rejects_local_pools_above_one() {
    let registry = ActorRegistry::new();
    assert!(!registry.dev_multi_workers());
    assert_eq!(registry.placement_mode(), PlacementMode::Production);

    // A pool of one is fine.
    registry
        .spawn_pool::<Worker>("ok", 1, WorkerCfg::new())
        .unwrap();

    let err = registry
        .spawn_pool::<Worker>("many", 3, WorkerCfg::new())
        .unwrap_err();
    assert!(matches!(err, SpawnError::MultiWorkerDisabled { count: 3 }));

    // scale_local above one is likewise rejected in production.
    let scale_err = registry
        .scale_local::<Worker>("ok", 2, WorkerCfg::new())
        .await
        .unwrap_err();
    assert!(matches!(
        scale_err,
        ScaleError::MultiWorkerDisabled { count: 2 }
    ));
}

#[test]
fn dev_registry_reports_development_placement_mode() {
    let registry = ActorRegistry::new_dev();
    assert!(registry.dev_multi_workers());
    assert_eq!(registry.placement_mode(), PlacementMode::DevelopmentMulti);
}

#[tokio::test]
async fn zero_count_is_rejected() {
    let registry = ActorRegistry::new_dev();
    let err = registry
        .spawn_pool::<Worker>("p", 0, WorkerCfg::new())
        .unwrap_err();
    assert!(matches!(err, SpawnError::ZeroCount));
}

// ---------------------------------------------------------------------------
// Stop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stop_removes_the_group_and_ends_instances() {
    let registry = ActorRegistry::new();
    let actor = registry.spawn::<Worker>("w", WorkerCfg::new()).unwrap();
    assert!(registry.contains("w"));

    registry.stop("w").unwrap();
    assert!(!registry.contains("w"));
    assert_eq!(registry.instance_count("w"), 0);

    // The outstanding ref's instance winds down; sends eventually fail.
    for _ in 0..200 {
        if actor.send(WorkerMsg::Inc).is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !actor.is_alive() || actor.send(WorkerMsg::Inc).is_err(),
        "a stopped actor stops accepting work"
    );

    let missing = registry.stop("w").unwrap_err();
    assert!(matches!(missing, crafty_actor::StopError::NotFound(_)));
}

#[tokio::test]
async fn ping_actor_ask_round_trips() {
    let registry = ActorRegistry::new();
    let ping = registry.spawn::<Ping>("ping", ()).unwrap();
    let reply = ping.ask(|port| port).await.unwrap();
    assert_eq!(reply, "pong");
}

#[tokio::test(start_paused = true)]
async fn ask_times_out_when_the_actor_never_replies() {
    let registry = ActorRegistry::new();
    let mute = registry.spawn::<Mute>("mute", ()).unwrap();

    // The handler parks the port and never answers; the caller must give up at
    // the ask deadline (virtual clock auto-advances under `start_paused`).
    let err = mute.ask(|port| port).await.unwrap_err();
    assert!(
        matches!(err, AskError::Timeout(_)),
        "expected a caller-side timeout, got {err:?}"
    );
}

#[derive(Debug, thiserror::Error)]
#[error("stall")]
struct StallError;

enum StallMsg {
    Stall,
    Queued,
}

struct StallWorker;

impl UserActor for StallWorker {
    type Config = ();
    type Message = StallMsg;
    type Error = StallError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self)
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            StallMsg::Stall => {
                std::future::pending::<()>().await;
                Ok(())
            }
            StallMsg::Queued => Ok(()),
        }
    }
}

#[tokio::test]
async fn local_actor_introspection_reports_mailbox_depth_and_uptime() {
    let registry = ActorRegistry::new_dev();
    let actor = registry.spawn::<StallWorker>("stall", ()).unwrap();
    actor.send(StallMsg::Stall).unwrap();
    actor.send(StallMsg::Queued).unwrap();
    actor.send(StallMsg::Queued).unwrap();
    tokio::task::yield_now().await;

    let views = registry.local_actor_introspection();
    assert_eq!(views.len(), 1, "{views:?}");
    assert_eq!(views[0].name, "stall");
    assert_eq!(views[0].instance, 0);
    assert_eq!(views[0].mailbox_depth, 2, "two messages still queued");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let views = registry.local_actor_introspection();
    assert!(views[0].uptime_secs >= 1, "uptime: {}", views[0].uptime_secs);
}

#[tokio::test]
async fn local_actor_introspection_tracks_per_instance_mailbox_depth() {
    let registry = ActorRegistry::new_dev();
    let pool = registry.spawn_pool::<StallWorker>("stall", 2, ()).unwrap();
    let ids = pool.instance_ids();
    assert_eq!(ids.len(), 2);

    let salt = crafty_actor::group_salt(pool.name());
    let key_for = |target: u32| -> String {
        for n in 0..1000 {
            let key = format!("tenant-{n}");
            let idx = crafty_actor::pick_index(crafty_actor::ring_hash_key(&key), 2, salt);
            if ids[idx] == target {
                return key;
            }
        }
        panic!("no routing key for instance {target}");
    };
    let busy = key_for(ids[0]);
    let idle = key_for(ids[1]);

    pool.send_keyed(&busy, StallMsg::Stall).unwrap();
    pool.send_keyed(&busy, StallMsg::Queued).unwrap();
    pool.send_keyed(&busy, StallMsg::Queued).unwrap();
    pool.send_keyed(&idle, StallMsg::Stall).unwrap();
    pool.send_keyed(&idle, StallMsg::Queued).unwrap();
    tokio::task::yield_now().await;

    let views = registry.local_actor_introspection();
    assert_eq!(views.len(), 2, "{views:?}");
    let mut depths: Vec<i64> = views.iter().map(|v| v.mailbox_depth).collect();
    depths.sort_unstable();
    assert_eq!(depths, vec![1, 2], "per-instance depths: {views:?}");
}
