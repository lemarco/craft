//! Tests for OTP-style actor restart/supervision policies (backlog E14,
//! ADR 026 §5).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use craft_actor::{ActorObserver, ActorRegistry, RestartPolicy, RpcReplyPort, UserActor};

#[derive(Debug, thiserror::Error)]
#[error("boom")]
struct Boom;

/// Config carries a shared "starts" counter so tests can observe how often the
/// actor was (re)constructed via `start`.
#[derive(Clone)]
struct Cfg {
    starts: Arc<AtomicUsize>,
}

/// A counter that fails (returns `Err`) on the `Fail` message and reports its
/// live in-memory count on `Get`. A restart rebuilds fresh state → count 0.
enum Msg {
    Inc,
    Fail,
    Get(RpcReplyPort<u64>),
}

struct Flaky {
    count: u64,
}

impl UserActor for Flaky {
    type Config = Cfg;
    type Message = Msg;
    type Error = Boom;

    fn start(cfg: Self::Config) -> Result<Self, Self::Error> {
        cfg.starts.fetch_add(1, Ordering::SeqCst);
        Ok(Self { count: 0 })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            Msg::Inc => {
                self.count += 1;
                Ok(())
            }
            Msg::Fail => Err(Boom),
            Msg::Get(port) => {
                let _ = port.reply(self.count);
                Ok(())
            }
        }
    }
}

/// Records the supervision hooks the registry fires (Track H telemetry).
#[derive(Default)]
struct RecordingObserver {
    restarts: Mutex<Vec<(String, u32, u32)>>,
    escalations: Mutex<Vec<(String, u32)>>,
}

impl ActorObserver for RecordingObserver {
    fn on_restart(&self, name: &str, instance: u32, count: u32) {
        self.restarts
            .lock()
            .unwrap()
            .push((name.to_string(), instance, count));
    }

    fn on_escalated(&self, name: &str, instance: u32) {
        self.escalations
            .lock()
            .unwrap()
            .push((name.to_string(), instance));
    }
}

fn cfg() -> (Cfg, Arc<AtomicUsize>) {
    let starts = Arc::new(AtomicUsize::new(0));
    (
        Cfg {
            starts: starts.clone(),
        },
        starts,
    )
}

#[tokio::test]
async fn never_policy_keeps_state_and_does_not_restart() {
    let registry = ActorRegistry::new();
    let (cfg, starts) = cfg();
    let actor = registry.spawn::<Flaky>("f", cfg).unwrap();

    actor.send(Msg::Inc).unwrap();
    actor.send(Msg::Inc).unwrap();
    actor.send(Msg::Fail).unwrap(); // non-fatal under `Never`
    actor.send(Msg::Inc).unwrap();

    let count = actor.ask(Msg::Get).await.unwrap();
    assert_eq!(count, 3, "state survives the error; no restart");
    assert_eq!(actor.restart_count(), 0);
    assert_eq!(starts.load(Ordering::SeqCst), 1, "constructed exactly once");
    assert!(actor.is_alive());
}

#[tokio::test]
async fn always_policy_restarts_with_fresh_state() {
    let registry = ActorRegistry::new();
    let (cfg, starts) = cfg();
    let actor = registry
        .spawn_supervised::<Flaky>("f", cfg, RestartPolicy::Always)
        .unwrap();

    actor.send(Msg::Inc).unwrap();
    actor.send(Msg::Inc).unwrap();
    actor.send(Msg::Fail).unwrap(); // triggers a restart → fresh state
    let count = actor.ask(Msg::Get).await.unwrap();

    assert_eq!(count, 0, "restart reset the in-memory count");
    assert_eq!(actor.restart_count(), 1);
    assert_eq!(starts.load(Ordering::SeqCst), 2, "started, then restarted");
    assert!(actor.is_alive(), "still serving after restart");
}

#[tokio::test]
async fn on_failure_restarts_within_budget_then_escalates() {
    let registry = ActorRegistry::new();
    let (cfg, starts) = cfg();
    let actor = registry
        .spawn_supervised::<Flaky>(
            "f",
            cfg,
            RestartPolicy::OnFailure {
                max_restarts: 2,
                window: Duration::from_secs(60),
            },
        )
        .unwrap();

    // Two failures are within budget (2 restarts), so the actor recovers.
    actor.send(Msg::Fail).unwrap();
    actor.send(Msg::Fail).unwrap();
    let count = actor.ask(Msg::Get).await.unwrap();
    assert_eq!(count, 0);
    assert_eq!(actor.restart_count(), 2);
    assert!(actor.is_alive(), "recovered within the restart budget");

    // The third failure exhausts the budget → escalate (stop the instance).
    actor.send(Msg::Fail).unwrap();
    // Give the task a moment to wind down.
    for _ in 0..50 {
        if !actor.is_alive() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(!actor.is_alive(), "budget exhausted → instance stopped");
    assert_eq!(
        actor.restart_count(),
        2,
        "the escalating failure is not counted as a restart"
    );
    assert_eq!(starts.load(Ordering::SeqCst), 3, "start + 2 restarts");
}

#[tokio::test]
async fn observer_sees_restarts_and_escalation() {
    let registry = ActorRegistry::new();
    let observer = Arc::new(RecordingObserver::default());
    registry.set_observer(observer.clone());

    let (cfg, _starts) = cfg();
    let actor = registry
        .spawn_supervised::<Flaky>(
            "f",
            cfg,
            RestartPolicy::OnFailure {
                max_restarts: 1,
                window: Duration::from_secs(60),
            },
        )
        .unwrap();

    // First failure restarts within budget → one restart notification.
    actor.send(Msg::Fail).unwrap();
    let _ = actor.ask(Msg::Get).await.unwrap(); // barrier: restart applied
    {
        let restarts = observer.restarts.lock().unwrap();
        assert_eq!(restarts.as_slice(), &[("f".to_string(), 0, 1)]);
    }

    // Second failure exhausts the budget → escalation notification.
    actor.send(Msg::Fail).unwrap();
    for _ in 0..50 {
        if !observer.escalations.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let escalations = observer.escalations.lock().unwrap();
    assert_eq!(escalations.as_slice(), &[("f".to_string(), 0)]);
}
