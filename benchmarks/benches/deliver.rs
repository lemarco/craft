//! `deliver` — local actor mailbox delivery throughput (backlog T10).
//!
//! Measures [`ActorRegistry::deliver_local`]: resolve the group, decode the
//! wire payload into the actor's `Message`, and enqueue it on the instance's
//! serial mailbox (an unbounded channel, so the send never blocks). This is the
//! hot path the `/actor/deliver` handler runs for every inbound cast.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use trembita_actor::trembita_proto;
use trembita_actor::{ActorRegistry, MessageDecodeError, UserActor};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use std::hint::black_box;

#[derive(Debug, Serialize, Deserialize)]
enum Work {
    Add(u64),
}

#[derive(Debug)]
struct WorkerError;
impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerError {}

struct Worker {
    counter: Arc<AtomicU64>,
}

impl UserActor for Worker {
    type Config = Arc<AtomicU64>;
    type Message = Work;
    type Error = WorkerError;

    fn start(counter: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker { counter })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            Work::Add(n) => {
                self.counter.fetch_add(n, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        trembita_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))
    }
}

fn bench_deliver(c: &mut Criterion) {
    // A multi-thread runtime so the actor task actually drains the mailbox
    // concurrently with the benched producer.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let counter = Arc::new(AtomicU64::new(0));
    let registry = {
        let _guard = rt.enter();
        let registry = ActorRegistry::new();
        registry
            .spawn::<Worker>("worker", counter.clone())
            .expect("spawn worker");
        registry
    };
    let payload = trembita_proto::encode(&Work::Add(1)).unwrap();

    let mut group = c.benchmark_group("deliver");
    group.throughput(Throughput::Elements(1));
    group.bench_function("local_mailbox", |b| {
        b.iter(|| {
            registry
                .deliver_local("worker", 0, black_box(&payload))
                .unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, bench_deliver);
criterion_main!(benches);
